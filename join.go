package leasering

import (
	"context"
	"fmt"
	"time"

	"go-leasering/database"
)

// proposeJoin creates join proposals for all vnodes this node wants to claim.
func (r *Ring) proposeJoin(ctx context.Context, queries *database.Queries) error {
	var (
		positions = r.getMyVNodePositions()
		now       = time.Now()
		expiresAt = now.Add(r.options.proposalTTL)
	)

	// First, refresh ring state to find predecessors
	if err := r.refreshRingState(ctx, queries); err != nil {
		return fmt.Errorf("failed to refresh ring state: %w", err)
	}

	// Create proposals for each vnode position
	for i, position := range positions {
		var predecessorPos = r.findPredecessor(position)

		// Bootstrap case: empty ring, no predecessor
		if predecessorPos == -1 {
			// Directly write lease for bootstrap node
			var lease = &database.LeaseRecord{
				RingID:    r.ringID,
				Position:  position,
				NodeID:    r.nodeID,
				VNodeIdx:  i,
				ExpiresAt: now.Add(r.options.leaseTTL),
			}
			if err := queries.SetLease(ctx, lease); err != nil {
				return fmt.Errorf("failed to set bootstrap lease: %w", err)
			}
			continue
		}

		// Normal case: create proposal
		var proposal = &database.ProposalRecord{
			RingID:         r.ringID,
			PredecessorPos: predecessorPos,
			NewNodeID:      r.nodeID,
			NewVNodeIdx:    i,
			ProposedPos:    position,
			ExpiresAt:      expiresAt,
		}

		if err := queries.SetProposal(ctx, proposal); err != nil {
			return fmt.Errorf("failed to set proposal: %w", err)
		}
	}

	return nil
}

// acceptProposals scans for proposals targeting this node's vnodes and accepts them.
// Optimized to fetch all proposals once and filter in-memory.
func (r *Ring) acceptProposals(ctx context.Context, queries *database.Queries) error {
	// Get all our vnode positions
	r.mu.RLock()
	var myPositions = make(map[int]bool)
	for _, vnode := range r.vnodes {
		if vnode.NodeID == r.nodeID {
			myPositions[vnode.Position] = true
		}
	}
	r.mu.RUnlock()

	// Early exit if we have no vnodes
	if len(myPositions) == 0 {
		return nil
	}

	// Fetch all proposals for the ring in one query
	var allProposals, err = queries.ListAllProposals(ctx, r.ringID)
	if err != nil {
		return fmt.Errorf("failed to list all proposals: %w", err)
	}

	// Build map: predecessorPos -> []*ProposalRecord
	var proposalsByPred = make(map[int][]*database.ProposalRecord)
	for _, proposal := range allProposals {
		proposalsByPred[proposal.PredecessorPos] = append(proposalsByPred[proposal.PredecessorPos], proposal)
	}

	var now = time.Now()

	// Process proposals for each of our vnodes
	for position := range myPositions {
		var proposals = proposalsByPred[position]
		if len(proposals) == 0 {
			continue
		}

		// Process each proposal
		for _, proposal := range proposals {
			// Check if expired
			if now.After(proposal.ExpiresAt) {
				// Delete expired proposal
				if err := queries.DeleteProposal(ctx, proposal.RingID, proposal.PredecessorPos,
					proposal.NewNodeID, proposal.NewVNodeIdx); err != nil {
					return fmt.Errorf("failed to delete expired proposal: %w", err)
				}
				continue
			}

			// Check if proposed position is still available
			var existingLease, getErr = queries.GetLease(ctx, r.ringID, proposal.ProposedPos)
			if getErr != nil {
				return fmt.Errorf("failed to check existing lease: %w", getErr)
			}

			if existingLease != nil {
				// Position taken, reject proposal by deleting it
				if err := queries.DeleteProposal(ctx, proposal.RingID, proposal.PredecessorPos,
					proposal.NewNodeID, proposal.NewVNodeIdx); err != nil {
					return fmt.Errorf("failed to delete rejected proposal: %w", err)
				}
				continue
			}

			// Accept proposal by writing the lease
			var (
				leaseTTL = now.Add(r.options.leaseTTL)
				lease = &database.LeaseRecord{
					RingID:    proposal.RingID,
					Position:  proposal.ProposedPos,
					NodeID:    proposal.NewNodeID,
					VNodeIdx:  proposal.NewVNodeIdx,
					ExpiresAt: leaseTTL,
				}
			)

			if err := queries.SetLease(ctx, lease); err != nil {
				return fmt.Errorf("failed to accept proposal: %w", err)
			}

			// Delete the accepted proposal
			if err := queries.DeleteProposal(ctx, proposal.RingID, proposal.PredecessorPos,
				proposal.NewNodeID, proposal.NewVNodeIdx); err != nil {
				return fmt.Errorf("failed to delete accepted proposal: %w", err)
			}

			// Immediately update our local ring state to reflect the new vnode.
			// This ensures we stop processing work for keyspace we just gave away.
			var newVNode = VNode{
				NodeID:    proposal.NewNodeID,
				Index:     proposal.NewVNodeIdx,
				Position:  proposal.ProposedPos,
				ExpiresAt: leaseTTL,
			}
			r.addVNode(newVNode)
		}
	}

	return nil
}

// checkJoinConfirmation verifies if this node's join proposals have been accepted.
// Returns true if all vnodes have active leases.
func (r *Ring) checkJoinConfirmation(ctx context.Context, queries *database.Queries) (bool, error) {
	var positions = r.getMyVNodePositions()

	for _, position := range positions {
		var lease, err = queries.GetLease(ctx, r.ringID, position)
		if err != nil {
			return false, fmt.Errorf("failed to get lease: %w", err)
		}

		if lease == nil || lease.NodeID != r.nodeID {
			// Lease not yet created or assigned to someone else
			return false, nil
		}
	}

	// All vnodes have leases
	return true, nil
}

// refreshRingState reads all leases from the database and rebuilds the in-memory ring.
func (r *Ring) refreshRingState(ctx context.Context, queries *database.Queries) error {
	var leases, err = queries.ListLeases(ctx, r.ringID)
	if err != nil {
		return fmt.Errorf("failed to list leases: %w", err)
	}

	// Convert database records to domain types
	var domainLeases = make([]*Lease, len(leases))
	for i, record := range leases {
		domainLeases[i] = &Lease{
			Position:  record.Position,
			NodeID:    record.NodeID,
			VNodeIdx:  record.VNodeIdx,
			ExpiresAt: record.ExpiresAt,
		}
	}

	r.rebuildFromLeases(domainLeases)
	return nil
}
