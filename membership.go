package leasering

import (
	"context"
	"fmt"
	"time"
)

// Membership handles the join/leave protocol for ring membership.
type membership struct {
	ring        *Ring
	store       *leaseStore
	nodeID      string
	leaseTTL    time.Duration
	proposalTTL time.Duration
}

// NewMembership creates a new Membership coordinator.
func newMembership(ring *Ring, store *leaseStore, nodeID string, leaseTTL, proposalTTL time.Duration) *membership {
	return &membership{
		ring:        ring,
		store:       store,
		nodeID:      nodeID,
		leaseTTL:    leaseTTL,
		proposalTTL: proposalTTL,
	}
}

// ProposeJoin creates join proposals for all vnodes this node wants to claim.
func (m *membership) ProposeJoin(ctx context.Context) error {
	positions := m.ring.getMyVNodePositions()

	if err := m.RefreshRingState(ctx); err != nil {
		return fmt.Errorf("failed to refresh ring state: %w", err)
	}

	// For all the positions, we have to find who's currently
	// owning them and propose a take-over
	for i, position := range positions {
		_, ownerPos := m.ring.findHandoffBounds(position)

		// If noone owns it, we "force" ourselves onto the position
		if ownerPos == -1 {
			lease := &lease{
				Position:  position,
				NodeID:    m.nodeID,
				VNodeIdx:  i,
				ExpiresAt: time.Now().Add(m.leaseTTL),
			}
			if err := m.store.SetLease(ctx, lease); err != nil {
				return fmt.Errorf("failed to set bootstrap lease: %w", err)
			}
			continue
		}

		proposal := &proposal{
			PredecessorPos: ownerPos,
			NewNodeID:      m.nodeID,
			NewVNodeIdx:    i,
			ProposedPos:    position,
			ExpiresAt:      time.Now().Add(m.proposalTTL),
		}

		if err := m.store.SetProposal(ctx, proposal); err != nil {
			return fmt.Errorf("failed to set proposal: %w", err)
		}
	}

	return nil
}

// AcceptProposals scans for proposals targeting this node's vnodes and accepts them.
func (m *membership) AcceptProposals(ctx context.Context) error {
	myPositions := m.ring.getMyPositions()

	// If we own nothing, we can accept nothing.
	if len(myPositions) == 0 {
		return nil
	}

	allProposals, err := m.store.ListProposalsForPredecessors(ctx, myPositions)
	if err != nil {
		return fmt.Errorf("failed to list proposals for predecessors: %w", err)
	}

	proposalsByPred := make(map[int][]*proposal)
	for _, proposal := range allProposals {
		proposalsByPred[proposal.PredecessorPos] = append(proposalsByPred[proposal.PredecessorPos], proposal)
	}

	acceptedAny := false
	for _, position := range myPositions {
		proposals := proposalsByPred[position]
		if len(proposals) == 0 {
			continue
		}

		proposalsByPos := make(map[int][]*proposal)
		for _, p := range proposals {
			proposalsByPos[p.ProposedPos] = append(proposalsByPos[p.ProposedPos], p)
		}

		for _, posProposals := range proposalsByPos {
			if len(posProposals) == 0 {
				continue
			}

			var (
				winner                       = winningProposal(posProposals)
				targetOwnerPos               = winner.PredecessorPos
				previousOwnerStart, ownerPos = m.ring.findHandoffBounds(winner.ProposedPos)
			)
			if ownerPos != targetOwnerPos || ownerPos == winner.ProposedPos {
				if err := m.deleteProposals(ctx, posProposals); err != nil {
					return fmt.Errorf("failed to delete rejected proposal: %w", err)
				}
				continue
			}

			var (
				leaseTTL = time.Now().Add(m.leaseTTL)
				lease    = &lease{
					Position:  winner.ProposedPos,
					NodeID:    winner.NewNodeID,
					VNodeIdx:  winner.NewVNodeIdx,
					ExpiresAt: leaseTTL,
				}
			)

			m.ring.removeOwnedPartitionsInRange(previousOwnerStart, winner.ProposedPos)

			insertErr := m.store.InsertLeaseIfTargetOwned(ctx, lease, targetOwnerPos, m.nodeID)
			if insertErr != nil {
				if refreshErr := m.RefreshRingState(ctx); refreshErr != nil {
					return fmt.Errorf("failed to refresh ring state after rejected proposal: %w", refreshErr)
				}
				if err := m.deleteProposals(ctx, posProposals); err != nil {
					return fmt.Errorf("failed to delete rejected proposal: %w", err)
				}
				continue
			}
			acceptedAny = true

			if err := m.deleteProposals(ctx, posProposals); err != nil {
				return fmt.Errorf("failed to delete processed proposal: %w", err)
			}
		}
	}

	if acceptedAny {
		if err := m.RefreshRingState(ctx); err != nil {
			return fmt.Errorf("failed to refresh ring state after accepting proposals: %w", err)
		}
	}

	return nil
}

func (m *membership) deleteProposals(ctx context.Context, proposals []*proposal) error {
	for _, p := range proposals {
		if err := m.store.DeleteProposal(ctx, p.PredecessorPos, p.NewNodeID, p.NewVNodeIdx); err != nil {
			return err
		}
	}
	return nil
}

// CheckJoinConfirmation verifies if this node's join proposals have been accepted.
func (m *membership) CheckJoinConfirmation(ctx context.Context) (bool, error) {
	positions := m.ring.getMyVNodePositions()

	for _, position := range positions {
		lease, err := m.store.GetLease(ctx, position)
		if err != nil {
			return false, fmt.Errorf("failed to get lease: %w", err)
		}

		if lease == nil || lease.NodeID != m.nodeID {
			return false, nil
		}
	}

	return true, nil
}

// RefreshRingState reads all leases from the database and rebuilds the in-memory ring.
func (m *membership) RefreshRingState(ctx context.Context) error {
	leases, err := m.store.ListLeases(ctx)
	if err != nil {
		return fmt.Errorf("failed to list leases: %w", err)
	}

	m.ring.rebuildFromLeases(leases)
	return nil
}

// CheckIfEvicted checks if this node has been evicted from the ring.
// A node is considered evicted if none of its expected vnode positions have active leases.
// This uses the already-refreshed local ring state, so it doesn't make any database calls.
func (m *membership) CheckIfEvicted(ctx context.Context) (bool, error) {
	positions := m.ring.getMyVNodePositions()

	// Check the local ring state (already refreshed by RefreshRingState)
	for _, position := range positions {
		vnode, found := m.ring.getVNodeAtPosition(position)
		if found && vnode.NodeID == m.nodeID {
			// Found at least one of our leases - we're not evicted
			return false, nil
		}
	}

	// None of our expected positions have our leases - we've been evicted
	return true, nil
}

// RenewLeases renews all of this node's leases.
func (m *membership) RenewLeases(ctx context.Context) error {
	positions := m.ring.getMyVNodePositions()

	expiresAt, err := m.store.RenewLeases(ctx, m.nodeID, positions, m.leaseTTL)
	if err != nil {
		m.ring.clearOwnedPartitions()
		return fmt.Errorf("failed to renew leases: %w", err)
	}

	// Update local vnodes with new expiration time
	m.ring.updateMyVNodeExpirations(expiresAt)

	return nil
}

// CleanupExpiredRecords removes expired leases and proposals from the database.
func (m *membership) CleanupExpiredRecords(ctx context.Context) error {
	if err := m.store.DeleteExpiredLeases(ctx); err != nil {
		return fmt.Errorf("failed to delete expired leases: %w", err)
	}

	if err := m.store.DeleteExpiredProposals(ctx); err != nil {
		return fmt.Errorf("failed to delete expired proposals: %w", err)
	}

	return nil
}

// Leave removes all of this node's leases from the ring.
func (m *membership) Leave(ctx context.Context) error {
	positions := m.ring.getMyVNodePositions()

	for _, position := range positions {
		if err := m.store.DeleteLease(ctx, position); err != nil {
			return fmt.Errorf("failed to delete lease at position %d: %w", position, err)
		}
	}

	return nil
}

// CleanupNodeData removes all leases and proposals for the current node-id.
// This is used when retrying join with a new node-id after a partial join failure.
func (m *membership) CleanupNodeData(ctx context.Context) error {
	positions := m.ring.getMyVNodePositions()

	if err := m.RefreshRingState(ctx); err != nil {
		return fmt.Errorf("failed to refresh ring state: %w", err)
	}

	for i, position := range positions {
		lease, err := m.store.GetLease(ctx, position)
		if err != nil {
			return fmt.Errorf("failed to get lease at position %d: %w", position, err)
		}
		if lease != nil && lease.NodeID == m.nodeID {
			if err := m.store.DeleteLease(ctx, position); err != nil {
				return fmt.Errorf("failed to delete lease at position %d: %w", position, err)
			}
		}

		_, ownerPos := m.ring.findHandoffBounds(position)
		if ownerPos != -1 {
			_ = m.store.DeleteProposal(ctx, ownerPos, m.nodeID, i)
		}
	}

	return nil
}

func winningProposal(proposals []*proposal) *proposal {
	var winner *proposal
	for _, p := range proposals {
		if winner == nil || p.NewNodeID < winner.NewNodeID {
			winner = p
		}
	}
	return winner
}
