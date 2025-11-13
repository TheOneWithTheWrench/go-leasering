package leasering

import (
	"context"
	"fmt"
	"time"
)

// Membership handles the join/leave protocol for ring membership.
type membership struct {
	ring       *Ring
	store      *leaseStore
	nodeID     string
	leaseTTL   time.Duration
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
	var positions = m.ring.getMyVNodePositions()

	var leases, err = m.store.ListLeases(ctx)
	if err != nil {
		return fmt.Errorf("failed to list leases: %w", err)
	}

	// Check if all existing leases are expired (dead ring scenario).
	// Use 5s grace period to avoid false positives during normal operation.
	var allExpired = true
	if len(leases) > 0 {
		var gracePeriod = time.Now().Add(-5 * time.Second)
		for _, lease := range leases {
			if lease.ExpiresAt.After(gracePeriod) {
				allExpired = false
				break
			}
		}
	}

	if err := m.RefreshRingState(ctx); err != nil {
		return fmt.Errorf("failed to refresh ring state: %w", err)
	}

	for i, position := range positions {
		var predecessorPos = m.ring.findPredecessor(position)

		if predecessorPos == -1 || allExpired {
			var lease = &lease{
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

		var proposal = &proposal{
			PredecessorPos: predecessorPos,
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
	var myPositions = m.ring.getMyPositions()
	if len(myPositions) == 0 {
		return nil
	}

	var allProposals, err = m.store.ListAllProposals(ctx)
	if err != nil {
		return fmt.Errorf("failed to list all proposals: %w", err)
	}

	var proposalsByPred = make(map[int][]*proposal)
	for _, proposal := range allProposals {
		proposalsByPred[proposal.PredecessorPos] = append(proposalsByPred[proposal.PredecessorPos], proposal)
	}

	var now = time.Now()
	for position := range myPositions {
		var proposals = proposalsByPred[position]
		if len(proposals) == 0 {
			continue
		}

		var proposalsByPos = make(map[int][]*proposal)
		for _, p := range proposals {
			proposalsByPos[p.ProposedPos] = append(proposalsByPos[p.ProposedPos], p)
		}

		for _, posProposals := range proposalsByPos {
			var validProposals []*proposal
			for _, p := range posProposals {
				if now.After(p.ExpiresAt) {
					if err := m.store.DeleteProposal(ctx, p.PredecessorPos, p.NewNodeID, p.NewVNodeIdx); err != nil {
						return fmt.Errorf("failed to delete expired proposal: %w", err)
					}
					continue
				}
				validProposals = append(validProposals, p)
			}

			if len(validProposals) == 0 {
				continue
			}

			// Pick winner lexicographically by node_id for deterministic collision resolution
			var winner *proposal
			for _, p := range validProposals {
				if winner == nil || p.NewNodeID < winner.NewNodeID {
					winner = p
				}
			}

			var (
				leaseTTL = now.Add(m.leaseTTL)
				lease    = &lease{
					Position:  winner.ProposedPos,
					NodeID:    winner.NewNodeID,
					VNodeIdx:  winner.NewVNodeIdx,
					ExpiresAt: leaseTTL,
				}
			)

			// InsertLease will fail if position already has a lease (atomic operation)
			insertErr := m.store.InsertLease(ctx, lease)
			if insertErr != nil {
				for _, p := range validProposals {
					if err := m.store.DeleteProposal(ctx, p.PredecessorPos, p.NewNodeID, p.NewVNodeIdx); err != nil {
						return fmt.Errorf("failed to delete rejected proposal: %w", err)
					}
				}
				continue
			}

			m.ring.addVNode(vnode{
				NodeID:    winner.NewNodeID,
				Index:     winner.NewVNodeIdx,
				Position:  winner.ProposedPos,
				ExpiresAt: leaseTTL,
			})

			for _, p := range validProposals {
				if err := m.store.DeleteProposal(ctx, p.PredecessorPos, p.NewNodeID, p.NewVNodeIdx); err != nil {
					return fmt.Errorf("failed to delete processed proposal: %w", err)
				}
			}
		}
	}

	return nil
}

// CheckJoinConfirmation verifies if this node's join proposals have been accepted.
func (m *membership) CheckJoinConfirmation(ctx context.Context) (bool, error) {
	var positions = m.ring.getMyVNodePositions()

	for _, position := range positions {
		var lease, err = m.store.GetLease(ctx, position)
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
	var leases, err = m.store.ListLeases(ctx)
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
	var positions = m.ring.getMyVNodePositions()

	// Check the local ring state (already refreshed by RefreshRingState)
	for _, position := range positions {
		var vnode, found = m.ring.getVNodeAtPosition(position)
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
	var positions = m.ring.getMyVNodePositions()

	if err := m.store.RenewLeases(ctx, m.nodeID, positions, m.leaseTTL); err != nil {
		return err
	}

	// Update local vnodes with new expiration time
	var expiresAt = time.Now().Add(m.leaseTTL)
	m.ring.updateMyVNodeExpirations(expiresAt)

	return nil
}

// CleanupExpiredLeases removes expired leases of immediate successors.
func (m *membership) CleanupExpiredLeases(ctx context.Context) error {
	var successorPositions = m.ring.getMySuccessorPositions()

	var now = time.Now()
	for _, pos := range successorPositions {
		v, found := m.ring.getVNodeAtPosition(pos)
		if found && isExpired(v, now) {
			if err := m.store.DeleteLease(ctx, pos); err != nil {
				return fmt.Errorf("failed to delete expired lease at position %d: %w", pos, err)
			}
		}
	}

	return nil
}

// Leave removes all of this node's leases from the ring.
func (m *membership) Leave(ctx context.Context) error {
	var positions = m.ring.getMyVNodePositions()

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
	var positions = m.ring.getMyVNodePositions()

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

		predecessorPos := m.ring.findPredecessor(position)
		if predecessorPos != -1 {
			_ = m.store.DeleteProposal(ctx, predecessorPos, m.nodeID, i)
		}
	}

	return nil
}
