package leasering

import (
	"context"
	"fmt"
	"time"

	"go-leasering/database"
)

// LeaseStore handles all database operations for leases and proposals.
type leaseStore struct {
	ringID  string
	queries *database.Queries
}

// NewLeaseStore creates a new LeaseStore for the given ring.
func newLeaseStore(ringID string, queries *database.Queries) *leaseStore {
	return &leaseStore{
		ringID:  ringID,
		queries: queries,
	}
}

// ListLeases returns all active leases in the ring.
func (ls *leaseStore) ListLeases(ctx context.Context) ([]*lease, error) {
	var records, err = ls.queries.ListLeases(ctx, ls.ringID)
	if err != nil {
		return nil, fmt.Errorf("failed to list leases: %w", err)
	}

	var leases = make([]*lease, len(records))
	for i, record := range records {
		leases[i] = &lease{
			Position:  record.Position,
			NodeID:    record.NodeID,
			VNodeIdx:  record.VNodeIdx,
			ExpiresAt: record.ExpiresAt,
		}
	}

	return leases, nil
}

// GetLease returns the lease at the given position, or nil if not found.
func (ls *leaseStore) GetLease(ctx context.Context, position int) (*lease, error) {
	var record, err = ls.queries.GetLease(ctx, ls.ringID, position)
	if err != nil {
		return nil, fmt.Errorf("failed to get lease at position %d: %w", position, err)
	}

	if record == nil {
		return nil, nil
	}

	return &lease{
		Position:  record.Position,
		NodeID:    record.NodeID,
		VNodeIdx:  record.VNodeIdx,
		ExpiresAt: record.ExpiresAt,
	}, nil
}

// SetLease writes a lease to the database.
func (ls *leaseStore) SetLease(ctx context.Context, lease *lease) error {
	var record = &database.LeaseRecord{
		RingID:    ls.ringID,
		Position:  lease.Position,
		NodeID:    lease.NodeID,
		VNodeIdx:  lease.VNodeIdx,
		ExpiresAt: lease.ExpiresAt,
	}

	if err := ls.queries.SetLease(ctx, record); err != nil {
		return fmt.Errorf("failed to set lease at position %d: %w", lease.Position, err)
	}

	return nil
}

// DeleteLease removes a lease from the database.
func (ls *leaseStore) DeleteLease(ctx context.Context, position int) error {
	if err := ls.queries.DeleteLease(ctx, ls.ringID, position); err != nil {
		return fmt.Errorf("failed to delete lease at position %d: %w", position, err)
	}
	return nil
}

// ListAllProposals returns all active proposals in the ring.
func (ls *leaseStore) ListAllProposals(ctx context.Context) ([]*proposal, error) {
	var records, err = ls.queries.ListAllProposals(ctx, ls.ringID)
	if err != nil {
		return nil, fmt.Errorf("failed to list proposals: %w", err)
	}

	var proposals = make([]*proposal, len(records))
	for i, record := range records {
		proposals[i] = &proposal{
			PredecessorPos: record.PredecessorPos,
			NewNodeID:      record.NewNodeID,
			NewVNodeIdx:    record.NewVNodeIdx,
			ProposedPos:    record.ProposedPos,
			ExpiresAt:      record.ExpiresAt,
		}
	}

	return proposals, nil
}

// SetProposal writes a proposal to the database.
func (ls *leaseStore) SetProposal(ctx context.Context, proposal *proposal) error {
	var record = &database.ProposalRecord{
		RingID:         ls.ringID,
		PredecessorPos: proposal.PredecessorPos,
		NewNodeID:      proposal.NewNodeID,
		NewVNodeIdx:    proposal.NewVNodeIdx,
		ProposedPos:    proposal.ProposedPos,
		ExpiresAt:      proposal.ExpiresAt,
	}

	if err := ls.queries.SetProposal(ctx, record); err != nil {
		return fmt.Errorf("failed to set proposal: %w", err)
	}

	return nil
}

// DeleteProposal removes a proposal from the database.
func (ls *leaseStore) DeleteProposal(ctx context.Context, predecessorPos int, newNodeID string, newVNodeIdx int) error {
	if err := ls.queries.DeleteProposal(ctx, ls.ringID, predecessorPos, newNodeID, newVNodeIdx); err != nil {
		return fmt.Errorf("failed to delete proposal: %w", err)
	}
	return nil
}

// RenewLeases renews all leases for the given positions.
func (ls *leaseStore) RenewLeases(ctx context.Context, nodeID string, positions []int, ttl time.Duration) error {
	var expiresAt = time.Now().Add(ttl)

	for i, position := range positions {
		var l = &lease{
			Position:  position,
			NodeID:    nodeID,
			VNodeIdx:  i,
			ExpiresAt: expiresAt,
		}

		if err := ls.SetLease(ctx, l); err != nil {
			return err
		}
	}

	return nil
}
