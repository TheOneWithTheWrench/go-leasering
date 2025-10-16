package leasering

import (
	"context"
	"fmt"
	"time"

	"go-leasering/database"
)

// LeaseStore handles all database operations for leases and proposals.
type LeaseStore struct {
	ringID  string
	queries *database.Queries
}

// NewLeaseStore creates a new LeaseStore for the given ring.
func NewLeaseStore(ringID string, queries *database.Queries) *LeaseStore {
	return &LeaseStore{
		ringID:  ringID,
		queries: queries,
	}
}

// ListLeases returns all active leases in the ring.
func (ls *LeaseStore) ListLeases(ctx context.Context) ([]*Lease, error) {
	var records, err = ls.queries.ListLeases(ctx, ls.ringID)
	if err != nil {
		return nil, fmt.Errorf("failed to list leases: %w", err)
	}

	var leases = make([]*Lease, len(records))
	for i, record := range records {
		leases[i] = &Lease{
			Position:  record.Position,
			NodeID:    record.NodeID,
			VNodeIdx:  record.VNodeIdx,
			ExpiresAt: record.ExpiresAt,
		}
	}

	return leases, nil
}

// GetLease returns the lease at the given position, or nil if not found.
func (ls *LeaseStore) GetLease(ctx context.Context, position int) (*Lease, error) {
	var record, err = ls.queries.GetLease(ctx, ls.ringID, position)
	if err != nil {
		return nil, fmt.Errorf("failed to get lease at position %d: %w", position, err)
	}

	if record == nil {
		return nil, nil
	}

	return &Lease{
		Position:  record.Position,
		NodeID:    record.NodeID,
		VNodeIdx:  record.VNodeIdx,
		ExpiresAt: record.ExpiresAt,
	}, nil
}

// SetLease writes a lease to the database.
func (ls *LeaseStore) SetLease(ctx context.Context, lease *Lease) error {
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
func (ls *LeaseStore) DeleteLease(ctx context.Context, position int) error {
	if err := ls.queries.DeleteLease(ctx, ls.ringID, position); err != nil {
		return fmt.Errorf("failed to delete lease at position %d: %w", position, err)
	}
	return nil
}

// ListAllProposals returns all active proposals in the ring.
func (ls *LeaseStore) ListAllProposals(ctx context.Context) ([]*Proposal, error) {
	var records, err = ls.queries.ListAllProposals(ctx, ls.ringID)
	if err != nil {
		return nil, fmt.Errorf("failed to list proposals: %w", err)
	}

	var proposals = make([]*Proposal, len(records))
	for i, record := range records {
		proposals[i] = &Proposal{
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
func (ls *LeaseStore) SetProposal(ctx context.Context, proposal *Proposal) error {
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
func (ls *LeaseStore) DeleteProposal(ctx context.Context, predecessorPos int, newNodeID string, newVNodeIdx int) error {
	if err := ls.queries.DeleteProposal(ctx, ls.ringID, predecessorPos, newNodeID, newVNodeIdx); err != nil {
		return fmt.Errorf("failed to delete proposal: %w", err)
	}
	return nil
}

// RenewLeases renews all leases for the given positions.
func (ls *LeaseStore) RenewLeases(ctx context.Context, nodeID string, positions []int, ttl time.Duration) error {
	var expiresAt = time.Now().Add(ttl)

	for i, position := range positions {
		var lease = &Lease{
			Position:  position,
			NodeID:    nodeID,
			VNodeIdx:  i,
			ExpiresAt: expiresAt,
		}

		if err := ls.SetLease(ctx, lease); err != nil {
			return err
		}
	}

	return nil
}
