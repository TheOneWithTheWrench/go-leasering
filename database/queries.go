package database

import (
	"context"
	"database/sql"
	"fmt"
)

// DBTX is an interface that both sql.DB and sql.Tx implement.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Queries provides table-aware database operations.
type Queries struct {
	db        DBTX
	tableName string
}

// NewQueries creates a new Queries instance with the given table name.
func NewQueries(db DBTX, tableName string) *Queries {
	return &Queries{
		db:        db,
		tableName: tableName,
	}
}

var (
	getLeasesSQL = `
SELECT ring_id, position, node_id, vnode_idx, expires_at
FROM %s_leases
WHERE ring_id = $1
ORDER BY position ASC;`

	getLeaseSQL = `
SELECT ring_id, position, node_id, vnode_idx, expires_at
FROM %s_leases
WHERE ring_id = $1 AND position = $2;`

	setLeaseSQL = `
INSERT INTO %s_leases (ring_id, position, node_id, vnode_idx, expires_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (ring_id, position)
DO UPDATE SET
    node_id = EXCLUDED.node_id,
    vnode_idx = EXCLUDED.vnode_idx,
    expires_at = EXCLUDED.expires_at;`

	deleteLeaseSQL = `
DELETE FROM %s_leases
WHERE ring_id = $1 AND position = $2;`

	getProposalsSQL = `
SELECT ring_id, predecessor_pos, new_node_id, new_vnode_idx, proposed_pos, expires_at
FROM %s_proposals
WHERE ring_id = $1 AND predecessor_pos = $2;`

	getAllProposalsSQL = `
SELECT ring_id, predecessor_pos, new_node_id, new_vnode_idx, proposed_pos, expires_at
FROM %s_proposals
WHERE ring_id = $1;`

	setProposalSQL = `
INSERT INTO %s_proposals (ring_id, predecessor_pos, new_node_id, new_vnode_idx, proposed_pos, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (ring_id, predecessor_pos, new_node_id, new_vnode_idx)
DO UPDATE SET
    proposed_pos = EXCLUDED.proposed_pos,
    expires_at = EXCLUDED.expires_at;`

	deleteProposalSQL = `
DELETE FROM %s_proposals
WHERE ring_id = $1 AND predecessor_pos = $2 AND new_node_id = $3 AND new_vnode_idx = $4;`
)

// ListLeases returns all leases for a ring, ordered by position.
func (q *Queries) ListLeases(ctx context.Context, ringID string) ([]*LeaseRecord, error) {
	var (
		query = fmt.Sprintf(getLeasesSQL, q.tableName)
		rows, err = q.db.QueryContext(ctx, query, ringID)
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list leases: %w", err)
	}
	defer rows.Close()

	var leases []*LeaseRecord
	for rows.Next() {
		var lease LeaseRecord
		if err := rows.Scan(&lease.RingID, &lease.Position, &lease.NodeID, &lease.VNodeIdx, &lease.ExpiresAt); err != nil {
			return nil, fmt.Errorf("failed to scan lease: %w", err)
		}
		leases = append(leases, &lease)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return leases, nil
}

// GetLease retrieves a single lease by position.
func (q *Queries) GetLease(ctx context.Context, ringID string, position int) (*LeaseRecord, error) {
	var (
		query = fmt.Sprintf(getLeaseSQL, q.tableName)
		lease LeaseRecord
		err   = q.db.QueryRowContext(ctx, query, ringID, position).Scan(
			&lease.RingID, &lease.Position, &lease.NodeID, &lease.VNodeIdx, &lease.ExpiresAt,
		)
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get lease: %w", err)
	}

	return &lease, nil
}

// SetLease inserts or updates a lease.
func (q *Queries) SetLease(ctx context.Context, lease *LeaseRecord) error {
	var query = fmt.Sprintf(setLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to set lease: %w", err)
	}
	return nil
}

// DeleteLease removes a lease by position.
func (q *Queries) DeleteLease(ctx context.Context, ringID string, position int) error {
	var query = fmt.Sprintf(deleteLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID, position)
	if err != nil {
		return fmt.Errorf("failed to delete lease: %w", err)
	}
	return nil
}

// ListProposals returns all proposals for a given predecessor position.
func (q *Queries) ListProposals(ctx context.Context, ringID string, predecessorPos int) ([]*ProposalRecord, error) {
	var (
		query = fmt.Sprintf(getProposalsSQL, q.tableName)
		rows, err = q.db.QueryContext(ctx, query, ringID, predecessorPos)
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list proposals: %w", err)
	}
	defer rows.Close()

	var proposals []*ProposalRecord
	for rows.Next() {
		var proposal ProposalRecord
		if err := rows.Scan(&proposal.RingID, &proposal.PredecessorPos, &proposal.NewNodeID,
			&proposal.NewVNodeIdx, &proposal.ProposedPos, &proposal.ExpiresAt); err != nil {
			return nil, fmt.Errorf("failed to scan proposal: %w", err)
		}
		proposals = append(proposals, &proposal)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return proposals, nil
}

// ListAllProposals returns all proposals for a ring.
// This is more efficient than calling ListProposals multiple times.
func (q *Queries) ListAllProposals(ctx context.Context, ringID string) ([]*ProposalRecord, error) {
	var (
		query = fmt.Sprintf(getAllProposalsSQL, q.tableName)
		rows, err = q.db.QueryContext(ctx, query, ringID)
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list all proposals: %w", err)
	}
	defer rows.Close()

	var proposals []*ProposalRecord
	for rows.Next() {
		var proposal ProposalRecord
		if err := rows.Scan(&proposal.RingID, &proposal.PredecessorPos, &proposal.NewNodeID,
			&proposal.NewVNodeIdx, &proposal.ProposedPos, &proposal.ExpiresAt); err != nil {
			return nil, fmt.Errorf("failed to scan proposal: %w", err)
		}
		proposals = append(proposals, &proposal)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return proposals, nil
}

// SetProposal inserts or updates a proposal.
func (q *Queries) SetProposal(ctx context.Context, proposal *ProposalRecord) error {
	var query = fmt.Sprintf(setProposalSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		proposal.RingID, proposal.PredecessorPos, proposal.NewNodeID,
		proposal.NewVNodeIdx, proposal.ProposedPos, proposal.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to set proposal: %w", err)
	}
	return nil
}

// DeleteProposal removes a proposal.
func (q *Queries) DeleteProposal(ctx context.Context, ringID string, predecessorPos int, newNodeID string, newVNodeIdx int) error {
	var query = fmt.Sprintf(deleteProposalSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID, predecessorPos, newNodeID, newVNodeIdx)
	if err != nil {
		return fmt.Errorf("failed to delete proposal: %w", err)
	}
	return nil
}
