package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrLeaseNotRenewed = errors.New("lease not renewed")

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
	listActiveLeasesSQL = `
SELECT ring_id, position, node_id, vnode_idx, expires_at
FROM %s_leases
WHERE ring_id = $1
  AND expires_at > NOW()
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

	insertLeaseSQL = `
INSERT INTO %s_leases (ring_id, position, node_id, vnode_idx, expires_at)
VALUES ($1, $2, $3, $4, $5);`

	renewLeaseSQL = `
UPDATE %s_leases
SET expires_at = $5
WHERE ring_id = $1
  AND position = $2
  AND node_id = $3
  AND vnode_idx = $4
  AND expires_at > NOW();`

	deleteLeaseSQL = `
DELETE FROM %s_leases
WHERE ring_id = $1 AND position = $2;`

	deleteExpiredLeaseSQL = `
DELETE FROM %s_leases
WHERE ring_id = $1
  AND position = $2
  AND node_id = $3
  AND vnode_idx = $4
  AND expires_at <= NOW();`

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

// ListLeases returns all active leases for a ring, ordered by position.
func (q *Queries) ListLeases(ctx context.Context, ringID string) ([]*LeaseRecord, error) {
	var (
		query     = fmt.Sprintf(listActiveLeasesSQL, q.tableName)
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
	query := fmt.Sprintf(setLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to set lease: %w", err)
	}
	return nil
}

// InsertLease inserts a new lease. Returns error if a lease already exists at this position.
// This is used when accepting proposals to ensure atomic claim of a position.
func (q *Queries) InsertLease(ctx context.Context, lease *LeaseRecord) error {
	query := fmt.Sprintf(insertLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert lease: %w", err)
	}
	return nil
}

// RenewLease extends an existing lease only if the same node still owns it and it has not expired.
func (q *Queries) RenewLease(ctx context.Context, lease *LeaseRecord) error {
	query := fmt.Sprintf(renewLeaseSQL, q.tableName)
	result, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to renew lease: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check renewed lease rows: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: position %d", ErrLeaseNotRenewed, lease.Position)
	}

	return nil
}

// DeleteLease removes a lease by position.
func (q *Queries) DeleteLease(ctx context.Context, ringID string, position int) error {
	query := fmt.Sprintf(deleteLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID, position)
	if err != nil {
		return fmt.Errorf("failed to delete lease: %w", err)
	}
	return nil
}

// DeleteExpiredLease removes a lease only if the same vnode still owns it and it is expired in the database.
// Zero rows affected means the lease was renewed or taken over by another vnode, so we don't want to delete it.
func (q *Queries) DeleteExpiredLease(ctx context.Context, lease *LeaseRecord) error {
	query := fmt.Sprintf(deleteExpiredLeaseSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx,
	)
	if err != nil {
		return fmt.Errorf("failed to delete expired lease: %w", err)
	}
	return nil
}

// ListProposals returns all proposals for a given predecessor position.
func (q *Queries) ListProposals(ctx context.Context, ringID string, predecessorPos int) ([]*ProposalRecord, error) {
	var (
		query     = fmt.Sprintf(getProposalsSQL, q.tableName)
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
		query     = fmt.Sprintf(getAllProposalsSQL, q.tableName)
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
	query := fmt.Sprintf(setProposalSQL, q.tableName)
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
	query := fmt.Sprintf(deleteProposalSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID, predecessorPos, newNodeID, newVNodeIdx)
	if err != nil {
		return fmt.Errorf("failed to delete proposal: %w", err)
	}
	return nil
}
