package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrLeaseNotInserted = errors.New("lease not inserted")
	ErrLeaseNotRenewed  = errors.New("lease not renewed")
)

// DBTX is an interface that both sql.DB and sql.Tx implement.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func closeRows(rows *sql.Rows, err *error) {
	if closeErr := rows.Close(); closeErr != nil && *err == nil {
		*err = fmt.Errorf("failed to close rows: %w", closeErr)
	}
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

	insertLeaseIfPredecessorOwnedSQL = `
INSERT INTO %s_leases (ring_id, position, node_id, vnode_idx, expires_at)
SELECT $1::varchar, $2::integer, $3::varchar, $4::integer, $5::timestamptz
WHERE EXISTS (
    SELECT 1
    FROM %s_leases
    WHERE ring_id = $1
      AND position = $6
      AND node_id = $7
      AND expires_at > NOW()
);`

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

	deleteExpiredLeasesSQL = `
DELETE FROM %s_leases
WHERE ring_id = $1
  AND expires_at <= NOW();`

	getProposalsSQL = `
SELECT ring_id, predecessor_pos, new_node_id, new_vnode_idx, proposed_pos, expires_at
FROM %s_proposals
WHERE ring_id = $1 AND predecessor_pos = $2;`

	getProposalsForPredecessorsSQL = `
SELECT ring_id, predecessor_pos, new_node_id, new_vnode_idx, proposed_pos, expires_at
FROM %s_proposals
WHERE ring_id = $1
  AND predecessor_pos IN (%s)
  AND expires_at > NOW();`

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

	deleteExpiredProposalsSQL = `
DELETE FROM %s_proposals
WHERE ring_id = $1
  AND expires_at <= NOW();`
)

// ListLeases returns all active leases for a ring, ordered by position.
func (q *Queries) ListLeases(ctx context.Context, ringID string) (leases []*LeaseRecord, err error) {
	var (
		query = fmt.Sprintf(listActiveLeasesSQL, q.tableName)
		rows  *sql.Rows
	)
	rows, err = q.db.QueryContext(ctx, query, ringID)
	if err != nil {
		return nil, fmt.Errorf("failed to list leases: %w", err)
	}
	defer closeRows(rows, &err)

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

// InsertLeaseIfPredecessorOwned inserts a lease only if the accepter still owns an active predecessor lease.
func (q *Queries) InsertLeaseIfPredecessorOwned(ctx context.Context, lease *LeaseRecord, predecessorPos int, accepterNodeID string) error {
	query := fmt.Sprintf(insertLeaseIfPredecessorOwnedSQL, q.tableName, q.tableName)
	result, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt, predecessorPos, accepterNodeID,
	)
	if err != nil {
		return fmt.Errorf("failed to insert lease if predecessor owned: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check inserted lease rows: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: predecessor position %d", ErrLeaseNotInserted, predecessorPos)
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

// DeleteExpiredLeases removes all expired leases for a ring.
func (q *Queries) DeleteExpiredLeases(ctx context.Context, ringID string) error {
	query := fmt.Sprintf(deleteExpiredLeasesSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID)
	if err != nil {
		return fmt.Errorf("failed to delete expired leases: %w", err)
	}
	return nil
}

// ListProposals returns all proposals for a given predecessor position.
func (q *Queries) ListProposals(ctx context.Context, ringID string, predecessorPos int) (proposals []*ProposalRecord, err error) {
	var (
		query = fmt.Sprintf(getProposalsSQL, q.tableName)
		rows  *sql.Rows
	)
	rows, err = q.db.QueryContext(ctx, query, ringID, predecessorPos)
	if err != nil {
		return nil, fmt.Errorf("failed to list proposals: %w", err)
	}
	defer closeRows(rows, &err)

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

// ListProposalsForPredecessors returns proposals for a set of predecessor positions.
func (q *Queries) ListProposalsForPredecessors(ctx context.Context, ringID string, predecessorPositions []int) (proposals []*ProposalRecord, err error) {
	if len(predecessorPositions) == 0 {
		return nil, nil
	}

	var (
		placeholders = make([]string, len(predecessorPositions))
		args         = make([]any, 0, len(predecessorPositions)+1)
	)
	args = append(args, ringID)

	for i, position := range predecessorPositions {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, position)
	}

	var (
		query = fmt.Sprintf(getProposalsForPredecessorsSQL, q.tableName, strings.Join(placeholders, ", "))
		rows  *sql.Rows
	)
	rows, err = q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list proposals for predecessors: %w", err)
	}
	defer closeRows(rows, &err)

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

// DeleteExpiredProposals removes all expired proposals for a ring.
func (q *Queries) DeleteExpiredProposals(ctx context.Context, ringID string) error {
	query := fmt.Sprintf(deleteExpiredProposalsSQL, q.tableName)
	_, err := q.db.ExecContext(ctx, query, ringID)
	if err != nil {
		return fmt.Errorf("failed to delete expired proposals: %w", err)
	}
	return nil
}
