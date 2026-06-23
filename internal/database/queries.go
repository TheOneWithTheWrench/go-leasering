package database

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"
	"text/template"
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

//go:embed queries/*.sql.tmpl
var queryTemplateFiles embed.FS

var queryTemplates = template.Must(template.ParseFS(queryTemplateFiles, "queries/*.sql.tmpl"))

type queryTemplateData struct {
	LeasesTable          string
	ProposalsTable       string
	PositionPlaceholders string
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

func (q *Queries) renderQuery(name string, positionPlaceholders string) (string, error) {
	var (
		buf  bytes.Buffer
		data = queryTemplateData{
			LeasesTable:          q.tableName + LeasesTableSuffix,
			ProposalsTable:       q.tableName + ProposalsTableSuffix,
			PositionPlaceholders: positionPlaceholders,
		}
	)

	if err := queryTemplates.ExecuteTemplate(&buf, name, data); err != nil {
		return "", fmt.Errorf("failed to render query %q: %w", name, err)
	}

	return buf.String(), nil
}

// ListLeases returns all active leases for a ring, ordered by position.
func (q *Queries) ListLeases(ctx context.Context, ringID string) (leases []*LeaseRecord, err error) {
	var (
		query string
		rows  *sql.Rows
	)
	query, err = q.renderQuery("listActiveLeases", "")
	if err != nil {
		return nil, err
	}
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
	query, err := q.renderQuery("getLease", "")
	if err != nil {
		return nil, err
	}

	var (
		lease   LeaseRecord
		scanErr = q.db.QueryRowContext(ctx, query, ringID, position).Scan(
			&lease.RingID, &lease.Position, &lease.NodeID, &lease.VNodeIdx, &lease.ExpiresAt,
		)
	)
	if scanErr == sql.ErrNoRows {
		return nil, nil
	}
	if scanErr != nil {
		return nil, fmt.Errorf("failed to get lease: %w", scanErr)
	}

	return &lease, nil
}

// GetActiveLeases retrieves non-expired leases by position.
func (q *Queries) GetActiveLeases(ctx context.Context, ringID string, positions []int) (leases []*LeaseRecord, err error) {
	if len(positions) == 0 {
		return nil, nil
	}

	var (
		placeholders = make([]string, len(positions))
		args         = make([]any, 0, len(positions)+1)
	)
	args = append(args, ringID)

	for i, position := range positions {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, position)
	}

	var rows *sql.Rows
	query, err := q.renderQuery("getActiveLeases", strings.Join(placeholders, ", "))
	if err != nil {
		return nil, err
	}
	rows, err = q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get active leases: %w", err)
	}
	defer closeRows(rows, &err)

	for rows.Next() {
		var lease LeaseRecord
		if err := rows.Scan(&lease.RingID, &lease.Position, &lease.NodeID, &lease.VNodeIdx, &lease.ExpiresAt); err != nil {
			return nil, fmt.Errorf("failed to scan active lease: %w", err)
		}
		leases = append(leases, &lease)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return leases, nil
}

// SetLease inserts or updates a lease.
func (q *Queries) SetLease(ctx context.Context, lease *LeaseRecord) error {
	query, err := q.renderQuery("setLease", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query,
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
	query, err := q.renderQuery("insertLease", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert lease: %w", err)
	}
	return nil
}

// InsertLeaseIfTargetOwned inserts a lease only if the accepter still owns an active target lease.
func (q *Queries) InsertLeaseIfTargetOwned(ctx context.Context, lease *LeaseRecord, targetPos int, accepterNodeID string) error {
	query, err := q.renderQuery("insertLeaseIfTargetOwned", "")
	if err != nil {
		return err
	}

	result, err := q.db.ExecContext(ctx, query,
		lease.RingID, lease.Position, lease.NodeID, lease.VNodeIdx, lease.ExpiresAt, targetPos, accepterNodeID,
	)
	if err != nil {
		return fmt.Errorf("failed to insert lease if target owned: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check inserted lease rows: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: target position %d", ErrLeaseNotInserted, targetPos)
	}

	return nil
}

// RenewLease extends an existing lease only if the same node still owns it and it has not expired.
func (q *Queries) RenewLease(ctx context.Context, lease *LeaseRecord) error {
	query, err := q.renderQuery("renewLease", "")
	if err != nil {
		return err
	}

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

// DeleteLeaseIfOwned removes a lease only if it is still owned by nodeID.
func (q *Queries) DeleteLeaseIfOwned(ctx context.Context, ringID string, position int, nodeID string) error {
	query, err := q.renderQuery("deleteLeaseIfOwned", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query, ringID, position, nodeID)
	if err != nil {
		return fmt.Errorf("failed to delete lease: %w", err)
	}
	return nil
}

// DeleteExpiredLeases removes all expired leases for a ring.
func (q *Queries) DeleteExpiredLeases(ctx context.Context, ringID string) error {
	query, err := q.renderQuery("deleteExpiredLeases", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query, ringID)
	if err != nil {
		return fmt.Errorf("failed to delete expired leases: %w", err)
	}
	return nil
}

// ListProposals returns all proposals for a given predecessor position.
func (q *Queries) ListProposals(ctx context.Context, ringID string, predecessorPos int) (proposals []*ProposalRecord, err error) {
	var (
		query string
		rows  *sql.Rows
	)
	query, err = q.renderQuery("listProposals", "")
	if err != nil {
		return nil, err
	}
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

	var rows *sql.Rows
	query, err := q.renderQuery("listProposalsForPredecessors", strings.Join(placeholders, ", "))
	if err != nil {
		return nil, err
	}
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
	query, err := q.renderQuery("setProposal", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query,
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
	query, err := q.renderQuery("deleteProposal", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query, ringID, predecessorPos, newNodeID, newVNodeIdx)
	if err != nil {
		return fmt.Errorf("failed to delete proposal: %w", err)
	}
	return nil
}

// DeleteExpiredProposals removes all expired proposals for a ring.
func (q *Queries) DeleteExpiredProposals(ctx context.Context, ringID string) error {
	query, err := q.renderQuery("deleteExpiredProposals", "")
	if err != nil {
		return err
	}

	_, err = q.db.ExecContext(ctx, query, ringID)
	if err != nil {
		return fmt.Errorf("failed to delete expired proposals: %w", err)
	}
	return nil
}
