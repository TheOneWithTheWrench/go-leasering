package database

import (
	"database/sql"
	"fmt"
)

const (
	MaxIdentifierLength  = 63
	LeasesTableSuffix    = "_leases"
	ProposalsTableSuffix = "_proposals"
	MaxRingIDLength      = MaxIdentifierLength - len(ProposalsTableSuffix)
)

var (
	createLeasesTableSQL = `
CREATE TABLE IF NOT EXISTS %s_leases (
    ring_id       VARCHAR       NOT NULL,
    position      INTEGER       NOT NULL,
    node_id       VARCHAR       NOT NULL,
    vnode_idx     INTEGER       NOT NULL,
    expires_at    TIMESTAMPTZ   NOT NULL,

    PRIMARY KEY (ring_id, position)
);`

	createProposalsTableSQL = `
CREATE TABLE IF NOT EXISTS %s_proposals (
    ring_id           VARCHAR       NOT NULL,
    predecessor_pos   INTEGER       NOT NULL,
    new_node_id       VARCHAR       NOT NULL,
    new_vnode_idx     INTEGER       NOT NULL,
    proposed_pos      INTEGER       NOT NULL,
    expires_at        TIMESTAMPTZ   NOT NULL,

    PRIMARY KEY (ring_id, predecessor_pos, new_node_id, new_vnode_idx)
);`

	createLeasesActiveIndexSQL = `
CREATE INDEX IF NOT EXISTS %s_leases_active_idx
ON %s_leases (ring_id, position, expires_at);`

	createProposalsExpiresIndexSQL = `
CREATE INDEX IF NOT EXISTS %s_proposals_expires_idx
ON %s_proposals (ring_id, expires_at);`
)

// Migrate creates the leases and proposals tables.
func Migrate(db *sql.DB, tableName string) error {
	if err := createLeasesTable(db, tableName); err != nil {
		return err
	}

	if err := createProposalsTable(db, tableName); err != nil {
		return err
	}

	if err := createIndexes(db, tableName); err != nil {
		return err
	}

	return nil
}

func createLeasesTable(db *sql.DB, tableName string) error {
	var query = fmt.Sprintf(createLeasesTableSQL, tableName)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create leases table: %w", err)
	}
	return nil
}

func createProposalsTable(db *sql.DB, tableName string) error {
	var query = fmt.Sprintf(createProposalsTableSQL, tableName)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create proposals table: %w", err)
	}
	return nil
}

func createIndexes(db *sql.DB, tableName string) error {
	var queries = []string{
		fmt.Sprintf(createLeasesActiveIndexSQL, tableName, tableName),
		fmt.Sprintf(createProposalsExpiresIndexSQL, tableName, tableName),
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to create indexes: %w", err)
		}
	}

	return nil
}
