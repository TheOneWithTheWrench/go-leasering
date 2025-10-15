package database

import (
	"database/sql"
	"fmt"
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

	createProposalsIndexSQL = `
CREATE INDEX IF NOT EXISTS %s_proposals_predecessor_idx
ON %s_proposals (ring_id, predecessor_pos);`
)

// Migrate creates the leases and proposals tables with indexes.
func Migrate(db *sql.DB, tableName string) error {
	if err := createLeasesTable(db, tableName); err != nil {
		return err
	}

	if err := createProposalsTable(db, tableName); err != nil {
		return err
	}

	if err := createProposalsIndex(db, tableName); err != nil {
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

func createProposalsIndex(db *sql.DB, tableName string) error {
	var (
		indexName = fmt.Sprintf("%s_proposals_predecessor_idx", tableName)
		query     = fmt.Sprintf(createProposalsIndexSQL, indexName, tableName)
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create proposals index: %w", err)
	}
	return nil
}
