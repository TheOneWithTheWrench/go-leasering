package database

import (
	"bytes"
	"database/sql"
	"embed"
	"fmt"
	"text/template"
)

const (
	MaxIdentifierLength  = 63
	LeasesTableSuffix    = "_leases"
	ProposalsTableSuffix = "_proposals"
	MaxRingIDLength      = MaxIdentifierLength - len(ProposalsTableSuffix)
)

//go:embed migrations/*.sql.tmpl
var migrationTemplateFiles embed.FS

var migrationTemplates = template.Must(template.ParseFS(migrationTemplateFiles, "migrations/*.sql.tmpl"))

type migrationTemplateData struct {
	LeasesTable           string
	ProposalsTable        string
	LeasesActiveIndex     string
	ProposalsExpiresIndex string
}

func newMigrationTemplateData(tableName string) migrationTemplateData {
	return migrationTemplateData{
		LeasesTable:           tableName + LeasesTableSuffix,
		ProposalsTable:        tableName + ProposalsTableSuffix,
		LeasesActiveIndex:     tableName + "_leases_active_idx",
		ProposalsExpiresIndex: tableName + "_proposals_expires_idx",
	}
}

func renderMigrationTemplate(name string, tableName string) (string, error) {
	var buf bytes.Buffer
	if err := migrationTemplates.ExecuteTemplate(&buf, name, newMigrationTemplateData(tableName)); err != nil {
		return "", fmt.Errorf("failed to render migration template %q: %w", name, err)
	}
	return buf.String(), nil
}

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
	query, err := renderMigrationTemplate("createLeasesTable", tableName)
	if err != nil {
		return err
	}
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create leases table: %w", err)
	}
	return nil
}

func createProposalsTable(db *sql.DB, tableName string) error {
	query, err := renderMigrationTemplate("createProposalsTable", tableName)
	if err != nil {
		return err
	}
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create proposals table: %w", err)
	}
	return nil
}

func createIndexes(db *sql.DB, tableName string) error {
	var templates = []string{
		"createLeasesActiveIndex",
		"createProposalsExpiresIndex",
	}

	for _, tmpl := range templates {
		query, err := renderMigrationTemplate(tmpl, tableName)
		if err != nil {
			return err
		}
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to create indexes: %w", err)
		}
	}

	return nil
}
