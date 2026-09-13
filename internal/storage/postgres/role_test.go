package postgres

import (
	"context"
	"testing"
)

// Applying schema.sql leaves a ricochet role that can log in and reach every
// table and sequence. The grants always named the role; what the schema did
// not do was create it, so on a fresh cluster the first grant failed and the
// rest of the file never ran. The role on the test cluster may predate this
// (the file leaves an existing role alone), so the fresh-cluster path is
// proved by applying the schema with the role name substituted; this test
// pins the outcome the file guarantees either way.
func TestSchemaLeavesTheServiceRoleAbleToWork(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()

	var canLogin bool
	if err := store.Pool().QueryRow(ctx,
		`SELECT rolcanlogin FROM pg_roles WHERE rolname = 'ricochet'`).Scan(&canLogin); err != nil {
		t.Fatalf("role ricochet: %v", err)
	}
	if !canLogin {
		t.Error("role ricochet cannot log in")
	}

	var tables, sequences []string
	rows, err := store.Pool().Query(ctx, `
		SELECT tablename FROM pg_tables WHERE schemaname = 'public'
		  AND NOT has_table_privilege('ricochet', 'public.' || quote_ident(tablename), 'SELECT, INSERT, UPDATE, DELETE')`)
	if err != nil {
		t.Fatalf("table privileges: %v", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	rows, err = store.Pool().Query(ctx, `
		SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'
		  AND NOT has_sequence_privilege('ricochet', 'public.' || quote_ident(sequencename), 'USAGE, SELECT')`)
	if err != nil {
		t.Fatalf("sequence privileges: %v", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		sequences = append(sequences, name)
	}
	rows.Close()

	if len(tables) > 0 {
		t.Errorf("ricochet lacks table privileges on %v", tables)
	}
	if len(sequences) > 0 {
		t.Errorf("ricochet lacks sequence privileges on %v", sequences)
	}
}
