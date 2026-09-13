package postgres

import (
	"context"
	"testing"
)

// schema.sql carries no views, and the reserved block_store table is there.
//
// mailbox_stats and message_priority_stats had no reader, and the first
// recomputed message_count with a join over every stored message when the
// mailboxes table has kept that count by trigger since the counter column
// arrived. They are dropped in the upgrade section so an existing database
// loses them on the next schema run, which is what this checks: the test
// database has schema.sql applied to it, upgrade section included.
func TestSchemaCarriesNoDeadViews(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()

	var views []string
	rows, err := store.Pool().Query(ctx, `SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY viewname`)
	if err != nil {
		t.Fatalf("list views: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		views = append(views, name)
	}
	if len(views) > 0 {
		t.Errorf("schema defines views nothing reads: %v", views)
	}

	var reserved bool
	if err := store.Pool().QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'block_store')`,
	).Scan(&reserved); err != nil {
		t.Fatalf("check block_store: %v", err)
	}
	if !reserved {
		t.Error("block_store is gone; it is reserved for the roadmap's body offload (see schema.sql)")
	}
}
