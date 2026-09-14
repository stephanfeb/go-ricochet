package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/twostack/go-ricochet/internal/core"
)

// TestCollectionQueryTotalsUseRecordCount pins backlog N11: an unfiltered
// first page reports the collection's maintained record_count rather than
// counting rows, a filtered page counts its matches only when asked, and a
// cursor page reports no total. The proof that the unfiltered total is the
// record_count and not a COUNT(*) is a deliberate desync: record_count is set
// to a wrong value and the query must return that wrong value, which a scan
// never would.
func TestCollectionQueryTotalsUseRecordCount(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	coll, err := store.CreateCollection(ctx, owner, "totals/n11", "Totals", core.VisibilityPrivate)
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	// Three "tool" items and two "supply" items, five in all.
	for i, tag := range []string{"tool", "tool", "tool", "supply", "supply"} {
		key := fmt.Sprintf("k%02d", i)
		body := []byte(fmt.Sprintf(`{"tag":%q}`, tag))
		if _, _, err := store.PutCollectionItem(ctx, coll.ID, key, body, owner, nil); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Desync the maintained count so a record_count read and a real COUNT(*)
	// give different answers.
	if _, err := store.pool.Exec(ctx,
		`UPDATE collections SET record_count = 12345 WHERE id = $1`, coll.ID); err != nil {
		t.Fatalf("desync record_count: %v", err)
	}

	// Unfiltered first page: total is the (desynced) record_count, proving no
	// scan; only `limit` items come back.
	res, err := store.QueryCollection(ctx, coll.ID, map[string]any{}, "", true, 2, 0, "", false)
	if err != nil {
		t.Fatalf("unfiltered query: %v", err)
	}
	if res.TotalCount != 12345 {
		t.Errorf("unfiltered TotalCount = %d, want 12345 (the record_count, not a COUNT of 5)", res.TotalCount)
	}
	if len(res.Items) != 2 {
		t.Errorf("unfiltered page returned %d items, want the limit of 2", len(res.Items))
	}

	// Key listing: same, its total is the record_count.
	_, keyTotal, _, err := store.ListCollectionKeys(ctx, coll.ID, 2, 0, "")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if keyTotal != 12345 {
		t.Errorf("ListCollectionKeys total = %d, want 12345", keyTotal)
	}

	// Filtered without wantTotal: no total.
	res, err = store.QueryCollection(ctx, coll.ID, map[string]any{"tag": "tool"}, "", true, 10, 0, "", false)
	if err != nil {
		t.Fatalf("filtered query: %v", err)
	}
	if res.TotalCount != -1 {
		t.Errorf("filtered TotalCount without wantTotal = %d, want -1", res.TotalCount)
	}
	if len(res.Items) != 3 {
		t.Errorf("filtered query returned %d items, want 3 tools", len(res.Items))
	}

	// Filtered with wantTotal: a real count of matches, which is 3 — not the
	// desynced record_count, since a filtered total must scan.
	res, err = store.QueryCollection(ctx, coll.ID, map[string]any{"tag": "tool"}, "", true, 10, 0, "", true)
	if err != nil {
		t.Fatalf("filtered query with total: %v", err)
	}
	if res.TotalCount != 3 {
		t.Errorf("filtered TotalCount with wantTotal = %d, want 3 (the matches, counted)", res.TotalCount)
	}

	// A cursor page reports no total whatever the filter.
	res, err = store.QueryCollection(ctx, coll.ID, map[string]any{}, "", true, 2, 0, "", false)
	if err != nil {
		t.Fatalf("first page for cursor: %v", err)
	}
	if res.NextCursor == "" {
		t.Fatal("expected a next cursor from a 2-item page of 5 items")
	}
	page2, err := store.QueryCollection(ctx, coll.ID, map[string]any{}, "", true, 2, 0, res.NextCursor, false)
	if err != nil {
		t.Fatalf("cursor page: %v", err)
	}
	if page2.TotalCount != -1 {
		t.Errorf("cursor page TotalCount = %d, want -1", page2.TotalCount)
	}
}
