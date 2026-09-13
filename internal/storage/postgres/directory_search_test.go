package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twostack/go-ricochet/internal/storage"
)

// A directory search is a literal: LIKE wildcards in it match themselves,
// a word matches through the full-text index, a name prefix matches, and an
// over-long query is refused rather than run.
func TestBrowseDirectoryTreatsWildcardsLiterally(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	alice, bob := newTestPeer(t).String(), newTestPeer(t).String()

	for _, e := range []*storage.DirectoryEntry{
		{OwnerPeerID: alice, DisplayName: "Alice Cryptographer", Bio: "Loves encryption and coffee"},
		{OwnerPeerID: bob, DisplayName: "Bob Builder", Bio: "Builds things"},
	} {
		if err := store.UpsertDirectoryEntry(ctx, e); err != nil {
			t.Fatalf("upsert %s: %v", e.DisplayName, err)
		}
	}
	t.Cleanup(func() {
		_ = store.RemoveDirectoryEntry(ctx, alice)
		_ = store.RemoveDirectoryEntry(ctx, bob)
	})

	names := func(query string) []string {
		t.Helper()
		page, err := store.BrowseDirectory(ctx, query, "", 100)
		if err != nil {
			t.Fatalf("browse %q: %v", query, err)
		}
		var out []string
		for _, e := range page.Entries {
			if e.OwnerPeerID == alice || e.OwnerPeerID == bob {
				out = append(out, e.DisplayName)
			}
		}
		return out
	}

	cases := []struct {
		query string
		want  []string
	}{
		{"%", nil},
		{"_", nil},
		{"%%%", nil},
		{"A_ice", nil},
		{`\`, nil},
		{"encryption", []string{"Alice Cryptographer"}},
		{"encrypt", []string{"Alice Cryptographer"}},
		{"builds", []string{"Bob Builder"}},
		{"Ali", []string{"Alice Cryptographer"}},
		{"ali", []string{"Alice Cryptographer"}},
		{"ali cryp", []string{"Alice Cryptographer"}},
		{"alice builder", nil},
		{"encryption'); --", []string{"Alice Cryptographer"}},
		{"encryption'); drop table directory_listings; --", nil},
		{"nonexistent", nil},
	}
	for _, c := range cases {
		got := names(c.query)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("query %q returned %v, want %v", c.query, got, c.want)
		}
	}

	if _, err := store.BrowseDirectory(ctx, strings.Repeat("a", storage.MaxDirectoryQueryLength+1), "", 10); !errors.Is(err, storage.ErrDirectoryQueryTooLong) {
		t.Fatalf("over-long query returned %v, want ErrDirectoryQueryTooLong", err)
	}
	if _, err := store.BrowseDirectory(ctx, strings.Repeat("a", storage.MaxDirectoryQueryLength), "", 10); err != nil {
		t.Fatalf("query at the cap refused: %v", err)
	}
}
