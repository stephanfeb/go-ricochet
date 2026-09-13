package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	client "github.com/twostack/go-ricochet/pkg/client"
)

func TestJoinDirectory(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listing := client.DirectoryListing{
		DisplayName: "Alice",
		Bio:         "Cryptography enthusiast",
	}

	if err := c.JoinDirectory(ctx, listing); err != nil {
		t.Fatalf("join directory: %v", err)
	}

	// Verify the entry exists via get
	entry, err := c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get directory entry: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil directory entry")
	}
	if entry.DisplayName != "Alice" {
		t.Errorf("expected displayName 'Alice', got %q", entry.DisplayName)
	}
	if entry.Bio != "Cryptography enthusiast" {
		t.Errorf("expected bio 'Cryptography enthusiast', got %q", entry.Bio)
	}
}

func TestBrowseDirectory(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Join with multiple clients
	clients := make([]*client.Client, 3)
	names := []string{"Alice", "Bob", "Charlie"}
	for i, name := range names {
		clients[i] = newTestClient(t, server)
		listing := client.DirectoryListing{
			DisplayName: name,
			Bio:         name + " bio",
		}
		if err := clients[i].JoinDirectory(ctx, listing); err != nil {
			t.Fatalf("join directory for %s: %v", name, err)
		}
	}

	// Browse all
	result, err := clients[0].BrowseDirectory(ctx)
	if err != nil {
		t.Fatalf("browse directory: %v", err)
	}
	if len(result.Entries) < 3 {
		t.Errorf("expected at least 3 entries, got %d", len(result.Entries))
	}
}

// The directory is shared by every test that ever ran against the database
// and listings outlive their peers, so the search term is minted per run:
// searching for a fixed word found the previous run's Alice as well.
func TestSearchDirectory(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c1 := newTestClient(t, server)
	c2 := newTestClient(t, server)
	word := fmt.Sprintf("zq%x", time.Now().UnixNano())

	if err := c1.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Alice Cryptographer",
		Bio:         "Loves " + word,
	}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := c2.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Bob Builder",
		Bio:         "Builds things",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	result, err := c1.BrowseDirectory(ctx, client.WithDirectoryQuery(word))
	if err != nil {
		t.Fatalf("browse with search: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry matching %q, got %d", word, len(result.Entries))
	}
	if got := result.Entries[0]; got.OwnerPeerID != c1.PeerID().String() || got.DisplayName != "Alice Cryptographer" {
		t.Errorf("expected Alice (%s), got %q (%s)", c1.PeerID(), got.DisplayName, got.OwnerPeerID)
	}
}

func TestLeaveDirectory(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Join
	if err := c.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Alice",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Verify joined
	entry, err := c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry == nil {
		t.Fatal("expected entry after join")
	}

	// Leave
	if err := c.LeaveDirectory(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}

	// Verify removed
	entry, err = c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get after leave: %v", err)
	}
	if entry != nil {
		t.Error("expected nil entry after leave")
	}
}

func TestUpdateDirectory(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Join
	if err := c.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Alice",
		Bio:         "Original bio",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Update (join again with new data)
	if err := c.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Alice Updated",
		Bio:         "New bio",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Verify
	entry, err := c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if entry.DisplayName != "Alice Updated" {
		t.Errorf("expected 'Alice Updated', got %q", entry.DisplayName)
	}
	if entry.Bio != "New bio" {
		t.Errorf("expected 'New bio', got %q", entry.Bio)
	}
}

func TestDirectoryPagination(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Join 5 clients
	var firstClient *client.Client
	for i := 0; i < 5; i++ {
		c := newTestClient(t, server)
		if i == 0 {
			firstClient = c
		}
		if err := c.JoinDirectory(ctx, client.DirectoryListing{
			DisplayName: "User" + string(rune('A'+i)),
			Bio:         "Bio",
		}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
	}

	// Browse with limit of 2
	result, err := firstClient.BrowseDirectory(ctx, client.WithDirectoryLimit(2))
	if err != nil {
		t.Fatalf("browse page 1: %v", err)
	}
	if len(result.Entries) != 2 {
		t.Errorf("expected 2 entries on page 1, got %d", len(result.Entries))
	}
	if !result.HasMore {
		t.Error("expected hasMore=true on page 1")
	}
	if result.NextCursor == "" {
		t.Error("expected non-empty cursor")
	}

	// Get page 2
	result2, err := firstClient.BrowseDirectory(ctx,
		client.WithDirectoryLimit(2),
		client.WithDirectoryCursor(result.NextCursor),
	)
	if err != nil {
		t.Fatalf("browse page 2: %v", err)
	}
	if len(result2.Entries) != 2 {
		t.Errorf("expected 2 entries on page 2, got %d", len(result2.Entries))
	}

	// Get page 3.
	result3, err := firstClient.BrowseDirectory(ctx,
		client.WithDirectoryLimit(2),
		client.WithDirectoryCursor(result2.NextCursor),
	)
	if err != nil {
		t.Fatalf("browse page 3: %v", err)
	}
	if len(result3.Entries) > 2 {
		t.Errorf("page 3 returned %d entries against a limit of 2", len(result3.Entries))
	}

	// The pages must not overlap. This is what a cursor is for, and it holds
	// however many entries the directory contains -- unlike "page 3 has
	// exactly one", which was only true when this test was the sole occupant
	// of the database. The directory is server-wide and the test database is
	// shared, so an absolute count belongs to every test at once.
	//
	// TestDirectoryWalkTerminatesWithoutRepeating covers reaching the end.
	seen := map[string]int{}
	for _, page := range []*client.DirectoryBrowseResult{result, result2, result3} {
		for _, e := range page.Entries {
			seen[e.OwnerPeerID]++
		}
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("entry %s appeared on %d of the three pages; the cursor is not advancing",
				id[:12], count)
		}
	}
}

func TestDirectoryMaterialization(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// PUT a directory-listing document
	listing := []byte(`{"listed":true,"displayName":"Materialized Alice","bio":"From document"}`)
	_, err := c.PutDocument(ctx, c.PeerID(), "directory-listing", listing,
		client.WithContentType("application/json"))
	if err != nil {
		t.Fatalf("put directory-listing doc: %v", err)
	}

	// Verify materialized directory entry
	entry, err := c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry == nil {
		t.Fatal("expected materialized entry")
	}
	if entry.DisplayName != "Materialized Alice" {
		t.Errorf("expected 'Materialized Alice', got %q", entry.DisplayName)
	}
}

func TestDirectoryMaterializationOnDelete(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Join via direct DIRECTORY operation
	if err := c.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "ToDelete",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Also PUT the document (so we can delete it)
	listing := []byte(`{"listed":true,"displayName":"ToDelete"}`)
	_, err := c.PutDocument(ctx, c.PeerID(), "directory-listing", listing,
		client.WithContentType("application/json"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Delete the document
	_, err = c.DeleteDocument(ctx, c.PeerID(), "directory-listing")
	if err != nil {
		t.Fatalf("delete doc: %v", err)
	}

	// Verify directory entry was removed
	entry, err := c.GetDirectoryEntry(ctx, c.PeerID())
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if entry != nil {
		t.Error("expected nil entry after document deletion")
	}
}

func TestDirectoryEmptyBrowse(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Search for something that doesn't exist
	result, err := c.BrowseDirectory(ctx, client.WithDirectoryQuery("nonexistent"))
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result.Entries))
	}
}

// Walking the directory to exhaustion has to terminate, and each entry has to
// appear exactly once.
//
// This is the property the old page-by-page test did not state, and it is the
// one that broke. The cursor is "{RFC3339Nano}:{peerId}" and the parser split
// on the first colon, which lands inside the timestamp's own "14:13:20". The
// parse failed, the failure was swallowed, and every page ran with no cursor —
// so browse returned the same first page forever with hasMore set. A client
// looping until hasMore went false never stopped.
func TestDirectoryWalkTerminatesWithoutRepeating(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The directory is server-wide and the test database is shared, so the
	// walk sees every test's entries. What this asserts is that the walk
	// terminates and that *these* entries each appear exactly once.
	const entries = 7
	var walker *client.Client
	mine := map[string]bool{}
	for i := 0; i < entries; i++ {
		c := newTestClient(t, server)
		if i == 0 {
			walker = c
		}
		if err := c.JoinDirectory(ctx, client.DirectoryListing{
			DisplayName: fmt.Sprintf("Walker%02d", i),
			Bio:         "walk",
		}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		mine[c.PeerID().String()] = true
	}

	seen := map[string]int{}
	cursor := ""
	pages := 0

	// The bound is the safety net: without the fix this loop never ends, so a
	// test asserting termination has to be able to fail rather than hang. It
	// is generous because the shared database holds other tests' entries too.
	const maxPages = 200
	for pages < maxPages {
		page, err := walker.BrowseDirectory(ctx,
			client.WithDirectoryLimit(2),
			client.WithDirectoryCursor(cursor),
		)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++

		for _, e := range page.Entries {
			seen[e.OwnerPeerID]++
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == "" {
			t.Fatal("hasMore is set but no cursor was returned; the walk cannot continue")
		}
		cursor = page.NextCursor
	}

	if pages >= maxPages {
		t.Fatalf("walked %d pages of 2 without reaching the end; the cursor is not advancing", pages)
	}

	for id := range mine {
		switch seen[id] {
		case 1:
			// As it should be.
		case 0:
			t.Errorf("entry %s never appeared in the walk", id[:12])
		default:
			t.Errorf("entry %s returned %d times, want once", id[:12], seen[id])
		}
	}
}

// A cursor the server cannot read is the caller's mistake, and it has to say
// so. Falling back to the first page is what let the parse failure above go
// unnoticed for as long as it did.
func TestUnreadableDirectoryCursorIsRejected(t *testing.T) {
	server := newTestServer(t)
	c := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.JoinDirectory(ctx, client.DirectoryListing{DisplayName: "Solo"}); err != nil {
		t.Fatalf("join: %v", err)
	}

	for _, bad := range []string{"nonsense", "not-a-time:12D3KooWfake", "12D3KooWfake"} {
		_, err := c.BrowseDirectory(ctx, client.WithDirectoryCursor(bad))
		if err == nil {
			t.Errorf("cursor %q was accepted; an unreadable cursor must not silently "+
				"return the first page", bad)
			continue
		}
		if got := client.Status(err); got != 400 {
			t.Errorf("cursor %q gave status %d, want 400", bad, got)
		}
	}
}
