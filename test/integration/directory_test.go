package integration_test

import (
	"context"
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

func TestSearchDirectory(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c1 := newTestClient(t, server)
	c2 := newTestClient(t, server)

	if err := c1.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Alice Cryptographer",
		Bio:         "Loves encryption",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := c2.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: "Bob Builder",
		Bio:         "Builds things",
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Search for "encryption"
	result, err := c1.BrowseDirectory(ctx, client.WithDirectoryQuery("encryption"))
	if err != nil {
		t.Fatalf("browse with search: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry matching 'encryption', got %d", len(result.Entries))
	}
	if len(result.Entries) > 0 && result.Entries[0].DisplayName != "Alice Cryptographer" {
		t.Errorf("expected Alice, got %q", result.Entries[0].DisplayName)
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

	// Get page 3 (should have 1 remaining)
	result3, err := firstClient.BrowseDirectory(ctx,
		client.WithDirectoryLimit(2),
		client.WithDirectoryCursor(result2.NextCursor),
	)
	if err != nil {
		t.Fatalf("browse page 3: %v", err)
	}
	if len(result3.Entries) != 1 {
		t.Errorf("expected 1 entry on page 3, got %d", len(result3.Entries))
	}
	if result3.HasMore {
		t.Error("expected hasMore=false on last page")
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
