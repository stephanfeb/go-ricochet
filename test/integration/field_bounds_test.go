package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/pkg/client"
	"github.com/twostack/go-ricochet/pkg/wire"
)

// Every free-text field a client can send is bounded, and a value past the
// bound is refused with an error naming the field rather than stored.
func TestClientStringsAreBounded(t *testing.T) {
	srv := newTestServer(t)
	c := newTestClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	over := func(n int) string { return strings.Repeat("x", n+1) }
	refused := func(field string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s past its bound was accepted", field)
		}
		if !strings.Contains(err.Error(), field) {
			t.Errorf("%s past its bound refused with %q, which does not name the field", field, err)
		}
	}

	_, err := c.PutDocument(ctx, c.PeerID(), "bounded/doc", []byte("{}"),
		client.WithContentType(over(wire.MaxContentTypeLength)))
	refused("Content-Type", err)

	refused("displayName", c.JoinDirectory(ctx, client.DirectoryListing{DisplayName: over(wire.MaxDisplayNameLength)}))
	refused("bio", c.JoinDirectory(ctx, client.DirectoryListing{DisplayName: "ok", Bio: over(wire.MaxBioLength)}))
	refused("avatarHash", c.JoinDirectory(ctx, client.DirectoryListing{DisplayName: "ok", AvatarHash: over(wire.MaxAvatarHashLength)}))
	refused("extras", c.JoinDirectory(ctx, client.DirectoryListing{DisplayName: "ok",
		Extras: map[string]any{"blob": over(wire.MaxDirectoryExtrasBytes)}}))

	refused("name", c.CreateCollection(ctx, "bounded/coll", over(wire.MaxCollectionNameLength)))
	if err := c.CreateCollection(ctx, "bounded/coll", "fine"); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	_, err = c.PutCollectionItem(ctx, "bounded/coll", over(wire.MaxCollectionKeyLength), []byte(`{"a":1}`))
	refused("key", err)

	refused("title", c.CreateFeed(ctx, "bounded/feed", over(wire.MaxFeedTitleLength), ""))
	refused("description", c.CreateFeed(ctx, "bounded/feed", "ok", over(wire.MaxFeedDescriptionLength)))
	if err := c.CreateFeed(ctx, "bounded/feed", "ok", "fine"); err != nil {
		t.Fatalf("create feed: %v", err)
	}
	_, err = c.AppendFeedEntry(ctx, "bounded/feed", []byte(`{"a":1}`), over(wire.MaxEntryTypeLength))
	refused("entryType", err)

	// Values at the bound are stored.
	if _, err := c.PutDocument(ctx, c.PeerID(), "bounded/doc", []byte("{}"),
		client.WithContentType(strings.Repeat("x", wire.MaxContentTypeLength))); err != nil {
		t.Fatalf("content type at the bound refused: %v", err)
	}
	if err := c.JoinDirectory(ctx, client.DirectoryListing{
		DisplayName: strings.Repeat("x", wire.MaxDisplayNameLength),
		Bio:         strings.Repeat("x", wire.MaxBioLength),
	}); err != nil {
		t.Fatalf("listing at the bounds refused: %v", err)
	}
}
