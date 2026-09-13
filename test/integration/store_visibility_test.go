package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	client "github.com/twostack/go-ricochet/pkg/client"
	"github.com/twostack/go-ricochet/pkg/wire"
)

// forbidden reports whether err is the server's 403.
func forbidden(err error) bool {
	var fe *client.ForbiddenError
	return errors.As(err, &fe)
}

// Documents and collections are private by default and feeds public; the
// owner shares or publishes them and grants readers, and every read on the
// wire honours it. Two clients, one server, PostgreSQL behind it.
func TestStoreVisibilityEndToEnd(t *testing.T) {
	server := newTestServer(t)
	owner := newTestClient(t, server)
	friend := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := owner.PeerID()

	// Documents: private by default, the friend gets a 403 and an empty list.
	if _, err := owner.PutDocument(ctx, ownerID, "vis/diary", []byte("mine")); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := friend.GetDocument(ctx, ownerID, "vis/diary")
	if err != nil || resp.Status != 403 || len(resp.Content) != 0 {
		t.Fatalf("friend get of a private document = %+v, %v; want 403 with no content", resp, err)
	}
	if _, err := friend.HeadDocument(ctx, ownerID, "vis/diary"); !forbidden(err) {
		t.Errorf("friend head of a private document: %v, want 403", err)
	}
	docs, err := friend.ListDocuments(ctx, ownerID)
	if err != nil || len(docs) != 0 {
		t.Errorf("friend list = %v, %v; want nothing", docs, err)
	}
	if _, err := friend.GrantDocumentReader(ctx, "vis/diary", friend.PeerID()); err == nil {
		t.Error("friend managing the owner's document succeeded")
	}

	// Share it with the friend.
	access, err := owner.GrantDocumentReader(ctx, "vis/diary", friend.PeerID())
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if access.Visibility != "private" || len(access.Readers) != 1 || access.Readers[0].PeerID != friend.PeerID().String() {
		t.Errorf("access after grant = %+v, want private with the friend listed", access)
	}
	if access, err = owner.SetDocumentVisibility(ctx, "vis/diary", wire.VisibilityShared); err != nil || access.Visibility != "shared" {
		t.Fatalf("set shared: %+v, %v", access, err)
	}
	resp, err = friend.GetDocument(ctx, ownerID, "vis/diary")
	if err != nil || resp.Status != 200 || string(resp.Content) != "mine" || resp.Visibility != "shared" {
		t.Errorf("friend get of a shared document = %+v, %v; want the body and visibility shared", resp, err)
	}
	docs, err = friend.ListDocuments(ctx, ownerID)
	if err != nil || len(docs) != 1 || docs[0].Path != "vis/diary" || docs[0].Visibility != "shared" {
		t.Errorf("friend list = %+v, %v; want the shared document", docs, err)
	}
	stranger := newTestClient(t, server)
	if resp, err := stranger.GetDocument(ctx, ownerID, "vis/diary"); err != nil || resp.Status != 403 {
		t.Errorf("stranger get of a shared document = %+v, %v; want 403", resp, err)
	}
	if _, err := owner.RevokeDocumentReader(ctx, "vis/diary", friend.PeerID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if resp, err := friend.GetDocument(ctx, ownerID, "vis/diary"); err != nil || resp.Status != 403 {
		t.Errorf("revoked friend = %+v, %v; want 403", resp, err)
	}

	// Public at creation.
	if _, err := owner.PutDocument(ctx, ownerID, "vis/blog", []byte("hello"), client.WithVisibility(wire.VisibilityPublic)); err != nil {
		t.Fatalf("put public: %v", err)
	}
	if resp, err := stranger.GetDocument(ctx, ownerID, "vis/blog"); err != nil || resp.Status != 200 {
		t.Errorf("stranger get of a public document = %+v, %v; want 200", resp, err)
	}

	// Feeds: public by default, private on request.
	if err := owner.CreateFeed(ctx, "vis/news", "News", ""); err != nil {
		t.Fatal(err)
	}
	if err := owner.CreateFeed(ctx, "vis/drafts", "Drafts", "", client.WithFeedVisibility(wire.VisibilityPrivate)); err != nil {
		t.Fatal(err)
	}
	if info, err := stranger.GetFeed(ctx, ownerID, "vis/news"); err != nil || info == nil || info.Visibility != "public" {
		t.Errorf("stranger get of a public feed = %+v, %v", info, err)
	}
	if _, err := stranger.GetFeed(ctx, ownerID, "vis/drafts"); !forbidden(err) {
		t.Errorf("stranger get of a private feed: %v, want 403", err)
	}
	feeds, err := stranger.ListFeeds(ctx, ownerID)
	if err != nil || len(feeds) != 1 || feeds[0].Path != "vis/news" {
		t.Errorf("stranger feed list = %+v, %v; want only the public feed", feeds, err)
	}
	if _, err := owner.SetFeedVisibility(ctx, "vis/news", wire.VisibilityPrivate); err != nil {
		t.Fatal(err)
	}
	if _, err := stranger.GetFeed(ctx, ownerID, "vis/news"); !forbidden(err) {
		t.Errorf("stranger get of a feed made private: %v, want 403", err)
	}

	// Collections: private by default.
	if err := owner.CreateCollection(ctx, "vis/ledger", "Ledger"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.PutCollectionItem(ctx, "vis/ledger", "jan", []byte(`{"total":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := stranger.GetCollectionItem(ctx, ownerID, "vis/ledger", "jan"); !forbidden(err) {
		t.Errorf("stranger get of a private collection item: %v, want 403", err)
	}
	if _, err := owner.SetCollectionVisibility(ctx, "vis/ledger", wire.VisibilityPublic); err != nil {
		t.Fatal(err)
	}
	if item, err := stranger.GetCollectionItem(ctx, ownerID, "vis/ledger", "jan"); err != nil || item == nil {
		t.Errorf("stranger get of a public collection item = %+v, %v", item, err)
	}
	if err := owner.CreateCollection(ctx, "vis/menu", "Menu", client.WithCollectionVisibility(wire.VisibilityShared)); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.GrantCollectionReader(ctx, "vis/menu", friend.PeerID()); err != nil {
		t.Fatal(err)
	}
	if info, err := friend.GetCollection(ctx, ownerID, "vis/menu"); err != nil || info == nil || info.Visibility != "shared" {
		t.Errorf("friend get of a shared collection = %+v, %v", info, err)
	}
	if _, err := stranger.GetCollection(ctx, ownerID, "vis/menu"); !forbidden(err) {
		t.Errorf("stranger get of a shared collection: %v, want 403", err)
	}
}
