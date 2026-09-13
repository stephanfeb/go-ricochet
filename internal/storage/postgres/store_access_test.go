package postgres

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// The three stores carry a visibility and a reader list. This checks the SQL
// side of it: the column round-trips through create, put and set; the reader
// list is granted, listed, revoked and goes with its resource; and each
// listing shows a reader only what the predicate admits.
func TestStoreVisibilityAndReaders(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner, friend, stranger := newTestPeer(t), newTestPeer(t), newTestPeer(t)

	// Defaults: documents and collections private, feeds public.
	if _, err := store.PutDocument(ctx, owner, "diary", []byte("d"), "text/plain", owner, nil, nil); err != nil {
		t.Fatal(err)
	}
	doc, _ := store.GetDocument(ctx, owner, "diary")
	if doc.Visibility != core.VisibilityPrivate {
		t.Errorf("new document visibility = %v, want private", doc.Visibility)
	}
	feed, err := store.CreateFeed(ctx, owner, "news", "News", "", false, core.VisibilityPublic)
	if err != nil {
		t.Fatal(err)
	}
	if feed.Visibility != core.VisibilityPublic {
		t.Errorf("new feed visibility = %v, want public", feed.Visibility)
	}
	coll, err := store.CreateCollection(ctx, owner, "ledger", "Ledger", core.VisibilityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if coll.Visibility != core.VisibilityPrivate {
		t.Errorf("new collection visibility = %v, want private", coll.Visibility)
	}

	// A put with a visibility changes it; one without keeps it.
	public := core.VisibilityPublic
	if _, err := store.PutDocument(ctx, owner, "diary", []byte("d2"), "text/plain", owner, nil, &public); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutDocument(ctx, owner, "diary", []byte("d3"), "text/plain", owner, nil, nil); err != nil {
		t.Fatal(err)
	}
	if doc, _ = store.HeadDocument(ctx, owner, "diary"); doc.Visibility != core.VisibilityPublic {
		t.Errorf("after put with public then a plain put: %v, want public", doc.Visibility)
	}
	if _, err := store.PatchDocument(ctx, owner, "diary", map[string]any{}, owner, nil); err == nil {
		// A patch of non-JSON fails; that is fine, the point is it never touches visibility.
		t.Log("patch of a text document unexpectedly succeeded")
	}

	// Set by (owner, path); absent paths report false.
	for _, kind := range []storage.StoreKind{storage.StoreDocument, storage.StoreFeed, storage.StoreCollection} {
		path := map[storage.StoreKind]string{storage.StoreDocument: "diary", storage.StoreFeed: "news", storage.StoreCollection: "ledger"}[kind]
		found, err := store.SetStoreVisibility(ctx, kind, owner, path, core.VisibilityShared)
		if err != nil || !found {
			t.Fatalf("set %s visibility: found=%v err=%v", kind, found, err)
		}
		if found, _ := store.SetStoreVisibility(ctx, kind, owner, "nowhere", core.VisibilityShared); found {
			t.Errorf("set %s visibility of an absent path reported found", kind)
		}
	}
	if doc, _ = store.HeadDocument(ctx, owner, "diary"); doc.Visibility != core.VisibilityShared {
		t.Errorf("document after set: %v, want shared", doc.Visibility)
	}
	if feed, _ = store.GetFeed(ctx, owner, "news"); feed.Visibility != core.VisibilityShared {
		t.Errorf("feed after set: %v, want shared", feed.Visibility)
	}
	if coll, _ = store.GetCollection(ctx, owner, "ledger"); coll.Visibility != core.VisibilityShared {
		t.Errorf("collection after set: %v, want shared", coll.Visibility)
	}

	// Reader lists: grant twice is one row; revoke removes it; listing is ordered.
	ids := map[storage.StoreKind]int64{storage.StoreDocument: doc.ID, storage.StoreFeed: feed.ID, storage.StoreCollection: coll.ID}
	for kind, id := range ids {
		for i := 0; i < 2; i++ {
			if err := store.GrantStoreReader(ctx, kind, id, friend); err != nil {
				t.Fatalf("grant %s: %v", kind, err)
			}
		}
		readers, err := store.ListStoreReaders(ctx, kind, id)
		if err != nil || len(readers) != 1 || readers[0].PeerID != friend.String() || readers[0].GrantedAt.IsZero() {
			t.Fatalf("%s readers = %+v (err %v), want the friend once with a timestamp", kind, readers, err)
		}
		if ok, _ := store.IsStoreReader(ctx, kind, id, friend); !ok {
			t.Errorf("%s: friend not reported as reader", kind)
		}
		if ok, _ := store.IsStoreReader(ctx, kind, id, stranger); ok {
			t.Errorf("%s: stranger reported as reader", kind)
		}
	}

	// Listings per reader.
	if _, err := store.PutDocument(ctx, owner, "blog", []byte("b"), "text/plain", owner, nil, &public); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateFeed(ctx, owner, "drafts", "Drafts", "", false, core.VisibilityPrivate); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateCollection(ctx, owner, "menu", "Menu", core.VisibilityPublic); err != nil {
		t.Fatal(err)
	}
	docPaths := func(reader peer.ID) []string {
		page, _, err := store.ListDocuments(ctx, owner, reader, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, d := range page {
			out = append(out, d.Path)
		}
		return out
	}
	feedPaths := func(reader peer.ID) []string {
		feeds, err := store.ListFeeds(ctx, owner, reader)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, f := range feeds {
			out = append(out, f.Path)
		}
		return out
	}
	collPaths := func(reader peer.ID) []string {
		colls, err := store.ListCollections(ctx, owner, reader)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, c := range colls {
			out = append(out, c.Path)
		}
		return out
	}
	type view struct {
		reader             peer.ID
		docs, feeds, colls []string
	}
	for _, v := range []view{
		{owner, []string{"blog", "diary"}, []string{"drafts", "news"}, []string{"ledger", "menu"}},
		{friend, []string{"blog", "diary"}, []string{"news"}, []string{"ledger", "menu"}},
		{stranger, []string{"blog"}, nil, []string{"menu"}},
	} {
		if got := docPaths(v.reader); !equal(got, v.docs) {
			t.Errorf("documents for %s = %v, want %v", v.reader.String()[:8], got, v.docs)
		}
		if got := feedPaths(v.reader); !equal(got, v.feeds) {
			t.Errorf("feeds for %s = %v, want %v", v.reader.String()[:8], got, v.feeds)
		}
		if got := collPaths(v.reader); !equal(got, v.colls) {
			t.Errorf("collections for %s = %v, want %v", v.reader.String()[:8], got, v.colls)
		}
	}

	// Revoke, and the reader list goes with its resource.
	if err := store.RevokeStoreReader(ctx, storage.StoreFeed, feed.ID, friend); err != nil {
		t.Fatal(err)
	}
	if got := feedPaths(friend); len(got) != 0 {
		t.Errorf("feeds for the revoked friend = %v, want none", got)
	}
	if _, err := store.DeleteDocument(ctx, owner, "diary"); err != nil {
		t.Fatal(err)
	}
	if readers, _ := store.ListStoreReaders(ctx, storage.StoreDocument, doc.ID); len(readers) != 0 {
		t.Errorf("readers of a deleted document = %+v, want none (cascade)", readers)
	}
	if _, err := store.SetStoreVisibility(ctx, "mailbox", owner, "x", core.VisibilityPublic); err == nil {
		t.Error("unknown store kind accepted")
	}
	if _, err := store.SetStoreVisibility(ctx, storage.StoreFeed, owner, "news", core.Visibility(9)); err == nil {
		t.Error("invalid visibility accepted")
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
