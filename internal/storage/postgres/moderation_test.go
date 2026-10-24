package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

func newPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPeerBansAreKeptUpdatedAndLifted(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	who, mod := newPeer(t).String(), newPeer(t).String()
	t.Cleanup(func() { _, _ = s.DeletePeerBan(ctx, who) })

	at := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.PutPeerBan(ctx, &storage.PeerBan{PeerID: who, Reason: "spam", BannedBy: mod, BannedAt: at}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPeerBan(ctx, &storage.PeerBan{PeerID: who, Reason: "harassment", BannedBy: mod, BannedAt: at}); err != nil {
		t.Fatalf("banning again: %v", err)
	}
	bans, err := s.ListPeerBans(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found *storage.PeerBan
	for _, b := range bans {
		if b.PeerID == who {
			found = b
		}
	}
	if found == nil || found.Reason != "harassment" || found.BannedBy != mod || !found.BannedAt.Equal(at) {
		t.Fatalf("kept ban = %+v", found)
	}
	if ok, err := s.DeletePeerBan(ctx, who); err != nil || !ok {
		t.Fatalf("unban = %v, %v", ok, err)
	}
	if ok, _ := s.DeletePeerBan(ctx, who); ok {
		t.Error("unbanned twice")
	}
}

func TestFeedEntriesContainingFindsByBytesInTheRightFeedsOnly(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	author, other := newPeer(t), newPeer(t)
	feed := func(owner peer.ID, path string) *storage.FeedRecord {
		f, err := s.CreateFeed(ctx, owner, path, "", "", true, core.VisibilityPublic)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = s.DeleteFeed(ctx, owner, path) })
		return f
	}
	add := func(f *storage.FeedRecord, by peer.ID, content string) {
		if _, err := s.AppendFeedEntry(ctx, f.ID, []byte(content), by, "activity"); err != nil {
			t.Fatal(err)
		}
	}
	mine := feed(author, "microblog")
	thread := feed(other, "microblog/threads/root")
	similar := feed(author, "microblogx")
	add(mine, author, `{"id":"p_1%"}`)
	add(mine, author, `{"id":"p2"}`)
	add(thread, author, `{"id":"r","target":{"id":"p_1%"}}`)
	add(thread, other, `{"id":"p_1%"}`)
	add(similar, author, `{"id":"p_1%"}`)
	add(mine, author, string([]byte{0xff, 0xfe, 'p', '_', '1', '%'})) // not UTF-8

	got, err := s.FeedEntriesContaining(ctx, author, "microblog", []byte("p_1%"))
	if err != nil {
		t.Fatal(err)
	}
	// Mine (two, one not UTF-8) and my reply in another's thread; not the
	// other person's entry, not a feed that only starts the same.
	if len(got) != 3 {
		t.Fatalf("matches = %d, want 3: %+v", len(got), got)
	}
	ids := []int64{got[0].ID, got[1].ID}
	if n, err := s.DeleteFeedEntries(ctx, ids); err != nil || n != 2 {
		t.Fatalf("delete = %d, %v", n, err)
	}
	if got[0].FeedID != mine.ID || got[1].FeedID != mine.ID {
		t.Fatalf("first matches not in my feed: %+v", got[:2])
	}
	if n, _ := s.CountFeedEntries(ctx, mine.ID); n != 1 {
		t.Errorf("entries left in mine = %d, want 1 (p2)", n)
	}
}

func TestDeleteDocumentsWithPrefixTakesOnlyThatPrefix(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	owner, other := newPeer(t), newPeer(t)
	for _, d := range []struct {
		owner peer.ID
		path  string
	}{{owner, "microblog/media/p_1-img0"}, {owner, "microblog/media/p_1-img1"}, {owner, "microblog/media/pX1-img0"}, {other, "microblog/media/p_1-img0"}} {
		if _, err := s.PutDocument(ctx, d.owner, d.path, []byte{1}, "image/jpeg", d.owner, nil, storage.DocumentWriteOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = s.DeleteDocument(ctx, d.owner, d.path) })
	}
	n, err := s.DeleteDocumentsWithPrefix(ctx, owner, "microblog/media/p_1-img")
	if err != nil || n != 2 {
		t.Fatalf("deleted = %d, %v; want 2 (an underscore is not a wildcard)", n, err)
	}
	if d, _ := s.GetDocument(ctx, other, "microblog/media/p_1-img0"); d == nil {
		t.Error("another owner's document went")
	}
}
