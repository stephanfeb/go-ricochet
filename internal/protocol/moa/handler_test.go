package moa_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/moderation"
	"github.com/stephanfeb/go-ricochet/internal/protocol/maa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/moa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/protocoltest"
	"github.com/stephanfeb/go-ricochet/internal/protocol/sfa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// closed records the peers whose connections a ban closed.
type closed []peer.ID

func (c *closed) ClosePeer(id peer.ID) error { *c = append(*c, id); return nil }

type harness struct {
	env       *protocoltest.Env
	bans      *moderation.Bans
	closed    *closed
	moderator peer.ID
	call      func(from peer.ID, req moa.Request) *moa.Response
	feed      func(from peer.ID, req sfa.FeedRequest) *sfa.FeedResponse
}

func setup(t *testing.T) *harness {
	t.Helper()
	env := protocoltest.New(t)
	h := &harness{env: env, bans: moderation.NewBans(env.Store), closed: &closed{}, moderator: protocoltest.PeerID(t)}
	mods, err := moderation.ParseModerators([]string{h.moderator.String()})
	if err != nil {
		t.Fatal(err)
	}
	env.Registry.Provide(moderation.RegistryKey, h.bans)
	env.Registry.Provide(moderation.ModeratorsKey, mods)
	env.Registry.Provide(moa.DisconnectorKey, moa.Disconnector(h.closed))
	p := moa.NewPipeline(env.Logger, env.Pool, env.Registry)
	h.call = func(from peer.ID, req moa.Request) *moa.Response {
		return protocoltest.Decode[moa.Response](t, protocoltest.Exchange(t, p, from, req))
	}
	fp := sfa.NewPipeline(env.Logger, env.Pool, env.Registry)
	h.feed = func(from peer.ID, req sfa.FeedRequest) *sfa.FeedResponse {
		return protocoltest.Decode[sfa.FeedResponse](t, protocoltest.Exchange(t, fp, from, req))
	}
	return h
}

func (h *harness) entry(t *testing.T, owner peer.ID, path string, by peer.ID, json string) {
	t.Helper()
	ctx := context.Background()
	feed, _ := h.env.Store.GetFeed(ctx, owner, path)
	if feed == nil {
		var err error
		feed, err = h.env.Store.CreateFeed(ctx, owner, path, "", "", path != "microblog", core.VisibilityPublic)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.env.Store.AppendFeedEntry(ctx, feed.ID, []byte(json), by, "activity"); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyModeratorsMayModerate(t *testing.T) {
	h := setup(t)
	stranger := protocoltest.PeerID(t)
	resp := h.call(stranger, moa.Request{Operation: moa.OpBan, PeerID: protocoltest.PeerID(t).String()})
	if resp.Success || resp.Status != wire.StatusForbidden {
		t.Fatalf("ban by a stranger = %+v, want 403", resp)
	}
	if len(h.bans.List()) != 0 {
		t.Fatal("a stranger's ban was kept")
	}
}

func TestBanClosesConnectionsRemovesListingHidesContentAndUnbanRestores(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	mallory, reader := protocoltest.PeerID(t), protocoltest.PeerID(t)
	if err := h.env.Store.UpsertDirectoryEntry(ctx, &storage.DirectoryEntry{OwnerPeerID: mallory.String(), DisplayName: "Mallory"}); err != nil {
		t.Fatal(err)
	}
	h.entry(t, mallory, "microblog", mallory, `{"v":2,"id":"p1","type":"create"}`)

	resp := h.call(h.moderator, moa.Request{Operation: moa.OpBan, PeerID: mallory.String(), Reason: "  spam  "})
	if !resp.Success || resp.Ban == nil || resp.Ban.Reason != "spam" || resp.Ban.BannedBy != h.moderator.String() {
		t.Fatalf("ban = %+v", resp)
	}
	if !resp.ConnectionsClosed || len(*h.closed) != 1 || (*h.closed)[0] != mallory {
		t.Errorf("connections closed = %v, want mallory's", *h.closed)
	}
	if !resp.ListingRemoved {
		t.Error("listing not reported removed")
	}
	if e, _ := h.env.Store.GetDirectoryEntry(ctx, mallory.String()); e != nil {
		t.Errorf("listing still there: %+v", e)
	}
	if !h.bans.IsBanned(mallory) {
		t.Error("not banned in memory")
	}
	if kept, _ := h.env.Store.ListPeerBans(ctx); len(kept) != 1 {
		t.Errorf("bans kept = %d, want 1", len(kept))
	}

	// Their public feed is hidden from others, but not deleted.
	get := sfa.FeedRequest{Operation: sfa.OpGET, OwnerPeerID: mallory.String(), Path: "microblog", SequenceNumber: intp(1)}
	if r := h.feed(reader, get); r.Status != sfa.StatusForbidden {
		t.Errorf("reading a banned peer's post = %+v, want 403", r)
	}
	list := sfa.FeedRequest{Operation: sfa.OpLIST, OwnerPeerID: mallory.String()}
	if r := h.feed(reader, list); r.Status != sfa.StatusOK || strings.Contains(string(protocoltest.FromBase64(t, r.Body)), `"microblog"`) {
		t.Errorf("listing a banned peer's feeds = %+v, want an empty list", r)
	}

	got := h.call(h.moderator, moa.Request{Operation: moa.OpGetBan, PeerID: mallory.String()})
	if got.Ban == nil {
		t.Errorf("getBan = %+v", got)
	}
	if l := h.call(h.moderator, moa.Request{Operation: moa.OpListBans}); len(l.Bans) != 1 {
		t.Errorf("listBans = %+v", l)
	}

	un := h.call(h.moderator, moa.Request{Operation: moa.OpUnban, PeerID: mallory.String()})
	if !un.Success || !un.WasBanned || h.bans.IsBanned(mallory) {
		t.Fatalf("unban = %+v", un)
	}
	if r := h.feed(reader, get); r.Status != sfa.StatusOK {
		t.Errorf("reading after the unban = %+v, want 200", r)
	}
}

func intp(n int) *int { return &n }

func TestAModeratorCannotBeBanned(t *testing.T) {
	h := setup(t)
	resp := h.call(h.moderator, moa.Request{Operation: moa.OpBan, PeerID: h.moderator.String()})
	if resp.Success || resp.Status != wire.StatusBadRequest {
		t.Fatalf("self-ban = %+v, want 400", resp)
	}
}

func TestRemovePostTakesThePostItsEditsItsThreadAndImagesOnly(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	author, replier := protocoltest.PeerID(t), protocoltest.PeerID(t)

	h.entry(t, author, "microblog", author, `{"v":2,"id":"p1","type":"create","object":{"content":"bad"}}`)
	h.entry(t, author, "microblog", author, `{"v":2,"id":"p2","type":"create","object":{"content":"mentions p1 in text"}}`)
	h.entry(t, author, "microblog", author, `{"v":2,"id":"e1","type":"update","target":{"id":"p1","author":"x"}}`)
	h.entry(t, author, "microblog/threads/p1", replier, `{"v":2,"id":"r1","type":"create","target":{"id":"p1"}}`)
	h.entry(t, replier, "microblog", replier, `{"v":2,"id":"x1","type":"react","target":{"id":"p1"}}`)
	for _, path := range []string{"microblog/media/p1-img0", "microblog/media/p1-img1", "microblog/media/p2-img0"} {
		if _, err := h.env.Store.PutDocument(ctx, author, path, []byte{1}, "image/jpeg", author, nil, nil); err != nil {
			t.Fatal(err)
		}
	}

	resp := h.call(h.moderator, moa.Request{Operation: moa.OpRemovePost, PeerID: author.String(), PostID: "p1"})
	if !resp.Success || resp.Removal == nil {
		t.Fatalf("removePost = %+v", resp)
	}
	r := resp.Removal
	if r.Entries != 2 || !r.ThreadRemoved || r.Media != 2 {
		t.Errorf("removal = %+v, want the post and its edit, the thread, 2 images", r)
	}

	feed, _ := h.env.Store.GetFeed(ctx, author, "microblog")
	left, _, _ := h.env.Store.GetFeedEntries(ctx, feed.ID, nil, nil, "", 100)
	if len(left) != 1 || !strings.Contains(string(left[0].Content), `"id":"p2"`) {
		t.Errorf("author's feed left = %d entries, want only p2 (which only mentions p1)", len(left))
	}
	if f, _ := h.env.Store.GetFeed(ctx, author, "microblog/threads/p1"); f != nil {
		t.Error("thread still there")
	}
	theirs, _ := h.env.Store.GetFeed(ctx, replier, "microblog")
	if n, _ := h.env.Store.CountFeedEntries(ctx, theirs.ID); n != 1 {
		t.Errorf("someone else's reaction removed: %d left, want 1", n)
	}
	if d, _ := h.env.Store.GetDocument(ctx, author, "microblog/media/p2-img0"); d == nil {
		t.Error("another post's image removed")
	}

	again := h.call(h.moderator, moa.Request{Operation: moa.OpRemovePost, PeerID: author.String(), PostID: "p1"})
	if again.Success || again.Status != wire.StatusNotFound {
		t.Errorf("removing it twice = %+v, want 404", again)
	}
}

func TestRemovePostOfAReplyRemovesItFromTheThreadItIsIn(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	root, replier := protocoltest.PeerID(t), protocoltest.PeerID(t)
	h.entry(t, root, "microblog", root, `{"v":2,"id":"p1","type":"create"}`)
	h.entry(t, root, "microblog/threads/p1", replier, `{"v":2,"id":"r1","type":"create","target":{"id":"p1"}}`)
	h.entry(t, root, "microblog/threads/p1", root, `{"v":2,"id":"r2","type":"create","target":{"id":"p1"}}`)

	resp := h.call(h.moderator, moa.Request{Operation: moa.OpRemovePost, PeerID: replier.String(), PostID: "r1"})
	if !resp.Success || resp.Removal.Entries != 1 || resp.Removal.ThreadRemoved {
		t.Fatalf("removePost of a reply = %+v", resp)
	}
	thread, _ := h.env.Store.GetFeed(ctx, root, "microblog/threads/p1")
	if n, _ := h.env.Store.CountFeedEntries(ctx, thread.ID); n != 1 {
		t.Errorf("thread has %d entries, want the root author's reply only", n)
	}
	if n, _ := h.env.Store.CountFeedEntries(ctx, mustFeed(t, h, root, "microblog")); n != 1 {
		t.Errorf("the root post was touched")
	}
}

func mustFeed(t *testing.T, h *harness, owner peer.ID, path string) int64 {
	t.Helper()
	f, err := h.env.Store.GetFeed(context.Background(), owner, path)
	if err != nil || f == nil {
		t.Fatalf("feed %s: %v", path, err)
	}
	return f.ID
}

func TestRemovePostRefusesAPath(t *testing.T) {
	h := setup(t)
	resp := h.call(h.moderator, moa.Request{Operation: moa.OpRemovePost, PeerID: protocoltest.PeerID(t).String(), PostID: "../x%"})
	if resp.Success || resp.Status != wire.StatusBadRequest {
		t.Fatalf("removePost with a path = %+v, want 400", resp)
	}
}

func TestABannedPeerReadsOnlyItsTrustSafetyFolderAndIsToldItIsBanned(t *testing.T) {
	h := setup(t)
	mallory := protocoltest.PeerID(t)
	if _, err := h.bans.Ban(context.Background(), mallory, h.moderator, "spam", time.Now()); err != nil {
		t.Fatal(err)
	}
	maaPipe := maa.NewPipeline(h.env.Logger, h.env.Pool, h.env.Registry)
	ask := func(p *forge.Pipeline, req map[string]any) map[string]any {
		t.Helper()
		reply := protocoltest.Exchange(t, p, mallory, req)
		var out map[string]any
		if json.Unmarshal(reply, &out) != nil {
			return map[string]any{"compound": true} // a retrieve answers with a compound frame
		}
		return out
	}
	banned := func(what string, r map[string]any) {
		t.Helper()
		// Each protocol has its own envelope (errorMessage, error, headers.Error).
		if !strings.Contains(fmt.Sprint(r), wire.BannedMessage) {
			t.Errorf("%s = %v, want a 'banned' refusal", what, r)
		}
	}

	if r := ask(maaPipe, map[string]any{"operationType": "retrieve", "peerId": mallory.String(), "folderPath": wire.NoticesFolder}); r["compound"] != true {
		t.Errorf("reading its trust-safety folder = %v, want the messages", r)
	}
	if r := ask(maaPipe, map[string]any{"operationType": "markDelivered", "folderPath": wire.NoticesFolder, "messageIds": []string{"x"}}); r["success"] == false && strings.Contains(fmt.Sprint(r), "banned") {
		t.Errorf("acknowledging its notices was refused: %v", r)
	}
	banned("reading its inbox", ask(maaPipe, map[string]any{"operationType": "retrieve", "peerId": mallory.String(), "folderPath": "inbox"}))
	banned("reading someone else's folder", ask(maaPipe, map[string]any{"operationType": "retrieve", "peerId": h.moderator.String(), "folderPath": wire.NoticesFolder}))
	banned("expunging", ask(maaPipe, map[string]any{"operationType": "expunge", "folderPath": wire.NoticesFolder}))
	banned("reading a feed", ask(sfa.NewPipeline(h.env.Logger, h.env.Pool, h.env.Registry),
		map[string]any{"operation": sfa.OpLIST, "ownerPeerId": h.moderator.String()}))
}
