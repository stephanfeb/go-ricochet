package sfa_test

import (
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/protocol/protocoltest"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
)

func setup(t *testing.T, configure ...func(*core.ServerConfig)) (func(from peer.ID, req sfa.FeedRequest) *sfa.FeedResponse, peer.ID) {
	t.Helper()
	env := protocoltest.New(t, configure...)
	p := sfa.NewPipeline(env.Logger, env.Pool, env.Registry)
	owner := protocoltest.PeerID(t)
	call := func(from peer.ID, req sfa.FeedRequest) *sfa.FeedResponse {
		if req.OwnerPeerID == "" {
			req.OwnerPeerID = owner.String()
		}
		return protocoltest.Decode[sfa.FeedResponse](t, protocoltest.Exchange(t, p, from, req))
	}
	return call, owner
}

func intp(n int) *int { return &n }

func TestCreateAppendGetAndList(t *testing.T) {
	call, owner := setup(t)

	resp := call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "news", Title: "News", Description: "what's new"})
	if resp.Status != sfa.StatusCreated {
		t.Fatalf("create: %+v", resp)
	}

	for i, body := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		resp := call(owner, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "news", Body: protocoltest.Base64([]byte(body)), EntryType: "post"})
		if resp.Status != sfa.StatusCreated || resp.Headers["X-Sequence"] != float64(i+1) || resp.Headers["ETag"] == "" {
			t.Fatalf("append %d: %+v, want 201 at sequence %d with an ETag", i, resp, i+1)
		}
	}

	resp = call(protocoltest.PeerID(t), sfa.FeedRequest{Operation: sfa.OpGET, Path: "news"})
	if resp.Status != sfa.StatusOK || resp.Headers["X-Sequence"] != float64(3) {
		t.Errorf("get feed by anyone: %+v, want 200 at sequence 3", resp)
	}
	var feed map[string]any
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &feed); err != nil {
		t.Fatal(err)
	}
	if feed["title"] != "News" {
		t.Errorf("feed body = %v", feed)
	}

	resp = call(owner, sfa.FeedRequest{Operation: sfa.OpGET, Path: "news", SequenceNumber: intp(2)})
	if resp.Status != sfa.StatusOK || string(protocoltest.FromBase64(t, resp.Body)) != `{"n":2}` || resp.Headers["X-Entry-Type"] != "post" {
		t.Errorf("get entry 2: %+v", resp)
	}
	if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpGET, Path: "news", SequenceNumber: intp(9)}); resp.Status != sfa.StatusNotFound {
		t.Errorf("get absent entry: %+v, want 404", resp)
	}

	resp = call(owner, sfa.FeedRequest{Operation: sfa.OpGET, Path: "news", FromSequence: intp(2), Limit: intp(1)})
	if resp.Status != sfa.StatusOK || resp.Headers["X-Has-More"] != true || resp.Headers["X-Next-Sequence"] != float64(3) {
		t.Errorf("page from 2 limit 1: %+v, want more and next sequence 3", resp)
	}
	var page struct {
		Entries []struct {
			Seq     int    `json:"seq"`
			Content string `json:"content"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Seq != 2 || string(protocoltest.FromBase64(t, page.Entries[0].Content)) != `{"n":2}` {
		t.Errorf("page = %+v, want entry 2 alone", page.Entries)
	}

	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "alerts", Title: "Alerts"})
	resp = call(protocoltest.PeerID(t), sfa.FeedRequest{Operation: sfa.OpLIST})
	if resp.Status != sfa.StatusOK {
		t.Fatalf("list: %+v", resp)
	}
	var feeds []map[string]any
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &feeds); err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 2 || feeds[0]["path"] != "alerts" || feeds[1]["path"] != "news" {
		t.Errorf("list = %v, want alerts then news", feeds)
	}

	if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpDELETE, Path: "news"}); resp.Status != sfa.StatusNoContent {
		t.Errorf("delete: %+v, want 204", resp)
	}
	if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpDELETE, Path: "news"}); resp.Status != sfa.StatusNotFound {
		t.Errorf("second delete: %+v, want 404", resp)
	}
}

// Only the owner writes to a feed, unless the owner created it as
// collaborative, in which case anyone may append but still not delete.
func TestCollaborativeFeeds(t *testing.T) {
	call, owner := setup(t)
	stranger := protocoltest.PeerID(t)

	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "mine", Title: "Mine"})
	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "ours", Title: "Ours", Collaborative: true})

	entry := protocoltest.Base64([]byte(`{"hi":true}`))
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "mine", Body: entry}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger append to a private feed: %+v, want 403", resp)
	}
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "ours", Body: entry}); resp.Status != sfa.StatusCreated {
		t.Errorf("stranger append to a collaborative feed: %+v, want 201", resp)
	}
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "nowhere", Body: entry}); resp.Status != sfa.StatusNotFound {
		t.Errorf("stranger append to an absent feed: %+v, want 404, not a feed planted under the owner", resp)
	}
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "planted", Title: "x"}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger create under the owner: %+v, want 403", resp)
	}
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpDELETE, Path: "ours"}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger delete of a collaborative feed: %+v, want 403", resp)
	}
	resp := call(owner, sfa.FeedRequest{Operation: sfa.OpGET, Path: "ours", SequenceNumber: intp(1)})
	if resp.Status != sfa.StatusOK || resp.Headers["X-Created-By"] != stranger.String() {
		t.Errorf("collaborative entry: %+v, want it attributed to the contributor", resp)
	}
}

func TestFeedEntryCapIsA507(t *testing.T) {
	call, owner := setup(t, func(cfg *core.ServerConfig) { cfg.MaxEntriesPerFeed = 2 })
	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "small", Title: "Small"})
	entry := protocoltest.Base64([]byte(`{}`))
	for i := 0; i < 2; i++ {
		if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "small", Body: entry}); resp.Status != sfa.StatusCreated {
			t.Fatalf("append %d: %+v", i, resp)
		}
	}
	resp := call(owner, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "small", Body: entry})
	if resp.Status != wire.StatusInsufficientStorage {
		t.Errorf("append past the cap: %+v, want 507", resp)
	}
}

func TestFeedRequestValidation(t *testing.T) {
	call, owner := setup(t)
	cases := map[string]sfa.FeedRequest{
		"bad owner":   {Operation: sfa.OpGET, OwnerPeerID: "nope", Path: "x"},
		"bad path":    {Operation: sfa.OpGET, Path: "/x"},
		"unknown op":  {Operation: "SUBSCRIBE", Path: "x"},
		"bad body":    {Operation: sfa.OpAPPEND, Path: "x", Body: "!!"},
		"title bound": {Operation: sfa.OpCREATE, Path: "x", Title: string(make([]byte, wire.MaxFeedTitleLength+1))},
	}
	for name, req := range cases {
		if name == "bad body" {
			call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "x", Title: "x"})
		}
		if resp := call(owner, req); resp.Status != sfa.StatusBadRequest {
			t.Errorf("%s: %+v, want 400", name, resp)
		}
	}
	if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpGET, Path: "absent"}); resp.Status != sfa.StatusNotFound {
		t.Errorf("get absent feed: %+v, want 404", resp)
	}
}
