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

// A feed is a publication and so public by default; the owner can make it
// shared with a reader list or private. The check covers metadata, entries
// and pages, the listing, and each feed of a BATCH_GET on its own.
func TestFeedVisibility(t *testing.T) {
	call, owner := setup(t)
	stranger := protocoltest.PeerID(t)
	friend := protocoltest.PeerID(t)
	entry := protocoltest.Base64([]byte(`{"n":1}`))

	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "news", Title: "News"})
	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "drafts", Title: "Drafts", Visibility: "private"})
	call(owner, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "news", Body: entry})
	call(owner, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "drafts", Body: entry})
	if resp := call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "odd", Title: "Odd", Visibility: "secret"}); resp.Status != sfa.StatusBadRequest {
		t.Errorf("create with an unknown visibility: %+v, want 400", resp)
	}

	reads := map[string]sfa.FeedRequest{
		"metadata": {Operation: sfa.OpGET, Path: "drafts"},
		"entry":    {Operation: sfa.OpGET, Path: "drafts", SequenceNumber: intp(1)},
		"page":     {Operation: sfa.OpGET, Path: "drafts", FromSequence: intp(1)},
	}
	for name, req := range reads {
		if resp := call(stranger, req); resp.Status != sfa.StatusForbidden || resp.Body != "" {
			t.Errorf("stranger %s of a private feed: %+v, want 403 and no body", name, resp)
		}
		if resp := call(owner, req); resp.Status != sfa.StatusOK {
			t.Errorf("owner %s of a private feed: %+v, want 200", name, resp)
		}
	}
	resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpGET, Path: "news"})
	if resp.Status != sfa.StatusOK {
		t.Fatalf("stranger GET of a public feed: %+v, want 200", resp)
	}
	var meta map[string]any
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["visibility"] != "public" {
		t.Errorf("feed metadata = %v, want visibility public", meta)
	}

	listPaths := func(from peer.ID) []string {
		resp := call(from, sfa.FeedRequest{Operation: sfa.OpLIST})
		if resp.Status != sfa.StatusOK {
			t.Fatalf("list: %+v", resp)
		}
		var feeds []map[string]any
		if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &feeds); err != nil {
			t.Fatal(err)
		}
		paths := make([]string, 0, len(feeds))
		for _, f := range feeds {
			paths = append(paths, f["path"].(string)+":"+f["visibility"].(string))
		}
		return paths
	}
	if got := listPaths(stranger); len(got) != 1 || got[0] != "news:public" {
		t.Errorf("stranger list = %v, want only the public feed", got)
	}

	batch := func(from peer.ID) map[string]struct {
		Entries []any  `json:"entries"`
		Error   string `json:"error"`
	} {
		resp := call(from, sfa.FeedRequest{Operation: sfa.OpBATCH_GET, BatchQueries: []sfa.BatchQuery{
			{OwnerPeerID: owner.String(), Path: "news"},
			{OwnerPeerID: owner.String(), Path: "drafts"},
			{OwnerPeerID: owner.String(), Path: "nowhere"},
		}})
		if resp.Status != sfa.StatusOK {
			t.Fatalf("batch get: %+v", resp)
		}
		var body struct {
			Feeds map[string]struct {
				Entries []any  `json:"entries"`
				Error   string `json:"error"`
			} `json:"feeds"`
		}
		if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &body); err != nil {
			t.Fatal(err)
		}
		return body.Feeds
	}
	got := batch(stranger)
	if r := got[owner.String()+"/news"]; len(r.Entries) != 1 || r.Error != "" {
		t.Errorf("stranger batch, public feed = %+v, want its entry", r)
	}
	if r := got[owner.String()+"/drafts"]; len(r.Entries) != 0 || r.Error == "" {
		t.Errorf("stranger batch, private feed = %+v, want no entries and an error", r)
	}
	if r := got[owner.String()+"/nowhere"]; r.Error != "feed not found" {
		t.Errorf("stranger batch, absent feed = %+v, want feed not found", r)
	}
	if r := batch(owner)[owner.String()+"/drafts"]; len(r.Entries) != 1 {
		t.Errorf("owner batch, private feed = %+v, want its entry", r)
	}

	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpACCESS, Path: "drafts", AccessAction: "get"}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger ACCESS get: %+v, want 403", resp)
	}
	call(owner, sfa.FeedRequest{Operation: sfa.OpACCESS, Path: "drafts", AccessAction: "grant", ReaderPeerID: friend.String()})
	resp = call(owner, sfa.FeedRequest{Operation: sfa.OpACCESS, Path: "drafts", AccessAction: "set", Visibility: "shared"})
	if resp.Status != sfa.StatusOK || resp.Headers["Visibility"] != "shared" {
		t.Fatalf("set shared: %+v", resp)
	}
	for name, req := range reads {
		if resp := call(friend, req); resp.Status != sfa.StatusOK {
			t.Errorf("friend %s of a shared feed: %+v, want 200", name, resp)
		}
		if resp := call(stranger, req); resp.Status != sfa.StatusForbidden {
			t.Errorf("stranger %s of a shared feed: %+v, want 403", name, resp)
		}
	}
	if r := batch(friend)[owner.String()+"/drafts"]; len(r.Entries) != 1 {
		t.Errorf("friend batch, shared feed = %+v, want its entry", r)
	}
	call(owner, sfa.FeedRequest{Operation: sfa.OpACCESS, Path: "drafts", AccessAction: "revoke", ReaderPeerID: friend.String()})
	if resp := call(friend, sfa.FeedRequest{Operation: sfa.OpGET, Path: "drafts"}); resp.Status != sfa.StatusForbidden {
		t.Errorf("revoked reader: %+v, want 403", resp)
	}

	// Making a public feed private hides it, entries the stranger has already
	// seen included; a collaborative feed can still be private to read.
	call(owner, sfa.FeedRequest{Operation: sfa.OpCREATE, Path: "inbox", Title: "Inbox", Collaborative: true, Visibility: "private"})
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpAPPEND, Path: "inbox", Body: entry}); resp.Status != sfa.StatusCreated {
		t.Errorf("stranger append to a private collaborative feed: %+v, want 201", resp)
	}
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpGET, Path: "inbox", SequenceNumber: intp(1)}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger reading back the private collaborative feed: %+v, want 403", resp)
	}
	call(owner, sfa.FeedRequest{Operation: sfa.OpACCESS, Path: "news", AccessAction: "set", Visibility: "private"})
	if resp := call(stranger, sfa.FeedRequest{Operation: sfa.OpGET, Path: "news", SequenceNumber: intp(1)}); resp.Status != sfa.StatusForbidden {
		t.Errorf("stranger after the feed went private: %+v, want 403", resp)
	}
}
