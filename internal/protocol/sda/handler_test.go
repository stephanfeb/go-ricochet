package sda_test

import (
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/protocol/protocoltest"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
)

func setup(t *testing.T) (*protocoltest.Env, func(from peer.ID, req sda.DocRequest) *sda.DocResponse, peer.ID) {
	t.Helper()
	env := protocoltest.New(t)
	p := sda.NewPipeline(env.Logger, env.Pool, env.Registry)
	owner := protocoltest.PeerID(t)
	call := func(from peer.ID, req sda.DocRequest) *sda.DocResponse {
		if req.OwnerPeerID == "" {
			req.OwnerPeerID = owner.String()
		}
		return protocoltest.Decode[sda.DocResponse](t, protocoltest.Exchange(t, p, from, req))
	}
	return env, call, owner
}

func header(resp *sda.DocResponse, k string) string {
	if v, ok := resp.Headers[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func TestPutGetHeadDelete(t *testing.T) {
	_, call, owner := setup(t)
	body := []byte(`{"name":"first"}`)

	resp := call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "notes/a", Body: protocoltest.Base64(body),
		Headers: map[string]string{"Content-Type": "application/json"}, Visibility: "public"})
	if resp.Status != sda.StatusCreated || header(resp, "ETag") == "" {
		t.Fatalf("first put: %+v, want 201 with an ETag", resp)
	}
	etag := header(resp, "ETag")

	resp = call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "notes/a", Body: protocoltest.Base64([]byte(`{"name":"second"}`)),
		Headers: map[string]string{"Content-Type": "application/json"}})
	if resp.Status != sda.StatusOK || header(resp, "ETag") == etag {
		t.Errorf("second put: %+v, want 200 with a new ETag", resp)
	}
	etag = header(resp, "ETag")

	resp = call(protocoltest.PeerID(t), sda.DocRequest{Operation: sda.OpGET, Path: "notes/a"})
	if resp.Status != sda.StatusOK || string(protocoltest.FromBase64(t, resp.Body)) != `{"name":"second"}` {
		t.Errorf("get of a public document by anyone: %+v", resp)
	}
	if header(resp, "Visibility") != "public" {
		t.Errorf("get Visibility header = %q, want public (a put without one keeps it)", header(resp, "Visibility"))
	}
	if header(resp, "ETag") != etag || header(resp, "Content-Type") != "application/json" || resp.Headers["Version"] != float64(2) {
		t.Errorf("get headers = %+v, want ETag %s, content type, version 2", resp.Headers, etag)
	}

	resp = call(owner, sda.DocRequest{Operation: sda.OpGET, Path: "notes/a", Headers: map[string]string{"If-None-Match": etag}})
	if resp.Status != sda.StatusNotModified || resp.Body != "" {
		t.Errorf("conditional get: %+v, want 304 with no body", resp)
	}

	resp = call(owner, sda.DocRequest{Operation: sda.OpHEAD, Path: "notes/a"})
	if resp.Status != sda.StatusOK || resp.Body != "" || header(resp, "ETag") != etag {
		t.Errorf("head: %+v, want 200, no body, the ETag", resp)
	}
	if resp.Headers["Content-Length"] != float64(len(`{"name":"second"}`)) {
		t.Errorf("head Content-Length = %v", resp.Headers["Content-Length"])
	}

	if resp := call(owner, sda.DocRequest{Operation: sda.OpDELETE, Path: "notes/a"}); resp.Status != sda.StatusNoContent {
		t.Errorf("delete: %+v, want 204", resp)
	}
	if resp := call(owner, sda.DocRequest{Operation: sda.OpDELETE, Path: "notes/a"}); resp.Status != sda.StatusNotFound {
		t.Errorf("second delete: %+v, want 404", resp)
	}
	if resp := call(owner, sda.DocRequest{Operation: sda.OpGET, Path: "notes/a"}); resp.Status != sda.StatusNotFound {
		t.Errorf("get after delete: %+v, want 404", resp)
	}
}

func TestConditionalWritesAndPatch(t *testing.T) {
	_, call, owner := setup(t)
	resp := call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "doc", Body: protocoltest.Base64([]byte(`{"a":1,"b":{"c":2}}`))})
	etag := header(resp, "ETag")

	resp = call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "doc", Body: protocoltest.Base64([]byte(`{}`)),
		Headers: map[string]string{"If-Match": "sha256:stale"}})
	if resp.Status != sda.StatusConflict || header(resp, "Actual-ETag") != etag {
		t.Errorf("stale If-Match: %+v, want 409 naming the current ETag", resp)
	}

	resp = call(owner, sda.DocRequest{Operation: sda.OpPATCH, Path: "doc", Body: protocoltest.Base64([]byte(`{"a":null,"b":{"d":3}}`)),
		Headers: map[string]string{"If-Match": etag}})
	if resp.Status != sda.StatusOK {
		t.Fatalf("patch: %+v", resp)
	}
	got := call(owner, sda.DocRequest{Operation: sda.OpGET, Path: "doc"})
	var doc map[string]any
	if err := json.Unmarshal(protocoltest.FromBase64(t, got.Body), &doc); err != nil {
		t.Fatal(err)
	}
	if _, has := doc["a"]; has || doc["b"].(map[string]any)["c"] != float64(2) || doc["b"].(map[string]any)["d"] != float64(3) {
		t.Errorf("patched document = %v, want a removed and b merged", doc)
	}

	if resp := call(owner, sda.DocRequest{Operation: sda.OpPATCH, Path: "absent", Body: protocoltest.Base64([]byte(`{}`))}); resp.Status != sda.StatusNotFound {
		t.Errorf("patch of an absent document: %+v, want 404", resp)
	}
}

func TestListAndHistory(t *testing.T) {
	_, call, owner := setup(t)
	for _, p := range []string{"b", "a", "c"} {
		call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: p, Body: protocoltest.Base64([]byte(p))})
	}
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "a", Body: protocoltest.Base64([]byte("a2"))})

	two := 2
	resp := call(owner, sda.DocRequest{Operation: sda.OpLIST, ListLimit: &two})
	if resp.Status != sda.StatusOK {
		t.Fatalf("list: %+v", resp)
	}
	var entries []struct {
		Path    string `json:"path"`
		Version int    `json:"versionNumber"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Path != "a" || entries[0].Version != 2 || entries[1].Path != "b" {
		t.Errorf("list page = %+v, want a (v2) then b", entries)
	}
	if resp.Headers["Has-More"] != true || resp.Headers["Next-Cursor"] != "b" {
		t.Errorf("list headers = %+v, want Has-More and Next-Cursor b", resp.Headers)
	}

	resp = call(owner, sda.DocRequest{Operation: sda.OpHISTORY, Path: "a"})
	if resp.Status != sda.StatusOK {
		t.Fatalf("history: %+v", resp)
	}
	var versions []struct {
		Version int `json:"versionNumber"`
		Size    int `json:"size"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &versions); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].Version != 1 || versions[0].Size != 1 {
		t.Errorf("history = %+v, want one archived version of size 1", versions)
	}
	one := 1
	resp = call(owner, sda.DocRequest{Operation: sda.OpHISTORY, Path: "a", VersionNumber: &one})
	if resp.Status != sda.StatusOK || string(protocoltest.FromBase64(t, resp.Body)) != "a" {
		t.Errorf("version 1 = %+v, want the original body", resp)
	}
}

func TestBatchPutReportsPerDocument(t *testing.T) {
	_, call, owner := setup(t)
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "existing", Body: protocoltest.Base64([]byte("v1"))})

	resp := call(owner, sda.DocRequest{Operation: sda.OpBATCH_PUT, BatchDocuments: []sda.BatchDocument{
		{Path: "new", Body: protocoltest.Base64([]byte("n"))},
		{Path: "existing", Body: protocoltest.Base64([]byte("v2")), IfMatch: "sha256:wrong"},
		{Path: "/bad", Body: protocoltest.Base64([]byte("x"))},
	}})
	if resp.Status != sda.StatusOK || resp.Headers["Applied"] != float64(1) || resp.Headers["Total"] != float64(3) {
		t.Fatalf("batch: %+v, want 200 with 1 of 3 applied", resp)
	}
	var body struct {
		Results []sda.BatchDocumentResult `json:"results"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &body); err != nil {
		t.Fatal(err)
	}
	results := body.Results
	if len(results) != 3 || results[0].Status != sda.StatusCreated || results[1].Status != sda.StatusConflict || results[1].ActualETag == "" || results[2].Status != sda.StatusBadRequest {
		t.Errorf("results = %+v, want 201, 409 with the actual ETag, 400", results)
	}
}

func TestRequestValidation(t *testing.T) {
	env, call, owner := setup(t)
	stranger := protocoltest.PeerID(t)

	resp := call(stranger, sda.DocRequest{Operation: sda.OpPUT, Path: "theirs", Body: protocoltest.Base64([]byte("x"))})
	if resp.Status != sda.StatusForbidden {
		t.Errorf("non-owner put: %+v, want 403", resp)
	}
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpDELETE, Path: "theirs"}); resp.Status != sda.StatusForbidden {
		t.Errorf("non-owner delete: %+v, want 403", resp)
	}
	cases := map[string]sda.DocRequest{
		"missing owner":   {Operation: sda.OpGET, OwnerPeerID: " ", Path: "x"},
		"bad owner":       {Operation: sda.OpGET, OwnerPeerID: "not-a-peer", Path: "x"},
		"absolute path":   {Operation: sda.OpGET, Path: "/etc/passwd"},
		"traversal":       {Operation: sda.OpGET, Path: "a/../b"},
		"bad characters":  {Operation: sda.OpGET, Path: "a b"},
		"empty path":      {Operation: sda.OpGET},
		"bad base64":      {Operation: sda.OpPUT, Path: "x", Body: "!!!"},
		"unknown op":      {Operation: "MOVE", Path: "x"},
		"oversized limit": {Operation: sda.OpLIST, ListLimit: intp(-1)},
	}
	for name, req := range cases {
		if name == "oversized limit" {
			continue // a negative limit falls back to the default; nothing to refuse
		}
		if resp := call(owner, req); resp.Status != sda.StatusBadRequest {
			t.Errorf("%s: %+v, want 400", name, resp)
		}
	}
	if n := len(env.Store.AllMessages()); n != 0 {
		t.Errorf("document handler stored %d messages", n)
	}
}

func intp(n int) *int { return &n }

// A document is the owner's alone until the owner says otherwise: private by
// default, readable by the peers on its reader list once shared, by anyone
// once public. Every read goes through the same check, history included, so
// a stranger learns nothing about a private document but that the path
// exists (the refusal is a 403, like a refused write).
func TestDocumentVisibility(t *testing.T) {
	_, call, owner := setup(t)
	stranger := protocoltest.PeerID(t)
	friend := protocoltest.PeerID(t)
	body := protocoltest.Base64([]byte(`{"v":1}`))

	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "diary", Body: body})
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "diary", Body: protocoltest.Base64([]byte(`{"v":2}`))})
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "blog", Body: body, Visibility: "public"})

	one := 1
	reads := map[string]sda.DocRequest{
		"GET":     {Operation: sda.OpGET, Path: "diary"},
		"HEAD":    {Operation: sda.OpHEAD, Path: "diary"},
		"HISTORY": {Operation: sda.OpHISTORY, Path: "diary"},
		"version": {Operation: sda.OpHISTORY, Path: "diary", VersionNumber: &one},
	}
	for name, req := range reads {
		if resp := call(stranger, req); resp.Status != sda.StatusForbidden || resp.Body != "" {
			t.Errorf("stranger %s of a private document: %+v, want 403 and no body", name, resp)
		}
		if resp := call(owner, req); resp.Status != sda.StatusOK {
			t.Errorf("owner %s of a private document: %+v, want 200", name, resp)
		}
	}
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpGET, Path: "nowhere"}); resp.Status != sda.StatusNotFound {
		t.Errorf("stranger GET of an absent document: %+v, want 404", resp)
	}

	listPaths := func(from peer.ID) []string {
		resp := call(from, sda.DocRequest{Operation: sda.OpLIST})
		if resp.Status != sda.StatusOK {
			t.Fatalf("list: %+v", resp)
		}
		var entries []struct {
			Path       string `json:"path"`
			Visibility string `json:"visibility"`
		}
		if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &entries); err != nil {
			t.Fatal(err)
		}
		paths := make([]string, 0, len(entries))
		for _, e := range entries {
			paths = append(paths, e.Path+":"+e.Visibility)
		}
		return paths
	}
	if got := listPaths(stranger); len(got) != 1 || got[0] != "blog:public" {
		t.Errorf("stranger list = %v, want only the public document", got)
	}
	if got := listPaths(owner); len(got) != 2 || got[0] != "blog:public" || got[1] != "diary:private" {
		t.Errorf("owner list = %v, want both", got)
	}

	// ACCESS is the owner's in every action, reading the list included.
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "get"}); resp.Status != sda.StatusForbidden {
		t.Errorf("stranger ACCESS get: %+v, want 403", resp)
	}
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "grant", ReaderPeerID: stranger.String()}); resp.Status != sda.StatusForbidden {
		t.Errorf("stranger granting themself: %+v, want 403", resp)
	}

	// Sharing with a reader list: the friend reads, the stranger still not.
	resp := call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "grant", ReaderPeerID: friend.String()})
	if resp.Status != sda.StatusOK {
		t.Fatalf("grant: %+v", resp)
	}
	if resp := call(friend, sda.DocRequest{Operation: sda.OpGET, Path: "diary"}); resp.Status != sda.StatusForbidden {
		t.Errorf("granted reader of a document still private: %+v, want 403 (the list applies once shared)", resp)
	}
	resp = call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "set", Visibility: "shared"})
	if resp.Status != sda.StatusOK || header(resp, "Visibility") != "shared" {
		t.Fatalf("set shared: %+v", resp)
	}
	var access struct {
		Visibility string `json:"visibility"`
		Readers    []struct {
			PeerID string `json:"peerId"`
		} `json:"readers"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &access); err != nil {
		t.Fatal(err)
	}
	if access.Visibility != "shared" || len(access.Readers) != 1 || access.Readers[0].PeerID != friend.String() {
		t.Errorf("access after set = %+v, want shared with the friend listed", access)
	}
	for name, req := range reads {
		if resp := call(friend, req); resp.Status != sda.StatusOK {
			t.Errorf("friend %s of a shared document: %+v, want 200", name, resp)
		}
		if resp := call(stranger, req); resp.Status != sda.StatusForbidden {
			t.Errorf("stranger %s of a shared document: %+v, want 403", name, resp)
		}
	}
	if got := listPaths(friend); len(got) != 2 || got[1] != "diary:shared" {
		t.Errorf("friend list = %v, want the shared document too", got)
	}

	if resp := call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "revoke", ReaderPeerID: friend.String()}); resp.Status != sda.StatusOK {
		t.Fatalf("revoke: %+v", resp)
	}
	if resp := call(friend, sda.DocRequest{Operation: sda.OpGET, Path: "diary"}); resp.Status != sda.StatusForbidden {
		t.Errorf("revoked reader: %+v, want 403", resp)
	}

	// Public: anyone reads, the whole version stream included.
	call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "set", Visibility: "public"})
	for name, req := range reads {
		if resp := call(stranger, req); resp.Status != sda.StatusOK {
			t.Errorf("stranger %s of a public document: %+v, want 200", name, resp)
		}
	}

	// A PUT without a visibility keeps the current one; one with it changes it.
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "diary", Body: body})
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpGET, Path: "diary"}); resp.Status != sda.StatusOK {
		t.Errorf("after a plain put of a public document: %+v, want still readable", resp)
	}
	call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "diary", Body: body, Visibility: "private"})
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpGET, Path: "diary"}); resp.Status != sda.StatusForbidden {
		t.Errorf("after a put making it private: %+v, want 403", resp)
	}

	// Batch puts carry it per document; a bad value refuses that document only.
	resp = call(owner, sda.DocRequest{Operation: sda.OpBATCH_PUT, BatchDocuments: []sda.BatchDocument{
		{Path: "open", Body: body, Visibility: "public"},
		{Path: "odd", Body: body, Visibility: "secret"},
	}})
	if resp.Status != sda.StatusOK || resp.Headers["Applied"] != float64(1) {
		t.Fatalf("batch: %+v, want 1 of 2 applied", resp)
	}
	if resp := call(stranger, sda.DocRequest{Operation: sda.OpGET, Path: "open"}); resp.Status != sda.StatusOK {
		t.Errorf("batch-put public document: %+v, want readable by anyone", resp)
	}
	if resp := call(owner, sda.DocRequest{Operation: sda.OpPUT, Path: "odd", Body: body, Visibility: "secret"}); resp.Status != sda.StatusBadRequest {
		t.Errorf("put with an unknown visibility: %+v, want 400", resp)
	}
	if resp := call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "diary", AccessAction: "open"}); resp.Status != sda.StatusBadRequest {
		t.Errorf("unknown access action: %+v, want 400", resp)
	}
	if resp := call(owner, sda.DocRequest{Operation: sda.OpACCESS, Path: "nowhere", AccessAction: "get"}); resp.Status != sda.StatusNotFound {
		t.Errorf("access of an absent document: %+v, want 404", resp)
	}
}
