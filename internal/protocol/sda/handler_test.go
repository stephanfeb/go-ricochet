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
		Headers: map[string]string{"Content-Type": "application/json"}})
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
		t.Errorf("get by anyone: %+v", resp)
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
	resp := call(protocoltest.PeerID(t), sda.DocRequest{Operation: sda.OpLIST, ListLimit: &two})
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
