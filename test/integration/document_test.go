package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

func TestPutAndGetDocument(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	content := []byte(`{"name":"test","value":42}`)

	putResp, err := cl.PutDocument(ctx, ownerID, "docs/config",
		content, client.WithContentType("application/json"))
	if err != nil {
		t.Fatalf("put document: %v", err)
	}
	if putResp.ETag == "" {
		t.Fatal("expected ETag in put response")
	}

	getResp, err := cl.GetDocument(ctx, ownerID, "docs/config")
	if err != nil {
		t.Fatalf("get document: %v", err)
	}
	if !bytes.Equal(getResp.Content, content) {
		t.Errorf("content mismatch: got %q, want %q", getResp.Content, content)
	}
	if getResp.ETag != putResp.ETag {
		t.Errorf("ETag mismatch: get=%s, put=%s", getResp.ETag, putResp.ETag)
	}
}

func TestConditionalGet(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	content := []byte(`{"cached":true}`)

	putResp, err := cl.PutDocument(ctx, ownerID, "docs/cached", content)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// GET with matching ETag — should return 304 (no content).
	getResp, err := cl.GetDocument(ctx, ownerID, "docs/cached",
		client.WithIfNoneMatch(putResp.ETag))
	if err != nil {
		t.Fatalf("conditional get: %v", err)
	}
	if getResp.Status != 304 {
		t.Errorf("expected status 304, got %d", getResp.Status)
	}
}

func TestConditionalPut(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	_, err := cl.PutDocument(ctx, ownerID, "docs/versioned", []byte(`{"v":1}`))
	if err != nil {
		t.Fatalf("initial put: %v", err)
	}

	// PUT with stale ETag — should return 409 conflict.
	_, err = cl.PutDocument(ctx, ownerID, "docs/versioned", []byte(`{"v":2}`),
		client.WithIfMatch("stale-etag"))
	if err == nil {
		t.Fatal("expected error for conditional put with stale ETag")
	}
}

func TestPatchDocument(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	initial := []byte(`{"name":"original","count":1}`)

	_, err := cl.PutDocument(ctx, ownerID, "docs/patchable", initial,
		client.WithContentType("application/json"))
	if err != nil {
		t.Fatalf("initial put: %v", err)
	}

	patch := map[string]any{"count": 2, "extra": "field"}
	_, err = cl.PatchDocument(ctx, ownerID, "docs/patchable", patch)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	// Verify the merge.
	getResp, err := cl.GetDocument(ctx, ownerID, "docs/patchable")
	if err != nil {
		t.Fatalf("get after patch: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(getResp.Content, &result); err != nil {
		t.Fatalf("unmarshal patched doc: %v", err)
	}
	if result["name"] != "original" {
		t.Errorf("expected name=original, got %v", result["name"])
	}
	if result["extra"] != "field" {
		t.Errorf("expected extra=field, got %v", result["extra"])
	}
}

func TestHeadDocument(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	content := []byte(`{"head":"test"}`)

	_, err := cl.PutDocument(ctx, ownerID, "docs/head-test", content,
		client.WithContentType("application/json"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	meta, err := cl.HeadDocument(ctx, ownerID, "docs/head-test")
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if meta.ETag == "" {
		t.Error("expected non-empty ETag from HEAD")
	}
	if meta.ContentType != "application/json" {
		t.Errorf("content type: got %s, want application/json", meta.ContentType)
	}
	// N20: the client read HEAD's RFC 3339 Last-Modified as 0.
	if since := time.Since(time.UnixMilli(meta.LastModified)); since < 0 || since > time.Minute {
		t.Errorf("last modified: got %d, want the put's time in Unix milliseconds", meta.LastModified)
	}
}

func TestDeleteDocument(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	_, err := cl.PutDocument(ctx, ownerID, "docs/to-delete", []byte("delete me"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	deleted, err := cl.DeleteDocument(ctx, ownerID, "docs/to-delete")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Error("expected document to be deleted")
	}

	// GET should return 404.
	getResp, err := cl.GetDocument(ctx, ownerID, "docs/to-delete")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if getResp.Status != 404 {
		t.Errorf("expected status 404 after delete, got %d", getResp.Status)
	}
}

func TestListDocuments(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	for _, name := range []string{"list/a", "list/b", "list/c"} {
		_, err := cl.PutDocument(ctx, ownerID, name, []byte("content-"+name))
		if err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	docs, err := cl.ListDocuments(ctx, ownerID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(docs) < 3 {
		t.Errorf("expected at least 3 documents, got %d", len(docs))
	}
}

func TestDocumentNotFound(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	getResp, err := cl.GetDocument(ctx, ownerID, "does-not-exist")
	if err != nil {
		t.Fatalf("get non-existent: %v", err)
	}
	if getResp.Status != 404 {
		t.Errorf("expected status 404, got %d", getResp.Status)
	}
}

// N19: history over the wire, from turning it on to restoring a version.
func TestDocumentHistoryRoundTrip(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	self := cl.PeerID()

	if _, err := cl.PutDocument(ctx, self, "notes/draft", []byte("first")); err != nil {
		t.Fatalf("put first: %v", err)
	}
	if _, err := cl.PutDocument(ctx, self, "notes/draft", []byte("second"), client.WithHistory(3)); err != nil {
		t.Fatalf("put second with history: %v", err)
	}
	if _, err := cl.PutDocument(ctx, self, "notes/draft", []byte("third")); err != nil {
		t.Fatalf("put third: %v", err)
	}

	meta, err := cl.HeadDocument(ctx, self, "notes/draft")
	if err != nil || meta.History != 3 || meta.Version != 3 {
		t.Fatalf("head = %+v, %v; want history 3, version 3", meta, err)
	}
	versions, err := cl.GetDocumentHistory(ctx, self, "notes/draft", 0)
	if err != nil || len(versions) != 2 || versions[0].VersionNumber != 2 || versions[1].VersionNumber != 1 {
		t.Fatalf("history = %+v, %v; want versions 2 and 1", versions, err)
	}
	if since := time.Since(time.UnixMilli(versions[0].CreatedAt)); since < 0 || since > time.Minute {
		t.Errorf("version createdAt = %d, want Unix ms of the write", versions[0].CreatedAt)
	}
	first, err := cl.GetDocumentVersion(ctx, self, "notes/draft", 1)
	if err != nil || string(first.Content) != "first" || first.VersionNumber != 1 {
		t.Fatalf("version 1 = %+v, %v", first, err)
	}

	restored, err := cl.RestoreDocumentVersion(ctx, "notes/draft", 1, client.WithIfMatch(meta.ETag))
	if err != nil || restored.Version != 4 {
		t.Fatalf("restore = %+v, %v; want version 4", restored, err)
	}
	doc, err := cl.GetDocument(ctx, self, "notes/draft")
	if err != nil || string(doc.Content) != "first" || doc.Version != 4 {
		t.Fatalf("after restore = %+v, %v; want the first body as version 4", doc, err)
	}
	if _, err := cl.RestoreDocumentVersion(ctx, "notes/draft", 1, client.WithIfMatch(meta.ETag)); !errors.Is(err, client.ErrConflict) {
		t.Errorf("restore with a stale ETag: %v, want ErrConflict", err)
	}
	if _, err := cl.RestoreDocumentVersion(ctx, "notes/draft", 99); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("restore of a version never kept: %v, want ErrNotFound", err)
	}
}
