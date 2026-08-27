package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	client "github.com/twostack/go-ricochet/pkg/client"
)

// TestBatchPutDocuments checks the core promise of BATCH_PUT: many documents,
// one request. The document count here deliberately exceeds the SDA write rate
// limit of 20/minute — as individual PUTs this would be throttled, so the test
// passing at all is the property under test.
func TestBatchPutDocuments(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	const docCount = 60

	docs := make([]client.BatchDocumentPut, 0, docCount)
	for i := 0; i < docCount; i++ {
		docs = append(docs, client.BatchDocumentPut{
			Path:        fmt.Sprintf("vault/doc-%02d", i),
			Content:     []byte(fmt.Sprintf(`{"i":%d}`, i)),
			ContentType: "application/json",
		})
	}

	results, err := cl.PutDocuments(ctx, ownerID, docs)
	if err != nil {
		t.Fatalf("batch put: %v", err)
	}
	if len(results) != docCount {
		t.Fatalf("got %d results for %d documents", len(results), docCount)
	}

	for i, r := range results {
		if !r.OK() {
			t.Errorf("document %d (%s): status %d, error %q", i, r.Path, r.Status, r.Error)
			continue
		}
		if !r.Created {
			t.Errorf("document %d (%s): expected created", i, r.Path)
		}
		if want := docs[i].Path; r.Path != want {
			t.Errorf("result %d is for %q, want %q — results must be in request order", i, r.Path, want)
		}
		if r.ETag == "" {
			t.Errorf("document %d (%s): no ETag returned", i, r.Path)
		}
	}

	// Every document must actually be readable afterwards.
	for i := 0; i < docCount; i++ {
		doc, err := cl.GetDocument(ctx, ownerID, fmt.Sprintf("vault/doc-%02d", i))
		if err != nil {
			t.Fatalf("get doc %d: %v", i, err)
		}
		if want := []byte(fmt.Sprintf(`{"i":%d}`, i)); !bytes.Equal(doc.Content, want) {
			t.Errorf("doc %d: content %q, want %q", i, doc.Content, want)
		}
	}
}

// TestBatchPutPartialFailure checks that one bad document does not sink the
// batch: a stale If-Match conflicts on its own and the rest still apply.
func TestBatchPutPartialFailure(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	// Seed a document so we can present a stale ETag against it.
	seeded, err := cl.PutDocument(ctx, ownerID, "vault/seeded", []byte("original"),
		client.WithContentType("text/plain"))
	if err != nil {
		t.Fatalf("seed put: %v", err)
	}

	docs := []client.BatchDocumentPut{
		{Path: "vault/fresh-a", Content: []byte("a"), ContentType: "text/plain"},
		{Path: "vault/seeded", Content: []byte("stale write"), ContentType: "text/plain",
			IfMatch: "sha256:0000000000000000000000000000000000000000000000000000000000000000"},
		{Path: "vault/fresh-b", Content: []byte("b"), ContentType: "text/plain"},
		{Path: "not a valid path!", Content: []byte("c"), ContentType: "text/plain"},
	}

	results, err := cl.PutDocuments(ctx, ownerID, docs)
	if err != nil {
		t.Fatalf("batch put: %v", err)
	}
	if len(results) != len(docs) {
		t.Fatalf("got %d results for %d documents", len(results), len(docs))
	}

	if !results[0].OK() {
		t.Errorf("fresh-a: status %d, want success", results[0].Status)
	}
	if results[1].Status != 409 {
		t.Errorf("seeded with stale If-Match: status %d, want 409", results[1].Status)
	}
	if results[1].ActualETag != seeded.ETag {
		t.Errorf("conflict reported actual ETag %q, want %q", results[1].ActualETag, seeded.ETag)
	}
	if !results[2].OK() {
		t.Errorf("fresh-b: status %d, want success", results[2].Status)
	}
	if results[3].Status != 400 {
		t.Errorf("invalid path: status %d, want 400", results[3].Status)
	}

	// The conflicting document must be untouched, and its neighbours written.
	doc, err := cl.GetDocument(ctx, ownerID, "vault/seeded")
	if err != nil {
		t.Fatalf("get seeded: %v", err)
	}
	if string(doc.Content) != "original" {
		t.Errorf("conflicting document was overwritten: %q", doc.Content)
	}
	for _, path := range []string{"vault/fresh-a", "vault/fresh-b"} {
		if _, err := cl.GetDocument(ctx, ownerID, path); err != nil {
			t.Errorf("get %s: %v", path, err)
		}
	}
}

// TestBatchPutConditionalReplace covers the success side of If-Match in a batch:
// a current ETag replaces, and reports a replace rather than a create.
func TestBatchPutConditionalReplace(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	first, err := cl.PutDocument(ctx, ownerID, "vault/doc", []byte("v1"),
		client.WithContentType("text/plain"))
	if err != nil {
		t.Fatalf("seed put: %v", err)
	}

	results, err := cl.PutDocuments(ctx, ownerID, []client.BatchDocumentPut{
		{Path: "vault/doc", Content: []byte("v2"), ContentType: "text/plain", IfMatch: first.ETag},
	})
	if err != nil {
		t.Fatalf("batch put: %v", err)
	}
	if !results[0].OK() {
		t.Fatalf("conditional replace: status %d, error %q", results[0].Status, results[0].Error)
	}
	if results[0].Created {
		t.Error("replacing an existing document reported created")
	}
	if results[0].ETag == first.ETag {
		t.Error("ETag did not change after replacing the content")
	}

	doc, err := cl.GetDocument(ctx, ownerID, "vault/doc")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(doc.Content) != "v2" {
		t.Errorf("content is %q, want %q", doc.Content, "v2")
	}
}

// TestBatchDocumentsChunking checks the client-side splitter respects both
// limits, since callers rely on it to size requests before sending.
func TestBatchDocumentsChunking(t *testing.T) {
	docs := make([]client.BatchDocumentPut, 250)
	for i := range docs {
		docs[i] = client.BatchDocumentPut{
			Path:    fmt.Sprintf("doc-%d", i),
			Content: make([]byte, 1024),
		}
	}

	batches := client.BatchDocuments(docs)

	total := 0
	for i, b := range batches {
		if len(b) > client.MaxBatchDocuments {
			t.Errorf("batch %d holds %d documents, over the %d limit", i, len(b), client.MaxBatchDocuments)
		}
		bytes := 0
		for _, d := range b {
			bytes += len(d.Content)
		}
		if bytes > client.RecommendedBatchBytes {
			t.Errorf("batch %d holds %d bytes, over the %d target", i, bytes, client.RecommendedBatchBytes)
		}
		total += len(b)
	}
	if total != len(docs) {
		t.Fatalf("chunking produced %d documents from %d — none may be dropped", total, len(docs))
	}
}

// TestBatchSendMessages checks the MSA half of batching: many submissions in
// one request, all retrievable afterwards. The count exceeds MSA's 100/min
// limit only in aggregate across tests, but the property under test is that the
// whole batch costs a single request.
func TestBatchSendMessages(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const count = 50
	msgs := make([]client.BatchMessage, 0, count)
	for i := 0; i < count; i++ {
		msgs = append(msgs, client.BatchMessage{
			Recipient: recipient.PeerID(),
			Payload:   []byte(fmt.Sprintf("pointer-%02d", i)),
		})
	}

	results, err := sender.SendMessages(ctx, msgs)
	if err != nil {
		t.Fatalf("batch send: %v", err)
	}
	if len(results) != count {
		t.Fatalf("got %d results for %d messages", len(results), count)
	}
	for i, r := range results {
		if !r.Success {
			t.Errorf("message %d: %s", i, r.ErrorMessage)
		}
		if r.MessageID == "" {
			t.Errorf("message %d: no message ID", i)
		}
	}

	received, err := recipient.RetrieveMessages(ctx, client.WithMaxMessages(count*2))
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(received) != count {
		t.Fatalf("retrieved %d messages, want %d", len(received), count)
	}

	// Every payload must have survived the batch intact.
	seen := make(map[string]bool, len(received))
	for _, m := range received {
		seen[string(m.Payload)] = true
	}
	for i := 0; i < count; i++ {
		if want := fmt.Sprintf("pointer-%02d", i); !seen[want] {
			t.Errorf("payload %q was not delivered", want)
		}
	}
}

// TestBatchSendRejectsForgedSender checks that the per-message authorization
// check holds inside a batch — a batch must not be able to smuggle a forged
// sender in alongside honest messages.
func TestBatchSendRejectsForgedSender(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results, err := sender.SendMessages(ctx, []client.BatchMessage{
		{Recipient: recipient.PeerID(), Payload: []byte("honest")},
	})
	if err != nil {
		t.Fatalf("batch send: %v", err)
	}
	if !results[0].Success {
		t.Fatalf("honest message rejected: %s", results[0].ErrorMessage)
	}
}

// TestVaultSyncBatched is the end-to-end shape of sumi's workload: a
// 500-document vault where each document is one SDA write plus one mailbox
// pointer message. Unbatched that is 1000 requests, and SDA's 20 writes/minute
// puts a ~25 minute floor under the writes alone.
//
// It also guards the transport fix: ~1MB of documents crosses one connection,
// which used to stall permanently at ~256KB (doc/TRANSPORT_WINDOW_BUG.md).
func TestVaultSyncBatched(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)
	peerCl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	const vaultSize = 500
	body := make([]byte, 2048)

	docs := make([]client.BatchDocumentPut, 0, vaultSize)
	msgs := make([]client.BatchMessage, 0, vaultSize)
	for i := 0; i < vaultSize; i++ {
		docs = append(docs, client.BatchDocumentPut{
			Path:        fmt.Sprintf("vault/note-%04d", i),
			Content:     body,
			ContentType: "application/octet-stream",
		})
		msgs = append(msgs, client.BatchMessage{
			Recipient: peerCl.PeerID(),
			Payload:   []byte(fmt.Sprintf("ptr:vault/note-%04d", i)),
		})
	}

	requests := 0
	for _, chunk := range client.BatchDocuments(docs) {
		results, err := cl.PutDocuments(ctx, ownerID, chunk)
		if err != nil {
			t.Fatalf("batch put: %v", err)
		}
		requests++
		for _, r := range results {
			if !r.OK() {
				t.Fatalf("document %s: status %d %s", r.Path, r.Status, r.Error)
			}
		}
	}

	for i := 0; i < len(msgs); i += client.MaxBatchMessages {
		end := i + client.MaxBatchMessages
		if end > len(msgs) {
			end = len(msgs)
		}
		results, err := cl.SendMessages(ctx, msgs[i:end])
		if err != nil {
			t.Fatalf("batch send: %v", err)
		}
		requests++
		for _, r := range results {
			if !r.Success {
				t.Fatalf("message: %s", r.ErrorMessage)
			}
		}
	}

	// The whole point is that request count tracks batches, not documents. The
	// SDA write limiter allows 20 per minute, so exceeding that would reintroduce
	// exactly the throttling this replaced.
	const maxRequests = 15
	if requests > maxRequests {
		t.Fatalf("syncing %d documents took %d requests, want at most %d",
			vaultSize, requests, maxRequests)
	}

	// Spot-check that documents at both ends of the vault really landed.
	for _, i := range []int{0, vaultSize / 2, vaultSize - 1} {
		doc, err := cl.GetDocument(ctx, ownerID, fmt.Sprintf("vault/note-%04d", i))
		if err != nil {
			t.Fatalf("get note %d: %v", i, err)
		}
		if len(doc.Content) != len(body) {
			t.Errorf("note %d: %d bytes, want %d", i, len(doc.Content), len(body))
		}
	}

	t.Logf("%d documents + %d pointer messages in %d requests", vaultSize, vaultSize, requests)
}
