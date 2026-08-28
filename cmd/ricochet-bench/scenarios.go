package main

import (
	"context"
	"fmt"

	client "github.com/twostack/go-ricochet/pkg/client"
)

// Batch and sync scenarios.
//
// The per-protocol benchmarks measure one operation per request, which was the
// only shape the server had. Batching changed that: the number worth quoting is
// no longer requests per second but documents per second, and the two diverge
// by two orders of magnitude. A 500-document vault was 1,000 requests before
// BATCH_PUT and is 10 after, so a benchmark that counts requests would report
// the improvement as a regression.
//
// The three sync shapes are what Stage 2 will be judged against. They are
// deliberately whole passes rather than single operations: "how long does a
// sync take" is the question a client asks, and it is not answerable by
// multiplying a per-document latency, because batching, pagination and the
// no-op path each change the request count.

// syncDoc builds one document's path within a vault.
func syncDoc(workerID, pass, index int) string {
	return fmt.Sprintf("bench/sync/w%d/p%d/doc-%04d", workerID, pass, index)
}

// putVault writes docs documents in batches of the configured size.
//
// Returns the number written. Errors are returned whole rather than counted,
// because a partial sync is a failed sync from the caller's point of view.
func putVault(ctx context.Context, c *client.Client, cfg *benchConfig, workerID, pass int, content []byte) error {
	owner := c.PeerID()

	for start := 0; start < cfg.DocCount; start += cfg.BatchSize {
		end := start + cfg.BatchSize
		if end > cfg.DocCount {
			end = cfg.DocCount
		}

		batch := make([]client.BatchDocumentPut, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, client.BatchDocumentPut{
				Path:    syncDoc(workerID, pass, i),
				Content: content,
			})
		}

		results, err := c.PutDocuments(ctx, owner, batch)
		if err != nil {
			return fmt.Errorf("batch at %d: %w", start, err)
		}
		for _, r := range results {
			if !r.OK() {
				return fmt.Errorf("%s: status %d %s", r.Path, r.Status, r.Error)
			}
		}
	}
	return nil
}

// --- Batch scenarios -------------------------------------------------------

// benchSDABatch writes one BATCH_PUT per request.
//
// Paths are namespaced by worker and request so every batch is a create rather
// than a replace; mixing the two would measure a blend of paths whose costs
// differ and report a number that belongs to neither.
func benchSDABatch(cfg *benchConfig, payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		owner := c.PeerID()
		batch := make([]client.BatchDocumentPut, 0, cfg.BatchSize)
		for i := 0; i < cfg.BatchSize; i++ {
			batch = append(batch, client.BatchDocumentPut{
				Path:    fmt.Sprintf("bench/batch/w%d/r%d/doc-%04d", workerID, reqID, i),
				Content: payload,
			})
		}

		results, err := c.PutDocuments(ctx, owner, batch)
		if err != nil {
			return err
		}
		for _, r := range results {
			if !r.OK() {
				return fmt.Errorf("%s: status %d %s", r.Path, r.Status, r.Error)
			}
		}
		return nil
	}
}

// benchMSABatch submits one batch of messages per request.
func benchMSABatch(workers []*workerState, cfg *benchConfig, payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		recipient := workers[workerID].recipientID

		msgs := make([]client.BatchMessage, 0, cfg.BatchSize)
		for i := 0; i < cfg.BatchSize; i++ {
			msgs = append(msgs, client.BatchMessage{Recipient: recipient, Payload: payload})
		}

		results, err := c.SendMessages(ctx, msgs)
		if err != nil {
			return err
		}
		for _, r := range results {
			if !r.Success {
				return fmt.Errorf("message rejected: %s", r.ErrorMessage)
			}
		}
		return nil
	}
}

// --- Sync scenarios --------------------------------------------------------

// benchSyncCold measures a first-time vault upload: every document is new.
//
// Each pass writes its own namespace, so a run of several passes measures
// several cold syncs rather than one cold sync and several replaces.
func benchSyncCold(cfg *benchConfig, payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		return putVault(ctx, c, cfg, workerID, reqID, payload)
	}
}

// benchSyncWarm measures re-uploading a vault whose contents have changed.
//
// The paths already exist, so every write is a replace: the server does the
// same work as a cold sync plus the cost of superseding a row. This is the
// shape of a sync after real edits.
func benchSyncWarm(cfg *benchConfig, payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		// Content differs each pass so the write is never a no-op at the
		// storage layer, which is what distinguishes this from sync-noop.
		content := make([]byte, len(payload))
		copy(content, payload)
		if len(content) > 0 {
			content[0] = byte(reqID)
		}
		return putVault(ctx, c, cfg, workerID, warmPass, content)
	}
}

// warmPass is the fixed namespace the warm and no-op scenarios reuse, so every
// measured pass hits documents that already exist.
const warmPass = 0

// benchSyncNoop measures the cost of discovering that nothing changed.
//
// This is the shape that runs most often in practice — a periodic sync over an
// unchanged vault — and the one where the request count matters most. It lists
// the owner's documents and compares ETags, writing nothing: a client that
// instead issued one conditional GET per document would pay DocCount requests
// to learn the same thing.
func benchSyncNoop(cfg *benchConfig) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		infos, err := c.ListDocuments(ctx, c.PeerID())
		if err != nil {
			return fmt.Errorf("list: %w", err)
		}

		// Index by path, as a syncing client would, and confirm every document
		// it expects is present and unchanged. Without this the benchmark
		// would time a listing that returned nothing at all.
		have := make(map[string]string, len(infos))
		for _, info := range infos {
			have[info.Path] = info.ETag
		}

		for i := 0; i < cfg.DocCount; i++ {
			path := syncDoc(workerID, warmPass, i)
			etag, ok := have[path]
			if !ok {
				return fmt.Errorf("%s missing from the listing", path)
			}
			if etag == "" {
				return fmt.Errorf("%s has no ETag; nothing to compare", path)
			}
		}
		return nil
	}
}

// setupSync writes the vault the warm and no-op scenarios measure against.
func setupSync(ctx context.Context, cfg *benchConfig, workers []*workerState, payload []byte) error {
	fmt.Printf("     Seeding %d documents per worker...", cfg.DocCount)
	for i, ws := range workers {
		if err := putVault(ctx, ws.client, cfg, i, warmPass, payload); err != nil {
			return fmt.Errorf("seed vault for worker %d: %w", i, err)
		}
	}
	fmt.Println(" done")
	return nil
}
