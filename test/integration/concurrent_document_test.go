package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

// TestConcurrentDocumentGet verifies that multiple concurrent GET requests
// from a single client complete without timeout or data corruption.
func TestConcurrentDocumentGet(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	const docCount = 20

	// Phase 1: Put N documents
	putContents := make([][]byte, docCount)
	putETags := make([]string, docCount)
	for i := 0; i < docCount; i++ {
		content := []byte(fmt.Sprintf(`{"doc":%d,"data":"payload-%d"}`, i, i))
		putContents[i] = content

		resp, err := cl.PutDocument(ctx, ownerID, fmt.Sprintf("bench/doc-%d", i),
			content, client.WithContentType("application/json"))
		if err != nil {
			t.Fatalf("put doc %d: %v", i, err)
		}
		putETags[i] = resp.ETag
	}

	t.Logf("Put %d documents", docCount)

	// Phase 2: GET all N documents concurrently
	var wg sync.WaitGroup
	errors := make(chan error, docCount)
	durations := make([]time.Duration, docCount)

	start := time.Now()

	for i := 0; i < docCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			reqStart := time.Now()
			resp, err := cl.GetDocument(ctx, ownerID, fmt.Sprintf("bench/doc-%d", idx))
			durations[idx] = time.Since(reqStart)

			if err != nil {
				errors <- fmt.Errorf("get doc %d: %w", idx, err)
				return
			}
			if resp.Status != 200 {
				errors <- fmt.Errorf("get doc %d: status %d", idx, resp.Status)
				return
			}
			if !bytes.Equal(resp.Content, putContents[idx]) {
				errors <- fmt.Errorf("get doc %d: content mismatch", idx)
				return
			}
			if resp.ETag != putETags[idx] {
				errors <- fmt.Errorf("get doc %d: etag mismatch: got %s want %s", idx, resp.ETag, putETags[idx])
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	totalDuration := time.Since(start)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		for _, err := range errs {
			t.Error(err)
		}
		t.Fatalf("%d/%d concurrent GETs failed", len(errs), docCount)
	}

	// Report timing
	var maxDuration time.Duration
	var totalLatency time.Duration
	for _, d := range durations {
		totalLatency += d
		if d > maxDuration {
			maxDuration = d
		}
	}
	avgLatency := totalLatency / time.Duration(docCount)

	t.Logf("Concurrent GET results (%d docs):", docCount)
	t.Logf("  Total wall-clock: %v", totalDuration)
	t.Logf("  Avg per-request:  %v", avgLatency)
	t.Logf("  Max per-request:  %v", maxDuration)

	// All 20 concurrent GETs should complete within 10 seconds
	if totalDuration > 10*time.Second {
		t.Errorf("concurrent GETs too slow: %v (expected <10s)", totalDuration)
	}
}

// TestConcurrentFeedCreate verifies that concurrent feed creation (the upsert fix)
// doesn't produce duplicate key errors. Uses a single client (same owner) to
// trigger the race condition on the same (owner, path) pair.
func TestConcurrentFeedCreate(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	const concurrency = 10

	var wg sync.WaitGroup
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// All goroutines try to create the same feed path for the same owner
			err := cl.CreateFeed(ctx, "microblog/posts", "My Posts", "")
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: create feed: %w", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		for _, err := range errs {
			t.Error(err)
		}
		t.Fatalf("%d/%d concurrent feed creates failed", len(errs), concurrency)
	}

	t.Logf("All %d concurrent feed creates succeeded (upsert working)", concurrency)
}
