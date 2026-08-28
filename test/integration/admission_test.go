package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// isOverloaded reports whether err is the server's 503 shed response.
func isOverloaded(err error) bool {
	return errors.Is(err, client.ErrOverloaded)
}

// TestAdmissionDoesNotCapThroughput is the property the whole mechanism exists
// for. Admission control bounds concurrent work, not work per unit time, so a
// client doing far more requests than the bound must still get all of them
// through — unlike a rate limit, where the count itself is the ceiling.
func TestAdmissionDoesNotCapThroughput(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Admission.MaxInFlight = 2
		cfg.Admission.MaxInFlightPerPeer = 2
		cfg.Admission.AcquireTimeout = 30 * time.Second
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	// 200 writes through a concurrency bound of 2. A rate limiter of 2 would
	// make this impossible; a concurrency bound of 2 only serialises it.
	const writes = 200
	for i := 0; i < writes; i++ {
		if _, err := cl.PutDocument(ctx, ownerID, fmt.Sprintf("throughput/doc-%03d", i), []byte("x")); err != nil {
			t.Fatalf("write %d of %d failed: %v", i, writes, err)
		}
	}

	docs, err := cl.ListDocuments(ctx, ownerID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	stored := 0
	for _, d := range docs {
		if strings.HasPrefix(d.Path, "throughput/") {
			stored++
		}
	}
	if stored != writes {
		t.Errorf("stored %d documents, want %d", stored, writes)
	}
}

// TestAdmissionShedsWhenSaturated checks the other half: when capacity really
// is exhausted, the server rejects rather than queueing without bound. The
// bound is one slot and the timeout is short, so concurrent callers must see
// a 503 rather than waiting indefinitely.
func TestAdmissionShedsWhenSaturated(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Admission.MaxInFlight = 1
		cfg.Admission.MaxInFlightPerPeer = 0
		cfg.Admission.AcquireTimeout = 1 * time.Millisecond
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	// Batches are the longest-running request we have, so they hold a slot
	// long enough for the contending callers to hit the timeout.
	batch := make([]client.BatchDocumentPut, 0, 100)
	for i := 0; i < 100; i++ {
		batch = append(batch, client.BatchDocumentPut{
			Path:    fmt.Sprintf("shed/doc-%03d", i),
			Content: []byte(strings.Repeat("x", 4096)),
		})
	}

	const callers = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	var shed, ok int

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			docs := make([]client.BatchDocumentPut, len(batch))
			copy(docs, batch)
			for j := range docs {
				docs[j].Path = fmt.Sprintf("shed/%02d-%03d", n, j)
			}
			_, err := cl.PutDocuments(ctx, ownerID, docs)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case isOverloaded(err):
				shed++
			default:
				t.Errorf("caller %d: unexpected error: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	if shed == 0 {
		t.Errorf("with one slot and a 1ms timeout, %d concurrent callers should have produced some 503s (all %d succeeded)", callers, ok)
	}
	if ok == 0 {
		t.Error("every caller was shed; the slot holder should still have completed")
	}
}

// TestAdmissionIsEnforcedPerPeer checks that the per-peer bound is what stops
// one client monopolising the server, which is what makes it safe to ship with
// per-peer rate limiting switched off.
func TestAdmissionIsEnforcedPerPeer(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Admission.MaxInFlight = 64
		cfg.Admission.MaxInFlightPerPeer = 1
		cfg.Admission.AcquireTimeout = 1 * time.Millisecond
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	batch := make([]client.BatchDocumentPut, 0, 100)
	for i := 0; i < 100; i++ {
		batch = append(batch, client.BatchDocumentPut{
			Path:    fmt.Sprintf("perpeer/doc-%03d", i),
			Content: []byte(strings.Repeat("x", 4096)),
		})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	shed := 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			docs := make([]client.BatchDocumentPut, len(batch))
			copy(docs, batch)
			for j := range docs {
				docs[j].Path = fmt.Sprintf("perpeer/%02d-%03d", n, j)
			}
			if _, err := cl.PutDocuments(ctx, ownerID, docs); isOverloaded(err) {
				mu.Lock()
				shed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// 63 of 64 global slots are free, so anything shed here was stopped by
	// the per-peer bound.
	if shed == 0 {
		t.Error("a single peer saturated its own bound but nothing was shed; the per-peer limit is not being enforced")
	}
}

// TestAdmissionDisabledAdmitsEverything confirms the escape hatch works, for
// a deployment that wants the database to be the only limit.
func TestAdmissionDisabledAdmitsEverything(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Admission.Enabled = false
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	var wg sync.WaitGroup
	errs := make([]error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, errs[n] = cl.PutDocument(ctx, ownerID, fmt.Sprintf("nolimit/doc-%02d", n), []byte("x"))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("write %d failed with admission control disabled: %v", i, err)
		}
	}
}
