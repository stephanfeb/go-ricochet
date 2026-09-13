package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/twostack/go-ricochet/internal/storage"
)

// Two patches carrying the same If-Match race for one document: exactly one
// may win, and the other must see a conflict rather than silently overwrite.
func TestConcurrentPatchesWithSameIfMatchAdmitOne(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "/race/if-match.json"

	put, err := store.PutDocument(ctx, owner, path, []byte(`{"n":0}`), "application/json", owner, nil, nil)
	if err != nil {
		t.Fatalf("seed document: %v", err)
	}
	etag := put.ContentHash

	const racers = 8
	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		mu       sync.Mutex
		wins     int
		conflict int
		others   []error
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ifMatch := etag
			_, err := store.PatchDocument(ctx, owner, path, map[string]any{"n": i + 1}, owner, &ifMatch)
			mu.Lock()
			defer mu.Unlock()
			var conflictErr *storage.DocumentConflictError
			switch {
			case err == nil:
				wins++
			case errors.As(err, &conflictErr):
				conflict++
			default:
				others = append(others, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(others) != 0 {
		t.Fatalf("unexpected errors: %v", others)
	}
	if wins != 1 || conflict != racers-1 {
		t.Fatalf("%d patches succeeded and %d conflicted, want 1 and %d", wins, conflict, racers-1)
	}
}

// Patches without If-Match compose: every concurrent patch's key survives,
// so no update is lost between the read and the write.
func TestConcurrentPatchesWithoutIfMatchAllApply(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "/race/merge.json"

	if _, err := store.PutDocument(ctx, owner, path, []byte(`{}`), "application/json", owner, nil, nil); err != nil {
		t.Fatalf("seed document: %v", err)
	}

	const racers = 12
	var (
		start = make(chan struct{})
		wg    sync.WaitGroup
		errs  = make(chan error, racers)
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			key := fmt.Sprintf("k%d", i)
			if _, err := store.PatchDocument(ctx, owner, path, map[string]any{key: i}, owner, nil); err != nil {
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("patch failed: %v", err)
	}

	doc, err := store.GetDocument(ctx, owner, path)
	if err != nil || doc == nil {
		t.Fatalf("get document: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(doc.Content, &got); err != nil {
		t.Fatalf("parse document: %v", err)
	}
	if len(got) != racers {
		t.Fatalf("document holds %d keys after %d concurrent patches: %s", len(got), racers, doc.Content)
	}
	if doc.VersionNumber != racers+1 {
		t.Fatalf("version = %d, want %d", doc.VersionNumber, racers+1)
	}
}
