package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/twostack/go-ricochet/internal/storage"
)

// Content the database cannot hold is the client's fault, not the server's.
// jsonb has no NUL character, so a \u0000 escape in otherwise valid JSON fails
// at insert with SQLSTATE 22P05; that used to pass through as an internal
// error, and the benchmark that first hit it measured an error path while
// reporting a 500 that sent operators looking for a server fault.
func TestCollectionContentTheDatabaseCannotHoldIsInvalidContent(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	coll, err := store.CreateCollection(ctx, owner, "content/nul", "NUL probe")
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}

	_, _, err = store.PutCollectionItem(ctx, coll.ID, "k", []byte("{\"data\":\"a\\u0000b\"}"), owner, nil)
	if !errors.Is(err, storage.ErrInvalidContent) {
		t.Fatalf("NUL escape: err = %v, want ErrInvalidContent", err)
	}

	// Ordinary content still lands, so the wrapping is not catching too much.
	if _, _, err := store.PutCollectionItem(ctx, coll.ID, "k", []byte(`{"data":"a b"}`), owner, nil); err != nil {
		t.Fatalf("plain content: %v", err)
	}
	// And an update on the existing key is wrapped the same way.
	_, _, err = store.PutCollectionItem(ctx, coll.ID, "k", []byte("{\"data\":\"\\u0000\"}"), owner, nil)
	if !errors.Is(err, storage.ErrInvalidContent) {
		t.Fatalf("NUL escape on update: err = %v, want ErrInvalidContent", err)
	}
}
