package wire_test

import (
	"fmt"
	"github.com/stephanfeb/go-ricochet/internal/storage"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
)

func newPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Every refusal classifies as a 403 whose message the client may see, and
// says "unauthorized:" first so a client can match on the prefix alone.
func TestAuthorizationRefusalsClassifyAsForbidden(t *testing.T) {
	caller, other := newPeer(t), newPeer(t)
	sc := &forge.StreamContext{PeerID: caller}

	refusals := map[string]error{
		"RequireOwner": wire.RequireOwner(sc, other, "write operations"),
		"RequireSelf":  wire.RequireSelf(sc, other.String(), "ownerPeerId"),
		"Forbidden":    wire.Forbidden("no read access"),
	}
	for name, err := range refusals {
		if err == nil {
			t.Errorf("%s: allowed a caller it should refuse", name)
			continue
		}
		status, retry := wire.Classify(err)
		if status != wire.StatusForbidden || retry != 0 {
			t.Errorf("%s: classified as %d with retry %v, want 403 and no hint", name, status, retry)
		}
		if msg := wire.ClientMessage(err); !strings.HasPrefix(msg, "unauthorized: ") {
			t.Errorf("%s: client message %q lacks the unauthorized: prefix", name, msg)
		}
	}

	allowed := map[string]error{
		"owner":       wire.RequireOwner(sc, caller, "write operations"),
		"self":        wire.RequireSelf(sc, caller.String(), "ownerPeerId"),
		"empty claim": wire.RequireSelf(sc, "", "ownerPeerId"),
	}
	for name, err := range allowed {
		if err != nil {
			t.Errorf("%s: refused the caller: %v", name, err)
		}
	}
}

// Content the database refuses is the client's request at fault, so it is a
// 400 carrying the database's own words, the way an invalid filter is.
func TestInvalidContentClassifiesAsBadRequest(t *testing.T) {
	err := fmt.Errorf("insert collection item: %w", fmt.Errorf("%w: unsupported Unicode escape sequence", storage.ErrInvalidContent))
	status, _ := wire.Classify(err)
	if status != wire.StatusBadRequest {
		t.Fatalf("status = %d, want %d", status, wire.StatusBadRequest)
	}
	if msg := wire.ClientMessage(err); !strings.Contains(msg, "unsupported Unicode escape sequence") {
		t.Fatalf("client message %q does not carry the database's reason", msg)
	}
}
