package wire

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// Read authorization for documents, feeds and collections. Reads used to be
// open to any peer; the three stores now carry the mailbox model: the owner
// reads, a public resource is read by anyone, a shared one by the peers on
// its reader list, and a private one by the owner alone. Writes stay owner
// only (a collaborative feed still takes APPEND from anyone), so this file
// is the read side and the ACCESS operation the owner manages it with.

// RequireReader refuses a caller who may not read the resource with the
// given owner, visibility and row id. The refusal is a 403 like a refused
// write, so a caller learns the path exists but nothing else about it.
func RequireReader(ctx context.Context, sc *forge.StreamContext, store storage.Storage, kind storage.StoreKind, id int64, ownerPeerID string, visibility core.Visibility) error {
	if sc.PeerID.String() == ownerPeerID {
		return nil
	}
	switch visibility {
	case core.VisibilityPublic:
		return nil
	case core.VisibilityShared:
		granted, err := store.IsStoreReader(ctx, kind, id, sc.PeerID)
		if err != nil {
			return fmt.Errorf("check %s reader: %w", kind, err)
		}
		if granted {
			return nil
		}
	}
	return Forbidden("read operations require owner access or a read grant")
}

// ACCESS operation actions, shared by the three store protocols.
const (
	AccessGet    = "get"
	AccessSet    = "set"
	AccessGrant  = "grant"
	AccessRevoke = "revoke"
)

// AccessRequest is the ACCESS operation's input once the handler has
// resolved the resource: which one, and what to do to it.
type AccessRequest struct {
	Kind       storage.StoreKind
	OwnerID    peer.ID
	Path       string
	ID         int64           // the resource's row id
	Current    core.Visibility // its visibility now
	Action     string          // one of the Access* actions
	Visibility string          // for set: the visibility name
	ReaderID   string          // for grant and revoke: the peer
}

// AccessResult is the outcome: a status, and either a body (the resource's
// visibility and reader list) or a client-facing error message.
type AccessResult struct {
	Status int
	Body   *core.StoreAccess
	Error  string
}

// StoreAccess applies an ACCESS action for the owner. Every action answers
// with the resulting visibility and reader list, so a client that grants
// sees the list it produced without a second round trip. Reads of a shared
// resource consult the list; a grant on a private one is kept and takes
// effect when the owner makes it shared, the same as a mailbox ACL.
func StoreAccess(ctx context.Context, store storage.Storage, req AccessRequest) AccessResult {
	fail := func(status int, msg string) AccessResult { return AccessResult{Status: status, Error: msg} }
	switch req.Action {
	case AccessGet:
	case AccessSet:
		v, err := core.VisibilityFromString(req.Visibility)
		if err != nil {
			return fail(400, err.Error())
		}
		found, err := store.SetStoreVisibility(ctx, req.Kind, req.OwnerID, req.Path, v)
		if err != nil {
			return fail(500, "failed to set visibility")
		}
		if !found {
			return fail(404, string(req.Kind)+" not found")
		}
		req.Current = v
	case AccessGrant, AccessRevoke:
		reader, err := peer.Decode(req.ReaderID)
		if err != nil {
			return fail(400, "invalid readerPeerId")
		}
		if req.Action == AccessGrant {
			err = store.GrantStoreReader(ctx, req.Kind, req.ID, reader)
		} else {
			err = store.RevokeStoreReader(ctx, req.Kind, req.ID, reader)
		}
		if err != nil {
			return fail(500, "failed to update readers")
		}
	default:
		return fail(400, "unknown accessAction: "+req.Action+" (want get, set, grant or revoke)")
	}

	readers, err := store.ListStoreReaders(ctx, req.Kind, req.ID)
	if err != nil {
		return fail(500, "failed to list readers")
	}
	body := &core.StoreAccess{Visibility: req.Current.String(), Readers: make([]core.StoreReader, 0, len(readers))}
	for _, r := range readers {
		body.Readers = append(body.Readers, core.StoreReader{PeerID: r.PeerID, GrantedAt: r.GrantedAt.UnixMilli()})
	}
	return AccessResult{Status: 200, Body: body}
}
