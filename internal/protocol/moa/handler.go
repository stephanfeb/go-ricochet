// Package moa is the Moderator Operations Agent: the protocol a moderator
// (a peer named in security.moderators, such as the Trust & Safety client)
// uses to ban and unban peers and to remove a microblog post.
//
// Every call is refused unless the connection's peer is a moderator, and
// every call that changes something is logged at WARN with the moderator,
// so the server log is an audit trail of what moderators did here.
package moa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/stephanfeb/go-p2p-forge"
	"github.com/stephanfeb/go-p2p-forge/codec"
	"github.com/stephanfeb/go-p2p-forge/middleware"

	"github.com/stephanfeb/go-ricochet/internal/metrics"
	"github.com/stephanfeb/go-ricochet/internal/moderation"
	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// ProtocolID is the moderation protocol identifier.
const ProtocolID = protocol.ID("/ricochet/moderation/1.0.0")

// Operations.
const (
	OpBan        = "ban"
	OpUnban      = "unban"
	OpGetBan     = "getBan"
	OpListBans   = "listBans"
	OpRemovePost = "removePost"
)

// The microblog layout OverNode uses (overnode_v2 MicroblogService): a
// post is an entry in its author's feed "microblog", replies are entries in
// the root author's "microblog/threads/<postId>", and images are documents
// "microblog/media/<postId>-img<N>".
const (
	microblogFeed  = "microblog"
	threadsPrefix  = "microblog/threads/"
	mediaPrefix    = "microblog/media/"
	maxReasonRunes = 500
)

// Request is the request envelope; operation picks the fields that matter.
type Request struct {
	Operation string `json:"operation"`

	// ban, unban, getBan: the peer. removePost: the post's author.
	PeerID string `json:"peerId,omitempty"`

	// ban: why, for the record.
	Reason string `json:"reason,omitempty"`

	// removePost: the post's id (the "id" of its feed entry).
	PostID string `json:"postId,omitempty"`
}

// Response is the response envelope.
type Response struct {
	Success      bool   `json:"success"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	Status       int    `json:"status,omitempty"`

	// ban, getBan: the ban, or none when the peer is not banned.
	Ban *storage.PeerBan `json:"ban,omitempty"`
	// listBans.
	Bans []*storage.PeerBan `json:"bans,omitempty"`
	// unban: whether there was a ban to lift.
	WasBanned bool `json:"wasBanned,omitempty"`
	// ban: what else it did.
	ConnectionsClosed bool `json:"connectionsClosed,omitempty"`
	ListingRemoved    bool `json:"listingRemoved,omitempty"`

	// removePost: what went.
	Removal *PostRemoval `json:"removal,omitempty"`
}

// PostRemoval is what removePost removed.
type PostRemoval struct {
	// Entries removed: the post, its edits and deletes, and the author's
	// own reactions and boosts of it.
	Entries int `json:"entries"`
	// Feeds the entries were in, as "owner/path".
	Feeds []string `json:"feeds,omitempty"`
	// Whether its replies thread went (the post was a thread's root).
	ThreadRemoved bool `json:"threadRemoved"`
	// Image documents removed.
	Media int `json:"media"`
}

// StatusCode is the status the response reports.
func (r *Response) StatusCode() int {
	switch {
	case r.Status != 0:
		return r.Status
	case r.Success:
		return wire.StatusOK
	default:
		return wire.StatusBadRequest
	}
}

// Disconnector closes every connection to a peer: the host's network.
type Disconnector interface {
	ClosePeer(peer.ID) error
}

// DisconnectorKey is the registry key of the Disconnector.
const DisconnectorKey = "moa.disconnector"

// NewPipeline creates the moderation pipeline.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	route := func(h forge.Middleware) forge.Middleware {
		return middleware.Chain(forge.JSONDeserialize[Request](), h)
	}
	routes := map[string]forge.Middleware{
		OpBan:        route(handleBan),
		OpUnban:      route(handleUnban),
		OpGetBan:     route(handleGetBan),
		OpListBans:   route(handleListBans),
		OpRemovePost: route(handleRemovePost),
	}
	return wire.Bounded(forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "moa", ""),
		wire.AccessLog("moa", ""),
		middleware.Recovery(),
		responseWriter(),
		forge.FrameDecodeMiddleware(pool),
		wire.RequestDeadline(reg),
		requireModerator(),
		middleware.OperationRouter("operation", routes),
	).WithRegistry(reg), reg)
}

// requireModerator refuses anyone not in security.moderators, before the
// request is read.
func requireModerator() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		if !moderation.ModeratorsFromStream(sc).Contains(sc.PeerID) {
			sc.Err = wire.Forbidden("moderation requires a moderator")
			return
		}
		next()
	}
}

func responseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()
		if sc.Err != nil && sc.Response == nil {
			status, _ := wire.Classify(sc.Err)
			wire.LogRejection(sc, status, sc.Err)
			sc.Response = &Response{Success: false, ErrorMessage: wire.ClientMessage(sc.Err), Status: status}
		}
		if sc.Response == nil {
			return
		}
		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal response", "error", err)
			return
		}
		if err := sc.WriteFrame(data); err != nil {
			sc.Logger.Error("failed to write response", "error", err)
		}
	}
}

func refuse(status int, msg string) *Response {
	return &Response{Success: false, ErrorMessage: msg, Status: status}
}

// target decodes the request's peerId.
func target(req *Request) (peer.ID, *Response) {
	if req.PeerID == "" {
		return "", refuse(wire.StatusBadRequest, "peerId is required")
	}
	id, err := peer.Decode(req.PeerID)
	if err != nil {
		return "", refuse(wire.StatusBadRequest, "peerId is not a peer ID")
	}
	return id, nil
}

func handleBan(sc *forge.StreamContext, _ func()) {
	req := sc.Request.(*Request)
	id, bad := target(req)
	if bad != nil {
		sc.Response = bad
		return
	}
	if id == sc.PeerID || moderation.ModeratorsFromStream(sc).Contains(id) {
		sc.Response = refuse(wire.StatusBadRequest, "a moderator cannot be banned")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if r := []rune(reason); len(r) > maxReasonRunes {
		reason = string(r[:maxReasonRunes])
	}
	bans := moderation.FromStream(sc)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ban, err := bans.Ban(sc.Ctx, id, sc.PeerID, reason, time.Now())
	if err != nil {
		sc.Logger.Error("ban not kept", "target", id.String(), "error", err)
		sc.Response = refuse(wire.StatusInternalError, "the ban could not be kept")
		return
	}
	resp := &Response{Success: true, Ban: ban}
	if d, ok := forge.ServiceFrom[Disconnector](sc, DisconnectorKey); ok && d != nil {
		resp.ConnectionsClosed = d.ClosePeer(id) == nil
	}
	if err := store.RemoveDirectoryEntry(sc.Ctx, id.String()); err != nil {
		sc.Logger.Error("banned peer's listing not removed", "target", id.String(), "error", err)
	} else {
		resp.ListingRemoved = true
	}
	sc.Logger.Warn("moderation: peer banned",
		"moderator", sc.PeerID.String(), "target", id.String(), "reason", reason)
	sc.Response = resp
}

func handleUnban(sc *forge.StreamContext, _ func()) {
	req := sc.Request.(*Request)
	id, bad := target(req)
	if bad != nil {
		sc.Response = bad
		return
	}
	was, err := moderation.FromStream(sc).Unban(sc.Ctx, id)
	if err != nil {
		sc.Logger.Error("unban not kept", "target", id.String(), "error", err)
		sc.Response = refuse(wire.StatusInternalError, "the unban could not be kept")
		return
	}
	sc.Logger.Warn("moderation: peer unbanned",
		"moderator", sc.PeerID.String(), "target", id.String(), "was_banned", was)
	sc.Response = &Response{Success: true, WasBanned: was}
}

func handleGetBan(sc *forge.StreamContext, _ func()) {
	req := sc.Request.(*Request)
	id, bad := target(req)
	if bad != nil {
		sc.Response = bad
		return
	}
	sc.Response = &Response{Success: true, Ban: moderation.FromStream(sc).Get(id)}
}

func handleListBans(sc *forge.StreamContext, _ func()) {
	sc.Response = &Response{Success: true, Bans: moderation.FromStream(sc).List()}
}

// handleRemovePost removes a microblog post: its entry in its author's
// feed, with the author's later edits, deletes, reactions and boosts of it;
// a reply's entry in the thread it is in; the post's own replies thread when
// it is a root; and its images. Nothing of anyone else's but the replies
// thread is touched.
func handleRemovePost(sc *forge.StreamContext, _ func()) {
	req := sc.Request.(*Request)
	author, bad := target(req)
	if bad != nil {
		sc.Response = bad
		return
	}
	postID := strings.TrimSpace(req.PostID)
	if postID == "" || strings.ContainsAny(postID, "/%_\\") || len(postID) > 128 {
		sc.Response = refuse(wire.StatusBadRequest, "postId is required, and is an id, not a path")
		return
	}
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")

	candidates, err := store.FeedEntriesContaining(sc.Ctx, author, microblogFeed, []byte(postID))
	if err != nil {
		sc.Logger.Error("post entries not found", "author", author.String(), "post", postID, "error", err)
		sc.Response = refuse(wire.StatusInternalError, "could not look the post up")
		return
	}
	var ids []int64
	feeds := map[string]bool{}
	for _, e := range candidates {
		if !entryIsAbout(e, author, postID) {
			continue
		}
		ids = append(ids, e.ID)
		feeds[e.FeedOwnerPeerID+"/"+e.FeedPath] = true
	}
	removal := &PostRemoval{}
	if removal.Entries, err = store.DeleteFeedEntries(sc.Ctx, ids); err != nil {
		sc.Logger.Error("post entries not removed", "author", author.String(), "post", postID, "error", err)
		sc.Response = refuse(wire.StatusInternalError, "could not remove the post")
		return
	}
	for f := range feeds {
		removal.Feeds = append(removal.Feeds, f)
	}
	if removal.ThreadRemoved, err = store.DeleteFeed(sc.Ctx, author, threadsPrefix+postID); err != nil {
		sc.Logger.Error("post thread not removed", "author", author.String(), "post", postID, "error", err)
	}
	if removal.Media, err = store.DeleteDocumentsWithPrefix(sc.Ctx, author, mediaPrefix+postID+"-img"); err != nil {
		sc.Logger.Error("post media not removed", "author", author.String(), "post", postID, "error", err)
	}

	sc.Logger.Warn("moderation: post removed",
		"moderator", sc.PeerID.String(), "author", author.String(), "post", postID,
		"entries", removal.Entries, "thread", removal.ThreadRemoved, "media", removal.Media)
	if removal.Entries == 0 && !removal.ThreadRemoved && removal.Media == 0 {
		sc.Response = &Response{Success: false, Status: wire.StatusNotFound,
			ErrorMessage: fmt.Sprintf("no post %s by %s here", postID, author), Removal: removal}
		return
	}
	sc.Response = &Response{Success: true, Removal: removal}
}

// entryIsAbout reports whether a feed entry is the post postID by author,
// or one of the author's own entries pointing at it (edit, delete, reaction,
// boost): its JSON "id" or "target.id" is the post's. Entries that are not
// JSON, or by someone else, are left alone.
func entryIsAbout(e *storage.FeedEntryMatch, author peer.ID, postID string) bool {
	if e.CreatedByPeerID != author.String() && !(e.CreatedByPeerID == "" && e.FeedOwnerPeerID == author.String()) {
		return false
	}
	var body struct {
		ID     string `json:"id"`
		Target *struct {
			ID string `json:"id"`
		} `json:"target"`
	}
	if err := json.NewDecoder(bytes.NewReader(e.Content)).Decode(&body); err != nil {
		return false
	}
	return body.ID == postID || (body.Target != nil && body.Target.ID == postID)
}
