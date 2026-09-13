package sfa

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda/mailboxes"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
	"github.com/twostack/go-ricochet/internal/ratelimit"
	"github.com/twostack/go-ricochet/internal/storage"
)

// ProtocolID is the SFA protocol identifier.
const ProtocolID = protocol.ID("/ricochet/store/feed/1.0.0")

// Feed operation constants.
const (
	OpCREATE    = "CREATE"
	OpGET       = "GET"
	OpAPPEND    = "APPEND"
	OpDELETE    = "DELETE"
	OpLIST      = "LIST"
	OpBATCH_GET = "BATCH_GET"
)

// Path validation constants.
const maxPathLength = 256

var validPathRe = regexp.MustCompile(`^[a-zA-Z0-9\-_/]+$`)

// HTTP-style status codes used in responses.
const (
	StatusOK                 = 200
	StatusCreated            = 201
	StatusNoContent          = 204
	StatusBadRequest         = 400
	StatusForbidden          = 403
	StatusNotFound           = 404
	StatusConflict           = 409
	StatusTooManyRequests    = 429
	StatusInternalError      = 500
	StatusServiceUnavailable = 503
)

// FeedRequest is the JSON request format for feed operations.
type FeedRequest struct {
	Operation   string            `json:"operation"`
	OwnerPeerID string            `json:"ownerPeerId"`
	Path        string            `json:"path,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"` // base64 encoded

	// CREATE parameters
	Title         string `json:"title,omitempty"`
	Description   string `json:"description,omitempty"`
	Collaborative bool   `json:"collaborative,omitempty"`

	// APPEND parameters
	EntryType string `json:"entryType,omitempty"`

	// GET parameters: single entry
	SequenceNumber *int `json:"sequenceNumber,omitempty"`

	// GET parameters: range query
	FromSequence *int `json:"fromSequence,omitempty"`
	ToSequence   *int `json:"toSequence,omitempty"`
	Limit        *int `json:"limit,omitempty"`

	// BATCH_GET parameters
	BatchQueries []BatchQuery `json:"batchQueries,omitempty"`
}

// BatchQuery describes a single feed to retrieve in a BATCH_GET request.
type BatchQuery struct {
	OwnerPeerID  string `json:"ownerPeerId"`
	Path         string `json:"path"`
	FromSequence *int   `json:"fromSequence,omitempty"`
	Limit        *int   `json:"limit,omitempty"`
}

// FeedResponse is the JSON response format for feed operations.
type FeedResponse struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    string         `json:"body,omitempty"` // base64 encoded
}

// StatusCode reports the response status. It is how the metrics middleware
// tells a request the handler refused — a 404 or a 409 — from one that
// succeeded: neither sets a pipeline error, so without this both would be
// counted as ok.
func (r *FeedResponse) StatusCode() int { return r.Status }

// NewPipeline creates a forge pipeline for the Store Feed Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).SFA

	return wire.Bounded(forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "sfa", ""),
		middleware.Recovery(),
		feedResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		wire.RequestDeadline(reg),
		middleware.DualRateLimitMiddleware(limiter, isWriteClassifier),
		admission.Middleware(admission.FromRegistry(reg)),
		capacity.WriteGate(capacity.FromRegistry(reg), isWriteClassifier),
		forge.JSONDeserialize[FeedRequest](),
		commonValidation(),
		middleware.OperationRouter("operation", map[string]forge.Middleware{
			OpCREATE:    createHandler,
			OpGET:       getHandler,
			OpAPPEND:    appendHandler,
			OpDELETE:    deleteHandler,
			OpLIST:      listHandler,
			OpBATCH_GET: batchGetHandler,
		}),
	).WithRegistry(reg), reg)
}

// isWriteClassifier inspects raw JSON bytes to classify CREATE, APPEND, and DELETE
// as write operations for dual-bucket rate limiting.
func isWriteClassifier(raw []byte) bool {
	s := string(raw)
	return strings.Contains(s, `"CREATE"`) ||
		strings.Contains(s, `"APPEND"`) ||
		strings.Contains(s, `"DELETE"`)
}

// feedResponseWriter writes a FeedResponse as a JSON frame after the downstream
// pipeline completes. On pipeline error, it converts the error into an error response
// so the client always gets a response.
func feedResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into error responses.
		if sc.Err != nil && sc.Response == nil {
			status, retryAfter := wire.Classify(sc.Err)
			wire.LogRejection(sc, status, sc.Err)
			headers := map[string]any{"Error": wire.ClientMessage(sc.Err)}
			if ms := wire.RetryAfterMs(retryAfter); ms > 0 {
				headers[wire.RetryAfterHeaderKey] = ms
			}
			sc.Response = &FeedResponse{Status: status, Headers: headers}
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

// commonValidation validates the ownerPeerId and path fields shared across operations.
// It parses the owner peer ID and stores it in the StreamContext values for downstream use.
// For non-LIST operations, it also validates the path.
// For write operations by non-owners, it enforces owner-only access with a collaborative
// APPEND exception (including auto-create of collaborative feeds).
func commonValidation() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		req := sc.Request.(*FeedRequest)

		// BATCH_GET has its own validation (multiple owners/paths in batchQueries).
		if req.Operation == OpBATCH_GET {
			next()
			return
		}

		// Parse owner peer ID
		if req.OwnerPeerID == "" {
			sc.Response = &FeedResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "ownerPeerId is required"}}
			return
		}
		ownerID, err := peer.Decode(req.OwnerPeerID)
		if err != nil {
			sc.Response = &FeedResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "invalid ownerPeerId"}}
			return
		}

		// Store parsed owner ID for downstream handlers.
		sc.Set("ownerID", ownerID)

		// Validate path for operations that require it
		if req.Operation != OpLIST {
			if err := validatePath(req.Path); err != nil {
				sc.Response = &FeedResponse{Status: StatusBadRequest,
					Headers: map[string]any{"Error": err.Error()}}
				return
			}
		}

		// Enforce owner-only access for write operations. The one exception
		// is APPEND to a feed its owner created as collaborative. A missing
		// feed is a 404 for a non-owner: the server used to create it on
		// their behalf, as collaborative, which let anyone plant feeds under
		// any identity that then showed in that identity's public listing
		// and that the owner could not make private again.
		callerID := sc.PeerID
		isWrite := req.Operation == OpCREATE || req.Operation == OpAPPEND || req.Operation == OpDELETE
		if isWrite && callerID != ownerID {
			if req.Operation != OpAPPEND {
				sc.Response = &FeedResponse{Status: StatusForbidden,
					Headers: map[string]any{"Error": "write operations require owner access"}}
				return
			}
			store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
			feed, err := store.GetFeed(sc.Ctx, ownerID, req.Path)
			if err != nil {
				sc.Response = &FeedResponse{Status: StatusInternalError,
					Headers: map[string]any{"Error": "failed to check feed"}}
				return
			}
			if feed == nil {
				sc.Response = &FeedResponse{Status: StatusNotFound,
					Headers: map[string]any{"Error": "feed not found"}}
				return
			}
			if !feed.CollaborativeMode {
				sc.Response = &FeedResponse{Status: StatusForbidden,
					Headers: map[string]any{"Error": "write operations require owner access"}}
				return
			}
			sc.Logger.Debug("allowing collaborative append",
				"feed", req.Path, "owner", ownerID, "contributor", callerID)
		}

		sc.Logger.Debug("handling feed request",
			"operation", req.Operation,
			"owner", req.OwnerPeerID,
			"path", req.Path,
			"caller", callerID.String(),
		)

		next()
	}
}

// createHandler handles the CREATE operation.
func createHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*FeedRequest)
	ownerID, _ := sc.Get("ownerID")

	ctx := sc.Ctx
	feed, err := store.CreateFeed(ctx, ownerID.(peer.ID), req.Path, req.Title, req.Description, req.Collaborative)
	if err != nil {
		sc.Logger.Error("failed to create feed", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"id":                feed.ID,
		"path":              feed.Path,
		"title":             feed.Title,
		"description":       feed.Description,
		"currentSequence":   feed.CurrentSequence,
		"collaborativeMode": feed.CollaborativeMode,
	})
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// getHandler handles the GET operation: feed metadata, single entry, or range queries.
func getHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*FeedRequest)
	ownerID, _ := sc.Get("ownerID")

	ctx := sc.Ctx

	// Look up the feed
	feed, err := store.GetFeed(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		sc.Logger.Error("failed to get feed", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}
	if feed == nil {
		sc.Response = &FeedResponse{Status: StatusNotFound}
		return
	}

	// Dispatch based on request parameters
	if req.SequenceNumber != nil {
		// Single entry by sequence number
		handleGetEntry(sc, store, feed, *req.SequenceNumber)
	} else if req.FromSequence != nil || req.ToSequence != nil || req.Limit != nil {
		// Range query
		handleGetEntries(sc, store, feed, req)
	} else {
		// Feed metadata
		handleGetMetadata(sc, feed)
	}
}

// handleGetMetadata returns feed metadata.
func handleGetMetadata(sc *forge.StreamContext, feed *storage.FeedRecord) {
	bodyBytes, err := json.Marshal(map[string]any{
		"title":           feed.Title,
		"description":     feed.Description,
		"currentSequence": feed.CurrentSequence,
		"lastEntryAt":     feed.LastEntryAt.UnixMilli(),
		"createdAt":       feed.CreatedAt.UnixMilli(),
	})
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
			"X-Sequence":   feed.CurrentSequence,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleGetEntry returns a single feed entry by sequence number.
func handleGetEntry(sc *forge.StreamContext, store storage.Storage, feed *storage.FeedRecord, seq int) {
	ctx := sc.Ctx
	entry, err := store.GetFeedEntry(ctx, feed.ID, seq)
	if err != nil {
		sc.Logger.Error("failed to get feed entry", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}
	if entry == nil {
		sc.Response = &FeedResponse{Status: StatusNotFound}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"ETag":         entry.ContentHash,
			"X-Sequence":   entry.SequenceNumber,
			"X-Entry-Type": entry.EntryType,
			"X-Created-By": entry.CreatedByPeerID,
			"Content-Type": feed.EntryContentType,
			"Created-At":   entry.CreatedAt.UnixMilli(),
		},
		Body: base64.StdEncoding.EncodeToString(entry.Content),
	}
}

// handleGetEntries returns a range of feed entries.
func handleGetEntries(sc *forge.StreamContext, store storage.Storage, feed *storage.FeedRecord, req *FeedRequest) {
	limit := 50
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}

	ctx := sc.Ctx
	entries, hasMore, err := store.GetFeedEntries(ctx, feed.ID, req.FromSequence, req.ToSequence, req.EntryType, limit)
	if err != nil {
		sc.Logger.Error("failed to get feed entries", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	type entryJSON struct {
		Seq       int    `json:"seq"`
		Type      string `json:"type,omitempty"`
		Content   string `json:"content"` // base64
		Hash      string `json:"hash"`
		CreatedAt int64  `json:"createdAt"`
		CreatedBy string `json:"createdBy,omitempty"`
	}

	result := make([]entryJSON, 0, len(entries))
	for _, e := range entries {
		result = append(result, entryJSON{
			Seq:       e.SequenceNumber,
			Type:      e.EntryType,
			Content:   base64.StdEncoding.EncodeToString(e.Content),
			Hash:      e.ContentHash,
			CreatedAt: e.CreatedAt.UnixMilli(),
			CreatedBy: e.CreatedByPeerID,
		})
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"entries": result,
	})
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	headers := map[string]any{
		"Content-Type": "application/json",
		"X-Has-More":   hasMore,
	}
	if hasMore && len(entries) > 0 {
		headers["X-Next-Sequence"] = entries[len(entries)-1].SequenceNumber + 1
	}

	sc.Response = &FeedResponse{
		Status:  StatusOK,
		Headers: headers,
		Body:    base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// appendHandler handles the APPEND operation: adds an entry to a feed.
func appendHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*FeedRequest)
	ownerID, _ := sc.Get("ownerID")
	callerID := sc.PeerID

	ctx := sc.Ctx

	// Look up the feed
	feed, err := store.GetFeed(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		sc.Logger.Error("failed to get feed", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}
	if feed == nil {
		sc.Response = &FeedResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "feed not found"}}
		return
	}

	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}}
		return
	}

	// Entries per feed are capped by the server. The feed's own max_entries
	// is a rolling window applied by maintenance; this is the ceiling under
	// which a collaborative feed cannot be filled without limit by whoever
	// may append to it.
	if cfg, ok := forge.ServiceFrom[*core.ServerConfig](sc, "config"); ok && cfg != nil && cfg.MaxEntriesPerFeed > 0 {
		count, err := store.CountFeedEntries(ctx, feed.ID)
		if err != nil {
			sc.Logger.Error("failed to count feed entries", "error", err)
			sc.Response = &FeedResponse{Status: StatusInternalError}
			return
		}
		if count >= cfg.MaxEntriesPerFeed {
			sc.Err = &mailboxes.QuotaExceededError{What: "entries per feed", Current: count, Max: cfg.MaxEntriesPerFeed}
			return
		}
	}

	entry, err := store.AppendFeedEntry(ctx, feed.ID, content, callerID, req.EntryType)
	if err != nil {
		sc.Logger.Error("failed to append feed entry", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"X-Sequence": entry.SequenceNumber,
			"ETag":       entry.ContentHash,
		},
	}
}

// deleteHandler handles the DELETE operation: removes a feed and all its entries.
func deleteHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*FeedRequest)
	ownerID, _ := sc.Get("ownerID")

	ctx := sc.Ctx
	deleted, err := store.DeleteFeed(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		sc.Logger.Error("failed to delete feed", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	if !deleted {
		sc.Response = &FeedResponse{Status: StatusNotFound}
		return
	}

	sc.Response = &FeedResponse{Status: StatusNoContent}
}

// listHandler handles the LIST operation: returns all feeds for an owner.
func listHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx := sc.Ctx
	feeds, err := store.ListFeeds(ctx, ownerID.(peer.ID))
	if err != nil {
		sc.Logger.Error("failed to list feeds", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	type feedEntry struct {
		Path            string `json:"path"`
		Title           string `json:"title"`
		Description     string `json:"description"`
		CurrentSequence int    `json:"currentSequence"`
		LastEntryAt     int64  `json:"lastEntryAt"`
		CreatedAt       int64  `json:"createdAt"`
	}

	entries := make([]feedEntry, 0, len(feeds))
	for _, f := range feeds {
		entries = append(entries, feedEntry{
			Path:            f.Path,
			Title:           f.Title,
			Description:     f.Description,
			CurrentSequence: f.CurrentSequence,
			LastEntryAt:     f.LastEntryAt.UnixMilli(),
			CreatedAt:       f.CreatedAt.UnixMilli(),
		})
	}

	bodyBytes, err := json.Marshal(entries)
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// batchGetHandler handles the BATCH_GET operation: retrieves entries from multiple feeds in one request.
func batchGetHandler(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*FeedRequest)

	if len(req.BatchQueries) == 0 {
		sc.Response = &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "batchQueries is required"}}
		return
	}
	if len(req.BatchQueries) > 50 {
		sc.Response = &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "batchQueries exceeds maximum of 50"}}
		return
	}

	// Validate all paths and build storage queries.
	queries := make([]storage.MultiFeedQuery, 0, len(req.BatchQueries))
	for _, bq := range req.BatchQueries {
		if bq.OwnerPeerID == "" || bq.Path == "" {
			continue
		}
		if err := validatePath(bq.Path); err != nil {
			continue
		}
		limit := 50
		if bq.Limit != nil && *bq.Limit > 0 {
			limit = *bq.Limit
		}
		queries = append(queries, storage.MultiFeedQuery{
			OwnerPeerID:  bq.OwnerPeerID,
			Path:         bq.Path,
			FromSequence: bq.FromSequence,
			Limit:        limit,
		})
	}

	ctx := sc.Ctx
	results, err := store.GetMultiFeedEntries(ctx, queries)
	if err != nil {
		sc.Logger.Error("failed to batch get feed entries", "error", err)
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	// Build response: {"feeds": {"owner/path": {"entries": [...], "hasMore": bool}}}
	type entryJSON struct {
		Seq       int    `json:"seq"`
		Type      string `json:"type,omitempty"`
		Content   string `json:"content"`
		Hash      string `json:"hash"`
		CreatedAt int64  `json:"createdAt"`
		CreatedBy string `json:"createdBy,omitempty"`
	}

	type feedResult struct {
		Entries []entryJSON `json:"entries"`
		HasMore bool        `json:"hasMore"`
		Error   string      `json:"error,omitempty"`
	}

	feeds := make(map[string]*feedResult, len(results))
	for key, mr := range results {
		fr := &feedResult{
			HasMore: mr.HasMore,
			Error:   mr.Error,
			Entries: make([]entryJSON, 0, len(mr.Entries)),
		}
		for _, e := range mr.Entries {
			fr.Entries = append(fr.Entries, entryJSON{
				Seq:       e.SequenceNumber,
				Type:      e.EntryType,
				Content:   base64.StdEncoding.EncodeToString(e.Content),
				Hash:      e.ContentHash,
				CreatedAt: e.CreatedAt.UnixMilli(),
				CreatedBy: e.CreatedByPeerID,
			})
		}
		feeds[key] = fr
	}

	bodyBytes, err := json.Marshal(map[string]any{"feeds": feeds})
	if err != nil {
		sc.Response = &FeedResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// =============================================================================
// Helpers
// =============================================================================

// validatePath checks that a feed path conforms to requirements.
func validatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	if len(path) > maxPathLength {
		return fmt.Errorf("path exceeds maximum length of %d characters", maxPathLength)
	}
	if path[0] == '/' {
		return fmt.Errorf("path must not start with /")
	}
	if containsDotDot(path) {
		return fmt.Errorf("path must not contain ..")
	}
	if !validPathRe.MatchString(path) {
		return fmt.Errorf("path contains invalid characters (allowed: alphanumeric, -, _, /)")
	}
	return nil
}

func containsDotDot(path string) bool {
	for i := 0; i < len(path)-1; i++ {
		if path[i] == '.' && path[i+1] == '.' {
			return true
		}
	}
	return false
}
