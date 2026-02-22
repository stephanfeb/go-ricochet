package sfa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/storage"
)

// ProtocolID is the SFA protocol identifier.
const ProtocolID = protocol.ID("/ricochet/store/feed/1.0.0")

// Feed operation constants.
const (
	OpCREATE = "CREATE"
	OpGET    = "GET"
	OpAPPEND = "APPEND"
	OpDELETE = "DELETE"
	OpLIST   = "LIST"
)

// Path validation constants.
const maxPathLength = 256

var validPathRe = regexp.MustCompile(`^[a-zA-Z0-9\-_/]+$`)

// HTTP-style status codes used in responses.
const (
	StatusOK              = 200
	StatusCreated         = 201
	StatusNoContent       = 204
	StatusBadRequest      = 400
	StatusForbidden       = 403
	StatusNotFound        = 404
	StatusConflict        = 409
	StatusTooManyRequests = 429
	StatusInternalError   = 500
)

// FeedRequest is the JSON request format for feed operations.
type FeedRequest struct {
	Operation   string            `json:"operation"`
	OwnerPeerID string            `json:"ownerPeerId"`
	Path        string            `json:"path,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"` // base64 encoded

	// CREATE parameters
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`

	// APPEND parameters
	EntryType string `json:"entryType,omitempty"`

	// GET parameters: single entry
	SequenceNumber *int `json:"sequenceNumber,omitempty"`

	// GET parameters: range query
	FromSequence *int `json:"fromSequence,omitempty"`
	ToSequence   *int `json:"toSequence,omitempty"`
	Limit        *int `json:"limit,omitempty"`
}

// FeedResponse is the JSON response format for feed operations.
type FeedResponse struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    string         `json:"body,omitempty"` // base64 encoded
}

// Handler is the Store Feed Access protocol handler.
type Handler struct {
	store  storage.Storage
	logger *slog.Logger

	// Rate limiting: separate buckets for read and write operations.
	rateLimitMu      sync.Mutex
	readHistory      map[string][]time.Time
	writeHistory     map[string][]time.Time
	rateLimitWindow  time.Duration
	maxReadRequests  int
	maxWriteRequests int
}

// NewHandler creates a new SFA handler.
func NewHandler(store storage.Storage, logger *slog.Logger) *Handler {
	return &Handler{
		store:            store,
		logger:           logger,
		readHistory:      make(map[string][]time.Time),
		writeHistory:     make(map[string][]time.Time),
		rateLimitWindow:  time.Minute,
		maxReadRequests:  100,
		maxWriteRequests: 20,
	}
}

// HandleStream handles an incoming feed access stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.Close()

	// Read length-prefixed frame
	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest})
		return
	}

	// Parse request
	var req FeedRequest
	if err := json.Unmarshal(data, &req); err != nil {
		h.logger.Error("failed to parse request", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest})
		return
	}

	h.logger.Debug("handling feed request",
		"operation", req.Operation,
		"owner", req.OwnerPeerID,
		"path", req.Path,
		"caller", callerID.String(),
	)

	// Determine if this is a read or write operation and check rate limits
	isWrite := req.Operation == OpCREATE || req.Operation == OpAPPEND || req.Operation == OpDELETE
	if isWrite {
		if !h.checkRateLimit(callerID, true) {
			h.writeResponse(s, &FeedResponse{Status: StatusTooManyRequests})
			return
		}
	} else {
		if !h.checkRateLimit(callerID, false) {
			h.writeResponse(s, &FeedResponse{Status: StatusTooManyRequests})
			return
		}
	}

	// Parse owner peer ID
	if req.OwnerPeerID == "" {
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "ownerPeerId is required"}})
		return
	}
	ownerID, err := peer.Decode(req.OwnerPeerID)
	if err != nil {
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid ownerPeerId"}})
		return
	}

	// Validate path for operations that require it
	if req.Operation != OpLIST {
		if err := validatePath(req.Path); err != nil {
			h.writeResponse(s, &FeedResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": err.Error()}})
			return
		}
	}

	// Enforce owner-only access for write operations
	if isWrite && callerID != ownerID {
		h.writeResponse(s, &FeedResponse{Status: StatusForbidden,
			Headers: map[string]any{"Error": "write operations require owner access"}})
		return
	}

	ctx := context.Background()

	switch req.Operation {
	case OpCREATE:
		h.handleCreate(ctx, s, &req, ownerID)
	case OpGET:
		h.handleGet(ctx, s, &req, ownerID)
	case OpAPPEND:
		h.handleAppend(ctx, s, &req, ownerID, callerID)
	case OpDELETE:
		h.handleDelete(ctx, s, &req, ownerID)
	case OpLIST:
		h.handleList(ctx, s, ownerID)
	default:
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "unknown operation: " + req.Operation}})
	}
}

// handleCreate creates a new feed.
func (h *Handler) handleCreate(ctx context.Context, s network.Stream, req *FeedRequest, ownerID peer.ID) {
	feed, err := h.store.CreateFeed(ctx, ownerID, req.Path, req.Title, req.Description)
	if err != nil {
		h.logger.Error("failed to create feed", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"id":              feed.ID,
		"path":            feed.Path,
		"title":           feed.Title,
		"description":     feed.Description,
		"currentSequence": feed.CurrentSequence,
	})
	if err != nil {
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &FeedResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleGet handles feed metadata, single entry, or range queries.
func (h *Handler) handleGet(ctx context.Context, s network.Stream, req *FeedRequest, ownerID peer.ID) {
	// Look up the feed
	feed, err := h.store.GetFeed(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get feed", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}
	if feed == nil {
		h.writeResponse(s, &FeedResponse{Status: StatusNotFound})
		return
	}

	// Dispatch based on request parameters
	if req.SequenceNumber != nil {
		// Single entry by sequence number
		h.handleGetEntry(ctx, s, feed, *req.SequenceNumber)
	} else if req.FromSequence != nil || req.ToSequence != nil || req.Limit != nil {
		// Range query
		h.handleGetEntries(ctx, s, feed, req)
	} else {
		// Feed metadata
		h.handleGetMetadata(s, feed)
	}
}

// handleGetMetadata returns feed metadata.
func (h *Handler) handleGetMetadata(s network.Stream, feed *storage.FeedRecord) {
	bodyBytes, err := json.Marshal(map[string]any{
		"title":           feed.Title,
		"description":     feed.Description,
		"currentSequence": feed.CurrentSequence,
		"lastEntryAt":     feed.LastEntryAt.UnixMilli(),
		"createdAt":       feed.CreatedAt.UnixMilli(),
	})
	if err != nil {
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":    "application/json",
			"X-Sequence":      feed.CurrentSequence,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleGetEntry returns a single feed entry by sequence number.
func (h *Handler) handleGetEntry(ctx context.Context, s network.Stream, feed *storage.FeedRecord, seq int) {
	entry, err := h.store.GetFeedEntry(ctx, feed.ID, seq)
	if err != nil {
		h.logger.Error("failed to get feed entry", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}
	if entry == nil {
		h.writeResponse(s, &FeedResponse{Status: StatusNotFound})
		return
	}

	h.writeResponse(s, &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"ETag":           entry.ContentHash,
			"X-Sequence":     entry.SequenceNumber,
			"X-Entry-Type":   entry.EntryType,
			"Content-Type":   feed.EntryContentType,
			"Created-At":     entry.CreatedAt.UnixMilli(),
		},
		Body: base64.StdEncoding.EncodeToString(entry.Content),
	})
}

// handleGetEntries returns a range of feed entries.
func (h *Handler) handleGetEntries(ctx context.Context, s network.Stream, feed *storage.FeedRecord, req *FeedRequest) {
	limit := 50
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}

	entries, hasMore, err := h.store.GetFeedEntries(ctx, feed.ID, req.FromSequence, req.ToSequence, req.EntryType, limit)
	if err != nil {
		h.logger.Error("failed to get feed entries", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	type entryJSON struct {
		Seq       int    `json:"seq"`
		Type      string `json:"type,omitempty"`
		Content   string `json:"content"`          // base64
		Hash      string `json:"hash"`
		CreatedAt int64  `json:"createdAt"`
	}

	result := make([]entryJSON, 0, len(entries))
	for _, e := range entries {
		result = append(result, entryJSON{
			Seq:       e.SequenceNumber,
			Type:      e.EntryType,
			Content:   base64.StdEncoding.EncodeToString(e.Content),
			Hash:      e.ContentHash,
			CreatedAt: e.CreatedAt.UnixMilli(),
		})
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"entries": result,
	})
	if err != nil {
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	headers := map[string]any{
		"Content-Type": "application/json",
		"X-Has-More":   hasMore,
	}
	if hasMore && len(entries) > 0 {
		headers["X-Next-Sequence"] = entries[len(entries)-1].SequenceNumber + 1
	}

	h.writeResponse(s, &FeedResponse{
		Status:  StatusOK,
		Headers: headers,
		Body:    base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleAppend adds an entry to a feed.
func (h *Handler) handleAppend(ctx context.Context, s network.Stream, req *FeedRequest, ownerID, callerID peer.ID) {
	// Look up the feed
	feed, err := h.store.GetFeed(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get feed", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}
	if feed == nil {
		h.writeResponse(s, &FeedResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "feed not found"}})
		return
	}

	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		h.writeResponse(s, &FeedResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}})
		return
	}

	entry, err := h.store.AppendFeedEntry(ctx, feed.ID, content, callerID, req.EntryType)
	if err != nil {
		h.logger.Error("failed to append feed entry", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &FeedResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"X-Sequence": entry.SequenceNumber,
			"ETag":       entry.ContentHash,
		},
	})
}

// handleDelete removes a feed and all its entries.
func (h *Handler) handleDelete(ctx context.Context, s network.Stream, req *FeedRequest, ownerID peer.ID) {
	deleted, err := h.store.DeleteFeed(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to delete feed", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	if !deleted {
		h.writeResponse(s, &FeedResponse{Status: StatusNotFound})
		return
	}

	h.writeResponse(s, &FeedResponse{Status: StatusNoContent})
}

// handleList returns all feeds for an owner.
func (h *Handler) handleList(ctx context.Context, s network.Stream, ownerID peer.ID) {
	feeds, err := h.store.ListFeeds(ctx, ownerID)
	if err != nil {
		h.logger.Error("failed to list feeds", "error", err)
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
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
		h.writeResponse(s, &FeedResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &FeedResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
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

// checkRateLimit enforces per-peer rate limiting with separate read/write buckets.
func (h *Handler) checkRateLimit(peerID peer.ID, isWrite bool) bool {
	h.rateLimitMu.Lock()
	defer h.rateLimitMu.Unlock()

	now := time.Now()
	key := peerID.String()
	cutoff := now.Add(-h.rateLimitWindow)

	var historyMap map[string][]time.Time
	var maxReqs int
	if isWrite {
		historyMap = h.writeHistory
		maxReqs = h.maxWriteRequests
	} else {
		historyMap = h.readHistory
		maxReqs = h.maxReadRequests
	}

	history := historyMap[key]
	filtered := history[:0]
	for _, ts := range history {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}

	if len(filtered) >= maxReqs {
		historyMap[key] = filtered
		return false
	}

	historyMap[key] = append(filtered, now)
	return true
}

// writeResponse marshals and writes a FeedResponse over the stream.
func (h *Handler) writeResponse(s network.Stream, resp *FeedResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("failed to marshal response", "error", err)
		return
	}
	if err := frame.WriteFrame(s, data); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}
}

