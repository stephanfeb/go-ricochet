package sca

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

// ProtocolID is the SCA protocol identifier.
const ProtocolID = protocol.ID("/ricochet/store/collection/1.0.0")

// Collection operation constants.
const (
	OpCREATE = "CREATE"
	OpGET    = "GET"
	OpPUT    = "PUT"
	OpDELETE = "DELETE"
	OpLIST   = "LIST"
	OpQUERY  = "QUERY"
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

// CollectionRequest is the JSON request format for collection operations.
type CollectionRequest struct {
	Operation   string            `json:"operation"`
	OwnerPeerID string            `json:"ownerPeerId"`
	Path        string            `json:"path,omitempty"`
	Key         string            `json:"key,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"` // base64 encoded

	// CREATE parameters
	Name string `json:"name,omitempty"`

	// QUERY parameters
	Filter    map[string]any `json:"filter,omitempty"`
	SortField string         `json:"sortField,omitempty"`
	SortAsc   *bool          `json:"sortAsc,omitempty"`
	Limit     *int           `json:"limit,omitempty"`
	Offset    *int           `json:"offset,omitempty"`
}

// CollectionResponse is the JSON response format for collection operations.
type CollectionResponse struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    string         `json:"body,omitempty"` // base64 encoded
}

// Handler is the Store Collection Access protocol handler.
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

// NewHandler creates a new SCA handler.
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

// HandleStream handles an incoming collection access stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.Close()

	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest})
		return
	}

	var req CollectionRequest
	if err := json.Unmarshal(data, &req); err != nil {
		h.logger.Error("failed to parse request", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest})
		return
	}

	h.logger.Debug("handling collection request",
		"operation", req.Operation,
		"owner", req.OwnerPeerID,
		"path", req.Path,
		"key", req.Key,
		"caller", callerID.String(),
	)

	// Determine if this is a read or write operation and check rate limits
	isWrite := req.Operation == OpCREATE || req.Operation == OpPUT || req.Operation == OpDELETE
	if isWrite {
		if !h.checkRateLimit(callerID, true) {
			h.writeResponse(s, &CollectionResponse{Status: StatusTooManyRequests})
			return
		}
	} else {
		if !h.checkRateLimit(callerID, false) {
			h.writeResponse(s, &CollectionResponse{Status: StatusTooManyRequests})
			return
		}
	}

	// Parse owner peer ID
	if req.OwnerPeerID == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "ownerPeerId is required"}})
		return
	}
	ownerID, err := peer.Decode(req.OwnerPeerID)
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid ownerPeerId"}})
		return
	}

	// Validate path for operations that require it
	needsPath := req.Operation != OpLIST || req.Path != ""
	if needsPath && req.Path != "" {
		if err := validatePath(req.Path); err != nil {
			h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": err.Error()}})
			return
		}
	}

	// Enforce owner-only access for write operations
	if isWrite && callerID != ownerID {
		h.writeResponse(s, &CollectionResponse{Status: StatusForbidden,
			Headers: map[string]any{"Error": "write operations require owner access"}})
		return
	}

	ctx := context.Background()

	switch req.Operation {
	case OpCREATE:
		h.handleCreate(ctx, s, &req, ownerID)
	case OpGET:
		if req.Key != "" {
			h.handleGetItem(ctx, s, &req, ownerID)
		} else {
			h.handleGetMetadata(ctx, s, &req, ownerID)
		}
	case OpPUT:
		h.handlePut(ctx, s, &req, ownerID, callerID)
	case OpDELETE:
		if req.Key != "" {
			h.handleDeleteItem(ctx, s, &req, ownerID)
		} else {
			h.handleDeleteCollection(ctx, s, &req, ownerID)
		}
	case OpLIST:
		if req.Path != "" {
			h.handleListKeys(ctx, s, &req, ownerID)
		} else {
			h.handleListCollections(ctx, s, ownerID)
		}
	case OpQUERY:
		h.handleQuery(ctx, s, &req, ownerID)
	default:
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "unknown operation: " + req.Operation}})
	}
}

// handleCreate creates a new collection.
func (h *Handler) handleCreate(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	if req.Path == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}})
		return
	}

	coll, err := h.store.CreateCollection(ctx, ownerID, req.Path, req.Name)
	if err != nil {
		h.logger.Error("failed to create collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"id":          coll.ID,
		"path":        coll.Path,
		"name":        coll.Name,
		"recordCount": coll.RecordCount,
	})
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleGetMetadata returns collection metadata.
func (h *Handler) handleGetMetadata(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	if req.Path == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}})
		return
	}

	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound})
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"path":           coll.Path,
		"name":           coll.Name,
		"recordCount":    coll.RecordCount,
		"lastModifiedAt": coll.LastModifiedAt.UnixMilli(),
		"createdAt":      coll.CreatedAt.UnixMilli(),
	})
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":   "application/json",
			"X-Record-Count": coll.RecordCount,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleGetItem returns a single collection item by key.
func (h *Handler) handleGetItem(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}})
		return
	}

	item, err := h.store.GetCollectionItem(ctx, coll.ID, req.Key)
	if err != nil {
		h.logger.Error("failed to get collection item", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if item == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "item not found"}})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
			"ETag":         item.ContentHash,
			"X-Version":    item.Version,
			"Updated-At":   item.UpdatedAt.UnixMilli(),
			"Created-At":   item.CreatedAt.UnixMilli(),
		},
		Body: base64.StdEncoding.EncodeToString(item.Content),
	})
}

// handlePut upserts a collection item.
func (h *Handler) handlePut(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID, callerID peer.ID) {
	if req.Path == "" || req.Key == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path and key are required"}})
		return
	}

	// Look up collection
	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}})
		return
	}

	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}})
		return
	}

	// Validate that content is valid JSON (required for JSONB storage)
	if !json.Valid(content) {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "body must be valid JSON"}})
		return
	}

	// Check If-Match header for optimistic locking
	var ifMatch *string
	if match, ok := req.Headers["If-Match"]; ok {
		ifMatch = &match
	}

	item, created, err := h.store.PutCollectionItem(ctx, coll.ID, req.Key, content, callerID, ifMatch)
	if err != nil {
		if _, ok := err.(*storage.CollectionItemConflictError); ok {
			h.writeResponse(s, &CollectionResponse{Status: StatusConflict,
				Headers: map[string]any{"Error": err.Error()}})
			return
		}
		h.logger.Error("failed to put collection item", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	status := StatusOK
	if created {
		status = StatusCreated
	}

	h.writeResponse(s, &CollectionResponse{
		Status: status,
		Headers: map[string]any{
			"ETag":      item.ContentHash,
			"X-Version": item.Version,
		},
	})
}

// handleDeleteCollection removes a collection and all its items.
func (h *Handler) handleDeleteCollection(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	if req.Path == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}})
		return
	}

	deleted, err := h.store.DeleteCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to delete collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if !deleted {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound})
		return
	}

	h.writeResponse(s, &CollectionResponse{Status: StatusNoContent})
}

// handleDeleteItem removes a single item from a collection.
func (h *Handler) handleDeleteItem(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}})
		return
	}

	deleted, err := h.store.DeleteCollectionItem(ctx, coll.ID, req.Key)
	if err != nil {
		h.logger.Error("failed to delete collection item", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if !deleted {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "item not found"}})
		return
	}

	h.writeResponse(s, &CollectionResponse{Status: StatusNoContent})
}

// handleListCollections returns all collections for an owner.
func (h *Handler) handleListCollections(ctx context.Context, s network.Stream, ownerID peer.ID) {
	collections, err := h.store.ListCollections(ctx, ownerID)
	if err != nil {
		h.logger.Error("failed to list collections", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	type collEntry struct {
		Path           string `json:"path"`
		Name           string `json:"name"`
		RecordCount    int    `json:"recordCount"`
		LastModifiedAt int64  `json:"lastModifiedAt"`
		CreatedAt      int64  `json:"createdAt"`
	}

	entries := make([]collEntry, 0, len(collections))
	for _, c := range collections {
		entries = append(entries, collEntry{
			Path:           c.Path,
			Name:           c.Name,
			RecordCount:    c.RecordCount,
			LastModifiedAt: c.LastModifiedAt.UnixMilli(),
			CreatedAt:      c.CreatedAt.UnixMilli(),
		})
	}

	bodyBytes, err := json.Marshal(entries)
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleListKeys returns all keys in a collection.
func (h *Handler) handleListKeys(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}})
		return
	}

	limit := 50
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}
	offset := 0
	if req.Offset != nil && *req.Offset >= 0 {
		offset = *req.Offset
	}

	keys, totalCount, err := h.store.ListCollectionKeys(ctx, coll.ID, limit, offset)
	if err != nil {
		h.logger.Error("failed to list collection keys", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"keys": keys,
	})
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":  "application/json",
			"X-Total-Count": totalCount,
			"X-Has-More":    offset+limit < totalCount,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleQuery queries collection items using JSONB filters.
func (h *Handler) handleQuery(ctx context.Context, s network.Stream, req *CollectionRequest, ownerID peer.ID) {
	if req.Path == "" {
		h.writeResponse(s, &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}})
		return
	}

	coll, err := h.store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to get collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}
	if coll == nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}})
		return
	}

	limit := 50
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}
	offset := 0
	if req.Offset != nil && *req.Offset >= 0 {
		offset = *req.Offset
	}
	sortAsc := true
	if req.SortAsc != nil {
		sortAsc = *req.SortAsc
	}

	result, err := h.store.QueryCollection(ctx, coll.ID, req.Filter, req.SortField, sortAsc, limit, offset)
	if err != nil {
		h.logger.Error("failed to query collection", "error", err)
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError,
			Headers: map[string]any{"Error": err.Error()}})
		return
	}

	type itemJSON struct {
		Key       string          `json:"key"`
		Content   json.RawMessage `json:"content"`
		Hash      string          `json:"hash"`
		Version   int             `json:"version"`
		UpdatedAt int64           `json:"updatedAt"`
	}

	items := make([]itemJSON, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, itemJSON{
			Key:       item.Key,
			Content:   json.RawMessage(item.Content),
			Hash:      item.ContentHash,
			Version:   item.Version,
			UpdatedAt: item.UpdatedAt.UnixMilli(),
		})
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"items": items,
	})
	if err != nil {
		h.writeResponse(s, &CollectionResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":  "application/json",
			"X-Total-Count": result.TotalCount,
			"X-Has-More":    result.HasMore,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// =============================================================================
// Helpers
// =============================================================================

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

func (h *Handler) writeResponse(s network.Stream, resp *CollectionResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("failed to marshal response", "error", err)
		return
	}
	if err := frame.WriteFrame(s, data); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}
}
