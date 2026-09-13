package sca

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
	"github.com/twostack/go-ricochet/internal/ratelimit"
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

// StatusCode reports the response status. It is how the metrics middleware
// tells a request the handler refused — a 404 or a 409 — from one that
// succeeded: neither sets a pipeline error, so without this both would be
// counted as ok.
func (r *CollectionResponse) StatusCode() int { return r.Status }

// NewPipeline creates a forge pipeline for the Store Collection Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).SCA

	return wire.Bounded(forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "sca", ""),
		wire.AccessLog("sca", ""),
		middleware.Recovery(),
		collectionResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		wire.RequestDeadline(reg),
		middleware.DualRateLimitMiddleware(limiter, isWriteClassifier),
		admission.Middleware(admission.FromRegistry(reg)),
		capacity.WriteGate(capacity.FromRegistry(reg), isWriteClassifier),
		forge.JSONDeserialize[CollectionRequest](),
		commonValidation(),
		middleware.OperationRouter("operation", map[string]forge.Middleware{
			OpCREATE: handleCreate,
			OpGET:    handleGet,
			OpPUT:    handlePut,
			OpDELETE: handleDelete,
			OpLIST:   handleList,
			OpQUERY:  handleQuery,
		}),
	).WithRegistry(reg), reg)
}

// isWriteClassifier inspects the raw JSON bytes to determine if a request is
// a write operation (CREATE, PUT, DELETE) for dual rate limiting.
func isWriteClassifier(raw []byte) bool {
	// Quick scan for the "operation" field value.
	type opOnly struct {
		Operation string `json:"operation"`
	}
	var op opOnly
	if err := json.Unmarshal(raw, &op); err != nil {
		return false
	}
	switch op.Operation {
	case OpCREATE, OpPUT, OpDELETE:
		return true
	default:
		return false
	}
}

// collectionResponseWriter writes a CollectionResponse as a JSON frame.
// On pipeline error, it converts the error into an error response so the
// client always gets a response.
func collectionResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into error responses.
		if sc.Err != nil && sc.Response == nil {
			status, retryAfter := wire.Classify(sc.Err)

			wire.LogRejection(sc, status, sc.Err)
			resp := &CollectionResponse{Status: status}
			resp.Headers = map[string]any{"Error": wire.ClientMessage(sc.Err)}
			if ms := wire.RetryAfterMs(retryAfter); ms > 0 {
				resp.Headers[wire.RetryAfterHeaderKey] = ms
			}
			sc.Response = resp
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

// commonValidation validates ownerPeerId presence, parses it, validates path
// where required, and enforces owner-only access for write operations.
func commonValidation() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		req := sc.Request.(*CollectionRequest)

		// Parse owner peer ID
		if req.OwnerPeerID == "" {
			sc.Response = &CollectionResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "ownerPeerId is required"}}
			return
		}
		ownerID, err := peer.Decode(req.OwnerPeerID)
		if err != nil {
			sc.Response = &CollectionResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "invalid ownerPeerId"}}
			return
		}

		// Store the parsed owner ID for downstream handlers.
		sc.Set("ownerID", ownerID)

		// Validate path for operations that require it
		needsPath := req.Operation != OpLIST || req.Path != ""
		if needsPath && req.Path != "" {
			if err := validatePath(req.Path); err != nil {
				sc.Response = &CollectionResponse{Status: StatusBadRequest,
					Headers: map[string]any{"Error": err.Error()}}
				return
			}
		}

		// A key and a name are index keys and listing text; bound them
		// before any operation sees them.
		if err := wire.CheckString("key", req.Key, wire.MaxCollectionKeyLength); err != nil {
			sc.Response = &CollectionResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": err.Error()}}
			return
		}
		if err := wire.CheckString("name", req.Name, wire.MaxCollectionNameLength); err != nil {
			sc.Response = &CollectionResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": err.Error()}}
			return
		}

		// Enforce owner-only access for write operations
		isWrite := req.Operation == OpCREATE || req.Operation == OpPUT || req.Operation == OpDELETE
		if isWrite && sc.PeerID != ownerID {
			sc.Response = &CollectionResponse{Status: StatusForbidden,
				Headers: map[string]any{"Error": "write operations require owner access"}}
			return
		}

		sc.Logger.Debug("handling collection request",
			"operation", req.Operation,
			"owner", req.OwnerPeerID,
			"path", req.Path,
			"key", req.Key,
			"caller", sc.PeerID.String(),
		)

		next()
	}
}

// ownerIDFrom retrieves the parsed owner peer.ID from the StreamContext.
func ownerIDFrom(sc *forge.StreamContext) peer.ID {
	v, _ := sc.Get("ownerID")
	return v.(peer.ID)
}

// handleCreate creates a new collection.
func handleCreate(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	if req.Path == "" {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}}
		return
	}

	ctx := sc.Ctx
	coll, err := store.CreateCollection(ctx, ownerID, req.Path, req.Name)
	if err != nil {
		sc.Logger.Error("failed to create collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"id":          coll.ID,
		"path":        coll.Path,
		"name":        coll.Name,
		"recordCount": coll.RecordCount,
	})
	if err != nil {
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusCreated,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleGet dispatches to getMetadata or getItem based on Key presence.
func handleGet(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*CollectionRequest)

	if req.Key != "" {
		handleGetItem(sc, next)
	} else {
		handleGetMetadata(sc, next)
	}
}

// handleGetMetadata returns collection metadata.
func handleGetMetadata(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	if req.Path == "" {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}}
		return
	}

	ctx := sc.Ctx
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound}
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
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":   "application/json",
			"X-Record-Count": coll.RecordCount,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleGetItem returns a single collection item by key.
func handleGetItem(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	ctx := sc.Ctx
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}}
		return
	}

	item, err := store.GetCollectionItem(ctx, coll.ID, req.Key)
	if err != nil {
		sc.Logger.Error("failed to get collection item", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if item == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "item not found"}}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
			"ETag":         item.ContentHash,
			"X-Version":    item.Version,
			"Updated-At":   item.UpdatedAt.UnixMilli(),
			"Created-At":   item.CreatedAt.UnixMilli(),
		},
		Body: base64.StdEncoding.EncodeToString(item.Content),
	}
}

// handlePut upserts a collection item.
func handlePut(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)
	callerID := sc.PeerID

	if req.Path == "" || req.Key == "" {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path and key are required"}}
		return
	}

	ctx := sc.Ctx

	// Look up collection
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}}
		return
	}

	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}}
		return
	}

	// Validate that content is valid JSON (required for JSONB storage)
	if !json.Valid(content) {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "body must be valid JSON"}}
		return
	}

	// Check If-Match header for optimistic locking
	var ifMatch *string
	if match, ok := req.Headers["If-Match"]; ok {
		ifMatch = &match
	}

	item, created, err := store.PutCollectionItem(ctx, coll.ID, req.Key, content, callerID, ifMatch)
	if err != nil {
		if _, ok := err.(*storage.CollectionItemConflictError); ok {
			sc.Response = &CollectionResponse{Status: StatusConflict,
				Headers: map[string]any{"Error": err.Error()}}
			return
		}
		sc.Logger.Error("failed to put collection item", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	status := StatusOK
	if created {
		status = StatusCreated
	}

	sc.Response = &CollectionResponse{
		Status: status,
		Headers: map[string]any{
			"ETag":      item.ContentHash,
			"X-Version": item.Version,
		},
	}
}

// handleDelete dispatches to deleteCollection or deleteItem based on Key presence.
func handleDelete(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*CollectionRequest)

	if req.Key != "" {
		handleDeleteItem(sc, next)
	} else {
		handleDeleteCollection(sc, next)
	}
}

// handleDeleteCollection removes a collection and all its items.
func handleDeleteCollection(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	if req.Path == "" {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}}
		return
	}

	ctx := sc.Ctx
	deleted, err := store.DeleteCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to delete collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if !deleted {
		sc.Response = &CollectionResponse{Status: StatusNotFound}
		return
	}

	sc.Response = &CollectionResponse{Status: StatusNoContent}
}

// handleDeleteItem removes a single item from a collection.
func handleDeleteItem(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	ctx := sc.Ctx
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}}
		return
	}

	deleted, err := store.DeleteCollectionItem(ctx, coll.ID, req.Key)
	if err != nil {
		sc.Logger.Error("failed to delete collection item", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if !deleted {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "item not found"}}
		return
	}

	sc.Response = &CollectionResponse{Status: StatusNoContent}
}

// handleList dispatches to listCollections or listKeys based on Path presence.
func handleList(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*CollectionRequest)

	if req.Path != "" {
		handleListKeys(sc, next)
	} else {
		handleListCollections(sc, next)
	}
}

// handleListCollections returns all collections for an owner.
func handleListCollections(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID := ownerIDFrom(sc)

	ctx := sc.Ctx
	collections, err := store.ListCollections(ctx, ownerID)
	if err != nil {
		sc.Logger.Error("failed to list collections", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
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
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleListKeys returns all keys in a collection.
func handleListKeys(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	ctx := sc.Ctx
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}}
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

	keys, totalCount, err := store.ListCollectionKeys(ctx, coll.ID, limit, offset)
	if err != nil {
		sc.Logger.Error("failed to list collection keys", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"keys": keys,
	})
	if err != nil {
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":  "application/json",
			"X-Total-Count": totalCount,
			"X-Has-More":    offset+limit < totalCount,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleQuery queries collection items using JSONB filters.
func handleQuery(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	req := sc.Request.(*CollectionRequest)
	ownerID := ownerIDFrom(sc)

	if req.Path == "" {
		sc.Response = &CollectionResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "path is required"}}
		return
	}

	ctx := sc.Ctx
	coll, err := store.GetCollection(ctx, ownerID, req.Path)
	if err != nil {
		sc.Logger.Error("failed to get collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}
	if coll == nil {
		sc.Response = &CollectionResponse{Status: StatusNotFound,
			Headers: map[string]any{"Error": "collection not found"}}
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

	result, err := store.QueryCollection(ctx, coll.ID, req.Filter, req.SortField, sortAsc, limit, offset)
	if err != nil {
		sc.Logger.Error("failed to query collection", "error", err)
		sc.Response = &CollectionResponse{Status: StatusInternalError,
			Headers: map[string]any{"Error": wire.ClientMessage(err)}}
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
		sc.Response = &CollectionResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &CollectionResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type":  "application/json",
			"X-Total-Count": result.TotalCount,
			"X-Has-More":    result.HasMore,
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
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
