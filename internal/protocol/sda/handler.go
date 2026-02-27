package sda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/storage"
)

// ProtocolID is the SDA protocol identifier.
const ProtocolID = protocol.ID("/ricochet/store/doc/1.0.0")

// Document operation constants.
const (
	OpGET       = "GET"
	OpPUT       = "PUT"
	OpPATCH     = "PATCH"
	OpHEAD      = "HEAD"
	OpDELETE    = "DELETE"
	OpLIST      = "LIST"
	OpHISTORY   = "HISTORY"
	OpDIRECTORY = "DIRECTORY"
)

// Path validation constants.
const (
	maxPathLength = 256
)

// validPathRe matches paths containing only alphanumeric characters, hyphens,
// underscores, and forward slashes.
var validPathRe = regexp.MustCompile(`^[a-zA-Z0-9\-_/]+$`)

// HTTP-style status codes used in responses.
const (
	StatusOK              = 200
	StatusCreated         = 201
	StatusNoContent       = 204
	StatusNotModified     = 304
	StatusBadRequest      = 400
	StatusForbidden       = 403
	StatusNotFound        = 404
	StatusConflict        = 409
	StatusPayloadTooLarge = 413
	StatusTooManyRequests = 429
	StatusInternalError   = 500
)

// DocRequest is the JSON request format for document operations.
type DocRequest struct {
	Operation   string            `json:"operation"`
	OwnerPeerID string            `json:"ownerPeerId"`
	Path        string            `json:"path,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"` // base64 encoded

	// PATCH-specific: JSON patch object sent as the body, decoded separately.
	// HISTORY-specific
	MaxVersions   *int `json:"maxVersions,omitempty"`
	VersionNumber *int `json:"versionNumber,omitempty"`

	// DIRECTORY-specific
	DirectoryAction string `json:"directoryAction,omitempty"`
	DirectoryQuery  string `json:"directoryQuery,omitempty"`
	DirectoryCursor string `json:"directoryCursor,omitempty"`
	DirectoryLimit  *int   `json:"directoryLimit,omitempty"`
}

// DocResponse is the JSON response format for document operations.
type DocResponse struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    string         `json:"body,omitempty"` // base64 encoded
}

// directoryListingPath is the well-known document path that triggers directory materialization.
const directoryListingPath = "directory-listing"

// NewPipeline creates a forge pipeline for the Store Document Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := middleware.NewDualBucket(time.Minute, 100, 20)

	routes := map[string]forge.Middleware{
		OpGET:       middleware.Chain(forge.JSONDeserialize[DocRequest](), handleGet),
		OpPUT:       middleware.Chain(forge.JSONDeserialize[DocRequest](), handlePut),
		OpPATCH:     middleware.Chain(forge.JSONDeserialize[DocRequest](), handlePatch),
		OpHEAD:      middleware.Chain(forge.JSONDeserialize[DocRequest](), handleHead),
		OpDELETE:    middleware.Chain(forge.JSONDeserialize[DocRequest](), handleDelete),
		OpLIST:      middleware.Chain(forge.JSONDeserialize[DocRequest](), handleList),
		OpHISTORY:   middleware.Chain(forge.JSONDeserialize[DocRequest](), handleHistory),
		OpDIRECTORY: middleware.Chain(forge.JSONDeserialize[DocRequest](), handleDirectory),
	}

	return forge.NewPipeline(logger,
		middleware.Recovery(),
		docResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.DualRateLimitMiddleware(limiter, isWriteClassifier),
		commonValidation(),
		middleware.OperationRouter("operation", routes),
	).WithRegistry(reg)
}

// isWriteClassifier peeks at the raw bytes to determine if the request is a write operation.
func isWriteClassifier(raw []byte) bool {
	var envelope struct {
		Operation       string `json:"operation"`
		DirectoryAction string `json:"directoryAction"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return false
	}
	switch envelope.Operation {
	case OpPUT, OpPATCH, OpDELETE:
		return true
	case OpDIRECTORY:
		return envelope.DirectoryAction == "join" || envelope.DirectoryAction == "leave"
	}
	return false
}

// docResponseWriter writes a DocResponse as a JSON frame. On pipeline error,
// it converts the error into an error response so the client always gets a response.
func docResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into error responses.
		if sc.Err != nil && sc.Response == nil {
			status := StatusInternalError
			if errors.Is(sc.Err, forge.ErrRateLimited) {
				status = StatusTooManyRequests
			}
			sc.Response = &DocResponse{
				Status:  status,
				Headers: map[string]any{"Error": sc.Err.Error()},
			}
		}

		if sc.Response == nil {
			return
		}

		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal response", "error", err)
			return
		}
		if err := codec.WriteFrame(sc.Stream, data); err != nil {
			sc.Logger.Error("failed to write response", "error", err)
		}
	}
}

// commonValidation parses the owner peer ID, validates the path, and checks
// owner-only access for write operations. It runs after deserialization and
// before operation dispatch, but since OperationRouter runs before
// JSONDeserialize in each route, this middleware operates on RawBytes to
// extract ownerPeerId and path before the per-route deserialize runs.
//
// Because OperationRouter is placed after commonValidation in the pipeline,
// commonValidation peeks at the raw JSON to perform pre-dispatch validation.
func commonValidation() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		// Peek at the raw bytes for common validation fields.
		var envelope struct {
			Operation   string `json:"operation"`
			OwnerPeerID string `json:"ownerPeerId"`
			Path        string `json:"path"`

			DirectoryAction string `json:"directoryAction"`
		}
		if err := json.Unmarshal(sc.RawBytes, &envelope); err != nil {
			sc.Response = &DocResponse{Status: StatusBadRequest}
			return
		}

		sc.Logger.Debug("handling document request",
			"operation", envelope.Operation,
			"owner", envelope.OwnerPeerID,
			"path", envelope.Path,
			"caller", sc.PeerID.String(),
		)

		// Parse owner peer ID.
		if envelope.OwnerPeerID == "" {
			sc.Response = &DocResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "ownerPeerId is required"}}
			return
		}
		ownerID, err := peer.Decode(envelope.OwnerPeerID)
		if err != nil {
			sc.Response = &DocResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "invalid ownerPeerId"}}
			return
		}
		sc.Set("ownerID", ownerID)

		// Validate path for operations that require it.
		if envelope.Operation != OpLIST && envelope.Operation != OpDIRECTORY {
			if err := validatePath(envelope.Path); err != nil {
				sc.Response = &DocResponse{Status: StatusBadRequest,
					Headers: map[string]any{"Error": err.Error()}}
				return
			}
		}

		// Enforce owner-only access for write operations.
		isWrite := envelope.Operation == OpPUT || envelope.Operation == OpPATCH || envelope.Operation == OpDELETE
		if envelope.Operation == OpDIRECTORY {
			isWrite = envelope.DirectoryAction == "join" || envelope.DirectoryAction == "leave"
		}
		if isWrite && sc.PeerID != ownerID {
			sc.Response = &DocResponse{Status: StatusForbidden,
				Headers: map[string]any{"Error": "write operations require owner access"}}
			return
		}

		next()
	}
}

// handleGet retrieves a document. Supports If-None-Match for conditional GET.
func handleGet(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	doc, err := store.GetDocument(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			sc.Response = &DocResponse{Status: StatusNotFound}
			return
		}
		sc.Logger.Error("failed to get document", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}
	if doc == nil {
		sc.Response = &DocResponse{Status: StatusNotFound}
		return
	}

	// Conditional GET: If-None-Match
	if ifNoneMatch, ok := req.Headers["If-None-Match"]; ok {
		if ifNoneMatch == doc.ContentHash {
			sc.Response = &DocResponse{
				Status: StatusNotModified,
				Headers: map[string]any{
					"ETag": doc.ContentHash,
				},
			}
			return
		}
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"ETag":          doc.ContentHash,
			"Content-Type":  doc.ContentType,
			"Last-Modified": doc.UpdatedAt.UnixMilli(),
			"Version":       doc.VersionNumber,
		},
		Body: base64.StdEncoding.EncodeToString(doc.Content),
	}
}

// handlePut creates or replaces a document. Supports If-Match for conditional PUT.
func handlePut(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}}
		return
	}

	contentType := "application/octet-stream"
	if ct, ok := req.Headers["Content-Type"]; ok {
		contentType = ct
	}

	// If-Match for conditional put (optimistic locking)
	var ifMatch *string
	if im, ok := req.Headers["If-Match"]; ok {
		ifMatch = &im
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := store.PutDocument(ctx, ownerID.(peer.ID), req.Path, content, contentType, sc.PeerID, ifMatch)
	if err != nil {
		sc.Response = writeErrorResponse(sc.Logger, err)
		return
	}

	status := StatusOK
	if result.Created {
		status = StatusCreated
	}

	// Materialize directory listing if applicable (fire-and-forget side effect).
	maybeUpdateDirectoryListing(ctx, sc.Logger, store, ownerID.(peer.ID), req.Path)

	sc.Response = &DocResponse{
		Status: status,
		Headers: map[string]any{
			"ETag":          result.ContentHash,
			"Last-Modified": result.UpdatedAt.UnixMilli(),
		},
	}
}

// handlePatch applies a partial update to a document.
func handlePatch(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	// Decode body as JSON patch object
	bodyBytes, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid base64 body"}}
		return
	}

	var patch map[string]any
	if err := json.Unmarshal(bodyBytes, &patch); err != nil {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "invalid JSON patch body"}}
		return
	}

	// If-Match for conditional patch
	var ifMatch *string
	if im, ok := req.Headers["If-Match"]; ok {
		ifMatch = &im
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := store.PatchDocument(ctx, ownerID.(peer.ID), req.Path, patch, sc.PeerID, ifMatch)
	if err != nil {
		sc.Response = writeErrorResponse(sc.Logger, err)
		return
	}

	// Materialize directory listing if applicable (fire-and-forget side effect).
	maybeUpdateDirectoryListing(ctx, sc.Logger, store, ownerID.(peer.ID), req.Path)

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"ETag":          result.ContentHash,
			"Last-Modified": result.UpdatedAt.UnixMilli(),
		},
	}
}

// handleHead returns document metadata without the body.
func handleHead(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	doc, err := store.GetDocument(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			sc.Response = &DocResponse{Status: StatusNotFound}
			return
		}
		sc.Logger.Error("failed to get document", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}
	if doc == nil {
		sc.Response = &DocResponse{Status: StatusNotFound}
		return
	}

	// Conditional: If-None-Match
	if ifNoneMatch, ok := req.Headers["If-None-Match"]; ok {
		if ifNoneMatch == doc.ContentHash {
			sc.Response = &DocResponse{
				Status: StatusNotModified,
				Headers: map[string]any{
					"ETag": doc.ContentHash,
				},
			}
			return
		}
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"ETag":           doc.ContentHash,
			"Content-Type":   doc.ContentType,
			"Content-Length": len(doc.Content),
			"Last-Modified":  doc.UpdatedAt.Format(time.RFC3339),
			"Version":        fmt.Sprintf("%d", doc.VersionNumber),
		},
	}
}

// handleDelete removes a document.
func handleDelete(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	deleted, err := store.DeleteDocument(ctx, ownerID.(peer.ID), req.Path)
	if err != nil {
		sc.Logger.Error("failed to delete document", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	if !deleted {
		sc.Response = &DocResponse{Status: StatusNotFound}
		return
	}

	// Remove directory listing if applicable (fire-and-forget side effect).
	maybeRemoveDirectoryListing(ctx, sc.Logger, store, ownerID.(peer.ID), req.Path)

	sc.Response = &DocResponse{Status: StatusNoContent}
}

// handleList returns all documents for an owner.
func handleList(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	docs, err := store.ListDocuments(ctx, ownerID.(peer.ID))
	if err != nil {
		sc.Logger.Error("failed to list documents", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	// Build a JSON array of document info
	type docEntry struct {
		Path        string `json:"path"`
		ContentType string `json:"contentType"`
		ContentHash string `json:"contentHash"`
		Size        int    `json:"size"`
		UpdatedAt   string `json:"updatedAt"`
		Version     int    `json:"versionNumber"`
	}

	entries := make([]docEntry, 0, len(docs))
	for _, d := range docs {
		entries = append(entries, docEntry{
			Path:        d.Path,
			ContentType: d.ContentType,
			ContentHash: d.ContentHash,
			Size:        len(d.Content),
			UpdatedAt:   d.UpdatedAt.Format(time.RFC3339),
			Version:     d.VersionNumber,
		})
	}

	bodyBytes, err := json.Marshal(entries)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleHistory returns version history for a document.
func handleHistory(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// If a specific version is requested, return that single version
	if req.VersionNumber != nil {
		version, err := store.GetDocumentAtVersion(ctx, ownerID.(peer.ID), req.Path, *req.VersionNumber)
		if err != nil {
			if errors.Is(err, storage.ErrDocumentNotFound) {
				sc.Response = &DocResponse{Status: StatusNotFound}
				return
			}
			sc.Logger.Error("failed to get document version", "error", err)
			sc.Response = &DocResponse{Status: StatusInternalError}
			return
		}

		sc.Response = &DocResponse{
			Status: StatusOK,
			Headers: map[string]any{
				"ETag":         version.ContentHash,
				"Content-Type": version.ContentType,
				"Version":      version.VersionNumber,
				"Created-At":   version.CreatedAt.UnixMilli(),
			},
			Body: base64.StdEncoding.EncodeToString(version.Content),
		}
		return
	}

	// Otherwise return version history listing
	versions, err := store.GetDocumentHistory(ctx, ownerID.(peer.ID), req.Path, req.MaxVersions)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			sc.Response = &DocResponse{Status: StatusNotFound}
			return
		}
		sc.Logger.Error("failed to get document history", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	type versionEntry struct {
		VersionNumber int    `json:"versionNumber"`
		ContentHash   string `json:"contentHash"`
		ContentType   string `json:"contentType"`
		CreatedAt     string `json:"createdAt"`
		Size          int    `json:"size"`
	}

	entries := make([]versionEntry, 0, len(versions))
	for _, v := range versions {
		entries = append(entries, versionEntry{
			VersionNumber: v.VersionNumber,
			ContentHash:   v.ContentHash,
			ContentType:   v.ContentType,
			CreatedAt:     v.CreatedAt.Format(time.RFC3339),
			Size:          len(v.Content),
		})
	}

	bodyBytes, err := json.Marshal(entries)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleDirectory handles DIRECTORY operations (join, leave, browse, get).
func handleDirectory(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)

	switch req.DirectoryAction {
	case "join":
		handleDirectoryJoin(sc, req)
	case "leave":
		handleDirectoryLeave(sc)
	case "browse":
		handleDirectoryBrowse(sc, req)
	case "get":
		handleDirectoryGet(sc)
	default:
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "unknown directoryAction: " + req.DirectoryAction}}
	}
}

func handleDirectoryJoin(sc *forge.StreamContext, req *DocRequest) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	// Decode listing from body
	var listing struct {
		DisplayName string         `json:"displayName"`
		Bio         string         `json:"bio"`
		AvatarHash  string         `json:"avatarHash"`
		Extras      map[string]any `json:"extras"`
	}

	if req.Body != "" {
		bodyBytes, err := base64.StdEncoding.DecodeString(req.Body)
		if err != nil {
			sc.Response = &DocResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "invalid base64 body"}}
			return
		}
		if err := json.Unmarshal(bodyBytes, &listing); err != nil {
			sc.Response = &DocResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": "invalid JSON body"}}
			return
		}
	}

	if listing.DisplayName == "" {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "displayName is required"}}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	entry := &storage.DirectoryEntry{
		OwnerPeerID: ownerID.(peer.ID).String(),
		DisplayName: listing.DisplayName,
		Bio:         listing.Bio,
		AvatarHash:  listing.AvatarHash,
		Extras:      listing.Extras,
	}

	if err := store.UpsertDirectoryEntry(ctx, entry); err != nil {
		sc.Logger.Error("failed to upsert directory entry", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &DocResponse{Status: StatusOK}
}

func handleDirectoryLeave(sc *forge.StreamContext) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := store.RemoveDirectoryEntry(ctx, ownerID.(peer.ID).String()); err != nil {
		sc.Logger.Error("failed to remove directory entry", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}
	sc.Response = &DocResponse{Status: StatusNoContent}
}

func handleDirectoryBrowse(sc *forge.StreamContext, req *DocRequest) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")

	limit := 20
	if req.DirectoryLimit != nil && *req.DirectoryLimit > 0 {
		limit = *req.DirectoryLimit
	}

	sc.Logger.Info("directory browse request",
		"query", req.DirectoryQuery,
		"cursor", req.DirectoryCursor,
		"limit", limit,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	page, err := store.BrowseDirectory(ctx, req.DirectoryQuery, req.DirectoryCursor, limit)
	if err != nil {
		sc.Logger.Error("failed to browse directory", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	entryCount := 0
	if page.Entries != nil {
		entryCount = len(page.Entries)
	}
	sc.Logger.Info("directory browse result",
		"entries", entryCount,
		"hasMore", page.HasMore,
		"nextCursor", page.NextCursor,
	)

	bodyBytes, err := json.Marshal(page)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

func handleDirectoryGet(sc *forge.StreamContext) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	entry, err := store.GetDirectoryEntry(ctx, ownerID.(peer.ID).String())
	if err != nil {
		sc.Logger.Error("failed to get directory entry", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}
	if entry == nil {
		sc.Response = &DocResponse{Status: StatusNotFound}
		return
	}

	bodyBytes, err := json.Marshal(entry)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// maybeUpdateDirectoryListing materializes a directory-listing document into
// the directory_listings table when a user PUTs or PATCHes the well-known path.
func maybeUpdateDirectoryListing(ctx context.Context, logger *slog.Logger, store storage.Storage, ownerID peer.ID, path string) {
	if path != directoryListingPath {
		return
	}

	doc, err := store.GetDocument(ctx, ownerID, path)
	if err != nil || doc == nil {
		return
	}

	var listing struct {
		Listed      *bool          `json:"listed"`
		DisplayName string         `json:"displayName"`
		Bio         string         `json:"bio"`
		AvatarHash  string         `json:"avatarHash"`
		Extras      map[string]any `json:"extras"`
	}
	if err := json.Unmarshal(doc.Content, &listing); err != nil {
		logger.Warn("invalid directory-listing document", "error", err)
		return
	}

	// If listed is explicitly false, remove from directory
	if listing.Listed != nil && !*listing.Listed {
		_ = store.RemoveDirectoryEntry(ctx, ownerID.String())
		return
	}

	if listing.DisplayName == "" {
		return
	}

	entry := &storage.DirectoryEntry{
		OwnerPeerID: ownerID.String(),
		DisplayName: listing.DisplayName,
		Bio:         listing.Bio,
		AvatarHash:  listing.AvatarHash,
		Extras:      listing.Extras,
	}
	if err := store.UpsertDirectoryEntry(ctx, entry); err != nil {
		logger.Warn("failed to materialize directory listing", "error", err)
	}
}

// maybeRemoveDirectoryListing removes a directory entry when the directory-listing
// document is deleted.
func maybeRemoveDirectoryListing(ctx context.Context, logger *slog.Logger, store storage.Storage, ownerID peer.ID, path string) {
	if path != directoryListingPath {
		return
	}
	_ = store.RemoveDirectoryEntry(ctx, ownerID.String())
}

// writeErrorResponse converts storage errors to an appropriate DocResponse.
func writeErrorResponse(logger *slog.Logger, err error) *DocResponse {
	var sizeErr *storage.DocumentSizeExceededError
	var conflictErr *storage.DocumentConflictError

	switch {
	case errors.Is(err, storage.ErrDocumentNotFound):
		return &DocResponse{Status: StatusNotFound}
	case errors.As(err, &sizeErr):
		return &DocResponse{Status: StatusPayloadTooLarge,
			Headers: map[string]any{"Error": sizeErr.Error()}}
	case errors.As(err, &conflictErr):
		return &DocResponse{Status: StatusConflict,
			Headers: map[string]any{
				"Error":         conflictErr.Error(),
				"Expected-ETag": conflictErr.ExpectedHash,
				"Actual-ETag":   conflictErr.ActualHash,
			}}
	default:
		logger.Error("document write error", "error", err)
		return &DocResponse{Status: StatusInternalError}
	}
}

// validatePath checks that a document path conforms to requirements.
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

// containsDotDot checks for ".." path traversal.
func containsDotDot(path string) bool {
	for i := 0; i < len(path)-1; i++ {
		if path[i] == '.' && path[i+1] == '.' {
			return true
		}
	}
	return false
}
