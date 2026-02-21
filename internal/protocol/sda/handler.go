package sda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"` // base64 encoded
}

// Handler is the Store Document Access protocol handler.
type Handler struct {
	store          storage.Storage
	logger         *slog.Logger

	// Rate limiting: separate buckets for read and write operations.
	rateLimitMu       sync.Mutex
	readHistory       map[string][]time.Time
	writeHistory      map[string][]time.Time
	rateLimitWindow   time.Duration
	maxReadRequests   int
	maxWriteRequests  int
}

// NewHandler creates a new SDA handler.
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

// HandleStream handles an incoming document access stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.CloseWrite()

	// Read length-prefixed frame
	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest})
		return
	}

	// Parse request
	var req DocRequest
	if err := json.Unmarshal(data, &req); err != nil {
		h.logger.Error("failed to parse request", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest})
		return
	}

	h.logger.Debug("handling document request",
		"operation", req.Operation,
		"owner", req.OwnerPeerID,
		"path", req.Path,
		"caller", callerID.String(),
	)

	// Determine if this is a read or write operation and check rate limits
	isWrite := req.Operation == OpPUT || req.Operation == OpPATCH || req.Operation == OpDELETE
	if req.Operation == OpDIRECTORY {
		isWrite = req.DirectoryAction == "join" || req.DirectoryAction == "leave"
	}
	if isWrite {
		if !h.checkRateLimit(callerID, true) {
			h.writeResponse(s, &DocResponse{Status: StatusTooManyRequests})
			return
		}
	} else {
		if !h.checkRateLimit(callerID, false) {
			h.writeResponse(s, &DocResponse{Status: StatusTooManyRequests})
			return
		}
	}

	// Parse owner peer ID
	if req.OwnerPeerID == "" {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "ownerPeerId is required"}})
		return
	}
	ownerID, err := peer.Decode(req.OwnerPeerID)
	if err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "invalid ownerPeerId"}})
		return
	}

	// Validate path for operations that require it
	if req.Operation != OpLIST && req.Operation != OpDIRECTORY {
		if err := validatePath(req.Path); err != nil {
			h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
				Headers: map[string]string{"Error": err.Error()}})
			return
		}
	}

	// Enforce owner-only access for write operations
	if isWrite && callerID != ownerID {
		h.writeResponse(s, &DocResponse{Status: StatusForbidden,
			Headers: map[string]string{"Error": "write operations require owner access"}})
		return
	}

	ctx := context.Background()

	switch req.Operation {
	case OpGET:
		h.handleGet(ctx, s, &req, ownerID)
	case OpPUT:
		h.handlePut(ctx, s, &req, ownerID, callerID)
	case OpPATCH:
		h.handlePatch(ctx, s, &req, ownerID, callerID)
	case OpHEAD:
		h.handleHead(ctx, s, &req, ownerID)
	case OpDELETE:
		h.handleDelete(ctx, s, &req, ownerID)
	case OpLIST:
		h.handleList(ctx, s, ownerID)
	case OpHISTORY:
		h.handleHistory(ctx, s, &req, ownerID)
	case OpDIRECTORY:
		h.handleDirectory(ctx, s, &req, ownerID)
	default:
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "unknown operation: " + req.Operation}})
	}
}

// handleGet retrieves a document. Supports If-None-Match for conditional GET.
func (h *Handler) handleGet(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
	doc, err := h.store.GetDocument(ctx, ownerID, req.Path)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			h.writeResponse(s, &DocResponse{Status: StatusNotFound})
			return
		}
		h.logger.Error("failed to get document", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}
	if doc == nil {
		h.writeResponse(s, &DocResponse{Status: StatusNotFound})
		return
	}

	// Conditional GET: If-None-Match
	if ifNoneMatch, ok := req.Headers["If-None-Match"]; ok {
		if ifNoneMatch == doc.ContentHash {
			h.writeResponse(s, &DocResponse{
				Status: StatusNotModified,
				Headers: map[string]string{
					"ETag": doc.ContentHash,
				},
			})
			return
		}
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"ETag":         doc.ContentHash,
			"Content-Type": doc.ContentType,
			"Last-Modified": doc.UpdatedAt.Format(time.RFC3339),
			"Version":      fmt.Sprintf("%d", doc.VersionNumber),
		},
		Body: base64.StdEncoding.EncodeToString(doc.Content),
	})
}

// handlePut creates or replaces a document. Supports If-Match for conditional PUT.
func (h *Handler) handlePut(ctx context.Context, s network.Stream, req *DocRequest, ownerID, callerID peer.ID) {
	// Decode body
	content, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "invalid base64 body"}})
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

	result, err := h.store.PutDocument(ctx, ownerID, req.Path, content, contentType, callerID, ifMatch)
	if err != nil {
		h.handleWriteError(s, err)
		return
	}

	status := StatusOK
	if result.Created {
		status = StatusCreated
	}

	h.writeResponse(s, &DocResponse{
		Status: status,
		Headers: map[string]string{
			"ETag":          result.ContentHash,
			"Last-Modified": result.UpdatedAt.Format(time.RFC3339),
		},
	})

	// Materialize directory listing if applicable
	h.maybeUpdateDirectoryListing(ctx, ownerID, req.Path)
}

// handlePatch applies a partial update to a document.
func (h *Handler) handlePatch(ctx context.Context, s network.Stream, req *DocRequest, ownerID, callerID peer.ID) {
	// Decode body as JSON patch object
	bodyBytes, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "invalid base64 body"}})
		return
	}

	var patch map[string]any
	if err := json.Unmarshal(bodyBytes, &patch); err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "invalid JSON patch body"}})
		return
	}

	// If-Match for conditional patch
	var ifMatch *string
	if im, ok := req.Headers["If-Match"]; ok {
		ifMatch = &im
	}

	result, err := h.store.PatchDocument(ctx, ownerID, req.Path, patch, callerID, ifMatch)
	if err != nil {
		h.handleWriteError(s, err)
		return
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"ETag":          result.ContentHash,
			"Last-Modified": result.UpdatedAt.Format(time.RFC3339),
		},
	})

	// Materialize directory listing if applicable
	h.maybeUpdateDirectoryListing(ctx, ownerID, req.Path)
}

// handleHead returns document metadata without the body.
func (h *Handler) handleHead(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
	doc, err := h.store.GetDocument(ctx, ownerID, req.Path)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			h.writeResponse(s, &DocResponse{Status: StatusNotFound})
			return
		}
		h.logger.Error("failed to get document", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}
	if doc == nil {
		h.writeResponse(s, &DocResponse{Status: StatusNotFound})
		return
	}

	// Conditional: If-None-Match
	if ifNoneMatch, ok := req.Headers["If-None-Match"]; ok {
		if ifNoneMatch == doc.ContentHash {
			h.writeResponse(s, &DocResponse{
				Status: StatusNotModified,
				Headers: map[string]string{
					"ETag": doc.ContentHash,
				},
			})
			return
		}
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"ETag":           doc.ContentHash,
			"Content-Type":   doc.ContentType,
			"Content-Length": fmt.Sprintf("%d", len(doc.Content)),
			"Last-Modified":  doc.UpdatedAt.Format(time.RFC3339),
			"Version":        fmt.Sprintf("%d", doc.VersionNumber),
		},
	})
}

// handleDelete removes a document.
func (h *Handler) handleDelete(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
	deleted, err := h.store.DeleteDocument(ctx, ownerID, req.Path)
	if err != nil {
		h.logger.Error("failed to delete document", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	if !deleted {
		h.writeResponse(s, &DocResponse{Status: StatusNotFound})
		return
	}

	h.writeResponse(s, &DocResponse{Status: StatusNoContent})

	// Remove directory listing if applicable
	h.maybeRemoveDirectoryListing(ctx, ownerID, req.Path)
}

// handleList returns all documents for an owner.
func (h *Handler) handleList(ctx context.Context, s network.Stream, ownerID peer.ID) {
	docs, err := h.store.ListDocuments(ctx, ownerID)
	if err != nil {
		h.logger.Error("failed to list documents", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	// Build a JSON array of document info
	type docEntry struct {
		Path         string `json:"path"`
		ContentType  string `json:"contentType"`
		ContentHash  string `json:"contentHash"`
		Size         int    `json:"size"`
		UpdatedAt    string `json:"updatedAt"`
		Version      int    `json:"versionNumber"`
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
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// handleHistory returns version history for a document.
func (h *Handler) handleHistory(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
	// If a specific version is requested, return that single version
	if req.VersionNumber != nil {
		version, err := h.store.GetDocumentAtVersion(ctx, ownerID, req.Path, *req.VersionNumber)
		if err != nil {
			if errors.Is(err, storage.ErrDocumentNotFound) {
				h.writeResponse(s, &DocResponse{Status: StatusNotFound})
				return
			}
			h.logger.Error("failed to get document version", "error", err)
			h.writeResponse(s, &DocResponse{Status: StatusInternalError})
			return
		}

		h.writeResponse(s, &DocResponse{
			Status: StatusOK,
			Headers: map[string]string{
				"ETag":         version.ContentHash,
				"Content-Type": version.ContentType,
				"Version":      fmt.Sprintf("%d", version.VersionNumber),
				"Created-At":   version.CreatedAt.Format(time.RFC3339),
			},
			Body: base64.StdEncoding.EncodeToString(version.Content),
		})
		return
	}

	// Otherwise return version history listing
	versions, err := h.store.GetDocumentHistory(ctx, ownerID, req.Path, req.MaxVersions)
	if err != nil {
		if errors.Is(err, storage.ErrDocumentNotFound) {
			h.writeResponse(s, &DocResponse{Status: StatusNotFound})
			return
		}
		h.logger.Error("failed to get document history", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
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
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// directoryListingPath is the well-known document path that triggers directory materialization.
const directoryListingPath = "directory-listing"

// handleDirectory handles DIRECTORY operations (join, leave, browse, get).
func (h *Handler) handleDirectory(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
	switch req.DirectoryAction {
	case "join":
		h.handleDirectoryJoin(ctx, s, req, ownerID)
	case "leave":
		h.handleDirectoryLeave(ctx, s, ownerID)
	case "browse":
		h.handleDirectoryBrowse(ctx, s, req)
	case "get":
		h.handleDirectoryGet(ctx, s, ownerID)
	default:
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "unknown directoryAction: " + req.DirectoryAction}})
	}
}

func (h *Handler) handleDirectoryJoin(ctx context.Context, s network.Stream, req *DocRequest, ownerID peer.ID) {
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
			h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
				Headers: map[string]string{"Error": "invalid base64 body"}})
			return
		}
		if err := json.Unmarshal(bodyBytes, &listing); err != nil {
			h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
				Headers: map[string]string{"Error": "invalid JSON body"}})
			return
		}
	}

	if listing.DisplayName == "" {
		h.writeResponse(s, &DocResponse{Status: StatusBadRequest,
			Headers: map[string]string{"Error": "displayName is required"}})
		return
	}

	entry := &storage.DirectoryEntry{
		OwnerPeerID: ownerID.String(),
		DisplayName: listing.DisplayName,
		Bio:         listing.Bio,
		AvatarHash:  listing.AvatarHash,
		Extras:      listing.Extras,
	}

	if err := h.store.UpsertDirectoryEntry(ctx, entry); err != nil {
		h.logger.Error("failed to upsert directory entry", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &DocResponse{Status: StatusOK})
}

func (h *Handler) handleDirectoryLeave(ctx context.Context, s network.Stream, ownerID peer.ID) {
	if err := h.store.RemoveDirectoryEntry(ctx, ownerID.String()); err != nil {
		h.logger.Error("failed to remove directory entry", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}
	h.writeResponse(s, &DocResponse{Status: StatusNoContent})
}

func (h *Handler) handleDirectoryBrowse(ctx context.Context, s network.Stream, req *DocRequest) {
	limit := 20
	if req.DirectoryLimit != nil && *req.DirectoryLimit > 0 {
		limit = *req.DirectoryLimit
	}

	page, err := h.store.BrowseDirectory(ctx, req.DirectoryQuery, req.DirectoryCursor, limit)
	if err != nil {
		h.logger.Error("failed to browse directory", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	bodyBytes, err := json.Marshal(page)
	if err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

func (h *Handler) handleDirectoryGet(ctx context.Context, s network.Stream, ownerID peer.ID) {
	entry, err := h.store.GetDirectoryEntry(ctx, ownerID.String())
	if err != nil {
		h.logger.Error("failed to get directory entry", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}
	if entry == nil {
		h.writeResponse(s, &DocResponse{Status: StatusNotFound})
		return
	}

	bodyBytes, err := json.Marshal(entry)
	if err != nil {
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
		return
	}

	h.writeResponse(s, &DocResponse{
		Status: StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	})
}

// maybeUpdateDirectoryListing materializes a directory-listing document into
// the directory_listings table when a user PUTs or PATCHes the well-known path.
func (h *Handler) maybeUpdateDirectoryListing(ctx context.Context, ownerID peer.ID, path string) {
	if path != directoryListingPath {
		return
	}

	doc, err := h.store.GetDocument(ctx, ownerID, path)
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
		h.logger.Warn("invalid directory-listing document", "error", err)
		return
	}

	// If listed is explicitly false, remove from directory
	if listing.Listed != nil && !*listing.Listed {
		_ = h.store.RemoveDirectoryEntry(ctx, ownerID.String())
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
	if err := h.store.UpsertDirectoryEntry(ctx, entry); err != nil {
		h.logger.Warn("failed to materialize directory listing", "error", err)
	}
}

// maybeRemoveDirectoryListing removes a directory entry when the directory-listing
// document is deleted.
func (h *Handler) maybeRemoveDirectoryListing(ctx context.Context, ownerID peer.ID, path string) {
	if path != directoryListingPath {
		return
	}
	_ = h.store.RemoveDirectoryEntry(ctx, ownerID.String())
}

// handleWriteError converts storage errors to appropriate response status codes.
func (h *Handler) handleWriteError(s network.Stream, err error) {
	var sizeErr *storage.DocumentSizeExceededError
	var conflictErr *storage.DocumentConflictError

	switch {
	case errors.Is(err, storage.ErrDocumentNotFound):
		h.writeResponse(s, &DocResponse{Status: StatusNotFound})
	case errors.As(err, &sizeErr):
		h.writeResponse(s, &DocResponse{Status: StatusPayloadTooLarge,
			Headers: map[string]string{"Error": sizeErr.Error()}})
	case errors.As(err, &conflictErr):
		h.writeResponse(s, &DocResponse{Status: StatusConflict,
			Headers: map[string]string{
				"Error":         conflictErr.Error(),
				"Expected-ETag": conflictErr.ExpectedHash,
				"Actual-ETag":   conflictErr.ActualHash,
			}})
	default:
		h.logger.Error("document write error", "error", err)
		h.writeResponse(s, &DocResponse{Status: StatusInternalError})
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

// writeResponse marshals and writes a DocResponse over the stream.
func (h *Handler) writeResponse(s network.Stream, resp *DocResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("failed to marshal response", "error", err)
		return
	}
	if err := frame.WriteFrame(s, data); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}
}
