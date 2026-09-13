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

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
	"github.com/twostack/go-ricochet/internal/ratelimit"
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
	OpBATCH_PUT = "BATCH_PUT"
)

// Batch write limits.
const (
	// maxBatchDocuments bounds how many documents one BATCH_PUT may carry.
	maxBatchDocuments = 100

	// maxBatchContentBytes bounds the batch's total decoded content. The
	// request must fit in a single frame (codec.MaxFrameSize, 10MB) and bodies
	// travel base64-encoded, so the decoded budget is well under that to leave
	// room for the ~33% encoding overhead plus the surrounding JSON.
	maxBatchContentBytes = 6 * 1024 * 1024
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
	StatusOK                 = 200
	StatusCreated            = 201
	StatusNoContent          = 204
	StatusNotModified        = 304
	StatusBadRequest         = 400
	StatusForbidden          = 403
	StatusNotFound           = 404
	StatusConflict           = 409
	StatusPayloadTooLarge    = 413
	StatusTooManyRequests    = 429
	StatusInternalError      = 500
	StatusServiceUnavailable = 503
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

	// BATCH_PUT-specific: the documents to write. All belong to OwnerPeerID.
	BatchDocuments []BatchDocument `json:"batchDocuments,omitempty"`

	// LIST-specific pagination. Absent cursor means the first page; absent
	// limit means the server default.
	ListCursor string `json:"listCursor,omitempty"`
	ListLimit  *int   `json:"listLimit,omitempty"`

	// DIRECTORY-specific
	DirectoryAction string `json:"directoryAction,omitempty"`
	DirectoryQuery  string `json:"directoryQuery,omitempty"`
	DirectoryCursor string `json:"directoryCursor,omitempty"`
	DirectoryLimit  *int   `json:"directoryLimit,omitempty"`
}

// BatchDocument is a single document within a BATCH_PUT request.
type BatchDocument struct {
	Path        string `json:"path"`
	Body        string `json:"body"` // base64
	ContentType string `json:"contentType,omitempty"`
	IfMatch     string `json:"ifMatch,omitempty"`
}

// BatchDocumentResult reports the outcome of one document in a BATCH_PUT. The
// results array is in request order, so a client can correlate by index even
// when the same path appears twice.
type BatchDocumentResult struct {
	Path       string `json:"path"`
	Status     int    `json:"status"`
	ETag       string `json:"etag,omitempty"`
	Created    bool   `json:"created,omitempty"`
	Error      string `json:"error,omitempty"`
	ActualETag string `json:"actualEtag,omitempty"` // on 409, the server's current ETag
}

// DocResponse is the JSON response format for document operations.
type DocResponse struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    string         `json:"body,omitempty"` // base64 encoded
}

// StatusCode reports the response status. It is how the metrics middleware
// tells a request the handler refused — a 404 or a 409 — from one that
// succeeded: neither sets a pipeline error, so without this both would be
// counted as ok.
func (r *DocResponse) StatusCode() int { return r.Status }

// directoryListingPath is the well-known document path that triggers directory materialization.
const directoryListingPath = "directory-listing"

// NewPipeline creates a forge pipeline for the Store Document Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).SDA

	routes := map[string]forge.Middleware{
		OpGET:       middleware.Chain(forge.JSONDeserialize[DocRequest](), handleGet),
		OpPUT:       middleware.Chain(forge.JSONDeserialize[DocRequest](), handlePut),
		OpPATCH:     middleware.Chain(forge.JSONDeserialize[DocRequest](), handlePatch),
		OpHEAD:      middleware.Chain(forge.JSONDeserialize[DocRequest](), handleHead),
		OpDELETE:    middleware.Chain(forge.JSONDeserialize[DocRequest](), handleDelete),
		OpLIST:      middleware.Chain(forge.JSONDeserialize[DocRequest](), handleList),
		OpBATCH_PUT: middleware.Chain(forge.JSONDeserialize[DocRequest](), handleBatchPut),
		OpHISTORY:   middleware.Chain(forge.JSONDeserialize[DocRequest](), handleHistory),
		OpDIRECTORY: middleware.Chain(forge.JSONDeserialize[DocRequest](), handleDirectory),
	}

	return wire.Bounded(forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "sda", ""),
		wire.AccessLog("sda", ""),
		middleware.Recovery(),
		docResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		wire.RequestDeadline(reg),
		middleware.DualRateLimitMiddleware(limiter, isWriteClassifier),
		admission.Middleware(admission.FromRegistry(reg)),
		capacity.WriteGate(capacity.FromRegistry(reg), isWriteClassifier),
		commonValidation(),
		middleware.OperationRouter("operation", routes),
	).WithRegistry(reg), reg)
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
	case OpPUT, OpPATCH, OpDELETE, OpBATCH_PUT:
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
			status, retryAfter := wire.Classify(sc.Err)
			wire.LogRejection(sc, status, sc.Err)
			headers := map[string]any{"Error": wire.ClientMessage(sc.Err)}
			if ms := wire.RetryAfterMs(retryAfter); ms > 0 {
				headers[wire.RetryAfterHeaderKey] = ms
			}
			sc.Response = &DocResponse{Status: status, Headers: headers}
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

		// Validate path for operations that require it. BATCH_PUT carries its
		// paths per document, so it validates them itself.
		if envelope.Operation != OpLIST && envelope.Operation != OpDIRECTORY &&
			envelope.Operation != OpBATCH_PUT {
			if err := validatePath(envelope.Path); err != nil {
				sc.Response = &DocResponse{Status: StatusBadRequest,
					Headers: map[string]any{"Error": err.Error()}}
				return
			}
		}

		// Enforce owner-only access for write operations.
		isWrite := envelope.Operation == OpPUT || envelope.Operation == OpPATCH ||
			envelope.Operation == OpDELETE || envelope.Operation == OpBATCH_PUT
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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
	if err := wire.CheckString("Content-Type", contentType, wire.MaxContentTypeLength); err != nil {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": err.Error()}}
		return
	}

	// If-Match for conditional put (optimistic locking)
	var ifMatch *string
	if im, ok := req.Headers["If-Match"]; ok {
		ifMatch = &im
	}

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

// handleBatchPut writes many documents in one request.
//
// This is the operation that decouples request count from document count: a
// vault sync that cost one rate-limited request per document now costs one per
// batch. Each document is written independently and reports its own status, so
// one conflicting document does not sink the rest of the batch -- a partial
// success is the normal outcome, not an error case.
//
// Writes are sequential rather than concurrent. Each PutDocument takes a row
// lock, and a batch frequently contains several revisions of the same path, so
// fanning out would contend on exactly the rows it is trying to write.
func handleBatchPut(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	if len(req.BatchDocuments) == 0 {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": "batchDocuments is required"}}
		return
	}
	if len(req.BatchDocuments) > maxBatchDocuments {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": fmt.Sprintf(
				"batchDocuments holds %d documents, maximum is %d",
				len(req.BatchDocuments), maxBatchDocuments)}}
		return
	}

	// Decode and validate everything before writing anything, so a malformed
	// batch is rejected outright rather than half-applied.
	type pending struct {
		path        string
		content     []byte
		contentType string
		ifMatch     *string
	}
	decoded := make([]pending, 0, len(req.BatchDocuments))
	results := make([]BatchDocumentResult, len(req.BatchDocuments))
	totalBytes := 0

	for i, bd := range req.BatchDocuments {
		results[i].Path = bd.Path

		if err := validatePath(bd.Path); err != nil {
			results[i].Status = StatusBadRequest
			results[i].Error = err.Error()
			decoded = append(decoded, pending{})
			continue
		}

		content, err := base64.StdEncoding.DecodeString(bd.Body)
		if err != nil {
			results[i].Status = StatusBadRequest
			results[i].Error = "invalid base64 body"
			decoded = append(decoded, pending{})
			continue
		}

		totalBytes += len(content)
		if totalBytes > maxBatchContentBytes {
			sc.Response = &DocResponse{Status: StatusPayloadTooLarge,
				Headers: map[string]any{"Error": fmt.Sprintf(
					"batch content exceeds %d bytes", maxBatchContentBytes)}}
			return
		}

		contentType := bd.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		if err := wire.CheckString("contentType", contentType, wire.MaxContentTypeLength); err != nil {
			results[i].Status = StatusBadRequest
			results[i].Error = err.Error()
			decoded = append(decoded, pending{})
			continue
		}
		var ifMatch *string
		if bd.IfMatch != "" {
			im := bd.IfMatch
			ifMatch = &im
		}

		decoded = append(decoded, pending{
			path:        bd.Path,
			content:     content,
			contentType: contentType,
			ifMatch:     ifMatch,
		})
	}

	ctx, cancel := context.WithTimeout(sc.Ctx, 60*time.Second)
	defer cancel()

	applied := 0
	for i, p := range decoded {
		if results[i].Status != 0 {
			continue // rejected during decoding
		}

		result, err := store.PutDocument(ctx, ownerID.(peer.ID), p.path,
			p.content, p.contentType, sc.PeerID, p.ifMatch)
		if err != nil {
			// Reuse the single-document error mapping so batch and non-batch
			// writes cannot report the same failure differently.
			errResp := writeErrorResponse(sc.Logger, err)
			results[i].Status = errResp.Status
			if msg, ok := errResp.Headers["Error"].(string); ok {
				results[i].Error = msg
			}
			if actual, ok := errResp.Headers["Actual-ETag"].(string); ok {
				results[i].ActualETag = actual
			}
			continue
		}

		results[i].Status = StatusOK
		if result.Created {
			results[i].Status = StatusCreated
			results[i].Created = true
		}
		results[i].ETag = result.ContentHash
		applied++

		maybeUpdateDirectoryListing(ctx, sc.Logger, store, ownerID.(peer.ID), p.path)
	}

	bodyBytes, err := json.Marshal(map[string]any{"results": results})
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	sc.Logger.Debug("batch put complete",
		"owner", ownerID.(peer.ID).String(),
		"documents", len(req.BatchDocuments),
		"applied", applied,
	)

	// The envelope status is 200 whenever the batch was processed; per-document
	// outcomes live in the body. A partial success is normal here, so a single
	// envelope status cannot describe the result on its own.
	sc.Response = &DocResponse{
		Status: StatusOK,
		Headers: map[string]any{
			"Content-Type": "application/json",
			"Applied":      applied,
			"Total":        len(req.BatchDocuments),
		},
		Body: base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleList returns one page of document metadata for an owner.
//
// The body stays a bare JSON array so existing clients keep parsing it
// unchanged; paging is advertised through the response headers, which clients
// that do not understand them simply ignore.
func handleList(sc *forge.StreamContext, next func()) {
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")
	req := sc.Request.(*DocRequest)

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
	defer cancel()

	limit := 0
	if req.ListLimit != nil {
		limit = *req.ListLimit
	}

	docs, hasMore, err := store.ListDocuments(ctx, ownerID.(peer.ID), req.ListCursor, limit)
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
			Size:        d.Size,
			UpdatedAt:   d.UpdatedAt.Format(time.RFC3339),
			Version:     d.VersionNumber,
		})
	}

	bodyBytes, err := json.Marshal(entries)
	if err != nil {
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	headers := map[string]any{
		"Content-Type": "application/json",
		"Has-More":     hasMore,
	}
	if hasMore && len(entries) > 0 {
		headers["Next-Cursor"] = entries[len(entries)-1].Path
	}

	sc.Response = &DocResponse{
		Status:  StatusOK,
		Headers: headers,
		Body:    base64.StdEncoding.EncodeToString(bodyBytes),
	}
}

// handleHistory returns version history for a document.
func handleHistory(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*DocRequest)
	store, _ := forge.ServiceFrom[storage.Storage](sc, "storage")
	ownerID, _ := sc.Get("ownerID")

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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
	if err := checkListingFields(listing.DisplayName, listing.Bio, listing.AvatarHash, listing.Extras); err != nil {
		sc.Response = &DocResponse{Status: StatusBadRequest,
			Headers: map[string]any{"Error": err.Error()}}
		return
	}

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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

	sc.Logger.Debug("directory browse request",
		"search", req.DirectoryQuery != "",
		"cursor", req.DirectoryCursor != "",
		"limit", limit,
	)

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
	defer cancel()

	page, err := store.BrowseDirectory(ctx, req.DirectoryQuery, req.DirectoryCursor, limit)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidCursor) || errors.Is(err, storage.ErrDirectoryQueryTooLong) {
			// The caller sent something the server cannot read, so this is a
			// 400. Answering the first page instead would look like success
			// and loop forever.
			sc.Logger.Warn("directory browse request refused", "error", err)
			sc.Response = &DocResponse{Status: StatusBadRequest,
				Headers: map[string]any{"Error": wire.ClientMessage(err)}}
			return
		}
		sc.Logger.Error("failed to browse directory", "error", err)
		sc.Response = &DocResponse{Status: StatusInternalError}
		return
	}

	entryCount := 0
	if page.Entries != nil {
		entryCount = len(page.Entries)
	}
	sc.Logger.Debug("directory browse result",
		"entries", entryCount,
		"hasMore", page.HasMore,
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

	ctx, cancel := context.WithTimeout(sc.Ctx, 30*time.Second)
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
	if err := checkListingFields(listing.DisplayName, listing.Bio, listing.AvatarHash, listing.Extras); err != nil {
		// The document itself was stored within the document size limit;
		// only its projection into the directory is refused.
		logger.Warn("directory-listing document not materialized", "error", err)
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
// checkListingFields bounds what a directory listing may carry; each field
// is stored and echoed in every browse result.
func checkListingFields(displayName, bio, avatarHash string, extras map[string]any) error {
	if err := wire.CheckString("displayName", displayName, wire.MaxDisplayNameLength); err != nil {
		return err
	}
	if err := wire.CheckString("bio", bio, wire.MaxBioLength); err != nil {
		return err
	}
	if err := wire.CheckString("avatarHash", avatarHash, wire.MaxAvatarHashLength); err != nil {
		return err
	}
	return wire.CheckJSONSize("extras", extras, wire.MaxDirectoryExtrasBytes)
}

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
