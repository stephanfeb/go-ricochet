package mma

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/protocol/frame"
)

// ProtocolID is the MMA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/admin/1.0.0")

// Operation type constants for the MMA protocol.
const (
	OpCreateMailbox   = "createMailbox"
	OpDeleteMailbox   = "deleteMailbox"
	OpGrantAccess     = "grantAccess"
	OpRevokeAccess    = "revokeAccess"
	OpListACL         = "listACL"
	OpListMailboxes   = "listMailboxes"
	OpUpdateConfig    = "updateConfig"
	OpQueryCapacity   = "queryCapacity"
	OpGetMailboxInfo  = "getMailboxInfo"
)

// AdminRequest is the top-level request envelope. The operationType field
// determines which specific fields are relevant.
type AdminRequest struct {
	OperationType string `json:"operationType"`

	// createMailbox / deleteMailbox / grantAccess / revokeAccess / listACL
	OwnerPeerID string `json:"ownerPeerId,omitempty"`
	FolderPath  string `json:"folderPath,omitempty"`

	// Dart-compatible: combined "peerId/folderPath" address string
	Address string `json:"address,omitempty"`
	// Dart-compatible: mailbox type as "type" field
	Type string `json:"type,omitempty"`

	// createMailbox
	MailboxType    string `json:"mailboxType,omitempty"`
	MaxMessages    *int   `json:"maxMessages,omitempty"`
	RetentionDays  *int   `json:"retentionDays,omitempty"`
	RetentionCount *int   `json:"retentionCount,omitempty"`

	// grantAccess / revokeAccess
	GranteePeerID  string `json:"granteePeerId,omitempty"`
	TargetPeerID   string `json:"targetPeerId,omitempty"` // Dart-compatible alias
	AccessMode     string `json:"accessMode,omitempty"`
}

// normalize populates FolderPath and MailboxType from Dart-compatible fields
// (Address, Type, TargetPeerID) when the Go-native fields are empty.
func (r *AdminRequest) normalize() {
	// Parse "peerId/folderPath" address into FolderPath
	if r.FolderPath == "" && r.Address != "" {
		parts := strings.SplitN(r.Address, "/", 2)
		if len(parts) == 2 {
			r.FolderPath = parts[1]
		}
	}
	// Use "type" as mailbox type if "mailboxType" not set
	if r.MailboxType == "" && r.Type != "" {
		r.MailboxType = r.Type
	}
	// Use "targetPeerId" as grantee if "granteePeerId" not set
	if r.GranteePeerID == "" && r.TargetPeerID != "" {
		r.GranteePeerID = r.TargetPeerID
	}
}

// AdminResponse is the generic response envelope.
type AdminResponse struct {
	Success      bool   `json:"success"`
	ErrorMessage string `json:"errorMessage,omitempty"`

	// listMailboxes
	Mailboxes []MailboxInfo `json:"mailboxes,omitempty"`

	// listACL
	ACL []ACLEntry `json:"acl,omitempty"`

	// queryCapacity
	Capacity *core.ServerCapacity `json:"capacity,omitempty"`
}

// MailboxInfo represents a mailbox in list responses.
type MailboxInfo struct {
	FolderPath    string `json:"folderPath"`
	Type          string `json:"type"`
	MaxMessages   int    `json:"maxMessages"`
	RetentionDays int    `json:"retentionDays"`
	CreatedAt     int64  `json:"createdAt"`
}

// ACLEntry represents an access control entry in list responses.
type ACLEntry struct {
	PeerID     string `json:"peerId"`
	AccessMode string `json:"accessMode"`
	GrantedAt  int64  `json:"grantedAt"`
}

// Handler is the Mailbox Management Agent protocol handler.
type Handler struct {
	mda            *mda.MailboxServer
	config         *core.ServerConfig
	logger         *slog.Logger
	rateLimitMu    sync.Mutex
	requestHistory map[string][]time.Time
	rateLimitWindow time.Duration
	maxRequests    int
}

// NewHandler creates a new MMA handler.
func NewHandler(mailboxServer *mda.MailboxServer, config *core.ServerConfig, logger *slog.Logger) *Handler {
	return &Handler{
		mda:             mailboxServer,
		config:          config,
		logger:          logger,
		requestHistory:  make(map[string][]time.Time),
		rateLimitWindow: time.Minute,
		maxRequests:     50,
	}
}

// HandleStream handles an incoming admin stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.Close()

	// Read length-prefixed frame
	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.sendError(s, "failed to read request")
		return
	}

	// Parse request
	var req AdminRequest
	if err := json.Unmarshal(data, &req); err != nil {
		h.logger.Error("failed to parse request", "error", err)
		h.sendError(s, "invalid request format")
		return
	}

	req.normalize()

	h.logger.Debug("handling admin request",
		"operation", req.OperationType,
		"caller", callerID.String(),
	)

	// Check rate limit
	if !h.checkRateLimit(callerID) {
		h.sendError(s, "rate limit exceeded")
		return
	}

	ctx := context.Background()

	switch req.OperationType {
	case OpCreateMailbox:
		h.handleCreateMailbox(ctx, s, &req, callerID)
	case OpDeleteMailbox:
		h.handleDeleteMailbox(ctx, s, &req, callerID)
	case OpGrantAccess:
		h.handleGrantAccess(ctx, s, &req, callerID)
	case OpRevokeAccess:
		h.handleRevokeAccess(ctx, s, &req, callerID)
	case OpListACL:
		h.handleListACL(ctx, s, &req, callerID)
	case OpListMailboxes:
		h.handleListMailboxes(ctx, s, &req, callerID)
	case OpUpdateConfig:
		h.handleUpdateConfig(ctx, s, &req, callerID)
	case OpGetMailboxInfo:
		h.handleGetMailboxInfo(ctx, s, &req, callerID)
	case OpQueryCapacity:
		h.handleQueryCapacity(ctx, s)
	default:
		h.sendError(s, "unknown operation type: "+req.OperationType)
	}
}

// handleCreateMailbox creates a new mailbox for the caller.
func (h *Handler) handleCreateMailbox(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	// Verify caller is the owner
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	// Parse mailbox type
	mbType := core.MailboxPrivate
	if req.MailboxType != "" {
		var err error
		mbType, err = core.MailboxTypeFromString(req.MailboxType)
		if err != nil {
			h.sendError(s, fmt.Sprintf("invalid mailbox type: %s", req.MailboxType))
			return
		}
	}

	folderPath := req.FolderPath
	if folderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	addr, err := core.NewMailboxAddress(callerID, folderPath, mbType)
	if err != nil {
		h.sendError(s, fmt.Sprintf("invalid mailbox address: %v", err))
		return
	}

	maxMessages := h.config.MaxMessagesPerMailbox
	if req.MaxMessages != nil {
		maxMessages = *req.MaxMessages
	}

	retentionDays := 30
	if req.RetentionDays != nil {
		retentionDays = *req.RetentionDays
	}

	if err := h.mda.CreateMailbox(ctx, addr, maxMessages, retentionDays, req.RetentionCount); err != nil {
		h.sendError(s, fmt.Sprintf("failed to create mailbox: %v", err))
		return
	}

	h.logger.Info("created mailbox",
		"path", addr.FullPath(),
		"type", mbType.String(),
		"caller", callerID.String(),
	)

	h.sendResponse(s, &AdminResponse{Success: true})
}

// handleDeleteMailbox deletes a mailbox owned by the caller.
func (h *Handler) handleDeleteMailbox(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	addr := &core.MailboxAddress{
		OwnerID:    callerID,
		FolderPath: req.FolderPath,
	}

	if err := h.mda.DeleteMailbox(ctx, addr); err != nil {
		h.sendError(s, fmt.Sprintf("failed to delete mailbox: %v", err))
		return
	}

	h.logger.Info("deleted mailbox",
		"path", addr.FullPath(),
		"caller", callerID.String(),
	)

	h.sendResponse(s, &AdminResponse{Success: true})
}

// handleGrantAccess grants access to a peer on a mailbox owned by the caller.
func (h *Handler) handleGrantAccess(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	if req.GranteePeerID == "" {
		h.sendError(s, "granteePeerId is required")
		return
	}

	granteePeerID, err := peer.Decode(req.GranteePeerID)
	if err != nil {
		h.sendError(s, fmt.Sprintf("invalid grantee peer ID: %v", err))
		return
	}

	accessMode := core.AccessReadOnly
	if req.AccessMode != "" {
		accessMode, err = core.AccessModeFromString(req.AccessMode)
		if err != nil {
			h.sendError(s, fmt.Sprintf("invalid access mode: %v", err))
			return
		}
	}

	// Find the mailbox
	record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to find mailbox: %v", err))
		return
	}
	if record == nil {
		h.sendError(s, "mailbox not found")
		return
	}

	if err := h.mda.Storage.GrantAccess(ctx, record.ID, granteePeerID, accessMode); err != nil {
		h.sendError(s, fmt.Sprintf("failed to grant access: %v", err))
		return
	}

	h.logger.Info("granted access",
		"mailbox", record.FullPath(),
		"grantee", granteePeerID.String(),
		"mode", accessMode.String(),
	)

	h.sendResponse(s, &AdminResponse{Success: true})
}

// handleRevokeAccess revokes a peer's access to a mailbox owned by the caller.
func (h *Handler) handleRevokeAccess(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	if req.GranteePeerID == "" {
		h.sendError(s, "granteePeerId is required")
		return
	}

	granteePeerID, err := peer.Decode(req.GranteePeerID)
	if err != nil {
		h.sendError(s, fmt.Sprintf("invalid grantee peer ID: %v", err))
		return
	}

	// Find the mailbox
	record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to find mailbox: %v", err))
		return
	}
	if record == nil {
		h.sendError(s, "mailbox not found")
		return
	}

	if err := h.mda.Storage.RevokeAccess(ctx, record.ID, granteePeerID); err != nil {
		h.sendError(s, fmt.Sprintf("failed to revoke access: %v", err))
		return
	}

	h.logger.Info("revoked access",
		"mailbox", record.FullPath(),
		"grantee", granteePeerID.String(),
	)

	h.sendResponse(s, &AdminResponse{Success: true})
}

// handleListACL lists the ACL entries for a mailbox owned by the caller.
func (h *Handler) handleListACL(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	// Find the mailbox
	record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to find mailbox: %v", err))
		return
	}
	if record == nil {
		h.sendError(s, "mailbox not found")
		return
	}

	aclRecords, err := h.mda.Storage.ListACL(ctx, record.ID)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to list ACL: %v", err))
		return
	}

	entries := make([]ACLEntry, 0, len(aclRecords))
	for _, r := range aclRecords {
		entries = append(entries, ACLEntry{
			PeerID:     r.PeerIDBase58,
			AccessMode: r.AccessMode.String(),
			GrantedAt:  r.GrantedAt.UnixMilli(),
		})
	}

	h.sendResponse(s, &AdminResponse{
		Success: true,
		ACL:     entries,
	})
}

// handleListMailboxes lists all mailboxes owned by the caller.
func (h *Handler) handleListMailboxes(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if err := h.verifyOwner(req.OwnerPeerID, callerID); err != nil {
		h.sendError(s, err.Error())
		return
	}

	records, err := h.mda.ListMailboxes(ctx, callerID)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to list mailboxes: %v", err))
		return
	}

	mailboxes := make([]MailboxInfo, 0, len(records))
	for _, r := range records {
		mailboxes = append(mailboxes, MailboxInfo{
			FolderPath:    r.FolderPath,
			Type:          r.Type.String(),
			MaxMessages:   r.MaxMessages,
			RetentionDays: r.RetentionDays,
			CreatedAt:     r.CreatedAt.UnixMilli(),
		})
	}

	h.sendResponse(s, &AdminResponse{
		Success:   true,
		Mailboxes: mailboxes,
	})
}

// handleUpdateConfig updates the configuration of a mailbox owned by the caller.
func (h *Handler) handleUpdateConfig(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to find mailbox: %v", err))
		return
	}
	if record == nil {
		h.sendError(s, "mailbox not found")
		return
	}

	if req.MaxMessages != nil {
		record.MaxMessages = *req.MaxMessages
	}
	if req.RetentionDays != nil {
		record.RetentionDays = *req.RetentionDays
	}
	if req.RetentionCount != nil {
		record.RetentionCount = req.RetentionCount
	}

	if err := h.mda.Storage.UpdateMailbox(ctx, record); err != nil {
		h.sendError(s, fmt.Sprintf("failed to update mailbox: %v", err))
		return
	}

	h.logger.Info("updated mailbox config",
		"path", record.FullPath(),
		"caller", callerID.String(),
	)

	h.sendResponse(s, &AdminResponse{Success: true})
}

// handleGetMailboxInfo returns info about a specific mailbox owned by the caller.
func (h *Handler) handleGetMailboxInfo(ctx context.Context, s network.Stream, req *AdminRequest, callerID peer.ID) {
	if req.FolderPath == "" {
		h.sendError(s, "folderPath is required")
		return
	}

	record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to find mailbox: %v", err))
		return
	}
	if record == nil {
		h.sendError(s, "mailbox not found")
		return
	}

	msgCount, err := h.mda.Storage.GetMessageCount(ctx, record.ID)
	if err != nil {
		h.sendError(s, fmt.Sprintf("failed to get message count: %v", err))
		return
	}

	// Return data in the format the Dart client expects (MailboxInfo)
	data := map[string]any{
		"address":        record.FullPath(),
		"type":           record.Type.String(),
		"messageCount":   msgCount,
		"createdAt":      record.CreatedAt.UnixMilli(),
		"lastAccessedAt": record.LastAccessAt.UnixMilli(),
		"maxMessages":    record.MaxMessages,
		"retentionDays":  record.RetentionDays,
		"retentionCount": record.RetentionCount,
	}

	resp := map[string]any{
		"success": true,
		"data":    data,
	}

	respData, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("failed to marshal response", "error", err)
		return
	}
	if err := frame.WriteFrame(s, respData); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}
}

// handleQueryCapacity returns server capacity metrics. This operation does not
// require owner verification -- any authenticated peer can query capacity.
func (h *Handler) handleQueryCapacity(_ context.Context, s network.Stream) {
	capacity := &core.ServerCapacity{
		TotalStorageBytes:     h.config.MaxStorageBytes,
		AvailableStorageBytes: h.config.MaxStorageBytes, // TODO: compute actual usage
	}

	h.sendResponse(s, &AdminResponse{
		Success:  true,
		Capacity: capacity,
	})
}

// verifyOwner checks that the caller peer ID matches the claimed owner peer ID.
func (h *Handler) verifyOwner(ownerPeerIDStr string, callerID peer.ID) error {
	if ownerPeerIDStr == "" {
		// If no ownerPeerId provided, the caller is assumed to be the owner.
		return nil
	}
	if ownerPeerIDStr != callerID.String() {
		return fmt.Errorf("unauthorized: caller %s is not the mailbox owner %s", callerID.String(), ownerPeerIDStr)
	}
	return nil
}

// checkRateLimit enforces per-peer rate limiting.
func (h *Handler) checkRateLimit(peerID peer.ID) bool {
	h.rateLimitMu.Lock()
	defer h.rateLimitMu.Unlock()

	now := time.Now()
	key := peerID.String()
	cutoff := now.Add(-h.rateLimitWindow)

	history := h.requestHistory[key]
	filtered := history[:0]
	for _, ts := range history {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}

	if len(filtered) >= h.maxRequests {
		h.requestHistory[key] = filtered
		return false
	}

	h.requestHistory[key] = append(filtered, now)
	return true
}

// sendResponse marshals and writes an AdminResponse.
func (h *Handler) sendResponse(s network.Stream, resp *AdminResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("failed to marshal response", "error", err)
		return
	}
	if err := frame.WriteFrame(s, data); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}
}

// sendError writes an error response.
func (h *Handler) sendError(s network.Stream, message string) {
	h.sendResponse(s, &AdminResponse{
		Success:      false,
		ErrorMessage: message,
	})
}
