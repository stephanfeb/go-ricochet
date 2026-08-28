package mma

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/ratelimit"
)

// ProtocolID is the MMA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/admin/1.0.0")

// Operation type constants for the MMA protocol.
const (
	OpCreateMailbox  = "createMailbox"
	OpDeleteMailbox  = "deleteMailbox"
	OpGrantAccess    = "grantAccess"
	OpRevokeAccess   = "revokeAccess"
	OpListACL        = "listACL"
	OpListMailboxes  = "listMailboxes"
	OpUpdateConfig   = "updateConfig"
	OpQueryCapacity  = "queryCapacity"
	OpGetMailboxInfo = "getMailboxInfo"
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
	GranteePeerID string `json:"granteePeerId,omitempty"`
	TargetPeerID  string `json:"targetPeerId,omitempty"` // Dart-compatible alias
	AccessMode    string `json:"accessMode,omitempty"`
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

// NewPipeline creates a forge pipeline for the Mailbox Management Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).MMA

	routes := map[string]forge.Middleware{
		OpCreateMailbox:  middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleCreateMailbox),
		OpDeleteMailbox:  middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleDeleteMailbox),
		OpGrantAccess:    middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleGrantAccess),
		OpRevokeAccess:   middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleRevokeAccess),
		OpListACL:        middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleListACL),
		OpListMailboxes:  middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleListMailboxes),
		OpUpdateConfig:   middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleUpdateConfig),
		OpQueryCapacity:  middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleQueryCapacity),
		OpGetMailboxInfo: middleware.Chain(forge.JSONDeserialize[AdminRequest](), handleGetMailboxInfo),
	}

	return forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "mma", ""),
		middleware.Recovery(),
		adminResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.RateLimitMiddleware(limiter),
		admission.Middleware(admission.FromRegistry(reg)),
		dartNormalize(),
		middleware.OperationRouter("operationType", routes),
	).WithRegistry(reg)
}

// adminResponseWriter writes an AdminResponse as a JSON frame. On pipeline
// error, it converts the error into an error response so the client always
// gets a response.
func adminResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into error responses.
		if sc.Err != nil && sc.Response == nil {
			errMsg := sc.Err.Error()
			if errors.Is(sc.Err, forge.ErrRateLimited) {
				errMsg = "rate limit exceeded"
			}
			sc.Response = &AdminResponse{
				Success:      false,
				ErrorMessage: errMsg,
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

// dartNormalize is a middleware that normalizes Dart client field names before
// deserialization. It applies the normalize() transformation by unmarshalling
// the raw bytes, normalizing, and re-marshalling back into sc.RawBytes.
func dartNormalize() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		var req AdminRequest
		if err := json.Unmarshal(sc.RawBytes, &req); err != nil {
			sc.Err = fmt.Errorf("invalid request format: %w", err)
			return
		}

		req.normalize()

		normalized, err := json.Marshal(&req)
		if err != nil {
			sc.Err = fmt.Errorf("failed to re-marshal normalized request: %w", err)
			return
		}
		sc.RawBytes = normalized

		next()
	}
}

// verifyOwner checks that the caller peer ID matches the claimed owner peer ID.
func verifyOwner(ownerPeerIDStr string, callerID peer.ID) error {
	if ownerPeerIDStr == "" {
		// If no ownerPeerId provided, the caller is assumed to be the owner.
		return nil
	}
	if ownerPeerIDStr != callerID.String() {
		return fmt.Errorf("unauthorized: caller %s is not the mailbox owner %s", callerID.String(), ownerPeerIDStr)
	}
	return nil
}

// handleCreateMailbox creates a new mailbox for the caller.
func handleCreateMailbox(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	config, _ := forge.ServiceFrom[*core.ServerConfig](sc, "config")
	callerID := sc.PeerID

	// Verify caller is the owner
	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	// Parse mailbox type
	mbType := core.MailboxPrivate
	if req.MailboxType != "" {
		var err error
		mbType, err = core.MailboxTypeFromString(req.MailboxType)
		if err != nil {
			sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("invalid mailbox type: %s", req.MailboxType)}
			return
		}
	}

	folderPath := req.FolderPath
	if folderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	addr, err := core.NewMailboxAddress(callerID, folderPath, mbType)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("invalid mailbox address: %v", err)}
		return
	}

	maxMessages := config.MaxMessagesPerMailbox
	if req.MaxMessages != nil {
		maxMessages = *req.MaxMessages
	}

	retentionDays := 30
	if req.RetentionDays != nil {
		retentionDays = *req.RetentionDays
	}

	ctx := context.Background()
	if err := mailboxServer.CreateMailbox(ctx, addr, maxMessages, retentionDays, req.RetentionCount); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to create mailbox: %v", err)}
		return
	}

	sc.Logger.Info("created mailbox",
		"path", addr.FullPath(),
		"type", mbType.String(),
		"caller", callerID.String(),
	)

	sc.Response = &AdminResponse{Success: true}
}

// handleDeleteMailbox deletes a mailbox owned by the caller.
func handleDeleteMailbox(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	addr := &core.MailboxAddress{
		OwnerID:    callerID,
		FolderPath: req.FolderPath,
	}

	ctx := context.Background()
	if err := mailboxServer.DeleteMailbox(ctx, addr); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to delete mailbox: %v", err)}
		return
	}

	sc.Logger.Info("deleted mailbox",
		"path", addr.FullPath(),
		"caller", callerID.String(),
	)

	sc.Response = &AdminResponse{Success: true}
}

// handleGrantAccess grants access to a peer on a mailbox owned by the caller.
func handleGrantAccess(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	if req.GranteePeerID == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "granteePeerId is required"}
		return
	}

	granteePeerID, err := peer.Decode(req.GranteePeerID)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("invalid grantee peer ID: %v", err)}
		return
	}

	accessMode := core.AccessReadOnly
	if req.AccessMode != "" {
		accessMode, err = core.AccessModeFromString(req.AccessMode)
		if err != nil {
			sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("invalid access mode: %v", err)}
			return
		}
	}

	ctx := context.Background()

	// Find the mailbox
	record, err := mailboxServer.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to find mailbox: %v", err)}
		return
	}
	if record == nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "mailbox not found"}
		return
	}

	if err := mailboxServer.Storage.GrantAccess(ctx, record.ID, granteePeerID, accessMode); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to grant access: %v", err)}
		return
	}

	sc.Logger.Info("granted access",
		"mailbox", record.FullPath(),
		"grantee", granteePeerID.String(),
		"mode", accessMode.String(),
	)

	sc.Response = &AdminResponse{Success: true}
}

// handleRevokeAccess revokes a peer's access to a mailbox owned by the caller.
func handleRevokeAccess(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	if req.GranteePeerID == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "granteePeerId is required"}
		return
	}

	granteePeerID, err := peer.Decode(req.GranteePeerID)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("invalid grantee peer ID: %v", err)}
		return
	}

	ctx := context.Background()

	// Find the mailbox
	record, err := mailboxServer.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to find mailbox: %v", err)}
		return
	}
	if record == nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "mailbox not found"}
		return
	}

	if err := mailboxServer.Storage.RevokeAccess(ctx, record.ID, granteePeerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to revoke access: %v", err)}
		return
	}

	sc.Logger.Info("revoked access",
		"mailbox", record.FullPath(),
		"grantee", granteePeerID.String(),
	)

	sc.Response = &AdminResponse{Success: true}
}

// handleListACL lists the ACL entries for a mailbox owned by the caller.
func handleListACL(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	ctx := context.Background()

	// Find the mailbox
	record, err := mailboxServer.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to find mailbox: %v", err)}
		return
	}
	if record == nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "mailbox not found"}
		return
	}

	aclRecords, err := mailboxServer.Storage.ListACL(ctx, record.ID)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to list ACL: %v", err)}
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

	sc.Response = &AdminResponse{
		Success: true,
		ACL:     entries,
	}
}

// handleListMailboxes lists all mailboxes owned by the caller.
func handleListMailboxes(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if err := verifyOwner(req.OwnerPeerID, callerID); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: err.Error()}
		return
	}

	ctx := context.Background()
	records, err := mailboxServer.ListMailboxes(ctx, callerID)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to list mailboxes: %v", err)}
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

	sc.Response = &AdminResponse{
		Success:   true,
		Mailboxes: mailboxes,
	}
}

// handleUpdateConfig updates the configuration of a mailbox owned by the caller.
func handleUpdateConfig(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	ctx := context.Background()

	record, err := mailboxServer.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to find mailbox: %v", err)}
		return
	}
	if record == nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "mailbox not found"}
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

	if err := mailboxServer.Storage.UpdateMailbox(ctx, record); err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to update mailbox: %v", err)}
		return
	}

	sc.Logger.Info("updated mailbox config",
		"path", record.FullPath(),
		"caller", callerID.String(),
	)

	sc.Response = &AdminResponse{Success: true}
}

// handleGetMailboxInfo returns info about a specific mailbox owned by the caller.
func handleGetMailboxInfo(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*AdminRequest)
	mailboxServer, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	if req.FolderPath == "" {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "folderPath is required"}
		return
	}

	ctx := context.Background()

	record, err := mailboxServer.Storage.FindMailbox(ctx, callerID, req.FolderPath)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to find mailbox: %v", err)}
		return
	}
	if record == nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: "mailbox not found"}
		return
	}

	msgCount, err := mailboxServer.Storage.GetMessageCount(ctx, record.ID)
	if err != nil {
		sc.Response = &AdminResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to get message count: %v", err)}
		return
	}

	// Return data in the format the Dart client expects (MailboxInfo).
	// This uses a map[string]any to match the original response shape.
	sc.Response = map[string]any{
		"success": true,
		"data": map[string]any{
			"address":        record.FullPath(),
			"type":           record.Type.String(),
			"messageCount":   msgCount,
			"createdAt":      record.CreatedAt.UnixMilli(),
			"lastAccessedAt": record.LastAccessAt.UnixMilli(),
			"maxMessages":    record.MaxMessages,
			"retentionDays":  record.RetentionDays,
			"retentionCount": record.RetentionCount,
		},
	}
}

// handleQueryCapacity returns server capacity metrics. This operation does not
// require owner verification -- any authenticated peer can query capacity.
// handleQueryCapacity answers with the server's real storage usage.
//
// It reads the most recent aggregate sample rather than querying: the figures
// scan every mailbox, and running that per admin request would make an
// observability endpoint into a load source. The sample's age travels with it
// in SampledAt.
//
// When no sample has completed the request fails rather than returning zeroes.
// This handler previously reported AvailableStorageBytes equal to the
// configured maximum and left everything else at zero, so it told every caller
// storage was 100% free no matter what was on disk. Refusing to answer is
// worse for the caller and better for the operator: an error gets
// investigated, a plausible wrong number does not.
func handleQueryCapacity(sc *forge.StreamContext, next func()) {
	sampler := capacity.FromRegistry(sc.Registry)

	view, err := sampler.Capacity()
	if err != nil {
		sc.Logger.Warn("capacity query before the first sample completed", "error", err)
		sc.Response = &AdminResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}
		return
	}

	sc.Response = &AdminResponse{
		Success:  true,
		Capacity: view,
	}
}
