package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"sort"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/internal/protocol/mma"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
)

// Client provides access to Ricochet store-and-forward services over libp2p streams.
type Client struct {
	host    host.Host
	cfg     Config
	privKey libp2pcrypto.PrivKey
}

// Config configures the Client.
type Config struct {
	PreferredServers  []ServerPreference
	ConnectionTimeout time.Duration // default 10s
	MessageTimeout    time.Duration // default 30s
}

// ServerPreference identifies a server with priority-based selection.
type ServerPreference struct {
	PeerID   peer.ID
	Priority int // lower = higher priority
	Weight   int
}

// SendResult is the outcome of a SendMessage call.
type SendResult struct {
	Success        bool
	MessageID      string
	StoredAtServer peer.ID
	ErrorMessage   string
}

// ACLEntry represents an access control entry returned by ListACL.
type ACLEntry struct {
	PeerID     string `json:"peerId"`
	AccessMode string `json:"accessMode"`
	GrantedAt  int64  `json:"grantedAt"`
}

// MailboxInfo represents a mailbox returned by ListMailboxes.
type MailboxInfo struct {
	FolderPath    string `json:"folderPath"`
	Type          string `json:"type"`
	MaxMessages   int    `json:"maxMessages"`
	RetentionDays int    `json:"retentionDays"`
	CreatedAt     int64  `json:"createdAt"`
}

// MailboxDetail is the live state of a single mailbox as returned by
// GetMailboxInfo. Unlike MailboxInfo (from ListMailboxes) it carries the current
// MessageCount, so MessageCount vs MaxMessages tells you whether a mailbox has
// hit its cap — once they're equal the server rejects new deposits
// (MailboxFullError), which surfaces to senders as a failed delivery.
type MailboxDetail struct {
	Address        string `json:"address"`
	Type           string `json:"type"`
	MessageCount   int    `json:"messageCount"`
	MaxMessages    int    `json:"maxMessages"`
	RetentionDays  int    `json:"retentionDays"`
	RetentionCount *int   `json:"retentionCount,omitempty"`
	CreatedAt      int64  `json:"createdAt"`
	LastAccessedAt int64  `json:"lastAccessedAt"`
}

// New creates a new Client with the given libp2p host and configuration.
func New(h host.Host, cfg Config) *Client {
	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = 10 * time.Second
	}
	if cfg.MessageTimeout == 0 {
		cfg.MessageTimeout = 30 * time.Second
	}
	privKey := h.Peerstore().PrivKey(h.ID())
	return &Client{host: h, cfg: cfg, privKey: privKey}
}

// PeerID returns the client's own peer ID.
func (c *Client) PeerID() peer.ID {
	return c.host.ID()
}

// selectServer returns the highest-priority preferred server.
func (c *Client) selectServer() (peer.ID, error) {
	if len(c.cfg.PreferredServers) == 0 {
		return "", fmt.Errorf("no preferred servers configured")
	}
	sorted := make([]ServerPreference, len(c.cfg.PreferredServers))
	copy(sorted, c.cfg.PreferredServers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})
	return sorted[0].PeerID, nil
}

// openStream opens a libp2p stream to the given server with the specified protocol.
func (c *Client) openStream(ctx context.Context, serverID peer.ID, pid protocol.ID) (network.Stream, error) {
	s, err := c.host.NewStream(ctx, serverID, pid)
	if err != nil {
		return nil, fmt.Errorf("open stream to %s: %w", serverID.String()[:12], err)
	}
	deadline := time.Now().Add(c.cfg.MessageTimeout)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	if err := s.SetDeadline(deadline); err != nil {
		s.Close()
		return nil, fmt.Errorf("set stream deadline: %w", err)
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// MSA operations
// ---------------------------------------------------------------------------

// SendMessage submits a message to a Ricochet server for delivery to the
// specified recipient. The server is selected from the preferred server list.
func (c *Client) SendMessage(ctx context.Context, recipient peer.ID, payload []byte, opts ...SendOption) (*SendResult, error) {
	cfg := sendConfig{
		Priority: PriorityNormal,
	}
	for _, o := range opts {
		o(&cfg)
	}

	msg := core.NewMessageWithDefaultExpiry(c.host.ID(), recipient, payload)
	msg.Priority = cfg.Priority
	msg.FolderPath = cfg.FolderPath
	msg.Persistent = cfg.Persistent
	if cfg.Expiry > 0 {
		msg.ExpiryTimestamp = time.Now().Add(cfg.Expiry).UnixMilli()
	}

	// Compress payload if requested.
	if cfg.Compress {
		compressedPayload, compressedFlags, err := CompressPayload(msg.Payload, msg.Flags, cfg.CompressionThreshold)
		if err != nil {
			return nil, fmt.Errorf("compress payload: %w", err)
		}
		msg.Payload = compressedPayload
		msg.Flags = compressedFlags
	}

	// Encrypt payload if requested (after compression, before frame encoding).
	if cfg.Encrypt {
		if c.privKey == nil {
			return nil, fmt.Errorf("encryption requires an Ed25519 private key in the host peerstore")
		}
		encrypted, encFlags, err := EncryptPayload(msg.Payload, recipient, c.privKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt payload: %w", err)
		}
		msg.Payload = encrypted
		msg.Flags |= encFlags
	}

	msgData, err := frame.EncodeMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, msa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, msgData); err != nil {
		return nil, fmt.Errorf("write message frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read ack frame: %w", err)
	}

	ack, err := frame.DecodeStoreAck(respData)
	if err != nil {
		return nil, fmt.Errorf("decode store ack: %w", err)
	}

	return &SendResult{
		Success:        ack.Success,
		MessageID:      ack.MessageID,
		StoredAtServer: serverID,
		ErrorMessage:   ack.ErrorMessage,
	}, nil
}

// ---------------------------------------------------------------------------
// MAA operations
// ---------------------------------------------------------------------------

// RetrieveMessages retrieves messages from the server. By default it retrieves
// messages for the client's own peer ID from the inbox folder.
func (c *Client) RetrieveMessages(ctx context.Context, opts ...RetrieveOption) ([]*core.Message, error) {
	cfg := retrieveConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	targetPeerID := c.host.ID()
	if cfg.TargetPeerID != nil {
		targetPeerID = *cfg.TargetPeerID
	}

	req := &core.RetrieveRequest{
		PeerID:       targetPeerID.String(),
		FolderPath:   cfg.FolderPath,
		FromSequence: cfg.FromSequence,
		MaxMessages:  cfg.MaxMessages,
		MinPriority:  cfg.MinPriority,
	}

	reqData, err := frame.EncodeRetrieveRequest(req)
	if err != nil {
		return nil, fmt.Errorf("encode retrieve request: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, maa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write retrieve frame: %w", err)
	}

	// Half-close the write side so the server knows we are done sending.
	if cw, ok := s.(interface{ CloseWrite() error }); ok {
		if err := cw.CloseWrite(); err != nil {
			return nil, fmt.Errorf("close write: %w", err)
		}
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read retrieve response: %w", err)
	}

	resp, err := frame.DecodeRetrieveResponse(respData)
	if err != nil {
		return nil, fmt.Errorf("decode retrieve response: %w", err)
	}

	// Decrypt then decompress message payloads.
	// Order: decrypt first (reverse of send: compress -> encrypt).
	for _, msg := range resp.Messages {
		if msg.Flags.IsEncrypted() {
			if c.privKey == nil {
				return nil, fmt.Errorf("decryption requires an Ed25519 private key in the host peerstore")
			}
			senderPeerID, err := peer.Decode(msg.SenderPeerID)
			if err != nil {
				return nil, fmt.Errorf("parse sender peer ID for decryption: %w", err)
			}
			decrypted, err := DecryptPayload(msg.Payload, senderPeerID, c.privKey)
			if err != nil {
				return nil, fmt.Errorf("decrypt message %s: %w", msg.MessageID, err)
			}
			msg.Payload = decrypted
			msg.Flags = msg.Flags.WithoutFlag(core.FlagEncrypted)
		}
		if msg.Flags.IsCompressed() {
			decompressed, err := DecompressPayload(msg.Payload, msg.Flags, MaxDecompressedSize)
			if err != nil {
				return nil, fmt.Errorf("decompress message %s: %w", msg.MessageID, err)
			}
			msg.Payload = decompressed
			msg.Flags = msg.Flags.WithoutFlag(core.FlagCompressed)
		}
	}

	return resp.Messages, nil
}

// MarkDelivered marks the given message IDs as delivered (seen) on the server.
func (c *Client) MarkDelivered(ctx context.Context, messageIDs []string) (*core.MarkDeliveredAck, error) {
	req := &core.MarkDeliveredRequest{
		OperationType: "markDelivered",
		MessageIDs:    messageIDs,
	}

	reqData, err := frame.EncodeMarkDelivered(req)
	if err != nil {
		return nil, fmt.Errorf("encode mark delivered: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, maa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write mark delivered frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read mark delivered ack: %w", err)
	}

	ack, err := frame.DecodeMarkDeliveredAck(respData)
	if err != nil {
		return nil, fmt.Errorf("decode mark delivered ack: %w", err)
	}

	return ack, nil
}

// UpdateFlags updates IMAP-style flags on a message.
func (c *Client) UpdateFlags(ctx context.Context, messageID string, addFlags, removeFlags uint32) (*core.UpdateFlagsAck, error) {
	req := &core.UpdateFlagsRequest{
		OperationType: "updateFlags",
		MessageID:     messageID,
		AddFlags:      addFlags,
		RemoveFlags:   removeFlags,
	}

	reqData, err := frame.EncodeUpdateFlags(req)
	if err != nil {
		return nil, fmt.Errorf("encode update flags: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, maa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write update flags frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read update flags ack: %w", err)
	}

	ack, err := frame.DecodeUpdateFlagsAck(respData)
	if err != nil {
		return nil, fmt.Errorf("decode update flags ack: %w", err)
	}

	return ack, nil
}

// Expunge deletes all messages with the \Deleted flag in the client's mailbox.
func (c *Client) Expunge(ctx context.Context, opts ...ExpungeOption) (*core.ExpungeAck, error) {
	cfg := expungeConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &core.ExpungeRequest{
		OperationType: "expunge",
		PeerID:        c.host.ID().String(),
		FolderPath:    cfg.FolderPath,
	}

	reqData, err := frame.EncodeExpunge(req)
	if err != nil {
		return nil, fmt.Errorf("encode expunge: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, maa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write expunge frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read expunge ack: %w", err)
	}

	ack, err := frame.DecodeExpungeAck(respData)
	if err != nil {
		return nil, fmt.Errorf("decode expunge ack: %w", err)
	}

	return ack, nil
}

// DeleteMessages immediately deletes messages by their IDs.
func (c *Client) DeleteMessages(ctx context.Context, messageIDs []string) (*core.DeleteMessagesAck, error) {
	req := &core.DeleteMessagesRequest{
		OperationType: "deleteMessages",
		MessageIDs:    messageIDs,
	}

	reqData, err := frame.EncodeDeleteMessages(req)
	if err != nil {
		return nil, fmt.Errorf("encode delete messages: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, maa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write delete messages frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read delete messages ack: %w", err)
	}

	ack, err := frame.DecodeDeleteMessagesAck(respData)
	if err != nil {
		return nil, fmt.Errorf("decode delete messages ack: %w", err)
	}

	return ack, nil
}

// ---------------------------------------------------------------------------
// MMA operations
// ---------------------------------------------------------------------------

// doAdminRaw performs one MMA request/response round-trip and returns the raw
// response frame. Most callers want doAdmin (which decodes the standard
// AdminResponse envelope); getMailboxInfo returns a non-standard {success,data}
// envelope, so it decodes the raw bytes itself.
func (c *Client) doAdminRaw(ctx context.Context, req *mma.AdminRequest) ([]byte, error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal admin request: %w", err)
	}

	serverID, err := c.selectServer()
	if err != nil {
		return nil, err
	}

	s, err := c.openStream(ctx, serverID, mma.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write admin frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read admin response: %w", err)
	}

	return respData, nil
}

// doAdmin is the shared helper for MMA admin operations that return the standard
// AdminResponse envelope.
func (c *Client) doAdmin(ctx context.Context, req *mma.AdminRequest) (*mma.AdminResponse, error) {
	respData, err := c.doAdminRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	var resp mma.AdminResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal admin response: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("admin operation %s failed: %s", req.OperationType, resp.ErrorMessage)
	}

	return &resp, nil
}

// CreateMailbox creates a new mailbox on the server.
func (c *Client) CreateMailbox(ctx context.Context, folderPath string, mailboxType core.MailboxType, opts ...MailboxOption) error {
	cfg := mailboxConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &mma.AdminRequest{
		OperationType:  mma.OpCreateMailbox,
		OwnerPeerID:    c.host.ID().String(),
		FolderPath:     folderPath,
		MailboxType:    mailboxType.String(),
		MaxMessages:    cfg.MaxMessages,
		RetentionDays:  cfg.RetentionDays,
		RetentionCount: cfg.RetentionCount,
	}

	_, err := c.doAdmin(ctx, req)
	return err
}

// DeleteMailbox deletes a mailbox on the server.
func (c *Client) DeleteMailbox(ctx context.Context, folderPath string) error {
	req := &mma.AdminRequest{
		OperationType: mma.OpDeleteMailbox,
		OwnerPeerID:   c.host.ID().String(),
		FolderPath:    folderPath,
	}

	_, err := c.doAdmin(ctx, req)
	return err
}

// GrantAccess grants a peer access to a mailbox.
func (c *Client) GrantAccess(ctx context.Context, folderPath string, grantee peer.ID, mode core.AccessMode) error {
	req := &mma.AdminRequest{
		OperationType: mma.OpGrantAccess,
		OwnerPeerID:   c.host.ID().String(),
		FolderPath:    folderPath,
		GranteePeerID: grantee.String(),
		AccessMode:    mode.String(),
	}

	_, err := c.doAdmin(ctx, req)
	return err
}

// RevokeAccess revokes a peer's access to a mailbox.
func (c *Client) RevokeAccess(ctx context.Context, folderPath string, grantee peer.ID) error {
	req := &mma.AdminRequest{
		OperationType: mma.OpRevokeAccess,
		OwnerPeerID:   c.host.ID().String(),
		FolderPath:    folderPath,
		GranteePeerID: grantee.String(),
	}

	_, err := c.doAdmin(ctx, req)
	return err
}

// ListACL returns the access control entries for a mailbox.
func (c *Client) ListACL(ctx context.Context, folderPath string) ([]ACLEntry, error) {
	req := &mma.AdminRequest{
		OperationType: mma.OpListACL,
		OwnerPeerID:   c.host.ID().String(),
		FolderPath:    folderPath,
	}

	resp, err := c.doAdmin(ctx, req)
	if err != nil {
		return nil, err
	}

	entries := make([]ACLEntry, 0, len(resp.ACL))
	for _, a := range resp.ACL {
		entries = append(entries, ACLEntry{
			PeerID:     a.PeerID,
			AccessMode: a.AccessMode,
			GrantedAt:  a.GrantedAt,
		})
	}

	return entries, nil
}

// ListMailboxes returns all mailboxes owned by the client.
func (c *Client) ListMailboxes(ctx context.Context) ([]MailboxInfo, error) {
	req := &mma.AdminRequest{
		OperationType: mma.OpListMailboxes,
		OwnerPeerID:   c.host.ID().String(),
	}

	resp, err := c.doAdmin(ctx, req)
	if err != nil {
		return nil, err
	}

	infos := make([]MailboxInfo, 0, len(resp.Mailboxes))
	for _, m := range resp.Mailboxes {
		infos = append(infos, MailboxInfo{
			FolderPath:    m.FolderPath,
			Type:          m.Type,
			MaxMessages:   m.MaxMessages,
			RetentionDays: m.RetentionDays,
			CreatedAt:     m.CreatedAt,
		})
	}

	return infos, nil
}

// GetMailboxInfo returns the live state of one of the caller's mailboxes,
// including its current MessageCount. This is the observability call for
// diagnosing a mailbox that may have hit its MaxMessages cap: once
// MessageCount == MaxMessages the server rejects new deposits, so senders see
// their deliveries fail. folderPath is the mailbox folder (e.g. "vault/<id>").
//
// The mailbox must be owned by this client's peer — the server resolves it
// against the caller's identity, so query a mailbox from the peer that owns it.
func (c *Client) GetMailboxInfo(ctx context.Context, folderPath string) (*MailboxDetail, error) {
	req := &mma.AdminRequest{
		OperationType: mma.OpGetMailboxInfo,
		OwnerPeerID:   c.host.ID().String(),
		FolderPath:    folderPath,
	}

	// getMailboxInfo replies with a non-standard {success, data} envelope (to
	// match the Dart client), so decode the raw frame rather than AdminResponse.
	respData, err := c.doAdminRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	var envelope struct {
		Success      bool           `json:"success"`
		ErrorMessage string         `json:"errorMessage"`
		Data         *MailboxDetail `json:"data"`
	}
	if err := json.Unmarshal(respData, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal mailbox info: %w", err)
	}
	if !envelope.Success {
		return nil, fmt.Errorf("admin operation %s failed: %s", req.OperationType, envelope.ErrorMessage)
	}
	if envelope.Data == nil {
		return nil, fmt.Errorf("mailbox info response had no data")
	}

	return envelope.Data, nil
}

// ---------------------------------------------------------------------------
// Directory operations
// ---------------------------------------------------------------------------

// DirectoryListing contains the data for a directory listing.
type DirectoryListing struct {
	DisplayName string         `json:"displayName"`
	Bio         string         `json:"bio,omitempty"`
	AvatarHash  string         `json:"avatarHash,omitempty"`
	Extras      map[string]any `json:"extras,omitempty"`
}

// DirectoryEntry is a single entry in directory browse results.
type DirectoryEntry struct {
	OwnerPeerID string         `json:"ownerPeerId"`
	DisplayName string         `json:"displayName"`
	Bio         string         `json:"bio"`
	AvatarHash  string         `json:"avatarHash"`
	ListedAt    string         `json:"listedAt"`
	UpdatedAt   string         `json:"updatedAt"`
	Extras      map[string]any `json:"extras,omitempty"`
}

// DirectoryBrowseResult is the result of a BrowseDirectory call.
type DirectoryBrowseResult struct {
	Entries    []*DirectoryEntry `json:"entries"`
	NextCursor string            `json:"nextCursor,omitempty"`
	HasMore    bool              `json:"hasMore"`
}

// JoinDirectory opts the client into the public directory on a server.
func (c *Client) JoinDirectory(ctx context.Context, listing DirectoryListing, opts ...DocOption) error {
	listingBytes, err := json.Marshal(listing)
	if err != nil {
		return fmt.Errorf("marshal listing: %w", err)
	}

	req := &sda.DocRequest{
		Operation:       sda.OpDIRECTORY,
		OwnerPeerID:     c.host.ID().String(),
		DirectoryAction: "join",
		Body:            base64.StdEncoding.EncodeToString(listingBytes),
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return err
	}

	if resp.Status >= 400 {
		errMsg := resp.Headers["Error"]
		if errMsg == "" {
			errMsg = fmt.Sprintf("join directory failed with status %d", resp.Status)
		}
		return fmt.Errorf("join directory: %s", errMsg)
	}

	return nil
}

// LeaveDirectory removes the client from the public directory on a server.
func (c *Client) LeaveDirectory(ctx context.Context, opts ...DocOption) error {
	req := &sda.DocRequest{
		Operation:       sda.OpDIRECTORY,
		OwnerPeerID:     c.host.ID().String(),
		DirectoryAction: "leave",
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return err
	}

	if resp.Status >= 400 {
		errMsg := resp.Headers["Error"]
		if errMsg == "" {
			errMsg = fmt.Sprintf("leave directory failed with status %d", resp.Status)
		}
		return fmt.Errorf("leave directory: %s", errMsg)
	}

	return nil
}

// BrowseDirectory browses the public directory with optional search and pagination.
func (c *Client) BrowseDirectory(ctx context.Context, opts ...DirectoryBrowseOption) (*DirectoryBrowseResult, error) {
	cfg := directoryBrowseConfig{Limit: 20}
	for _, o := range opts {
		o(&cfg)
	}

	req := &sda.DocRequest{
		Operation:       sda.OpDIRECTORY,
		OwnerPeerID:     c.host.ID().String(),
		DirectoryAction: "browse",
		DirectoryQuery:  cfg.Query,
		DirectoryCursor: cfg.Cursor,
		DirectoryLimit:  &cfg.Limit,
	}

	var docOpts []DocOption
	if cfg.ServerPeerID != nil {
		docOpts = append(docOpts, WithDocServer(*cfg.ServerPeerID))
	}

	resp, err := c.doDoc(ctx, req, docOpts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sda.StatusOK {
		return nil, fmt.Errorf("browse directory failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return &DirectoryBrowseResult{}, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode browse body: %w", err)
	}

	var result DirectoryBrowseResult
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal browse result: %w", err)
	}

	return &result, nil
}

// GetDirectoryEntry retrieves a specific peer's directory entry.
func (c *Client) GetDirectoryEntry(ctx context.Context, peerID peer.ID, opts ...DocOption) (*DirectoryEntry, error) {
	req := &sda.DocRequest{
		Operation:       sda.OpDIRECTORY,
		OwnerPeerID:     peerID.String(),
		DirectoryAction: "get",
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sda.StatusNotFound {
		return nil, nil
	}

	if resp.Status != sda.StatusOK {
		return nil, fmt.Errorf("get directory entry failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode entry body: %w", err)
	}

	var entry DirectoryEntry
	if err := json.Unmarshal(bodyBytes, &entry); err != nil {
		return nil, fmt.Errorf("unmarshal directory entry: %w", err)
	}

	return &entry, nil
}

// QueryCapacity returns the server's capacity metrics.
func (c *Client) QueryCapacity(ctx context.Context) (*core.ServerCapacity, error) {
	req := &mma.AdminRequest{
		OperationType: mma.OpQueryCapacity,
	}

	resp, err := c.doAdmin(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Capacity, nil
}

// ---------------------------------------------------------------------------
// SDA operations
// ---------------------------------------------------------------------------

// headerToInt64 extracts an int64 from a response header value that may be
// a float64 (from JSON unmarshal), a string, or an int.
func headerToInt64(v any) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case string:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n
		}
	}
	return 0
}

// doDoc is the shared helper for all SDA document operations.
func (c *Client) doDoc(ctx context.Context, req *sda.DocRequest, opts []DocOption) (*sda.DocResponse, error) {
	cfg := docConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	// Apply If-None-Match header.
	if cfg.IfNoneMatch != "" {
		if req.Headers == nil {
			req.Headers = make(map[string]string)
		}
		req.Headers["If-None-Match"] = cfg.IfNoneMatch
	}

	// Apply If-Match header.
	if cfg.IfMatch != "" {
		if req.Headers == nil {
			req.Headers = make(map[string]string)
		}
		req.Headers["If-Match"] = cfg.IfMatch
	}

	// Apply Content-Type header.
	if cfg.ContentType != "" {
		if req.Headers == nil {
			req.Headers = make(map[string]string)
		}
		req.Headers["Content-Type"] = cfg.ContentType
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal doc request: %w", err)
	}

	serverID := peer.ID("")
	if cfg.ServerPeerID != nil {
		serverID = *cfg.ServerPeerID
	} else {
		serverID, err = c.selectServer()
		if err != nil {
			return nil, err
		}
	}

	s, err := c.openStream(ctx, serverID, sda.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write doc frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read doc response: %w", err)
	}

	var resp sda.DocResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal doc response: %w", err)
	}

	return &resp, nil
}

// GetDocument retrieves a document by owner peer ID and path.
func (c *Client) GetDocument(ctx context.Context, ownerPeerID peer.ID, path string, opts ...DocOption) (*core.DocumentResponse, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpGET,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	result := &core.DocumentResponse{
		Status: resp.Status,
	}

	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag, _ = etag.(string)
	}
	if ct, ok := resp.Headers["Content-Type"]; ok {
		result.ContentType, _ = ct.(string)
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		result.LastModified = headerToInt64(lm)
	}

	if resp.Body != "" {
		content, err := base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decode document body: %w", err)
		}
		result.Content = content
	}

	return result, nil
}

// PutDocument creates or replaces a document.
func (c *Client) PutDocument(ctx context.Context, ownerPeerID peer.ID, path string, content []byte, opts ...DocOption) (*core.DocumentPutResponse, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpPUT,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
		Headers:     make(map[string]string),
		Body:        base64.StdEncoding.EncodeToString(content),
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("document put failed with status %d", resp.Status)
		}
		return nil, fmt.Errorf("document put: %s", errMsg)
	}

	result := &core.DocumentPutResponse{
		Status:  resp.Status,
		Created: resp.Status == sda.StatusCreated,
	}

	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag, _ = etag.(string)
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		result.LastModified = headerToInt64(lm)
	}

	return result, nil
}

// PatchDocument applies a partial update (merge-patch) to a document.
func (c *Client) PatchDocument(ctx context.Context, ownerPeerID peer.ID, path string, patch map[string]any, opts ...DocOption) (*core.DocumentPutResponse, error) {
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshal patch: %w", err)
	}

	req := &sda.DocRequest{
		Operation:   sda.OpPATCH,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
		Headers: map[string]string{
			"Content-Type": "application/merge-patch+json",
		},
		Body: base64.StdEncoding.EncodeToString(patchBytes),
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	result := &core.DocumentPutResponse{
		Status: resp.Status,
	}

	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag, _ = etag.(string)
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		result.LastModified = headerToInt64(lm)
	}

	return result, nil
}

// HeadDocument retrieves metadata for a document without its body.
func (c *Client) HeadDocument(ctx context.Context, ownerPeerID peer.ID, path string, opts ...DocOption) (*core.DocumentMetadata, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpHEAD,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sda.StatusNotFound {
		return nil, fmt.Errorf("document not found")
	}

	result := &core.DocumentMetadata{}

	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag, _ = etag.(string)
	}
	if ct, ok := resp.Headers["Content-Type"]; ok {
		result.ContentType, _ = ct.(string)
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		result.LastModified = headerToInt64(lm)
	}
	if cl, ok := resp.Headers["Content-Length"]; ok {
		result.ContentLength = int(headerToInt64(cl))
	}

	return result, nil
}

// DeleteDocument deletes a document. Returns true if the document existed and
// was deleted, false if it was not found.
func (c *Client) DeleteDocument(ctx context.Context, ownerPeerID peer.ID, path string, opts ...DocOption) (bool, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpDELETE,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return false, err
	}

	return resp.Status != sda.StatusNotFound, nil
}

// ListDocuments lists all documents for the given owner peer.
//
// The server pages listings, so this walks the cursor until it is exhausted and
// returns the complete set -- the contract callers already relied on when
// listing was unbounded server-side.
func (c *Client) ListDocuments(ctx context.Context, ownerPeerID peer.ID, opts ...DocOption) ([]core.DocumentInfo, error) {
	var (
		infos  []core.DocumentInfo
		cursor string
	)

	for {
		page, next, err := c.listDocumentPage(ctx, ownerPeerID, cursor, opts)
		if err != nil {
			return nil, err
		}
		infos = append(infos, page...)
		if next == "" {
			return infos, nil
		}
		cursor = next
	}
}

// listDocumentPage fetches one page and returns the cursor for the next one, or
// "" when the listing is complete.
func (c *Client) listDocumentPage(ctx context.Context, ownerPeerID peer.ID, cursor string, opts []DocOption) ([]core.DocumentInfo, string, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
		ListCursor:  cursor,
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, "", err
	}

	if resp.Status != sda.StatusOK {
		return nil, "", fmt.Errorf("list documents failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, "", nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("decode list body: %w", err)
	}

	// The server returns a list of docEntry objects; map them to core.DocumentInfo.
	type docEntry struct {
		Path          string `json:"path"`
		ContentType   string `json:"contentType"`
		ContentHash   string `json:"contentHash"`
		Size          int    `json:"size"`
		UpdatedAt     string `json:"updatedAt"`
		VersionNumber int    `json:"versionNumber"`
	}

	var entries []docEntry
	if err := json.Unmarshal(bodyBytes, &entries); err != nil {
		return nil, "", fmt.Errorf("unmarshal document list: %w", err)
	}

	infos := make([]core.DocumentInfo, 0, len(entries))
	for _, e := range entries {
		var lastMod int64
		if t, err := time.Parse(time.RFC3339, e.UpdatedAt); err == nil {
			lastMod = t.UnixMilli()
		}
		infos = append(infos, core.DocumentInfo{
			Path:         e.Path,
			ContentType:  e.ContentType,
			Size:         e.Size,
			ETag:         e.ContentHash,
			LastModified: lastMod,
		})
	}

	// A server that predates paging sends neither header; the absent Has-More
	// reads as false and the loop terminates after one page, as it always did.
	next, _ := resp.Headers["Next-Cursor"].(string)
	if hasMore, _ := resp.Headers["Has-More"].(bool); !hasMore {
		next = ""
	}

	return infos, next, nil
}
