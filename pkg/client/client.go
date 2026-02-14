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
		Priority: core.PriorityNormal,
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

// doAdmin is the shared helper for all MMA admin operations.
func (c *Client) doAdmin(ctx context.Context, req *mma.AdminRequest) (*mma.AdminResponse, error) {
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
		result.ETag = etag
	}
	if ct, ok := resp.Headers["Content-Type"]; ok {
		result.ContentType = ct
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		if t, err := time.Parse(time.RFC3339, lm); err == nil {
			result.LastModified = t.UnixMilli()
		}
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

	result := &core.DocumentPutResponse{
		Status:  resp.Status,
		Created: resp.Status == sda.StatusCreated,
	}

	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag = etag
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		if t, err := time.Parse(time.RFC3339, lm); err == nil {
			result.LastModified = t.UnixMilli()
		}
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
		result.ETag = etag
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		if t, err := time.Parse(time.RFC3339, lm); err == nil {
			result.LastModified = t.UnixMilli()
		}
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
		result.ETag = etag
	}
	if ct, ok := resp.Headers["Content-Type"]; ok {
		result.ContentType = ct
	}
	if lm, ok := resp.Headers["Last-Modified"]; ok {
		if t, err := time.Parse(time.RFC3339, lm); err == nil {
			result.LastModified = t.UnixMilli()
		}
	}
	if cl, ok := resp.Headers["Content-Length"]; ok {
		if n, err := strconv.Atoi(cl); err == nil {
			result.ContentLength = n
		}
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
func (c *Client) ListDocuments(ctx context.Context, ownerPeerID peer.ID, opts ...DocOption) ([]core.DocumentInfo, error) {
	req := &sda.DocRequest{
		Operation:   sda.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
	}

	resp, err := c.doDoc(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sda.StatusOK {
		return nil, fmt.Errorf("list documents failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode list body: %w", err)
	}

	// The server returns a list of docEntry objects; map them to core.DocumentInfo.
	type docEntry struct {
		Path         string `json:"path"`
		ContentType  string `json:"contentType"`
		ContentHash  string `json:"contentHash"`
		Size         int    `json:"size"`
		UpdatedAt    string `json:"updatedAt"`
		VersionNumber int   `json:"versionNumber"`
	}

	var entries []docEntry
	if err := json.Unmarshal(bodyBytes, &entries); err != nil {
		return nil, fmt.Errorf("unmarshal document list: %w", err)
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

	return infos, nil
}
