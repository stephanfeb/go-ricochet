package core

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	MaxHopCount    = 10
	MaxPayloadSize = 10 * 1024 * 1024 // 10MB
)

// Message represents a store-and-forward message.
type Message struct {
	MessageID        string          `json:"messageId"`
	RecipientPeerID  string          `json:"recipientPeerId"`
	SenderPeerID     string          `json:"senderPeerId"`
	Payload          []byte          `json:"-"` // handled separately for base64
	Priority         MessagePriority `json:"priority"`
	ExpiryTimestamp  int64           `json:"expiryTimestamp"`
	HopCount         int             `json:"hopCount"`
	Flags            SFMessageFlags  `json:"flags"`
	CreatedTimestamp int64           `json:"createdTimestamp"`
	FolderPath       string          `json:"folderPath,omitempty"`
	SequenceNumber   uint64          `json:"sequenceNumber,omitempty"`
	MsgFlags         MessageFlags    `json:"messageFlags,omitempty"`
	Persistent       bool            `json:"persistent,omitempty"`
}

// NewMessage creates a new message with a generated UUID and current timestamp.
func NewMessage(senderID, recipientID peer.ID, payload []byte) *Message {
	return &Message{
		MessageID:        uuid.New().String(),
		SenderPeerID:     senderID.String(),
		RecipientPeerID:  recipientID.String(),
		Payload:          payload,
		Priority:         PriorityNormal,
		HopCount:         0,
		Flags:            FlagNone,
		CreatedTimestamp: time.Now().UnixMilli(),
	}
}

// NewMessageWithDefaultExpiry creates a message with a 7-day expiry.
func NewMessageWithDefaultExpiry(senderID, recipientID peer.ID, payload []byte) *Message {
	msg := NewMessage(senderID, recipientID, payload)
	msg.ExpiryTimestamp = time.Now().Add(7 * 24 * time.Hour).UnixMilli()
	return msg
}

// IsExpired returns true if the message has expired.
func (m *Message) IsExpired() bool {
	if m.ExpiryTimestamp == 0 {
		return false
	}
	return time.Now().UnixMilli() > m.ExpiryTimestamp
}

// TimeUntilExpiry returns the duration until the message expires.
func (m *Message) TimeUntilExpiry() time.Duration {
	if m.ExpiryTimestamp == 0 {
		return time.Duration(0)
	}
	return time.Until(time.UnixMilli(m.ExpiryTimestamp))
}

// Age returns the age of the message.
func (m *Message) Age() time.Duration {
	return time.Since(time.UnixMilli(m.CreatedTimestamp))
}

// WithIncrementedHopCount returns a copy with incremented hop count and forwarded flag.
func (m *Message) WithIncrementedHopCount() (*Message, error) {
	if m.HopCount >= MaxHopCount {
		return nil, fmt.Errorf("max hop count (%d) exceeded", MaxHopCount)
	}
	copy := *m
	copy.HopCount++
	copy.Flags = copy.Flags.WithFlag(FlagForwarded)
	return &copy, nil
}

// SenderID returns the sender as a peer.ID.
func (m *Message) SenderID() (peer.ID, error) {
	return peer.Decode(m.SenderPeerID)
}

// RecipientID returns the recipient as a peer.ID.
func (m *Message) RecipientID() (peer.ID, error) {
	return peer.Decode(m.RecipientPeerID)
}

// ToJSON serializes the message to JSON with base64-encoded payload.
func (m *Message) ToJSON() ([]byte, error) {
	type jsonMsg struct {
		Message
		PayloadB64 string `json:"payload"`
	}
	jm := jsonMsg{
		Message:    *m,
		PayloadB64: base64.StdEncoding.EncodeToString(m.Payload),
	}
	return json.Marshal(jm)
}

// MessageFromJSON deserializes a message from JSON with base64-encoded payload.
func MessageFromJSON(data []byte) (*Message, error) {
	type jsonMsg struct {
		Message
		PayloadB64 string `json:"payload"`
	}
	var jm jsonMsg
	if err := json.Unmarshal(data, &jm); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	payload, err := base64.StdEncoding.DecodeString(jm.PayloadB64)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	msg := jm.Message
	msg.Payload = payload
	return &msg, nil
}

// ToMap returns a lightweight map representation (no payload).
func (m *Message) ToMap() map[string]any {
	return map[string]any{
		"messageId":        m.MessageID,
		"recipientPeerId":  m.RecipientPeerID,
		"senderPeerId":     m.SenderPeerID,
		"priority":         m.Priority,
		"expiryTimestamp":  m.ExpiryTimestamp,
		"hopCount":         m.HopCount,
		"flags":            uint32(m.Flags),
		"createdTimestamp": m.CreatedTimestamp,
		"folderPath":       m.FolderPath,
		"sequenceNumber":   m.SequenceNumber,
		"messageFlags":     uint32(m.MsgFlags),
		"persistent":       m.Persistent,
	}
}

// StoreAck is the acknowledgment response for a store operation.
type StoreAck struct {
	MessageID             string `json:"messageId"`
	Success               bool   `json:"success"`
	ErrorMessage          string `json:"errorMessage,omitempty"`
	EstimatedDeliveryTime int64  `json:"estimatedDeliveryTime,omitempty"`
}

// RetrieveRequest represents a request to retrieve messages.
type RetrieveRequest struct {
	PeerID       string           `json:"peerId"`
	FolderPath   string           `json:"folderPath,omitempty"`
	FromSequence *uint64          `json:"fromSequence,omitempty"`
	MaxMessages  *int             `json:"maxMessages,omitempty"`
	MinPriority  *MessagePriority `json:"minPriority,omitempty"`
}

// RetrieveResponse contains retrieved messages.
type RetrieveResponse struct {
	Messages []*Message `json:"messages"`
	HasMore  bool       `json:"hasMore"`
}

// ServerCapacity contains server capacity metrics.
type ServerCapacity struct {
	TotalStorageBytes     int64 `json:"totalStorageBytes"`
	UsedStorageBytes      int64 `json:"usedStorageBytes"`
	AvailableStorageBytes int64 `json:"availableStorageBytes"`
	MessageCount          int   `json:"messageCount"`
	ActiveMailboxes       int   `json:"activeMailboxes"`

	// HealthScore is the fraction of the storage budget still free: 1.0 on an
	// empty server, 0.0 when it is spent. It reports storage headroom only,
	// despite the name.
	HealthScore float64 `json:"healthScore"`

	// SampledAt is when these figures were taken. The aggregates scan, so they
	// are sampled on a timer rather than computed per request, and the age
	// travels with them — a cached number that has silently gone stale is
	// worse than no number.
	SampledAt time.Time `json:"sampledAt,omitzero"`
}

// UsagePercent returns the storage usage percentage.
func (c *ServerCapacity) UsagePercent() float64 {
	if c.TotalStorageBytes == 0 {
		return 0
	}
	return float64(c.UsedStorageBytes) / float64(c.TotalStorageBytes) * 100
}

// IsNearCapacity returns true if usage is above 90%.
func (c *ServerCapacity) IsNearCapacity() bool {
	return c.UsagePercent() > 90
}

// MarkDeliveredRequest marks messages as delivered (seen).
type MarkDeliveredRequest struct {
	OperationType string   `json:"operationType"`
	MessageIDs    []string `json:"messageIds"`
	FolderPath    string   `json:"folderPath,omitempty"`
}

// MarkDeliveredAck is the response for mark delivered.
type MarkDeliveredAck struct {
	Success      bool   `json:"success"`
	UpdatedCount int    `json:"updatedCount"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// UpdateFlagsRequest updates IMAP-style flags on a message.
type UpdateFlagsRequest struct {
	OperationType string `json:"operationType"`
	MessageID     string `json:"messageId"`
	AddFlags      uint32 `json:"addFlags"`
	RemoveFlags   uint32 `json:"removeFlags"`
}

// UpdateFlagsAck is the response for flag updates.
type UpdateFlagsAck struct {
	Success      bool    `json:"success"`
	NewFlags     *uint32 `json:"newFlags,omitempty"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

// ExpungeRequest deletes messages with the \Deleted flag.
type ExpungeRequest struct {
	OperationType string `json:"operationType"`
	PeerID        string `json:"peerId"`
	FolderPath    string `json:"folderPath,omitempty"`
}

// ExpungeAck is the response for expunge operations.
type ExpungeAck struct {
	Success      bool   `json:"success"`
	DeletedCount int    `json:"deletedCount"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// DeleteMessagesRequest immediately deletes messages by ID.
type DeleteMessagesRequest struct {
	OperationType string   `json:"operationType"`
	MessageIDs    []string `json:"messageIds"`
}

// DeleteMessagesAck is the response for delete operations.
type DeleteMessagesAck struct {
	Success      bool   `json:"success"`
	DeletedCount int    `json:"deletedCount"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// DocumentResponse is the response for document GET operations.
type DocumentResponse struct {
	Status       int    `json:"status"`
	ETag         string `json:"etag,omitempty"`
	ContentType  string `json:"contentType,omitempty"`
	LastModified int64  `json:"lastModified,omitempty"`
	Content      []byte `json:"content,omitempty"`
}

func (r *DocumentResponse) IsNotModified() bool { return r.Status == 304 }
func (r *DocumentResponse) IsNotFound() bool    { return r.Status == 404 }
func (r *DocumentResponse) IsSuccess() bool     { return r.Status == 200 }
func (r *DocumentResponse) IsForbidden() bool   { return r.Status == 403 }

// DocumentPutResponse is the response for document PUT/PATCH operations.
type DocumentPutResponse struct {
	Status       int    `json:"status"`
	ETag         string `json:"etag,omitempty"`
	LastModified int64  `json:"lastModified,omitempty"`
	Created      bool   `json:"created"`
}

func (r *DocumentPutResponse) IsSuccess() bool {
	return r.Status == 200 || r.Status == 201 || r.Status == 204
}
func (r *DocumentPutResponse) IsConflict() bool  { return r.Status == 409 }
func (r *DocumentPutResponse) IsForbidden() bool { return r.Status == 403 }

// DocumentMetadata contains document metadata (HEAD response).
type DocumentMetadata struct {
	ETag          string `json:"etag"`
	ContentType   string `json:"contentType"`
	LastModified  int64  `json:"lastModified"`
	ContentLength int    `json:"contentLength"`
}

// DocumentInfo represents a document in a LIST response.
type DocumentInfo struct {
	Path         string `json:"path"`
	ContentType  string `json:"contentType"`
	Size         int    `json:"size"`
	ETag         string `json:"etag"`
	LastModified int64  `json:"lastModified"`
}
