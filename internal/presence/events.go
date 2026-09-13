package presence

import (
	"encoding/json"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PresenceChange represents a single peer's state change.
type PresenceChange struct {
	PeerID     peer.ID       `json:"peerId"`
	State      PresenceState `json:"state"`
	TTLSeconds int           `json:"ttlSeconds"`
}

// PresenceEvent is a batch of presence changes published via GossipSub.
type PresenceEvent struct {
	Type      string           `json:"type"` // "event"
	ServerID  peer.ID          `json:"serverId"`
	Timestamp time.Time        `json:"timestamp"`
	Changes   []PresenceChange `json:"changes"`
}

// Encode serializes a PresenceEvent to JSON bytes.
func (e *PresenceEvent) Encode() ([]byte, error) {
	e.Type = "event"
	return json.Marshal(e)
}

// DecodePresenceEvent deserializes a PresenceEvent from JSON bytes.
func DecodePresenceEvent(data []byte) (*PresenceEvent, error) {
	var e PresenceEvent
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// PresenceHeartbeat is a periodic liveness signal from a server carrying
// its online set.
//
// A large online set is split across pages: every page of one heartbeat
// shares the HeartbeatSequence, Timestamp and OnlineCount, and carries its
// own slice of OnlinePeerIDs. A subscriber has the whole set once it holds
// all PageCount pages of a sequence. A heartbeat with PageCount of 0 or 1
// is complete on its own, so a message from before paging still reads the
// same way.
type PresenceHeartbeat struct {
	Type              string    `json:"type"` // "heartbeat"
	ServerID          peer.ID   `json:"serverId"`
	Timestamp         time.Time `json:"timestamp"`
	OnlineCount       int       `json:"onlineCount"`
	OnlinePeerIDs     []peer.ID `json:"onlinePeerIds"`
	HeartbeatSequence uint64    `json:"heartbeatSequence"`
	Page              int       `json:"page,omitempty"`
	PageCount         int       `json:"pageCount,omitempty"`
}

// Encode serializes a PresenceHeartbeat to JSON bytes.
func (h *PresenceHeartbeat) Encode() ([]byte, error) {
	h.Type = "heartbeat"
	return json.Marshal(h)
}

// DecodePresenceHeartbeat deserializes a PresenceHeartbeat from JSON bytes.
func DecodePresenceHeartbeat(data []byte) (*PresenceHeartbeat, error) {
	var h PresenceHeartbeat
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// messageType is a helper for detecting the message type from JSON.
type messageType struct {
	Type string `json:"type"`
}

// DetectMessageType returns the "type" field from a JSON message.
func DetectMessageType(data []byte) string {
	var mt messageType
	if err := json.Unmarshal(data, &mt); err != nil {
		return ""
	}
	return mt.Type
}

// VisibilityPreference controls who can see a peer's presence.
type VisibilityPreference int

const (
	// VisibilityEveryone allows all peers to see presence.
	VisibilityEveryone VisibilityPreference = iota
	// VisibilityNobody hides presence from all peers.
	VisibilityNobody
)
