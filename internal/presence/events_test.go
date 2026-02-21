package presence

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func generateTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}
	return id
}

func TestPresenceChange_EncodeDecode(t *testing.T) {
	pid := generateTestPeerID(t)
	change := PresenceChange{
		PeerID:     pid,
		State:      Online,
		TTLSeconds: 120,
	}

	event := &PresenceEvent{
		ServerID:  generateTestPeerID(t),
		Timestamp: time.Now().Truncate(time.Millisecond),
		Changes:   []PresenceChange{change},
	}

	data, err := event.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodePresenceEvent(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "event" {
		t.Errorf("expected type 'event', got %q", decoded.Type)
	}
	if decoded.ServerID != event.ServerID {
		t.Errorf("server ID mismatch")
	}
	if len(decoded.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(decoded.Changes))
	}
	if decoded.Changes[0].PeerID != pid {
		t.Errorf("peer ID mismatch")
	}
	if decoded.Changes[0].State != Online {
		t.Errorf("expected Online, got %v", decoded.Changes[0].State)
	}
	if decoded.Changes[0].TTLSeconds != 120 {
		t.Errorf("expected TTL 120, got %d", decoded.Changes[0].TTLSeconds)
	}
}

func TestPresenceHeartbeat_EncodeDecode(t *testing.T) {
	serverID := generateTestPeerID(t)
	peer1 := generateTestPeerID(t)
	peer2 := generateTestPeerID(t)

	hb := &PresenceHeartbeat{
		ServerID:          serverID,
		Timestamp:         time.Now().Truncate(time.Millisecond),
		OnlineCount:       2,
		OnlinePeerIDs:     []peer.ID{peer1, peer2},
		HeartbeatSequence: 42,
	}

	data, err := hb.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodePresenceHeartbeat(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Type != "heartbeat" {
		t.Errorf("expected type 'heartbeat', got %q", decoded.Type)
	}
	if decoded.ServerID != serverID {
		t.Errorf("server ID mismatch")
	}
	if decoded.OnlineCount != 2 {
		t.Errorf("expected 2 online, got %d", decoded.OnlineCount)
	}
	if len(decoded.OnlinePeerIDs) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(decoded.OnlinePeerIDs))
	}
	if decoded.HeartbeatSequence != 42 {
		t.Errorf("expected seq 42, got %d", decoded.HeartbeatSequence)
	}
}

func TestDetectMessageType(t *testing.T) {
	event := &PresenceEvent{
		ServerID:  generateTestPeerID(t),
		Timestamp: time.Now(),
		Changes:   []PresenceChange{},
	}
	data, _ := event.Encode()

	if typ := DetectMessageType(data); typ != "event" {
		t.Errorf("expected 'event', got %q", typ)
	}

	hb := &PresenceHeartbeat{
		ServerID:  generateTestPeerID(t),
		Timestamp: time.Now(),
	}
	data, _ = hb.Encode()

	if typ := DetectMessageType(data); typ != "heartbeat" {
		t.Errorf("expected 'heartbeat', got %q", typ)
	}

	if typ := DetectMessageType([]byte(`{}`)); typ != "" {
		t.Errorf("expected empty type, got %q", typ)
	}
}

func TestPresenceEvent_OfflineState(t *testing.T) {
	pid := generateTestPeerID(t)
	event := &PresenceEvent{
		ServerID:  generateTestPeerID(t),
		Timestamp: time.Now(),
		Changes: []PresenceChange{
			{PeerID: pid, State: Offline, TTLSeconds: 0},
		},
	}

	data, err := event.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodePresenceEvent(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Changes[0].State != Offline {
		t.Errorf("expected Offline, got %v", decoded.Changes[0].State)
	}
}

func TestPresenceEvent_MultipleChanges(t *testing.T) {
	peer1 := generateTestPeerID(t)
	peer2 := generateTestPeerID(t)
	peer3 := generateTestPeerID(t)

	event := &PresenceEvent{
		ServerID:  generateTestPeerID(t),
		Timestamp: time.Now(),
		Changes: []PresenceChange{
			{PeerID: peer1, State: Online, TTLSeconds: 120},
			{PeerID: peer2, State: Offline},
			{PeerID: peer3, State: Online, TTLSeconds: 60},
		},
	}

	data, err := event.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodePresenceEvent(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded.Changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(decoded.Changes))
	}
	if decoded.Changes[0].State != Online {
		t.Errorf("change 0: expected Online")
	}
	if decoded.Changes[1].State != Offline {
		t.Errorf("change 1: expected Offline")
	}
	if decoded.Changes[2].State != Online {
		t.Errorf("change 2: expected Online")
	}
}
