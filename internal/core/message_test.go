package core

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func generateTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}
	return pid
}

func TestNewMessage(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)
	payload := []byte("hello world")

	msg := NewMessage(sender, recipient, payload)

	if msg.MessageID == "" {
		t.Fatal("MessageID should not be empty")
	}
	if msg.SenderPeerID != sender.String() {
		t.Errorf("SenderPeerID = %q, want %q", msg.SenderPeerID, sender.String())
	}
	if msg.RecipientPeerID != recipient.String() {
		t.Errorf("RecipientPeerID = %q, want %q", msg.RecipientPeerID, recipient.String())
	}
	if msg.Priority != PriorityNormal {
		t.Errorf("Priority = %v, want PriorityNormal", msg.Priority)
	}
	if msg.HopCount != 0 {
		t.Errorf("HopCount = %d, want 0", msg.HopCount)
	}
	if msg.Flags != FlagNone {
		t.Errorf("Flags = %v, want FlagNone", msg.Flags)
	}
	if msg.CreatedTimestamp <= 0 {
		t.Errorf("CreatedTimestamp = %d, want > 0", msg.CreatedTimestamp)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Errorf("Payload = %v, want %v", msg.Payload, payload)
	}
}

func TestNewMessageWithDefaultExpiry(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)

	before := time.Now().Add(7 * 24 * time.Hour).UnixMilli()
	msg := NewMessageWithDefaultExpiry(sender, recipient, []byte("test"))
	after := time.Now().Add(7 * 24 * time.Hour).UnixMilli()

	if msg.ExpiryTimestamp < before || msg.ExpiryTimestamp > after {
		t.Errorf("ExpiryTimestamp = %d, want between %d and %d", msg.ExpiryTimestamp, before, after)
	}
}

func TestIsExpired(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)

	// Past expiry should be expired
	msg := NewMessage(sender, recipient, []byte("test"))
	msg.ExpiryTimestamp = time.Now().Add(-1 * time.Hour).UnixMilli()
	if !msg.IsExpired() {
		t.Error("message with past expiry should be expired")
	}

	// Future expiry should not be expired
	msg.ExpiryTimestamp = time.Now().Add(1 * time.Hour).UnixMilli()
	if msg.IsExpired() {
		t.Error("message with future expiry should not be expired")
	}

	// Zero expiry should not be expired
	msg.ExpiryTimestamp = 0
	if msg.IsExpired() {
		t.Error("message with zero expiry should not be expired")
	}
}

func TestWithIncrementedHopCount(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)

	original := NewMessage(sender, recipient, []byte("test"))
	original.HopCount = 3

	incremented, err := original.WithIncrementedHopCount()
	if err != nil {
		t.Fatalf("WithIncrementedHopCount returned error: %v", err)
	}

	// Check incremented copy
	if incremented.HopCount != 4 {
		t.Errorf("incremented HopCount = %d, want 4", incremented.HopCount)
	}
	if !incremented.Flags.IsForwarded() {
		t.Error("incremented message should have FlagForwarded set")
	}

	// Check original is not mutated
	if original.HopCount != 3 {
		t.Errorf("original HopCount = %d, want 3 (should not be mutated)", original.HopCount)
	}
	if original.Flags.IsForwarded() {
		t.Error("original should not have FlagForwarded set")
	}

	// Test at MaxHopCount
	maxMsg := NewMessage(sender, recipient, []byte("test"))
	maxMsg.HopCount = MaxHopCount
	_, err = maxMsg.WithIncrementedHopCount()
	if err == nil {
		t.Fatal("WithIncrementedHopCount at MaxHopCount should return error")
	}
}

func TestSenderID_RecipientID(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)

	msg := NewMessage(sender, recipient, []byte("test"))

	gotSender, err := msg.SenderID()
	if err != nil {
		t.Fatalf("SenderID() error: %v", err)
	}
	if gotSender != sender {
		t.Errorf("SenderID() = %v, want %v", gotSender, sender)
	}

	gotRecipient, err := msg.RecipientID()
	if err != nil {
		t.Fatalf("RecipientID() error: %v", err)
	}
	if gotRecipient != recipient {
		t.Errorf("RecipientID() = %v, want %v", gotRecipient, recipient)
	}
}

func TestToJSON_MessageFromJSON(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)
	payload := []byte("hello, this is a test payload with special chars: \x00\xff")

	original := NewMessageWithDefaultExpiry(sender, recipient, payload)
	original.HopCount = 2
	original.Flags = FlagEncrypted.WithFlag(FlagSigned)
	original.Priority = PriorityHigh
	original.FolderPath = "inbox/subfolder"
	original.SequenceNumber = 42
	original.MsgFlags = MsgFlagSeen.WithFlag(MsgFlagFlagged)
	original.Persistent = true

	jsonData, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}

	restored, err := MessageFromJSON(jsonData)
	if err != nil {
		t.Fatalf("MessageFromJSON() error: %v", err)
	}

	if restored.MessageID != original.MessageID {
		t.Errorf("MessageID: got %q, want %q", restored.MessageID, original.MessageID)
	}
	if restored.SenderPeerID != original.SenderPeerID {
		t.Errorf("SenderPeerID: got %q, want %q", restored.SenderPeerID, original.SenderPeerID)
	}
	if restored.RecipientPeerID != original.RecipientPeerID {
		t.Errorf("RecipientPeerID: got %q, want %q", restored.RecipientPeerID, original.RecipientPeerID)
	}
	if !bytes.Equal(restored.Payload, original.Payload) {
		t.Errorf("Payload: got %v, want %v", restored.Payload, original.Payload)
	}
	if restored.Priority != original.Priority {
		t.Errorf("Priority: got %v, want %v", restored.Priority, original.Priority)
	}
	if restored.ExpiryTimestamp != original.ExpiryTimestamp {
		t.Errorf("ExpiryTimestamp: got %d, want %d", restored.ExpiryTimestamp, original.ExpiryTimestamp)
	}
	if restored.HopCount != original.HopCount {
		t.Errorf("HopCount: got %d, want %d", restored.HopCount, original.HopCount)
	}
	if restored.Flags != original.Flags {
		t.Errorf("Flags: got %v, want %v", restored.Flags, original.Flags)
	}
	if restored.CreatedTimestamp != original.CreatedTimestamp {
		t.Errorf("CreatedTimestamp: got %d, want %d", restored.CreatedTimestamp, original.CreatedTimestamp)
	}
	if restored.FolderPath != original.FolderPath {
		t.Errorf("FolderPath: got %q, want %q", restored.FolderPath, original.FolderPath)
	}
	if restored.SequenceNumber != original.SequenceNumber {
		t.Errorf("SequenceNumber: got %d, want %d", restored.SequenceNumber, original.SequenceNumber)
	}
	if restored.MsgFlags != original.MsgFlags {
		t.Errorf("MsgFlags: got %v, want %v", restored.MsgFlags, original.MsgFlags)
	}
	if restored.Persistent != original.Persistent {
		t.Errorf("Persistent: got %v, want %v", restored.Persistent, original.Persistent)
	}
}

func TestToMap(t *testing.T) {
	sender := generateTestPeerID(t)
	recipient := generateTestPeerID(t)

	msg := NewMessage(sender, recipient, []byte("test"))
	m := msg.ToMap()

	expectedKeys := []string{
		"messageId", "recipientPeerId", "senderPeerId", "priority",
		"expiryTimestamp", "hopCount", "flags", "createdTimestamp",
		"folderPath", "sequenceNumber", "messageFlags", "persistent",
	}

	for _, key := range expectedKeys {
		if _, ok := m[key]; !ok {
			t.Errorf("ToMap() missing key %q", key)
		}
	}

	if m["messageId"] != msg.MessageID {
		t.Errorf("ToMap()[messageId] = %v, want %v", m["messageId"], msg.MessageID)
	}
	if m["senderPeerId"] != msg.SenderPeerID {
		t.Errorf("ToMap()[senderPeerId] = %v, want %v", m["senderPeerId"], msg.SenderPeerID)
	}
	if m["recipientPeerId"] != msg.RecipientPeerID {
		t.Errorf("ToMap()[recipientPeerId] = %v, want %v", m["recipientPeerId"], msg.RecipientPeerID)
	}
}
