package frame

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

func generateFrameTestPeerID(t *testing.T) peer.ID {
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

func TestWriteFrame_ReadFrame(t *testing.T) {
	data := []byte("hello, this is frame data")

	var buf bytes.Buffer
	if err := WriteFrame(&buf, data); err != nil {
		t.Fatalf("WriteFrame error: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame error: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("ReadFrame returned %v, want %v", got, data)
	}
}

func TestWriteFrame_MaxSize(t *testing.T) {
	// Exactly at MaxFrameSize should succeed
	exactData := make([]byte, MaxFrameSize)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, exactData); err != nil {
		t.Fatalf("WriteFrame with exact max size should succeed, got error: %v", err)
	}

	// One byte over should fail
	overData := make([]byte, MaxFrameSize+1)
	buf.Reset()
	if err := WriteFrame(&buf, overData); err == nil {
		t.Fatal("WriteFrame with size > MaxFrameSize should fail")
	}
}

func TestReadFrame_TruncatedLength(t *testing.T) {
	// Only 2 bytes when 4 are needed for the length prefix
	r := bytes.NewReader([]byte{0x00, 0x05})
	_, err := ReadFrame(r)
	if err == nil {
		t.Fatal("ReadFrame with truncated length should return error")
	}
}

func TestReadFrame_TruncatedData(t *testing.T) {
	// Length prefix says 100 bytes but only 50 available
	var buf bytes.Buffer
	lenBuf := make([]byte, 4)
	lenBuf[0] = 0
	lenBuf[1] = 0
	lenBuf[2] = 0
	lenBuf[3] = 100
	buf.Write(lenBuf)
	buf.Write(make([]byte, 50))

	_, err := ReadFrame(&buf)
	if err == nil {
		t.Fatal("ReadFrame with truncated data should return error")
	}
}

func TestEncodeMessage_DecodeMessage(t *testing.T) {
	sender := generateFrameTestPeerID(t)
	recipient := generateFrameTestPeerID(t)
	payload := []byte("test payload with binary \x00\x01\x02")

	original := core.NewMessage(sender, recipient, payload)
	original.Priority = core.PriorityHigh
	original.FolderPath = "inbox"

	encoded, err := EncodeMessage(original)
	if err != nil {
		t.Fatalf("EncodeMessage error: %v", err)
	}

	decoded, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeMessage error: %v", err)
	}

	if decoded.MessageID != original.MessageID {
		t.Errorf("MessageID: got %q, want %q", decoded.MessageID, original.MessageID)
	}
	if decoded.SenderPeerID != original.SenderPeerID {
		t.Errorf("SenderPeerID: got %q, want %q", decoded.SenderPeerID, original.SenderPeerID)
	}
	if decoded.RecipientPeerID != original.RecipientPeerID {
		t.Errorf("RecipientPeerID: got %q, want %q", decoded.RecipientPeerID, original.RecipientPeerID)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("Payload mismatch")
	}
	if decoded.Priority != original.Priority {
		t.Errorf("Priority: got %v, want %v", decoded.Priority, original.Priority)
	}
	if decoded.FolderPath != original.FolderPath {
		t.Errorf("FolderPath: got %q, want %q", decoded.FolderPath, original.FolderPath)
	}
}

func TestEncodeStoreAck_DecodeStoreAck(t *testing.T) {
	// Success case
	ack := &core.StoreAck{
		MessageID:             "test-msg-id",
		Success:               true,
		EstimatedDeliveryTime: 1234567890,
	}

	encoded, err := EncodeStoreAck(ack)
	if err != nil {
		t.Fatalf("EncodeStoreAck error: %v", err)
	}

	decoded, err := DecodeStoreAck(encoded)
	if err != nil {
		t.Fatalf("DecodeStoreAck error: %v", err)
	}

	if decoded.MessageID != ack.MessageID {
		t.Errorf("MessageID: got %q, want %q", decoded.MessageID, ack.MessageID)
	}
	if decoded.Success != ack.Success {
		t.Errorf("Success: got %v, want %v", decoded.Success, ack.Success)
	}
	if decoded.EstimatedDeliveryTime != ack.EstimatedDeliveryTime {
		t.Errorf("EstimatedDeliveryTime: got %d, want %d", decoded.EstimatedDeliveryTime, ack.EstimatedDeliveryTime)
	}

	// Error case
	errAck := &core.StoreAck{
		MessageID:    "test-msg-id-2",
		Success:      false,
		ErrorMessage: "mailbox full",
	}

	encoded2, err := EncodeStoreAck(errAck)
	if err != nil {
		t.Fatalf("EncodeStoreAck (error case) error: %v", err)
	}

	decoded2, err := DecodeStoreAck(encoded2)
	if err != nil {
		t.Fatalf("DecodeStoreAck (error case) error: %v", err)
	}

	if decoded2.Success {
		t.Error("error ack should have Success=false")
	}
	if decoded2.ErrorMessage != "mailbox full" {
		t.Errorf("ErrorMessage: got %q, want %q", decoded2.ErrorMessage, "mailbox full")
	}
}

func TestEncodeRetrieveRequest_DecodeRetrieveRequest(t *testing.T) {
	fromSeq := uint64(10)
	maxMsgs := 50
	minPri := core.PriorityHigh

	req := &core.RetrieveRequest{
		PeerID:       "test-peer-id",
		FolderPath:   "inbox/subfolder",
		FromSequence: &fromSeq,
		MaxMessages:  &maxMsgs,
		MinPriority:  &minPri,
	}

	encoded, err := EncodeRetrieveRequest(req)
	if err != nil {
		t.Fatalf("EncodeRetrieveRequest error: %v", err)
	}

	decoded, err := DecodeRetrieveRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeRetrieveRequest error: %v", err)
	}

	if decoded.PeerID != req.PeerID {
		t.Errorf("PeerID: got %q, want %q", decoded.PeerID, req.PeerID)
	}
	if decoded.FolderPath != req.FolderPath {
		t.Errorf("FolderPath: got %q, want %q", decoded.FolderPath, req.FolderPath)
	}
	if decoded.FromSequence == nil || *decoded.FromSequence != fromSeq {
		t.Errorf("FromSequence: got %v, want %d", decoded.FromSequence, fromSeq)
	}
	if decoded.MaxMessages == nil || *decoded.MaxMessages != maxMsgs {
		t.Errorf("MaxMessages: got %v, want %d", decoded.MaxMessages, maxMsgs)
	}
	if decoded.MinPriority == nil || *decoded.MinPriority != minPri {
		t.Errorf("MinPriority: got %v, want %v", decoded.MinPriority, minPri)
	}
}

func TestEncodeRetrieveResponse_DecodeRetrieveResponse(t *testing.T) {
	sender := generateFrameTestPeerID(t)
	recipient := generateFrameTestPeerID(t)

	// Test with 0 messages
	t.Run("zero_messages", func(t *testing.T) {
		resp := &core.RetrieveResponse{
			Messages: []*core.Message{},
			HasMore:  false,
		}

		encoded, err := EncodeRetrieveResponse(resp)
		if err != nil {
			t.Fatalf("EncodeRetrieveResponse error: %v", err)
		}

		decoded, err := DecodeRetrieveResponse(encoded)
		if err != nil {
			t.Fatalf("DecodeRetrieveResponse error: %v", err)
		}

		if len(decoded.Messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(decoded.Messages))
		}
		if decoded.HasMore {
			t.Error("HasMore should be false")
		}
	})

	// Test with 1 message
	t.Run("one_message", func(t *testing.T) {
		msg := core.NewMessage(sender, recipient, []byte("single message"))
		resp := &core.RetrieveResponse{
			Messages: []*core.Message{msg},
			HasMore:  false,
		}

		encoded, err := EncodeRetrieveResponse(resp)
		if err != nil {
			t.Fatalf("EncodeRetrieveResponse error: %v", err)
		}

		decoded, err := DecodeRetrieveResponse(encoded)
		if err != nil {
			t.Fatalf("DecodeRetrieveResponse error: %v", err)
		}

		if len(decoded.Messages) != 1 {
			t.Fatalf("expected 1 message, got %d", len(decoded.Messages))
		}
		if decoded.Messages[0].MessageID != msg.MessageID {
			t.Errorf("MessageID mismatch")
		}
	})

	// Test with 3 messages and HasMore=true
	t.Run("three_messages_has_more", func(t *testing.T) {
		msgs := []*core.Message{
			core.NewMessage(sender, recipient, []byte("payload one")),
			core.NewMessage(sender, recipient, []byte("payload two")),
			core.NewMessage(sender, recipient, []byte("payload three with more data")),
		}
		resp := &core.RetrieveResponse{
			Messages: msgs,
			HasMore:  true,
		}

		encoded, err := EncodeRetrieveResponse(resp)
		if err != nil {
			t.Fatalf("EncodeRetrieveResponse error: %v", err)
		}

		decoded, err := DecodeRetrieveResponse(encoded)
		if err != nil {
			t.Fatalf("DecodeRetrieveResponse error: %v", err)
		}

		if len(decoded.Messages) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(decoded.Messages))
		}
		if !decoded.HasMore {
			t.Error("HasMore should be true")
		}

		for i, msg := range decoded.Messages {
			if msg.MessageID != msgs[i].MessageID {
				t.Errorf("message[%d] MessageID: got %q, want %q", i, msg.MessageID, msgs[i].MessageID)
			}
			if !bytes.Equal(msg.Payload, msgs[i].Payload) {
				t.Errorf("message[%d] Payload mismatch", i)
			}
		}
	})
}

func TestEncodeCapacity_DecodeCapacity(t *testing.T) {
	cap := &core.ServerCapacity{
		TotalStorageBytes:     1000000000,
		UsedStorageBytes:      500000000,
		AvailableStorageBytes: 500000000,
		MessageCount:          1234,
		ActiveMailboxes:       56,
		HealthScore:           0.95,
	}

	encoded, err := EncodeCapacity(cap)
	if err != nil {
		t.Fatalf("EncodeCapacity error: %v", err)
	}

	decoded, err := DecodeCapacity(encoded)
	if err != nil {
		t.Fatalf("DecodeCapacity error: %v", err)
	}

	if decoded.TotalStorageBytes != cap.TotalStorageBytes {
		t.Errorf("TotalStorageBytes: got %d, want %d", decoded.TotalStorageBytes, cap.TotalStorageBytes)
	}
	if decoded.UsedStorageBytes != cap.UsedStorageBytes {
		t.Errorf("UsedStorageBytes: got %d, want %d", decoded.UsedStorageBytes, cap.UsedStorageBytes)
	}
	if decoded.AvailableStorageBytes != cap.AvailableStorageBytes {
		t.Errorf("AvailableStorageBytes: got %d, want %d", decoded.AvailableStorageBytes, cap.AvailableStorageBytes)
	}
	if decoded.MessageCount != cap.MessageCount {
		t.Errorf("MessageCount: got %d, want %d", decoded.MessageCount, cap.MessageCount)
	}
	if decoded.ActiveMailboxes != cap.ActiveMailboxes {
		t.Errorf("ActiveMailboxes: got %d, want %d", decoded.ActiveMailboxes, cap.ActiveMailboxes)
	}
	if decoded.HealthScore != cap.HealthScore {
		t.Errorf("HealthScore: got %f, want %f", decoded.HealthScore, cap.HealthScore)
	}
}

func TestEncodeMarkDelivered_DecodeMarkDelivered(t *testing.T) {
	req := &core.MarkDeliveredRequest{
		MessageIDs: []string{"msg-1", "msg-2", "msg-3"},
		FolderPath: "inbox",
	}

	encoded, err := EncodeMarkDelivered(req)
	if err != nil {
		t.Fatalf("EncodeMarkDelivered error: %v", err)
	}

	decoded, err := DecodeMarkDelivered(encoded)
	if err != nil {
		t.Fatalf("DecodeMarkDelivered error: %v", err)
	}

	if decoded.OperationType != "markDelivered" {
		t.Errorf("OperationType: got %q, want %q", decoded.OperationType, "markDelivered")
	}
	if len(decoded.MessageIDs) != 3 {
		t.Fatalf("MessageIDs length: got %d, want 3", len(decoded.MessageIDs))
	}
	for i, id := range decoded.MessageIDs {
		if id != req.MessageIDs[i] {
			t.Errorf("MessageIDs[%d]: got %q, want %q", i, id, req.MessageIDs[i])
		}
	}
	if decoded.FolderPath != "inbox" {
		t.Errorf("FolderPath: got %q, want %q", decoded.FolderPath, "inbox")
	}
}

func TestEncodeUpdateFlags_DecodeUpdateFlags(t *testing.T) {
	req := &core.UpdateFlagsRequest{
		MessageID:   "msg-123",
		AddFlags:    uint32(core.MsgFlagSeen | core.MsgFlagFlagged),
		RemoveFlags: uint32(core.MsgFlagDraft),
	}

	encoded, err := EncodeUpdateFlags(req)
	if err != nil {
		t.Fatalf("EncodeUpdateFlags error: %v", err)
	}

	decoded, err := DecodeUpdateFlags(encoded)
	if err != nil {
		t.Fatalf("DecodeUpdateFlags error: %v", err)
	}

	if decoded.OperationType != "updateFlags" {
		t.Errorf("OperationType: got %q, want %q", decoded.OperationType, "updateFlags")
	}
	if decoded.MessageID != req.MessageID {
		t.Errorf("MessageID: got %q, want %q", decoded.MessageID, req.MessageID)
	}
	if decoded.AddFlags != req.AddFlags {
		t.Errorf("AddFlags: got %d, want %d", decoded.AddFlags, req.AddFlags)
	}
	if decoded.RemoveFlags != req.RemoveFlags {
		t.Errorf("RemoveFlags: got %d, want %d", decoded.RemoveFlags, req.RemoveFlags)
	}
}

func TestEncodeExpunge_DecodeExpunge(t *testing.T) {
	req := &core.ExpungeRequest{
		PeerID:     "test-peer",
		FolderPath: "trash",
	}

	encoded, err := EncodeExpunge(req)
	if err != nil {
		t.Fatalf("EncodeExpunge error: %v", err)
	}

	decoded, err := DecodeExpunge(encoded)
	if err != nil {
		t.Fatalf("DecodeExpunge error: %v", err)
	}

	if decoded.OperationType != "expunge" {
		t.Errorf("OperationType: got %q, want %q", decoded.OperationType, "expunge")
	}
	if decoded.PeerID != req.PeerID {
		t.Errorf("PeerID: got %q, want %q", decoded.PeerID, req.PeerID)
	}
	if decoded.FolderPath != req.FolderPath {
		t.Errorf("FolderPath: got %q, want %q", decoded.FolderPath, req.FolderPath)
	}
}

func TestEncodeDeleteMessages_DecodeDeleteMessages(t *testing.T) {
	req := &core.DeleteMessagesRequest{
		MessageIDs: []string{"del-1", "del-2"},
	}

	encoded, err := EncodeDeleteMessages(req)
	if err != nil {
		t.Fatalf("EncodeDeleteMessages error: %v", err)
	}

	decoded, err := DecodeDeleteMessages(encoded)
	if err != nil {
		t.Fatalf("DecodeDeleteMessages error: %v", err)
	}

	if decoded.OperationType != "deleteMessages" {
		t.Errorf("OperationType: got %q, want %q", decoded.OperationType, "deleteMessages")
	}
	if len(decoded.MessageIDs) != 2 {
		t.Fatalf("MessageIDs length: got %d, want 2", len(decoded.MessageIDs))
	}
	for i, id := range decoded.MessageIDs {
		if id != req.MessageIDs[i] {
			t.Errorf("MessageIDs[%d]: got %q, want %q", i, id, req.MessageIDs[i])
		}
	}
}

func TestGetOperationType(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{"markDelivered", `{"operationType":"markDelivered"}`, "markDelivered"},
		{"updateFlags", `{"operationType":"updateFlags"}`, "updateFlags"},
		{"expunge", `{"operationType":"expunge"}`, "expunge"},
		{"deleteMessages", `{"operationType":"deleteMessages"}`, "deleteMessages"},
		{"empty_defaults_to_retrieve", `{}`, "retrieve"},
		{"missing_defaults_to_retrieve", `{"peerId":"test"}`, "retrieve"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetOperationType([]byte(tt.json))
			if err != nil {
				t.Fatalf("GetOperationType error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("GetOperationType = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestEncodeError(t *testing.T) {
	errMsg := "something went wrong"
	encoded, err := EncodeError(errMsg)
	if err != nil {
		t.Fatalf("EncodeError error: %v", err)
	}

	var result map[string]string
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("unmarshal EncodeError result: %v", err)
	}

	if val, ok := result["error"]; !ok {
		t.Error("EncodeError output missing 'error' key")
	} else if val != errMsg {
		t.Errorf("error value: got %q, want %q", val, errMsg)
	}

	// Verify it's valid JSON containing the error key
	if !strings.Contains(string(encoded), `"error"`) {
		t.Error("encoded output should contain '\"error\"' key")
	}
}
