package frame

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

const (
	// MaxFrameSize is the maximum allowed frame size (10MB).
	MaxFrameSize = 10 * 1024 * 1024
	// LengthPrefixSize is the size of the 4-byte length prefix.
	LengthPrefixSize = 4
)

// WriteFrame writes a length-prefixed frame to the writer.
// Format: 4-byte big-endian uint32 length + data bytes.
func WriteFrame(w io.Writer, data []byte) error {
	if len(data) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d > %d", len(data), MaxFrameSize)
	}
	lenBuf := make([]byte, LengthPrefixSize)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write frame length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write frame data: %w", err)
	}
	return nil
}

// ReadFrame reads a length-prefixed frame from the reader.
func ReadFrame(r io.Reader) ([]byte, error) {
	lenBuf := make([]byte, LengthPrefixSize)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, fmt.Errorf("read frame length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > MaxFrameSize {
		return nil, fmt.Errorf("frame too large: %d > %d", length, MaxFrameSize)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read frame data: %w", err)
	}
	return data, nil
}

// EncodeMessage serializes a Message to JSON bytes.
func EncodeMessage(msg *core.Message) ([]byte, error) {
	return msg.ToJSON()
}

// DecodeMessage deserializes a Message from JSON bytes.
func DecodeMessage(data []byte) (*core.Message, error) {
	return core.MessageFromJSON(data)
}

// EncodeStoreAck serializes a StoreAck to JSON bytes.
func EncodeStoreAck(ack *core.StoreAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeStoreAck deserializes a StoreAck from JSON bytes.
func DecodeStoreAck(data []byte) (*core.StoreAck, error) {
	var ack core.StoreAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("decode StoreAck: %w", err)
	}
	return &ack, nil
}

// EncodeRetrieveRequest serializes a RetrieveRequest to JSON bytes.
func EncodeRetrieveRequest(req *core.RetrieveRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodeRetrieveRequest deserializes a RetrieveRequest from JSON bytes.
func DecodeRetrieveRequest(data []byte) (*core.RetrieveRequest, error) {
	var req core.RetrieveRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("decode RetrieveRequest: %w", err)
	}
	return &req, nil
}

// EncodeRetrieveResponse serializes a RetrieveResponse as a compound frame:
// 4-byte metadata length + metadata JSON + (4-byte msg length + msg bytes)*N
func EncodeRetrieveResponse(resp *core.RetrieveResponse) ([]byte, error) {
	meta := map[string]any{
		"messageCount": len(resp.Messages),
		"hasMore":      resp.HasMore,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encode response metadata: %w", err)
	}

	// Calculate total size
	totalSize := LengthPrefixSize + len(metaBytes)
	var encodedMsgs [][]byte
	for _, msg := range resp.Messages {
		msgBytes, err := msg.ToJSON()
		if err != nil {
			return nil, fmt.Errorf("encode message: %w", err)
		}
		encodedMsgs = append(encodedMsgs, msgBytes)
		totalSize += LengthPrefixSize + len(msgBytes)
	}

	buf := make([]byte, 0, totalSize)

	// Write metadata length + metadata
	lenBuf := make([]byte, LengthPrefixSize)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(metaBytes)))
	buf = append(buf, lenBuf...)
	buf = append(buf, metaBytes...)

	// Write each message with length prefix
	for _, msgBytes := range encodedMsgs {
		binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBytes)))
		buf = append(buf, lenBuf...)
		buf = append(buf, msgBytes...)
	}

	return buf, nil
}

// DecodeRetrieveResponse parses a compound retrieve response frame.
func DecodeRetrieveResponse(data []byte) (*core.RetrieveResponse, error) {
	if len(data) < LengthPrefixSize {
		return nil, fmt.Errorf("retrieve response too short")
	}

	// Read metadata
	metaLen := binary.BigEndian.Uint32(data[:LengthPrefixSize])
	offset := LengthPrefixSize
	if uint32(len(data)) < uint32(offset)+metaLen {
		return nil, fmt.Errorf("retrieve response truncated at metadata")
	}
	var meta struct {
		MessageCount int  `json:"messageCount"`
		HasMore      bool `json:"hasMore"`
	}
	if err := json.Unmarshal(data[offset:offset+int(metaLen)], &meta); err != nil {
		return nil, fmt.Errorf("decode response metadata: %w", err)
	}
	offset += int(metaLen)

	// Read messages
	var messages []*core.Message
	for i := 0; i < meta.MessageCount; i++ {
		if len(data) < offset+LengthPrefixSize {
			return nil, fmt.Errorf("retrieve response truncated at message %d length", i)
		}
		msgLen := binary.BigEndian.Uint32(data[offset : offset+LengthPrefixSize])
		offset += LengthPrefixSize
		if uint32(len(data)) < uint32(offset)+msgLen {
			return nil, fmt.Errorf("retrieve response truncated at message %d data", i)
		}
		msg, err := core.MessageFromJSON(data[offset : offset+int(msgLen)])
		if err != nil {
			return nil, fmt.Errorf("decode message %d: %w", i, err)
		}
		messages = append(messages, msg)
		offset += int(msgLen)
	}

	return &core.RetrieveResponse{
		Messages: messages,
		HasMore:  meta.HasMore,
	}, nil
}

// EncodeCapacity serializes a ServerCapacity to JSON bytes.
func EncodeCapacity(cap *core.ServerCapacity) ([]byte, error) {
	return json.Marshal(cap)
}

// DecodeCapacity deserializes a ServerCapacity from JSON bytes.
func DecodeCapacity(data []byte) (*core.ServerCapacity, error) {
	var cap core.ServerCapacity
	if err := json.Unmarshal(data, &cap); err != nil {
		return nil, fmt.Errorf("decode ServerCapacity: %w", err)
	}
	return &cap, nil
}

// EncodeError serializes an error message to JSON bytes.
func EncodeError(message string) ([]byte, error) {
	return json.Marshal(map[string]string{"error": message})
}

// EncodeMarkDelivered serializes a MarkDeliveredRequest.
func EncodeMarkDelivered(req *core.MarkDeliveredRequest) ([]byte, error) {
	req.OperationType = "markDelivered"
	return json.Marshal(req)
}

// DecodeMarkDelivered deserializes a MarkDeliveredRequest.
func DecodeMarkDelivered(data []byte) (*core.MarkDeliveredRequest, error) {
	var req core.MarkDeliveredRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("decode MarkDeliveredRequest: %w", err)
	}
	return &req, nil
}

// EncodeMarkDeliveredAck serializes a MarkDeliveredAck.
func EncodeMarkDeliveredAck(ack *core.MarkDeliveredAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeMarkDeliveredAck deserializes a MarkDeliveredAck.
func DecodeMarkDeliveredAck(data []byte) (*core.MarkDeliveredAck, error) {
	var ack core.MarkDeliveredAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("decode MarkDeliveredAck: %w", err)
	}
	return &ack, nil
}

// EncodeUpdateFlags serializes an UpdateFlagsRequest.
func EncodeUpdateFlags(req *core.UpdateFlagsRequest) ([]byte, error) {
	req.OperationType = "updateFlags"
	return json.Marshal(req)
}

// DecodeUpdateFlags deserializes an UpdateFlagsRequest.
func DecodeUpdateFlags(data []byte) (*core.UpdateFlagsRequest, error) {
	var req core.UpdateFlagsRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("decode UpdateFlagsRequest: %w", err)
	}
	return &req, nil
}

// EncodeUpdateFlagsAck serializes an UpdateFlagsAck.
func EncodeUpdateFlagsAck(ack *core.UpdateFlagsAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeUpdateFlagsAck deserializes an UpdateFlagsAck.
func DecodeUpdateFlagsAck(data []byte) (*core.UpdateFlagsAck, error) {
	var ack core.UpdateFlagsAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("decode UpdateFlagsAck: %w", err)
	}
	return &ack, nil
}

// EncodeExpunge serializes an ExpungeRequest.
func EncodeExpunge(req *core.ExpungeRequest) ([]byte, error) {
	req.OperationType = "expunge"
	return json.Marshal(req)
}

// DecodeExpunge deserializes an ExpungeRequest.
func DecodeExpunge(data []byte) (*core.ExpungeRequest, error) {
	var req core.ExpungeRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("decode ExpungeRequest: %w", err)
	}
	return &req, nil
}

// EncodeExpungeAck serializes an ExpungeAck.
func EncodeExpungeAck(ack *core.ExpungeAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeExpungeAck deserializes an ExpungeAck.
func DecodeExpungeAck(data []byte) (*core.ExpungeAck, error) {
	var ack core.ExpungeAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("decode ExpungeAck: %w", err)
	}
	return &ack, nil
}

// EncodeDeleteMessages serializes a DeleteMessagesRequest.
func EncodeDeleteMessages(req *core.DeleteMessagesRequest) ([]byte, error) {
	req.OperationType = "deleteMessages"
	return json.Marshal(req)
}

// DecodeDeleteMessages deserializes a DeleteMessagesRequest.
func DecodeDeleteMessages(data []byte) (*core.DeleteMessagesRequest, error) {
	var req core.DeleteMessagesRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("decode DeleteMessagesRequest: %w", err)
	}
	return &req, nil
}

// EncodeDeleteMessagesAck serializes a DeleteMessagesAck.
func EncodeDeleteMessagesAck(ack *core.DeleteMessagesAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeDeleteMessagesAck deserializes a DeleteMessagesAck.
func DecodeDeleteMessagesAck(data []byte) (*core.DeleteMessagesAck, error) {
	var ack core.DeleteMessagesAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("decode DeleteMessagesAck: %w", err)
	}
	return &ack, nil
}

// GetOperationType extracts the operationType field from a JSON frame.
func GetOperationType(data []byte) (string, error) {
	var m struct {
		OperationType string `json:"operationType"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return "", fmt.Errorf("decode operationType: %w", err)
	}
	if m.OperationType == "" {
		return "retrieve", nil // default for backwards compatibility
	}
	return m.OperationType, nil
}
