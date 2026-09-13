package wire

import (
	"fmt"
	"strings"
)

// SFMessageType represents store-and-forward protocol message types.
type SFMessageType uint8

const (
	MsgTypeStoreMessage     SFMessageType = 0x30
	MsgTypeStoreAck         SFMessageType = 0x31
	MsgTypeRetrieveMessages SFMessageType = 0x32
	MsgTypeRetrieveResp     SFMessageType = 0x33
	MsgTypeMarkDelivered    SFMessageType = 0x34
	MsgTypeMarkDeliveredAck SFMessageType = 0x35
	MsgTypeForwardMessage   SFMessageType = 0x36
	MsgTypeForwardAck       SFMessageType = 0x37
	MsgTypeQueryCapacity    SFMessageType = 0x38
	MsgTypeCapacityResp     SFMessageType = 0x39
	MsgTypeSetPriority      SFMessageType = 0x3A
	MsgTypeSetExpiry        SFMessageType = 0x3B
	MsgTypeUpdateFlags      SFMessageType = 0x3C
	MsgTypeUpdateFlagsAck   SFMessageType = 0x3D
	MsgTypeExpunge          SFMessageType = 0x3E
	MsgTypeExpungeAck       SFMessageType = 0x3F
	MsgTypeDeleteMessages   SFMessageType = 0x40
	MsgTypeDeleteMsgsAck    SFMessageType = 0x41
)

var messageTypeNames = map[SFMessageType]string{
	MsgTypeStoreMessage:     "storeMessage",
	MsgTypeStoreAck:         "storeAck",
	MsgTypeRetrieveMessages: "retrieveMessages",
	MsgTypeRetrieveResp:     "retrieveResp",
	MsgTypeMarkDelivered:    "markDelivered",
	MsgTypeMarkDeliveredAck: "markDeliveredAck",
	MsgTypeForwardMessage:   "forwardMessage",
	MsgTypeForwardAck:       "forwardAck",
	MsgTypeQueryCapacity:    "queryCapacity",
	MsgTypeCapacityResp:     "capacityResp",
	MsgTypeSetPriority:      "setPriority",
	MsgTypeSetExpiry:        "setExpiry",
	MsgTypeUpdateFlags:      "updateFlags",
	MsgTypeUpdateFlagsAck:   "updateFlagsAck",
	MsgTypeExpunge:          "expunge",
	MsgTypeExpungeAck:       "expungeAck",
	MsgTypeDeleteMessages:   "deleteMessages",
	MsgTypeDeleteMsgsAck:    "deleteMessagesAck",
}

func (t SFMessageType) String() string {
	if name, ok := messageTypeNames[t]; ok {
		return fmt.Sprintf("SFMessageType.%s(0x%02X)", name, uint8(t))
	}
	return fmt.Sprintf("SFMessageType.unknown(0x%02X)", uint8(t))
}

func SFMessageTypeFromValue(v uint8) (SFMessageType, error) {
	t := SFMessageType(v)
	if _, ok := messageTypeNames[t]; !ok {
		return 0, fmt.Errorf("unknown SFMessageType value: 0x%02X", v)
	}
	return t, nil
}

// MessagePriority represents message priority levels.
type MessagePriority uint8

const (
	PriorityLow    MessagePriority = 0
	PriorityNormal MessagePriority = 1
	PriorityHigh   MessagePriority = 2
	PriorityUrgent MessagePriority = 3
)

var priorityNames = map[MessagePriority]string{
	PriorityLow:    "low",
	PriorityNormal: "normal",
	PriorityHigh:   "high",
	PriorityUrgent: "urgent",
}

func (p MessagePriority) String() string {
	if name, ok := priorityNames[p]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", uint8(p))
}

func MessagePriorityFromValue(v uint8) (MessagePriority, error) {
	if v > 3 {
		return 0, fmt.Errorf("invalid MessagePriority value: %d (must be 0-3)", v)
	}
	return MessagePriority(v), nil
}

// SFMessageFlags represents protocol-level flags as a bitmap.
type SFMessageFlags uint32

const (
	FlagEncrypted  SFMessageFlags = 1 << 0
	FlagSigned     SFMessageFlags = 1 << 1
	FlagCompressed SFMessageFlags = 1 << 2
	FlagRequireAck SFMessageFlags = 1 << 3
	FlagForwarded  SFMessageFlags = 1 << 4
	FlagNone       SFMessageFlags = 0
)

func (f SFMessageFlags) HasFlag(flag SFMessageFlags) bool {
	return f&flag != 0
}

func (f SFMessageFlags) WithFlag(flag SFMessageFlags) SFMessageFlags {
	return f | flag
}

func (f SFMessageFlags) WithoutFlag(flag SFMessageFlags) SFMessageFlags {
	return f &^ flag
}

func (f SFMessageFlags) IsEncrypted() bool  { return f.HasFlag(FlagEncrypted) }
func (f SFMessageFlags) IsSigned() bool     { return f.HasFlag(FlagSigned) }
func (f SFMessageFlags) IsCompressed() bool { return f.HasFlag(FlagCompressed) }
func (f SFMessageFlags) RequiresAck() bool  { return f.HasFlag(FlagRequireAck) }
func (f SFMessageFlags) IsForwarded() bool  { return f.HasFlag(FlagForwarded) }

func (f SFMessageFlags) String() string {
	if f == FlagNone {
		return "none"
	}
	var parts []string
	if f.IsEncrypted() {
		parts = append(parts, "encrypted")
	}
	if f.IsSigned() {
		parts = append(parts, "signed")
	}
	if f.IsCompressed() {
		parts = append(parts, "compressed")
	}
	if f.RequiresAck() {
		parts = append(parts, "requireAck")
	}
	if f.IsForwarded() {
		parts = append(parts, "forwarded")
	}
	return strings.Join(parts, "|")
}

// MessageFlags represents IMAP-style message flags as a bitmap.
type MessageFlags uint32

const (
	MsgFlagSeen    MessageFlags = 1 << 0
	MsgFlagFlagged MessageFlags = 1 << 1
	MsgFlagDeleted MessageFlags = 1 << 2
	MsgFlagDraft   MessageFlags = 1 << 3
	MsgFlagNone    MessageFlags = 0
)

func (f MessageFlags) HasFlag(flag MessageFlags) bool {
	return f&flag != 0
}

func (f MessageFlags) WithFlag(flag MessageFlags) MessageFlags {
	return f | flag
}

func (f MessageFlags) WithoutFlag(flag MessageFlags) MessageFlags {
	return f &^ flag
}

func (f MessageFlags) IsSeen() bool    { return f.HasFlag(MsgFlagSeen) }
func (f MessageFlags) IsFlagged() bool { return f.HasFlag(MsgFlagFlagged) }
func (f MessageFlags) IsDeleted() bool { return f.HasFlag(MsgFlagDeleted) }
func (f MessageFlags) IsDraft() bool   { return f.HasFlag(MsgFlagDraft) }

func (f MessageFlags) String() string {
	if f == MsgFlagNone {
		return "none"
	}
	var parts []string
	if f.IsSeen() {
		parts = append(parts, "seen")
	}
	if f.IsFlagged() {
		parts = append(parts, "flagged")
	}
	if f.IsDeleted() {
		parts = append(parts, "deleted")
	}
	if f.IsDraft() {
		parts = append(parts, "draft")
	}
	return strings.Join(parts, ",")
}
