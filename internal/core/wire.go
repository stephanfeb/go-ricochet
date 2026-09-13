package core

import (
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/pkg/wire"
)

// The wire model lives in pkg/wire so that clients outside this module can
// name the types. Every identifier is aliased here so server code keeps
// reading core.Message and friends.

const (
	MaxHopCount    = wire.MaxHopCount
	MaxPayloadSize = wire.MaxPayloadSize
)

type (
	Message               = wire.Message
	StoreAck              = wire.StoreAck
	RetrieveRequest       = wire.RetrieveRequest
	RetrieveResponse      = wire.RetrieveResponse
	ServerCapacity        = wire.ServerCapacity
	MarkDeliveredRequest  = wire.MarkDeliveredRequest
	MarkDeliveredAck      = wire.MarkDeliveredAck
	UpdateFlagsRequest    = wire.UpdateFlagsRequest
	UpdateFlagsAck        = wire.UpdateFlagsAck
	ExpungeRequest        = wire.ExpungeRequest
	ExpungeAck            = wire.ExpungeAck
	DeleteMessagesRequest = wire.DeleteMessagesRequest
	DeleteMessagesAck     = wire.DeleteMessagesAck
	DocumentResponse      = wire.DocumentResponse
	DocumentPutResponse   = wire.DocumentPutResponse
	DocumentMetadata      = wire.DocumentMetadata
	DocumentInfo          = wire.DocumentInfo

	SFMessageType   = wire.SFMessageType
	MessagePriority = wire.MessagePriority
	SFMessageFlags  = wire.SFMessageFlags
	MessageFlags    = wire.MessageFlags

	MailboxType = wire.MailboxType
	AccessMode  = wire.AccessMode
	Visibility  = wire.Visibility
	StoreReader = wire.StoreReader
	StoreAccess = wire.StoreAccess
)

const (
	MsgTypeStoreMessage     = wire.MsgTypeStoreMessage
	MsgTypeStoreAck         = wire.MsgTypeStoreAck
	MsgTypeRetrieveMessages = wire.MsgTypeRetrieveMessages
	MsgTypeRetrieveResp     = wire.MsgTypeRetrieveResp
	MsgTypeMarkDelivered    = wire.MsgTypeMarkDelivered
	MsgTypeMarkDeliveredAck = wire.MsgTypeMarkDeliveredAck
	MsgTypeForwardMessage   = wire.MsgTypeForwardMessage
	MsgTypeForwardAck       = wire.MsgTypeForwardAck
	MsgTypeQueryCapacity    = wire.MsgTypeQueryCapacity
	MsgTypeCapacityResp     = wire.MsgTypeCapacityResp
	MsgTypeSetPriority      = wire.MsgTypeSetPriority
	MsgTypeSetExpiry        = wire.MsgTypeSetExpiry
	MsgTypeUpdateFlags      = wire.MsgTypeUpdateFlags
	MsgTypeUpdateFlagsAck   = wire.MsgTypeUpdateFlagsAck
	MsgTypeExpunge          = wire.MsgTypeExpunge
	MsgTypeExpungeAck       = wire.MsgTypeExpungeAck
	MsgTypeDeleteMessages   = wire.MsgTypeDeleteMessages
	MsgTypeDeleteMsgsAck    = wire.MsgTypeDeleteMsgsAck

	PriorityLow    = wire.PriorityLow
	PriorityNormal = wire.PriorityNormal
	PriorityHigh   = wire.PriorityHigh
	PriorityUrgent = wire.PriorityUrgent

	FlagEncrypted  = wire.FlagEncrypted
	FlagSigned     = wire.FlagSigned
	FlagCompressed = wire.FlagCompressed
	FlagRequireAck = wire.FlagRequireAck
	FlagForwarded  = wire.FlagForwarded
	FlagNone       = wire.FlagNone

	MsgFlagSeen    = wire.MsgFlagSeen
	MsgFlagFlagged = wire.MsgFlagFlagged
	MsgFlagDeleted = wire.MsgFlagDeleted
	MsgFlagDraft   = wire.MsgFlagDraft
	MsgFlagNone    = wire.MsgFlagNone

	MailboxPrivate = wire.MailboxPrivate
	MailboxShared  = wire.MailboxShared
	MailboxPublic  = wire.MailboxPublic

	AccessReadOnly  = wire.AccessReadOnly
	AccessWriteOnly = wire.AccessWriteOnly
	AccessReadWrite = wire.AccessReadWrite

	VisibilityPrivate = wire.VisibilityPrivate
	VisibilityShared  = wire.VisibilityShared
	VisibilityPublic  = wire.VisibilityPublic
)

// NewMessage creates a message with a fresh ID and no expiry. See wire.NewMessage.
func NewMessage(senderID, recipientID peer.ID, payload []byte) *Message {
	return wire.NewMessage(senderID, recipientID, payload)
}

// NewMessageWithDefaultExpiry creates a message with the default expiry. See
// wire.NewMessageWithDefaultExpiry.
func NewMessageWithDefaultExpiry(senderID, recipientID peer.ID, payload []byte) *Message {
	return wire.NewMessageWithDefaultExpiry(senderID, recipientID, payload)
}

// MessageFromJSON decodes a message. See wire.MessageFromJSON.
func MessageFromJSON(data []byte) (*Message, error) { return wire.MessageFromJSON(data) }

// SFMessageTypeFromValue converts a wire value. See wire.SFMessageTypeFromValue.
func SFMessageTypeFromValue(v uint8) (SFMessageType, error) { return wire.SFMessageTypeFromValue(v) }

// MessagePriorityFromValue converts a wire value. See wire.MessagePriorityFromValue.
func MessagePriorityFromValue(v uint8) (MessagePriority, error) {
	return wire.MessagePriorityFromValue(v)
}

// MailboxTypeFromString parses a mailbox type name. See wire.MailboxTypeFromString.
func MailboxTypeFromString(s string) (MailboxType, error) { return wire.MailboxTypeFromString(s) }

// AccessModeFromString parses an access mode name. See wire.AccessModeFromString.
func AccessModeFromString(s string) (AccessMode, error) { return wire.AccessModeFromString(s) }

// VisibilityFromString parses a visibility name. See wire.VisibilityFromString.
func VisibilityFromString(s string) (Visibility, error) { return wire.VisibilityFromString(s) }
