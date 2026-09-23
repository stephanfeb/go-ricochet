package notify

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// ProtocolID is the protocol identifier for mailbox notifications.
const ProtocolID = protocol.ID("/ricochet/mailbox-notify/1.0.0")

// MailboxTopicPrefix is the GossipSub topic prefix for mailbox notifications.
const MailboxTopicPrefix = "/ricochet/mailbox/"

// Notification is the push notification for new messages. It lives in
// pkg/wire so clients outside this module can name it.
type Notification = wire.Notification

// MailboxTopic returns the GossipSub topic name for a mailbox.
func MailboxTopic(ownerPeerID, folderPath string) string {
	return fmt.Sprintf("%s%s/%s", MailboxTopicPrefix, ownerPeerID, folderPath)
}

// DecodeNotification deserializes a notification from JSON.
func DecodeNotification(data []byte) (*Notification, error) {
	return wire.DecodeNotification(data)
}
