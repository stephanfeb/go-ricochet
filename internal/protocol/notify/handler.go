package notify

import (
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// ProtocolID is the protocol identifier for mailbox notifications.
const ProtocolID = protocol.ID("/ricochet/mailbox-notify/1.0.0")

// MailboxTopicPrefix is the GossipSub topic prefix for mailbox notifications.
const MailboxTopicPrefix = "/ricochet/mailbox/"

// Notification represents a push notification for new messages.
type Notification struct {
	MailboxPath  string `json:"mailboxPath"`
	MailboxType  string `json:"mailboxType"`
	MessageCount int    `json:"messageCount"`
	Timestamp    int64  `json:"timestamp"`
	ServerPeerID string `json:"serverPeerId"`
}

// MailboxTopic returns the GossipSub topic name for a mailbox.
func MailboxTopic(ownerPeerID, folderPath string) string {
	return fmt.Sprintf("%s%s/%s", MailboxTopicPrefix, ownerPeerID, folderPath)
}

// Encode serializes a notification to JSON.
func (n *Notification) Encode() ([]byte, error) {
	return json.Marshal(n)
}

// DecodeNotification deserializes a notification from JSON.
func DecodeNotification(data []byte) (*Notification, error) {
	var n Notification
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return &n, nil
}
