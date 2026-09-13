package wire

import "encoding/json"

// Notification is a push notification that a mailbox has new messages. The
// server sends one over the notify protocol and publishes one on the
// mailbox's GossipSub topic.
type Notification struct {
	MailboxPath  string `json:"mailboxPath"`
	MailboxType  string `json:"mailboxType"`
	MessageCount int    `json:"messageCount"`
	Timestamp    int64  `json:"timestamp"`
	ServerPeerID string `json:"serverPeerId"`
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
