package mda

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/node"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/presence"
	"github.com/twostack/go-ricochet/internal/protocol/notify"
)

// Notifier sends push notifications when new messages are delivered.
type Notifier struct {
	host     host.Host
	node     *node.Node
	presence *presence.Monitor
	logger   *slog.Logger
}

// NewNotifier creates a new push notification sender.
func NewNotifier(h host.Host, node *node.Node, pm *presence.Monitor, logger *slog.Logger) *Notifier {
	return &Notifier{
		host:     h,
		node:     node,
		presence: pm,
		logger:   logger.With("component", "notifier"),
	}
}

// NotifyNewMessage sends a push notification for a newly delivered message.
// This is fire-and-forget -- it never blocks message delivery.
func (n *Notifier) NotifyNewMessage(ctx context.Context, addr *core.MailboxAddress, msg *core.Message) {
	notification := &notify.Notification{
		MailboxPath:  addr.FolderPath,
		MailboxType:  addr.Type.String(),
		MessageCount: 1,
		Timestamp:    time.Now().Unix(),
		ServerPeerID: n.host.ID().String(),
	}

	switch addr.Type {
	case core.MailboxPrivate:
		go n.notifyDirect(ctx, addr.OwnerID, notification)
	case core.MailboxShared, core.MailboxPublic:
		go n.notifyPubSub(ctx, addr.OwnerID.String(), addr.FolderPath, notification)
	}
}

// notifyDirect sends a notification via direct P2P stream.
func (n *Notifier) notifyDirect(ctx context.Context, ownerID peer.ID, notification *notify.Notification) {
	// Check presence first -- skip if offline.
	if n.presence != nil {
		notifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		status := n.presence.CheckPresence(notifyCtx, ownerID)
		if status.State == presence.Offline {
			n.logger.Debug("skipping notification for offline peer", "peer_id", ownerID.String())
			return
		}
	}

	// Create a timeout context for the notification.
	notifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := n.host.NewStream(notifyCtx, ownerID, notify.ProtocolID)
	if err != nil {
		n.logger.Debug("cannot open notification stream", "peer_id", ownerID.String(), "error", err)
		return
	}
	defer stream.Close()

	data, err := notification.Encode()
	if err != nil {
		n.logger.Warn("cannot encode notification", "error", err)
		return
	}

	if err := codec.WriteFrame(stream, data); err != nil {
		n.logger.Debug("cannot write notification", "peer_id", ownerID.String(), "error", err)
		return
	}

	n.logger.Debug("sent direct notification", "peer_id", ownerID.String(), "mailbox", notification.MailboxPath)
}

// notifyPubSub publishes a notification via GossipSub.
func (n *Notifier) notifyPubSub(ctx context.Context, ownerPeerID, folderPath string, notification *notify.Notification) {
	topic := notify.MailboxTopic(ownerPeerID, folderPath)

	if err := n.node.JoinTopic(topic); err != nil {
		n.logger.Warn("cannot join notification topic", "topic", topic, "error", err)
		return
	}

	data, err := notification.Encode()
	if err != nil {
		n.logger.Warn("cannot encode notification", "error", err)
		return
	}

	notifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := n.node.Publish(notifyCtx, topic, data); err != nil {
		n.logger.Debug("cannot publish notification", "topic", topic, "error", err)
		return
	}

	n.logger.Debug("published pubsub notification", "topic", topic)
}
