package mda

import (
	"context"
	"log/slog"
	"sync/atomic"
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

	workers chan struct{}
	dropped atomic.Int64
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

// WithWorkers bounds how many notifications may be in flight at once. This
// is worker_threads: each notification is a presence check and a stream
// open to a client that may be slow or gone, and before the bound there was
// one goroutine per delivery with nothing to stop a burst of deliveries
// becoming a burst of goroutines. Zero or less leaves the bound unset.
func (n *Notifier) WithWorkers(count int) *Notifier {
	if count > 0 {
		n.workers = make(chan struct{}, count)
	}
	return n
}

// Dropped reports how many notifications were skipped because every worker
// was busy.
func (n *Notifier) Dropped() int64 { return n.dropped.Load() }

// NotifyNewMessage sends a push notification for a newly delivered message.
// This is fire-and-forget -- it never blocks message delivery, and when all
// workers are busy the notification is dropped rather than queued: the
// client polls anyway, and a queue here would be a second mailbox.
func (n *Notifier) NotifyNewMessage(ctx context.Context, addr *core.MailboxAddress, msg *core.Message) {
	notification := &notify.Notification{
		MailboxPath:  addr.FolderPath,
		MailboxType:  addr.Type.String(),
		MessageCount: 1,
		Timestamp:    time.Now().Unix(),
		ServerPeerID: n.host.ID().String(),
	}

	// The request that delivered the message is answered before this
	// notification goes out, and its context is cancelled with it.
	ctx = context.WithoutCancel(ctx)

	release, ok := n.acquireWorker()
	if !ok {
		n.dropped.Add(1)
		n.logger.Debug("notification dropped: all workers busy",
			"mailbox", addr.FullPath(), "workers", cap(n.workers))
		return
	}

	go func() {
		defer release()
		switch addr.Type {
		case core.MailboxPrivate:
			n.notifyDirect(ctx, addr.OwnerID, notification)
		case core.MailboxShared, core.MailboxPublic:
			n.notifyPubSub(ctx, addr.OwnerID.String(), addr.FolderPath, notification)
		}
	}()
}

// acquireWorker takes a worker slot without waiting. With no bound
// configured every call succeeds.
func (n *Notifier) acquireWorker() (release func(), ok bool) {
	if n.workers == nil {
		return func() {}, true
	}
	select {
	case n.workers <- struct{}{}:
		return func() { <-n.workers }, true
	default:
		return nil, false
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
	notifyCtx, cancel := context.WithTimeout(ctx, notifyWriteTimeout)
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

	if err := codec.WriteFrameWithTimeout(stream, data, notifyWriteTimeout); err != nil {
		n.logger.Debug("cannot write notification", "peer_id", ownerID.String(), "error", err)
		return
	}

	n.logger.Debug("sent direct notification", "peer_id", ownerID.String(), "mailbox", notification.MailboxPath)
}

// notifyWriteTimeout bounds opening the notification stream and writing to
// it, so a client that has gone quiet cannot hold the notifier.
const notifyWriteTimeout = 5 * time.Second

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
