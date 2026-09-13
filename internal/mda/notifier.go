package mda

import (
	"context"
	"log/slog"
	"sync"
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

// PresenceChecker answers whether a peer is reachable before the notifier
// dials it. *presence.Monitor is the production implementation.
type PresenceChecker interface {
	CheckPresence(ctx context.Context, id peer.ID) *presence.PresenceStatus
}

// pubsubNode is the slice of *node.Node the notifier uses, so a test can
// stand in a recorder for it.
type pubsubNode interface {
	JoinTopic(name string) error
	LeaveTopic(name string) error
	Publish(ctx context.Context, topic string, data []byte) error
}

// defaultTopicIdle is how long a shared or public mailbox topic stays
// joined after its last notification. Every joined topic is a GossipSub
// mesh the server maintains, so one per mailbox ever notified on would grow
// without bound; a mailbox that goes quiet is re-joined on its next message.
const defaultTopicIdle = 15 * time.Minute

// Notifier sends push notifications when new messages are delivered.
type Notifier struct {
	host     host.Host
	node     pubsubNode
	presence PresenceChecker
	logger   *slog.Logger

	workers chan struct{}
	dropped atomic.Int64

	// topics is every mailbox topic currently joined, keyed to its last
	// notification, so idle ones can be left.
	topicsMu  sync.Mutex
	topics    map[string]time.Time
	topicIdle time.Duration
}

// NewNotifier creates a new push notification sender.
//
// A nil checker means every private-mailbox notification dials the
// recipient, reachable or not; pass the presence monitor whenever one
// exists. The check is an interface so a nil *presence.Monitor must not be
// passed as it: that is a non-nil interface holding a nil pointer.
func NewNotifier(h host.Host, node *node.Node, pm PresenceChecker, logger *slog.Logger) *Notifier {
	n := &Notifier{
		host:      h,
		presence:  pm,
		logger:    logger.With("component", "notifier"),
		topics:    make(map[string]time.Time),
		topicIdle: defaultTopicIdle,
	}
	if node != nil {
		n.node = node
	}
	if pm == nil {
		n.logger.Warn("no presence checker: push notifications will dial every recipient")
	}
	return n
}

// WithTopicIdle sets how long a mailbox topic stays joined after its last
// notification. Zero or less keeps the default.
func (n *Notifier) WithTopicIdle(d time.Duration) *Notifier {
	if d > 0 {
		n.topicIdle = d
	}
	return n
}

// Start runs the idle-topic sweep until ctx is cancelled.
func (n *Notifier) Start(ctx context.Context) {
	go n.sweepLoop(ctx)
}

// MailboxDeleted leaves the notification topic of a deleted shared or
// public mailbox. A private mailbox has no topic.
func (n *Notifier) MailboxDeleted(addr *core.MailboxAddress) {
	if addr.Type != core.MailboxShared && addr.Type != core.MailboxPublic {
		return
	}
	n.leaveTopic(notify.MailboxTopic(addr.OwnerID.String(), addr.FolderPath))
}

// JoinedTopics reports how many mailbox topics are currently joined.
func (n *Notifier) JoinedTopics() int {
	n.topicsMu.Lock()
	defer n.topicsMu.Unlock()
	return len(n.topics)
}

func (n *Notifier) sweepLoop(ctx context.Context) {
	interval := n.topicIdle / 4
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.leaveIdleTopics(time.Now())
		}
	}
}

// leaveIdleTopics leaves every topic whose last notification is older than
// the idle limit.
func (n *Notifier) leaveIdleTopics(now time.Time) {
	var idle []string
	n.topicsMu.Lock()
	for topic, last := range n.topics {
		if now.Sub(last) > n.topicIdle {
			idle = append(idle, topic)
		}
	}
	n.topicsMu.Unlock()
	for _, topic := range idle {
		n.leaveTopic(topic)
	}
}

func (n *Notifier) leaveTopic(topic string) {
	n.topicsMu.Lock()
	_, joined := n.topics[topic]
	delete(n.topics, topic)
	n.topicsMu.Unlock()
	if !joined || n.node == nil {
		return
	}
	if err := n.node.LeaveTopic(topic); err != nil {
		n.logger.Warn("cannot leave notification topic", "topic", topic, "error", err)
		return
	}
	n.logger.Debug("left notification topic", "topic", topic)
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
	if n.node == nil {
		return
	}

	// Join under the lock so a concurrent idle sweep cannot leave the topic
	// between the join and the publish.
	n.topicsMu.Lock()
	if err := n.node.JoinTopic(topic); err != nil {
		n.topicsMu.Unlock()
		n.logger.Warn("cannot join notification topic", "topic", topic, "error", err)
		return
	}
	n.topics[topic] = time.Now()
	n.topicsMu.Unlock()

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
