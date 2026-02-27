package presence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-p2p-forge/node"
)

// Tracker subscribes to presence topics from servers and maintains
// a local presence cache for contacts.
type Tracker struct {
	node     *node.Node
	cache    *Cache
	contacts map[peer.ID]bool
	mu       sync.RWMutex
	logger   *slog.Logger
	cancel   context.CancelFunc

	// Callback for presence changes
	onChange func(pid peer.ID, state PresenceState)
}

// NewTracker creates a new client-side presence tracker.
func NewTracker(node *node.Node, cache *Cache, logger *slog.Logger) *Tracker {
	return &Tracker{
		node:     node,
		cache:    cache,
		contacts: make(map[peer.ID]bool),
		logger:   logger.With("component", "presence-tracker"),
	}
}

// SetOnChange registers a callback for presence state changes.
func (t *Tracker) SetOnChange(fn func(peer.ID, PresenceState)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onChange = fn
}

// AddContact adds a peer to track presence for.
func (t *Tracker) AddContact(pid peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.contacts[pid] = true
}

// RemoveContact stops tracking presence for a peer.
func (t *Tracker) RemoveContact(pid peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.contacts, pid)
}

// SubscribeToServer subscribes to presence events from the given server.
func (t *Tracker) SubscribeToServer(ctx context.Context, serverID peer.ID) error {
	topic := PresenceTopic(serverID)

	if err := t.node.JoinTopic(topic); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()

	go t.subscriptionLoop(ctx, topic)

	t.logger.Info("subscribed to presence", "server", serverID, "topic", topic)
	return nil
}

// GetPresence returns the cached presence state for a peer.
func (t *Tracker) GetPresence(pid peer.ID) PresenceState {
	status := t.cache.Get(pid)
	if status == nil {
		return Unknown
	}
	return status.State
}

// GetOnlineContacts returns all contacts currently known to be online.
func (t *Tracker) GetOnlineContacts() []peer.ID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var online []peer.ID
	for pid := range t.contacts {
		status := t.cache.Get(pid)
		if status != nil && status.State == Online {
			online = append(online, pid)
		}
	}
	return online
}

// Stop cancels the tracker's subscription.
func (t *Tracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
	t.logger.Info("presence tracker stopped")
}

func (t *Tracker) subscriptionLoop(ctx context.Context, topic string) {
	sub := t.node.Subscribe(topic)
	if sub == nil {
		t.logger.Error("no subscription for presence topic", "topic", topic)
		return
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.logger.Warn("error reading presence subscription", "error", err)
			continue
		}

		t.handleMessage(msg.Data)
	}
}

func (t *Tracker) handleMessage(data []byte) {
	msgType := DetectMessageType(data)

	switch msgType {
	case "event":
		event, err := DecodePresenceEvent(data)
		if err != nil {
			t.logger.Warn("failed to decode presence event", "error", err)
			return
		}
		t.processEvent(event)
	case "heartbeat":
		hb, err := DecodePresenceHeartbeat(data)
		if err != nil {
			t.logger.Warn("failed to decode presence heartbeat", "error", err)
			return
		}
		t.processHeartbeat(hb)
	default:
		t.logger.Debug("unknown presence message type", "type", msgType)
	}
}

func (t *Tracker) processEvent(event *PresenceEvent) {
	now := time.Now()
	for _, change := range event.Changes {
		if !t.isTracked(change.PeerID) {
			continue
		}

		t.cache.Set(change.PeerID, &PresenceStatus{
			PeerID:    change.PeerID,
			State:     change.State,
			LastSeen:  now,
			CheckedAt: now,
		})

		t.mu.RLock()
		cb := t.onChange
		t.mu.RUnlock()
		if cb != nil {
			cb(change.PeerID, change.State)
		}
	}
}

func (t *Tracker) processHeartbeat(hb *PresenceHeartbeat) {
	now := time.Now()

	// Build set of online peers from heartbeat
	onlineSet := make(map[peer.ID]bool, len(hb.OnlinePeerIDs))
	for _, pid := range hb.OnlinePeerIDs {
		onlineSet[pid] = true
	}

	t.mu.RLock()
	contacts := make(map[peer.ID]bool, len(t.contacts))
	for pid := range t.contacts {
		contacts[pid] = true
	}
	cb := t.onChange
	t.mu.RUnlock()

	for pid := range contacts {
		var newState PresenceState
		if onlineSet[pid] {
			newState = Online
		} else {
			newState = Offline
		}

		old := t.cache.Get(pid)
		oldState := Unknown
		if old != nil {
			oldState = old.State
		}

		t.cache.Set(pid, &PresenceStatus{
			PeerID:    pid,
			State:     newState,
			LastSeen:  now,
			CheckedAt: now,
		})

		if newState != oldState && cb != nil {
			cb(pid, newState)
		}
	}
}

func (t *Tracker) isTracked(pid peer.ID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.contacts[pid]
}
