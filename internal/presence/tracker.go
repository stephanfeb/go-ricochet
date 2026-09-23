package presence

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-p2p-forge/node"
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

	// partial holds the pages of a multi-page heartbeat received so far,
	// per server, until the sequence is complete.
	partial map[peer.ID]*heartbeatAssembly
}

// heartbeatAssembly collects the pages of one heartbeat sequence.
type heartbeatAssembly struct {
	seq    uint64
	pages  map[int]struct{}
	total  int
	online map[peer.ID]bool
}

// NewTracker creates a new client-side presence tracker.
func NewTracker(node *node.Node, cache *Cache, logger *slog.Logger) *Tracker {
	return &Tracker{
		node:     node,
		cache:    cache,
		contacts: make(map[peer.ID]bool),
		partial:  make(map[peer.ID]*heartbeatAssembly),
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

	go t.subscriptionLoop(ctx, topic, serverID)

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

func (t *Tracker) subscriptionLoop(ctx context.Context, topic string, serverID peer.ID) {
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

		// GetFrom is the verified signer (the node runs StrictSign), not
		// the neighbour that relayed the message.
		if err := t.handleMessage(msg.GetFrom(), serverID, msg.Data); err != nil {
			t.logger.Warn("rejected presence message", "topic", topic, "from", msg.GetFrom(), "error", err)
		}
	}
}

// handleMessage applies a presence message signed by from on the topic of
// serverID. The topic is open to any publisher, so only the server the topic
// belongs to may speak on it, and the payload must name that same server;
// otherwise any peer could announce presence for peers it does not have.
func (t *Tracker) handleMessage(from, serverID peer.ID, data []byte) error {
	if from != serverID {
		return fmt.Errorf("published by %s on the topic of %s", from, serverID)
	}

	msgType := DetectMessageType(data)
	switch msgType {
	case "event":
		event, err := DecodePresenceEvent(data)
		if err != nil {
			return fmt.Errorf("decode presence event: %w", err)
		}
		if event.ServerID != from {
			return fmt.Errorf("event names server %s, signed by %s", event.ServerID, from)
		}
		t.processEvent(event)
	case "heartbeat":
		hb, err := DecodePresenceHeartbeat(data)
		if err != nil {
			return fmt.Errorf("decode presence heartbeat: %w", err)
		}
		if hb.ServerID != from {
			return fmt.Errorf("heartbeat names server %s, signed by %s", hb.ServerID, from)
		}
		t.processHeartbeat(hb)
	default:
		t.logger.Debug("unknown presence message type", "type", msgType)
	}
	return nil
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

// processHeartbeat reconciles contacts against a server's online set. A
// paged heartbeat is applied only once every page of its sequence has
// arrived; acting on one page alone would mark every contact on the other
// pages offline.
func (t *Tracker) processHeartbeat(hb *PresenceHeartbeat) {
	onlineSet, complete := t.assembleHeartbeat(hb)
	if !complete {
		return
	}
	t.reconcileContacts(onlineSet)
}

// assembleHeartbeat folds a page into the server's pending sequence and
// returns the full online set once the last page is in. A single-page
// heartbeat completes immediately. A page from a newer sequence discards
// whatever was pending: the server has moved on, and the old set would be
// stale anyway.
func (t *Tracker) assembleHeartbeat(hb *PresenceHeartbeat) (map[peer.ID]bool, bool) {
	if hb.PageCount <= 1 {
		onlineSet := make(map[peer.ID]bool, len(hb.OnlinePeerIDs))
		for _, pid := range hb.OnlinePeerIDs {
			onlineSet[pid] = true
		}
		return onlineSet, true
	}
	if hb.Page < 0 || hb.Page >= hb.PageCount {
		t.logger.Warn("heartbeat page out of range",
			"server", hb.ServerID, "page", hb.Page, "page_count", hb.PageCount)
		return nil, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	asm := t.partial[hb.ServerID]
	if asm == nil || asm.seq != hb.HeartbeatSequence || asm.total != hb.PageCount {
		asm = &heartbeatAssembly{
			seq:    hb.HeartbeatSequence,
			pages:  make(map[int]struct{}, hb.PageCount),
			total:  hb.PageCount,
			online: make(map[peer.ID]bool, hb.OnlineCount),
		}
		t.partial[hb.ServerID] = asm
	}
	asm.pages[hb.Page] = struct{}{}
	for _, pid := range hb.OnlinePeerIDs {
		asm.online[pid] = true
	}
	if len(asm.pages) < asm.total {
		return nil, false
	}
	delete(t.partial, hb.ServerID)
	return asm.online, true
}

// reconcileContacts sets every contact Online or Offline according to the
// online set and reports the ones that changed.
func (t *Tracker) reconcileContacts(onlineSet map[peer.ID]bool) {
	now := time.Now()

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
