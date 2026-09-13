package presence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/twostack/go-p2p-forge/node"
)

// PresenceConfig holds presence service configuration.
type PresenceConfig struct {
	// HeartbeatInterval is how often the full online set is published.
	HeartbeatInterval time.Duration

	// TimeoutDuration is the TTL advertised on Online change events: how
	// long a subscriber may trust the claim without a heartbeat refreshing
	// it. It does not evict peers on the server; connectedness does.
	TimeoutDuration time.Duration

	// CheckInterval is how often the service reconciles its online set with
	// the network, catching any connect or disconnect the notifier missed.
	CheckInterval time.Duration

	BatchWindow     time.Duration
	MaxBatchSize    int
	EnableBroadcast bool
}

// DefaultPresenceConfig returns sensible defaults.
func DefaultPresenceConfig() *PresenceConfig {
	return &PresenceConfig{
		HeartbeatInterval: 60 * time.Second,
		TimeoutDuration:   120 * time.Second,
		CheckInterval:     30 * time.Second,
		BatchWindow:       2 * time.Second,
		MaxBatchSize:      50,
		EnableBroadcast:   true,
	}
}

// heartbeatPageSize bounds the peer IDs carried by one heartbeat message.
//
// GossipSub drops messages over about 1 MiB. A peer ID is around 55 bytes
// as JSON, so a single message holds under 19k of them; a server with more
// connected peers than that would silently stop heartbeating. 4096 IDs is
// roughly 225 KiB, well inside the cap with room for the envelope.
const heartbeatPageSize = 4096

// PresenceTopic returns the GossipSub topic for a server's presence events.
func PresenceTopic(serverID peer.ID) string {
	return "/sf-network/presence/" + serverID.String()
}

// Service manages server-side presence tracking and broadcasting.
type Service struct {
	host   host.Host
	node   *node.Node
	cache  *Cache
	config *PresenceConfig
	logger *slog.Logger

	// connectedPeers is the online set: every peer the network has a
	// connection to, keyed to the time it was first seen connected. The
	// notifier keeps it current; the reconcile loop corrects any drift.
	connectedPeers map[peer.ID]time.Time
	mu             sync.RWMutex

	// Batching
	pendingChanges []PresenceChange
	batchMu        sync.Mutex
	batchTimer     *time.Timer

	// Heartbeat
	heartbeatSeq uint64

	// Lifecycle
	cancel context.CancelFunc
	topic  string
}

// NewService creates a new presence broadcasting service.
func NewService(h host.Host, node *node.Node, cache *Cache, cfg *PresenceConfig, logger *slog.Logger) *Service {
	if cfg == nil {
		cfg = DefaultPresenceConfig()
	}
	return &Service{
		host:           h,
		node:           node,
		cache:          cache,
		config:         cfg,
		logger:         logger.With("component", "presence-service"),
		connectedPeers: make(map[peer.ID]time.Time),
		topic:          PresenceTopic(h.ID()),
	}
}

// Name returns the service name for lifecycle logging.
func (s *Service) Name() string { return "presence" }

// Start joins the presence GossipSub topic and begins background loops.
func (s *Service) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// The topic is only needed to publish, so a service that does not
	// broadcast needs no node at all.
	if s.config.EnableBroadcast {
		if err := s.node.JoinTopic(s.topic); err != nil {
			cancel()
			return err
		}
	}

	// Register network notifiee for connect/disconnect callbacks
	s.host.Network().Notify(&notifiee{service: s})

	// Snapshot currently connected peers
	s.mu.Lock()
	for _, pid := range s.host.Network().Peers() {
		s.connectedPeers[pid] = time.Now()
	}
	s.mu.Unlock()

	// Start background goroutines
	go s.heartbeatLoop(ctx)
	go s.reconcileLoop(ctx)

	s.logger.Info("presence service started", "topic", s.topic)
	return nil
}

// Stop cancels all background goroutines.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.batchMu.Lock()
	if s.batchTimer != nil {
		s.batchTimer.Stop()
	}
	s.batchMu.Unlock()
	s.logger.Info("presence service stopped")
	return nil
}

// GetOnlinePeers returns currently tracked online peers.
func (s *Service) GetOnlinePeers() []peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]peer.ID, 0, len(s.connectedPeers))
	for pid := range s.connectedPeers {
		peers = append(peers, pid)
	}
	return peers
}

// GetOnlineCount returns the count of online peers.
func (s *Service) GetOnlineCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.connectedPeers)
}

// onPeerConnected is called when a peer connects.
func (s *Service) onPeerConnected(pid peer.ID) {
	now := time.Now()
	s.mu.Lock()
	s.connectedPeers[pid] = now
	s.mu.Unlock()

	s.markOnline(pid, now)
	s.logger.Debug("peer connected", "peer_id", pid)
}

// onPeerDisconnected is called when a peer disconnects.
func (s *Service) onPeerDisconnected(pid peer.ID) {
	now := time.Now()
	s.mu.Lock()
	delete(s.connectedPeers, pid)
	s.mu.Unlock()

	s.markOffline(pid, now)
	s.logger.Debug("peer disconnected", "peer_id", pid)
}

// markOnline records an Online status in the cache and queues the change
// for broadcast.
func (s *Service) markOnline(pid peer.ID, now time.Time) {
	s.cache.Set(pid, &PresenceStatus{
		PeerID:    pid,
		State:     Online,
		LastSeen:  now,
		CheckedAt: now,
	})
	s.queueChange(PresenceChange{
		PeerID:     pid,
		State:      Online,
		TTLSeconds: int(s.config.TimeoutDuration.Seconds()),
	})
}

// markOffline records an Offline status in the cache and queues the change
// for broadcast.
func (s *Service) markOffline(pid peer.ID, now time.Time) {
	s.cache.Set(pid, &PresenceStatus{
		PeerID:    pid,
		State:     Offline,
		LastSeen:  now,
		CheckedAt: now,
	})
	s.queueChange(PresenceChange{
		PeerID: pid,
		State:  Offline,
	})
}

// queueChange adds a presence change to the batch and starts the batch timer.
func (s *Service) queueChange(change PresenceChange) {
	if !s.config.EnableBroadcast {
		return
	}

	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	s.pendingChanges = append(s.pendingChanges, change)

	// Flush immediately if max batch size reached
	if len(s.pendingChanges) >= s.config.MaxBatchSize {
		go s.flushBatch()
		return
	}

	// Start or reset batch timer
	if s.batchTimer == nil {
		s.batchTimer = time.AfterFunc(s.config.BatchWindow, func() {
			s.flushBatch()
		})
	} else {
		s.batchTimer.Reset(s.config.BatchWindow)
	}
}

// flushBatch publishes all pending changes as a PresenceEvent via GossipSub.
func (s *Service) flushBatch() {
	s.batchMu.Lock()
	if len(s.pendingChanges) == 0 {
		s.batchMu.Unlock()
		return
	}
	changes := s.pendingChanges
	s.pendingChanges = nil
	if s.batchTimer != nil {
		s.batchTimer.Stop()
		s.batchTimer = nil
	}
	s.batchMu.Unlock()

	event := &PresenceEvent{
		ServerID:  s.host.ID(),
		Timestamp: time.Now(),
		Changes:   changes,
	}

	data, err := event.Encode()
	if err != nil {
		s.logger.Warn("failed to encode presence event", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.node.Publish(ctx, s.topic, data); err != nil {
		s.logger.Warn("failed to publish presence event", "error", err)
	} else {
		s.logger.Debug("published presence event", "changes", len(changes))
	}
}

// heartbeatLoop publishes PresenceHeartbeat every HeartbeatInterval.
func (s *Service) heartbeatLoop(ctx context.Context) {
	// Publish initial heartbeat
	s.publishHeartbeat(ctx)

	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.publishHeartbeat(ctx)
		}
	}
}

// publishHeartbeat publishes the online set as one heartbeat sequence,
// split across as many pages as heartbeatPageSize requires.
func (s *Service) publishHeartbeat(ctx context.Context) {
	if !s.config.EnableBroadcast {
		return
	}

	peerIDs := s.GetOnlinePeers()
	s.heartbeatSeq++
	pages := heartbeatPages(s.host.ID(), s.heartbeatSeq, time.Now(), peerIDs, heartbeatPageSize)

	for _, hb := range pages {
		data, err := hb.Encode()
		if err != nil {
			s.logger.Warn("failed to encode heartbeat", "error", err)
			return
		}
		if err := s.node.Publish(ctx, s.topic, data); err != nil {
			s.logger.Warn("failed to publish heartbeat", "error", err, "page", hb.Page)
			return
		}
	}
	s.logger.Debug("published heartbeat",
		"online_count", len(peerIDs), "seq", s.heartbeatSeq, "pages", len(pages))
}

// heartbeatPages splits an online set into heartbeat messages of at most
// pageSize peer IDs each. Every page carries the same sequence, timestamp
// and total count, so a subscriber can tell the pages of one heartbeat
// apart from the next. An empty set still produces one page: the heartbeat
// is a liveness signal as well as a roster.
func heartbeatPages(serverID peer.ID, seq uint64, at time.Time, peerIDs []peer.ID, pageSize int) []*PresenceHeartbeat {
	if pageSize <= 0 {
		pageSize = heartbeatPageSize
	}
	pageCount := (len(peerIDs) + pageSize - 1) / pageSize
	if pageCount == 0 {
		pageCount = 1
	}

	pages := make([]*PresenceHeartbeat, 0, pageCount)
	for page := 0; page < pageCount; page++ {
		start := page * pageSize
		end := start + pageSize
		if end > len(peerIDs) {
			end = len(peerIDs)
		}
		pages = append(pages, &PresenceHeartbeat{
			ServerID:          serverID,
			Timestamp:         at,
			OnlineCount:       len(peerIDs),
			OnlinePeerIDs:     peerIDs[start:end],
			HeartbeatSequence: seq,
			Page:              page,
			PageCount:         pageCount,
		})
	}
	return pages
}

// reconcileLoop periodically corrects the online set against the network.
func (s *Service) reconcileLoop(ctx context.Context) {
	interval := s.config.CheckInterval
	if interval <= 0 {
		interval = DefaultPresenceConfig().CheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile()
		}
	}
}

// reconcile makes the online set match the peers the network is connected
// to right now. A peer is online for exactly as long as it has a connection,
// however long that is: a peer connected for hours is still online. The
// notifier normally keeps the set current; this catches a notification
// that was missed or arrived out of order, and refreshes the cache entry
// of every connected peer so it does not expire underneath them.
func (s *Service) reconcile() {
	now := time.Now()
	connected := s.host.Network().Peers()
	connectedSet := make(map[peer.ID]struct{}, len(connected))
	for _, pid := range connected {
		connectedSet[pid] = struct{}{}
	}

	var gone, appeared []peer.ID
	s.mu.Lock()
	for pid := range s.connectedPeers {
		if _, ok := connectedSet[pid]; !ok {
			gone = append(gone, pid)
			delete(s.connectedPeers, pid)
		}
	}
	for _, pid := range connected {
		if _, ok := s.connectedPeers[pid]; !ok {
			appeared = append(appeared, pid)
			s.connectedPeers[pid] = now
		}
	}
	s.mu.Unlock()

	for _, pid := range gone {
		s.markOffline(pid, now)
		s.logger.Debug("peer no longer connected", "peer_id", pid)
	}
	for _, pid := range appeared {
		s.markOnline(pid, now)
		s.logger.Debug("peer connected without notification", "peer_id", pid)
	}
	for _, pid := range connected {
		s.cache.Set(pid, &PresenceStatus{
			PeerID:    pid,
			State:     Online,
			LastSeen:  now,
			CheckedAt: now,
		})
	}

	// Clean up expired cache entries
	s.cache.Cleanup()
}

// notifiee implements network.Notifiee for connection event callbacks.
type notifiee struct {
	service *Service
}

func (n *notifiee) Connected(_ network.Network, conn network.Conn) {
	n.service.onPeerConnected(conn.RemotePeer())
}

func (n *notifiee) Disconnected(net network.Network, conn network.Conn) {
	// Only fire if no remaining connections to this peer
	if len(net.ConnsToPeer(conn.RemotePeer())) == 0 {
		n.service.onPeerDisconnected(conn.RemotePeer())
	}
}

func (n *notifiee) Listen(network.Network, multiaddr.Multiaddr)      {}
func (n *notifiee) ListenClose(network.Network, multiaddr.Multiaddr) {}
