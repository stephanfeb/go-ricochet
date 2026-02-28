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
	HeartbeatInterval time.Duration
	TimeoutDuration   time.Duration
	BatchWindow       time.Duration
	MaxBatchSize      int
	EnableBroadcast   bool
}

// DefaultPresenceConfig returns sensible defaults.
func DefaultPresenceConfig() *PresenceConfig {
	return &PresenceConfig{
		HeartbeatInterval: 60 * time.Second,
		TimeoutDuration:   120 * time.Second,
		BatchWindow:       2 * time.Second,
		MaxBatchSize:      50,
		EnableBroadcast:   true,
	}
}

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

	// Connected peer tracking
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

	// Join GossipSub topic
	if err := s.node.JoinTopic(s.topic); err != nil {
		cancel()
		return err
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
	go s.timeoutCheckLoop(ctx)

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

	// Update cache
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

	s.logger.Debug("peer connected", "peer_id", pid)
}

// onPeerDisconnected is called when a peer disconnects.
func (s *Service) onPeerDisconnected(pid peer.ID) {
	now := time.Now()
	s.mu.Lock()
	delete(s.connectedPeers, pid)
	s.mu.Unlock()

	// Update cache
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

	s.logger.Debug("peer disconnected", "peer_id", pid)
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

func (s *Service) publishHeartbeat(ctx context.Context) {
	if !s.config.EnableBroadcast {
		return
	}

	s.mu.RLock()
	peerIDs := make([]peer.ID, 0, len(s.connectedPeers))
	for pid := range s.connectedPeers {
		peerIDs = append(peerIDs, pid)
	}
	s.mu.RUnlock()

	s.heartbeatSeq++
	hb := &PresenceHeartbeat{
		ServerID:          s.host.ID(),
		Timestamp:         time.Now(),
		OnlineCount:       len(peerIDs),
		OnlinePeerIDs:     peerIDs,
		HeartbeatSequence: s.heartbeatSeq,
	}

	data, err := hb.Encode()
	if err != nil {
		s.logger.Warn("failed to encode heartbeat", "error", err)
		return
	}

	if err := s.node.Publish(ctx, s.topic, data); err != nil {
		s.logger.Warn("failed to publish heartbeat", "error", err)
	} else {
		s.logger.Debug("published heartbeat", "online_count", len(peerIDs), "seq", s.heartbeatSeq)
	}
}

// timeoutCheckLoop removes peers that haven't been seen recently.
func (s *Service) timeoutCheckLoop(ctx context.Context) {
	interval := s.config.TimeoutDuration / 2
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkTimeouts()
		}
	}
}

func (s *Service) checkTimeouts() {
	now := time.Now()
	var timedOut []peer.ID

	s.mu.Lock()
	for pid, lastSeen := range s.connectedPeers {
		if now.Sub(lastSeen) > s.config.TimeoutDuration {
			timedOut = append(timedOut, pid)
			delete(s.connectedPeers, pid)
		}
	}
	s.mu.Unlock()

	for _, pid := range timedOut {
		s.cache.Set(pid, &PresenceStatus{
			PeerID:    pid,
			State:     Offline,
			CheckedAt: now,
		})
		s.queueChange(PresenceChange{
			PeerID: pid,
			State:  Offline,
		})
		s.logger.Debug("peer timed out", "peer_id", pid)
	}

	// Clean up expired cache entries
	s.cache.Cleanup()
}

// notifiee implements network.Notifiee for connection event callbacks.
type notifiee struct {
	service *Service
}

func (n *notifiee) Connected(net network.Network, conn network.Conn) {
	pid := conn.RemotePeer()
	conns := net.ConnsToPeer(pid)
	n.service.logger.Info("DIAG: peer connection event",
		"event", "connected",
		"peer_id", pid,
		"remote_addr", conn.RemoteMultiaddr(),
		"total_conns", len(conns),
		"conn_stat", conn.Stat(),
	)
	n.service.onPeerConnected(pid)
}

func (n *notifiee) Disconnected(net network.Network, conn network.Conn) {
	pid := conn.RemotePeer()
	remaining := net.ConnsToPeer(pid)
	n.service.logger.Info("DIAG: peer connection event",
		"event", "disconnected",
		"peer_id", pid,
		"remote_addr", conn.RemoteMultiaddr(),
		"remaining_conns", len(remaining),
	)
	// Only fire if no remaining connections to this peer
	if len(remaining) == 0 {
		n.service.onPeerDisconnected(pid)
	}
}

func (n *notifiee) Listen(network.Network, multiaddr.Multiaddr)      {}
func (n *notifiee) ListenClose(network.Network, multiaddr.Multiaddr) {}
