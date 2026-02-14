package presence

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Monitor actively checks peer presence by attempting connections
// and caches the results.
type Monitor struct {
	cache  *Cache
	host   host.Host
	logger *slog.Logger
	cancel context.CancelFunc
}

// NewMonitor creates a new presence monitor.
func NewMonitor(cache *Cache, h host.Host, logger *slog.Logger) *Monitor {
	return &Monitor{
		cache:  cache,
		host:   h,
		logger: logger.With("component", "presence-monitor"),
	}
}

// CheckPresence determines the current presence state of a peer.
// It first checks if the peer is already connected, and if not,
// attempts to establish a connection.
func (m *Monitor) CheckPresence(ctx context.Context, id peer.ID) *PresenceStatus {
	now := time.Now()
	status := &PresenceStatus{
		PeerID:    id,
		CheckedAt: now,
	}

	// Check if already connected.
	if m.host.Network().Connectedness(id) == network.Connected {
		status.State = Online
		status.LastSeen = now
		m.cache.Set(id, status)
		m.logger.Debug("peer is connected", "peer_id", id, "state", Online)
		return status
	}

	// Attempt to connect.
	addrInfo := peer.AddrInfo{ID: id}
	if err := m.host.Connect(ctx, addrInfo); err != nil {
		status.State = Offline
		// Preserve last seen from cache if available.
		if cached := m.cache.Get(id); cached != nil {
			status.LastSeen = cached.LastSeen
		}
		m.cache.Set(id, status)
		m.logger.Debug("peer is offline", "peer_id", id, "error", err)
		return status
	}

	status.State = Online
	status.LastSeen = now
	m.cache.Set(id, status)
	m.logger.Debug("peer connected successfully", "peer_id", id, "state", Online)
	return status
}

// StartPeriodicMonitoring launches a background goroutine that periodically
// checks the presence of the given peers at the specified interval.
func (m *Monitor) StartPeriodicMonitoring(ctx context.Context, peers []peer.ID, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	m.logger.Info("starting periodic presence monitoring",
		"peer_count", len(peers),
		"interval", interval,
	)

	go func() {
		// Run an initial check immediately.
		for _, id := range peers {
			if ctx.Err() != nil {
				return
			}
			m.CheckPresence(ctx, id)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, id := range peers {
					if ctx.Err() != nil {
						return
					}
					m.CheckPresence(ctx, id)
				}
				// Clean up expired cache entries after each round.
				m.cache.Cleanup()
			}
		}
	}()
}

// StopMonitoring cancels the periodic monitoring goroutine.
func (m *Monitor) StopMonitoring() {
	if m.cancel != nil {
		m.cancel()
		m.logger.Info("presence monitoring stopped")
	}
}

// GetStats returns monitoring statistics including cache stats.
func (m *Monitor) GetStats() map[string]any {
	stats := m.cache.GetStats()
	stats["host_peer_id"] = m.host.ID().String()
	stats["host_peers_connected"] = len(m.host.Network().Peers())
	return stats
}
