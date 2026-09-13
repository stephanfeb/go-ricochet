package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-p2p-forge/node"
)

const (
	// AnnounceTopic is the GossipSub topic for service announcements.
	AnnounceTopic = "/sf-network/services/announce"

	// HeartbeatTopic is the GossipSub topic for service heartbeats.
	HeartbeatTopic = "/sf-network/services/heartbeat"

	// staleThreshold is how old an entry can be before it is considered stale.
	staleThreshold = 2 * time.Hour
)

// SFServerInfo describes a store-and-forward server on the network.
type SFServerInfo struct {
	ServerID        peer.ID  `json:"server_id"`
	Capabilities    []string `json:"capabilities"`
	MaxStorage      string   `json:"max_storage"`
	RetentionPolicy string   `json:"retention_policy"`
	// Region is where this server runs (server_region); Regions is what it
	// serves (supported_regions).
	Region          string   `json:"region,omitempty"`
	Regions         []string `json:"regions"`
	UptimeScore     float64  `json:"uptime_score"`
	Timestamp       time.Time `json:"timestamp"`
	Port            int      `json:"port"`
}

// Registry provides service discovery for S&F servers using GossipSub.
type Registry struct {
	node      *node.Node
	config    *core.ServerConfig
	ownPeerID peer.ID
	servers   map[string]*SFServerInfo
	mu        sync.RWMutex
	logger    *slog.Logger
	cancel    context.CancelFunc

	// rejected counts announcements refused because they were malformed or
	// named a server other than their signer.
	rejected atomic.Int64
}

// RejectedAnnouncements reports how many announcements were refused.
func (r *Registry) RejectedAnnouncements() int64 { return r.rejected.Load() }

// NewRegistry creates a new service registry.
func NewRegistry(n *node.Node, cfg *core.ServerConfig, ownPeerID peer.ID, logger *slog.Logger) *Registry {
	return &Registry{
		node:      n,
		config:    cfg,
		ownPeerID: ownPeerID,
		servers:   make(map[string]*SFServerInfo),
		logger:    logger.With("component", "registry"),
	}
}

// Name returns the service name for lifecycle logging.
func (r *Registry) Name() string { return "registry" }

// Start joins the announcement topics and begins periodic announcements
// and subscription listening.
func (r *Registry) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	// Join the announce and heartbeat topics.
	if err := r.node.JoinTopic(AnnounceTopic); err != nil {
		cancel()
		return fmt.Errorf("join announce topic: %w", err)
	}
	if err := r.node.JoinTopic(HeartbeatTopic); err != nil {
		cancel()
		return fmt.Errorf("join heartbeat topic: %w", err)
	}

	r.logger.Info("registry started",
		"announce_topic", AnnounceTopic,
		"heartbeat_topic", HeartbeatTopic,
	)

	// Start periodic announcements.
	go r.announceLoop(ctx)

	// Start subscription listener.
	go r.subscriptionLoop(ctx)

	return nil
}

// Stop cancels the registry's background goroutines.
func (r *Registry) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.logger.Info("registry stopped")
	return nil
}

// GetAvailableServers returns all non-stale servers sorted by uptime score
// in descending order.
func (r *Registry) GetAvailableServers() []*SFServerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	var result []*SFServerInfo

	for _, info := range r.servers {
		if now.Sub(info.Timestamp) <= staleThreshold {
			result = append(result, info)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UptimeScore > result[j].UptimeScore
	})

	return result
}

// GetStats returns registry statistics.
func (r *Registry) GetStats() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	available := 0
	now := time.Now()
	for _, info := range r.servers {
		if now.Sub(info.Timestamp) <= staleThreshold {
			available++
		}
	}

	return map[string]any{
		"total_known":      len(r.servers),
		"available":        available,
		"rejected":         r.rejected.Load(),
		"own_peer_id":      r.ownPeerID.String(),
		"announce_topic":   AnnounceTopic,
		"heartbeat_topic":  HeartbeatTopic,
	}
}

// announceLoop periodically publishes this server's info to the announce topic.
func (r *Registry) announceLoop(ctx context.Context) {
	// Announce immediately on startup.
	r.announceServer(ctx)

	ticker := time.NewTicker(r.config.ServiceAnnouncementInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.announceServer(ctx)
		}
	}
}

// announceServer publishes a JSON-encoded SFServerInfo to the announce topic.
func (r *Registry) announceServer(ctx context.Context) {
	info := &SFServerInfo{
		ServerID:        r.ownPeerID,
		Capabilities:    []string{"store-and-forward", "mailbox", "document-sync"},
		MaxStorage:      r.config.MaxStorageHuman(),
		RetentionPolicy: r.config.RetentionPolicy.String(),
		Region:          r.config.ServerRegion,
		Regions:         r.config.SupportedRegions,
		UptimeScore:     1.0,
		Timestamp:       time.Now(),
		Port:            r.config.Port,
	}

	data, err := json.Marshal(info)
	if err != nil {
		r.logger.Error("failed to marshal server info", "error", err)
		return
	}

	if err := r.node.Publish(ctx, AnnounceTopic, data); err != nil {
		r.logger.Warn("failed to publish announcement", "error", err)
		return
	}

	r.logger.Debug("published server announcement", "peer_id", r.ownPeerID)
}

// subscriptionLoop reads messages from the announce topic and stores
// discovered servers.
func (r *Registry) subscriptionLoop(ctx context.Context) {
	sub := r.node.Subscribe(AnnounceTopic)
	if sub == nil {
		r.logger.Error("no subscription for announce topic")
		return
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Warn("error reading subscription", "error", err)
			continue
		}

		// GetFrom is the peer that signed the message, which GossipSub
		// verified (the node runs StrictSign); ReceivedFrom is only the
		// neighbour that relayed it.
		from := msg.GetFrom()
		if from == r.ownPeerID {
			continue
		}
		if err := r.handleAnnouncement(from, msg.Data); err != nil {
			r.rejected.Add(1)
			r.logger.Warn("rejected announcement", "from", from, "error", err)
		}
	}
}

// handleAnnouncement stores an announcement signed by from. The entry is
// keyed by the signer, and a payload naming any other server is refused:
// otherwise any peer on the topic could overwrite another server's entry.
func (r *Registry) handleAnnouncement(from peer.ID, data []byte) error {
	var info SFServerInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("decode announcement: %w", err)
	}
	if info.ServerID != from {
		return fmt.Errorf("announcement for %s signed by %s", info.ServerID, from)
	}

	r.mu.Lock()
	r.servers[from.String()] = &info
	r.mu.Unlock()

	r.logger.Debug("discovered server",
		"peer_id", info.ServerID,
		"regions", info.Regions,
		"uptime_score", info.UptimeScore,
	)
	return nil
}
