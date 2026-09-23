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

	"github.com/stephanfeb/go-p2p-forge/node"
	"github.com/stephanfeb/go-ricochet/internal/core"
)

const (
	// AnnounceTopic is the GossipSub topic for service announcements.
	AnnounceTopic = "/sf-network/services/announce"

	// HeartbeatTopic is the GossipSub topic for service heartbeats. A server
	// publishes one every HealthCheckInterval with the verdict of its own
	// readiness checks, so an entry stays fresh between hourly announcements
	// and an unhealthy server says so long before it goes stale.
	HeartbeatTopic = "/sf-network/services/heartbeat"

	// staleThreshold is how old an entry can be before it is considered stale.
	staleThreshold = 2 * time.Hour

	// uptimeSamples is how many self-check results the uptime score is taken
	// over: 24 hours at the default five-minute health_check interval. A
	// score over the whole process lifetime would let a bad hour disappear
	// into a good month; a window this size forgets it in a day.
	uptimeSamples = 288

	// checkTimeout bounds one self-check. The readiness endpoint uses the
	// same order of magnitude, and a database that takes longer than this to
	// answer a ping is not healthy whatever it eventually says.
	checkTimeout = 10 * time.Second
)

// SFServerInfo describes a store-and-forward server on the network.
type SFServerInfo struct {
	ServerID        peer.ID  `json:"server_id"`
	Capabilities    []string `json:"capabilities"`
	MaxStorage      string   `json:"max_storage"`
	RetentionPolicy string   `json:"retention_policy"`
	// Region is where this server runs (server_region); Regions is what it
	// serves (supported_regions).
	Region  string   `json:"region,omitempty"`
	Regions []string `json:"regions"`
	// UptimeScore is the fraction of the server's recent self-checks that
	// passed, in [0, 1]. It is measured, not asserted: a server that cannot
	// reach its database reports the failure here and in Healthy.
	UptimeScore float64 `json:"uptime_score"`
	// Healthy is the verdict of the server's latest self-check. A server
	// whose last word was unhealthy is not listed as available, however
	// fresh its entry.
	Healthy   bool      `json:"healthy"`
	Timestamp time.Time `json:"timestamp"`
	Port      int       `json:"port"`
}

// Heartbeat is what a server publishes on HeartbeatTopic after each
// self-check. It refreshes the entry an announcement created and carries
// nothing an announcement did not, so a heartbeat from a server that never
// announced is ignored rather than stored half-described.
type Heartbeat struct {
	ServerID    peer.ID   `json:"server_id"`
	Healthy     bool      `json:"healthy"`
	UptimeScore float64   `json:"uptime_score"`
	Timestamp   time.Time `json:"timestamp"`
}

// HealthCheck is a server's own readiness verdict. It returns nil when the
// server can serve a request it would otherwise accept.
type HealthCheck func(ctx context.Context) error

// Registry provides service discovery for S&F servers using GossipSub.
type Registry struct {
	node      *node.Node
	config    *core.ServerConfig
	ownPeerID peer.ID
	servers   map[string]*SFServerInfo
	mu        sync.RWMutex
	logger    *slog.Logger
	cancel    context.CancelFunc

	// rejected counts announcements and heartbeats refused because they
	// were malformed or named a server other than their signer.
	rejected atomic.Int64

	// check is the self-check run every HealthCheckInterval. Nil means the
	// server has nothing to verify, and every check passes.
	check HealthCheck

	// results is the ring of recent self-check verdicts the uptime score is
	// taken over; healthy is the latest one. Both are guarded by healthMu.
	healthMu sync.Mutex
	results  []bool
	next     int
	healthy  bool
	checks   int64
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

// WithHealthCheck sets the self-check that paces heartbeats and feeds the
// uptime score. It must be called before Start.
func (r *Registry) WithHealthCheck(check HealthCheck) *Registry {
	r.check = check
	return r
}

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

	// The first announcement carries a measured score, not a guess: check
	// once before anything is published.
	r.runCheck(ctx)

	r.logger.Info("registry started",
		"announce_topic", AnnounceTopic,
		"heartbeat_topic", HeartbeatTopic,
		"health_check_interval", r.config.HealthCheckInterval,
		"healthy", r.Healthy(),
	)

	// Start periodic announcements and heartbeats.
	go r.announceLoop(ctx)
	go r.heartbeatLoop(ctx)

	// Listen for other servers.
	go r.subscriptionLoop(ctx, AnnounceTopic, r.handleAnnouncement)
	go r.subscriptionLoop(ctx, HeartbeatTopic, r.handleHeartbeat)

	return nil
}

// runCheck runs the self-check once, records the verdict, and returns it.
func (r *Registry) runCheck(ctx context.Context) bool {
	healthy := true
	if r.check != nil {
		ctx, cancel := context.WithTimeout(ctx, checkTimeout)
		err := r.check(ctx)
		cancel()
		if err != nil {
			healthy = false
			r.logger.Warn("self-check failed", "error", err)
		}
	}

	r.healthMu.Lock()
	if len(r.results) < uptimeSamples {
		r.results = append(r.results, healthy)
	} else {
		r.results[r.next] = healthy
		r.next = (r.next + 1) % uptimeSamples
	}
	r.healthy = healthy
	r.checks++
	r.healthMu.Unlock()

	return healthy
}

// Healthy reports the verdict of the latest self-check. Before the first
// check it is true: nothing has failed yet.
func (r *Registry) Healthy() bool {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	return r.checks == 0 || r.healthy
}

// UptimeScore is the fraction of the last uptimeSamples self-checks that
// passed. With no checks recorded it is 1.0, for the same reason.
func (r *Registry) UptimeScore() float64 {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	if len(r.results) == 0 {
		return 1.0
	}
	passed := 0
	for _, ok := range r.results {
		if ok {
			passed++
		}
	}
	return float64(passed) / float64(len(r.results))
}

// Stop cancels the registry's background goroutines.
func (r *Registry) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.logger.Info("registry stopped")
	return nil
}

// GetAvailableServers returns every server whose entry is fresh and whose
// last self-check passed, sorted by uptime score in descending order.
func (r *Registry) GetAvailableServers() []*SFServerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	var result []*SFServerInfo

	for _, info := range r.servers {
		if info.Healthy && now.Sub(info.Timestamp) <= staleThreshold {
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
		if info.Healthy && now.Sub(info.Timestamp) <= staleThreshold {
			available++
		}
	}

	r.healthMu.Lock()
	checks := r.checks
	r.healthMu.Unlock()

	return map[string]any{
		"total_known":     len(r.servers),
		"available":       available,
		"rejected":        r.rejected.Load(),
		"own_peer_id":     r.ownPeerID.String(),
		"announce_topic":  AnnounceTopic,
		"heartbeat_topic": HeartbeatTopic,
		"healthy":         r.Healthy(),
		"uptime_score":    r.UptimeScore(),
		"self_checks":     checks,
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

// ownInfo is what this server announces about itself right now.
func (r *Registry) ownInfo() *SFServerInfo {
	return &SFServerInfo{
		ServerID:        r.ownPeerID,
		Capabilities:    []string{"store-and-forward", "mailbox", "document-sync"},
		MaxStorage:      r.config.MaxStorageHuman(),
		RetentionPolicy: r.config.RetentionPolicy.String(),
		Region:          r.config.ServerRegion,
		Regions:         r.config.SupportedRegions,
		UptimeScore:     r.UptimeScore(),
		Healthy:         r.Healthy(),
		Timestamp:       time.Now(),
		Port:            r.config.Port,
	}
}

// announceServer publishes a JSON-encoded SFServerInfo to the announce topic.
func (r *Registry) announceServer(ctx context.Context) {
	data, err := json.Marshal(r.ownInfo())
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

// heartbeatLoop runs the self-check every HealthCheckInterval and publishes
// the verdict. The first check already ran in Start.
func (r *Registry) heartbeatLoop(ctx context.Context) {
	r.publishHeartbeat(ctx)

	ticker := time.NewTicker(r.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runCheck(ctx)
			r.publishHeartbeat(ctx)
		}
	}
}

// publishHeartbeat publishes the latest self-check verdict to the heartbeat
// topic.
func (r *Registry) publishHeartbeat(ctx context.Context) {
	hb := &Heartbeat{
		ServerID:    r.ownPeerID,
		Healthy:     r.Healthy(),
		UptimeScore: r.UptimeScore(),
		Timestamp:   time.Now(),
	}

	data, err := json.Marshal(hb)
	if err != nil {
		r.logger.Error("failed to marshal heartbeat", "error", err)
		return
	}

	if err := r.node.Publish(ctx, HeartbeatTopic, data); err != nil {
		r.logger.Warn("failed to publish heartbeat", "error", err)
		return
	}

	r.logger.Debug("published heartbeat", "healthy", hb.Healthy, "uptime_score", hb.UptimeScore)
}

// subscriptionLoop reads messages from topic and hands each to handle,
// counting the ones it refuses.
func (r *Registry) subscriptionLoop(ctx context.Context, topic string, handle func(peer.ID, []byte) error) {
	sub := r.node.Subscribe(topic)
	if sub == nil {
		r.logger.Error("no subscription for topic", "topic", topic)
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
		if err := handle(from, msg.Data); err != nil {
			r.rejected.Add(1)
			r.logger.Warn("rejected message", "topic", topic, "from", from, "error", err)
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

	r.logger.Debug("discovered server",
		"peer_id", info.ServerID,
		"regions", info.Regions,
		"uptime_score", info.UptimeScore,
		"healthy", info.Healthy,
	)

	r.mu.Lock()
	r.servers[from.String()] = &info
	r.mu.Unlock()
	return nil
}

// handleHeartbeat refreshes the entry of the server that signed it, under
// the same rule as announcements: a heartbeat naming another server is
// refused. One from a server this registry has not seen announce is dropped
// without error; the announcement will follow within its interval.
func (r *Registry) handleHeartbeat(from peer.ID, data []byte) error {
	var hb Heartbeat
	if err := json.Unmarshal(data, &hb); err != nil {
		return fmt.Errorf("decode heartbeat: %w", err)
	}
	if hb.ServerID != from {
		return fmt.Errorf("heartbeat for %s signed by %s", hb.ServerID, from)
	}

	// Entries are replaced, never edited: GetAvailableServers hands out
	// pointers to them, and a caller may still be reading one.
	r.mu.Lock()
	defer r.mu.Unlock()
	current, known := r.servers[from.String()]
	if !known {
		r.logger.Debug("heartbeat from a server that has not announced", "peer_id", from)
		return nil
	}
	updated := *current
	updated.Healthy = hb.Healthy
	updated.UptimeScore = hb.UptimeScore
	updated.Timestamp = hb.Timestamp
	r.servers[from.String()] = &updated
	return nil
}
