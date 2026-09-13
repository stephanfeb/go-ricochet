package registry

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/twostack/go-p2p-forge/node"

	"github.com/twostack/go-ricochet/internal/core"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func randomPeer(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func announcement(t *testing.T, serverID peer.ID, region string) []byte {
	t.Helper()
	data, err := json.Marshal(&SFServerInfo{
		ServerID: serverID, Region: region, Regions: []string{region},
		UptimeScore: 1.0, Healthy: true, Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// An announcement is stored under its signer, and one that names a
// different server is refused so it cannot overwrite that server's entry.
func TestAnnouncementMustBeSignedByItsServer(t *testing.T) {
	observer, honest, forger := randomPeer(t), randomPeer(t), randomPeer(t)
	r := NewRegistry(nil, core.DefaultConfig(), observer, quiet())

	if err := r.handleAnnouncement(honest, announcement(t, honest, "eu")); err != nil {
		t.Fatalf("genuine announcement rejected: %v", err)
	}
	if err := r.handleAnnouncement(forger, announcement(t, honest, "forged")); err == nil {
		t.Fatal("announcement naming another server was accepted")
	}

	servers := r.GetAvailableServers()
	if len(servers) != 1 || servers[0].ServerID != honest || servers[0].Region != "eu" {
		t.Fatalf("servers = %+v, want only the honest entry for %s in eu", servers, honest)
	}
}

func newTestNode(t *testing.T) (host.Host, *node.Node) {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	n, err := node.New(context.Background(), &node.Config{DHTMode: node.DHTModeClient, EnablePubSub: true}, h, quiet())
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return h, n
}

func connect(t *testing.T, a, b host.Host) {
	t.Helper()
	a.Peerstore().AddAddrs(b.ID(), b.Addrs(), peerstore.PermanentAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal(msg)
}

// Through real GossipSub: a forged announcement published by a third node
// under an honest server's ID never displaces that server's own entry.
func TestForgedAnnouncementOverGossipSubIsIgnored(t *testing.T) {
	obsHost, obsNode := newTestNode(t)
	honHost, honNode := newTestNode(t)
	forHost, forNode := newTestNode(t)
	connect(t, obsHost, honHost)
	connect(t, obsHost, forHost)

	cfg := core.DefaultConfig()
	cfg.ServiceAnnouncementInterval = 200 * time.Millisecond
	cfg.ServerRegion = "eu"

	observer := NewRegistry(obsNode, cfg, obsHost.ID(), quiet())
	honest := NewRegistry(honNode, cfg, honHost.ID(), quiet())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := observer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := honest.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := forNode.JoinTopic(AnnounceTopic); err != nil {
		t.Fatal(err)
	}

	// The honest server keeps announcing; once the mesh forms, one lands.
	waitFor(t, 10*time.Second, func() bool {
		for _, s := range observer.GetAvailableServers() {
			if s.ServerID == honHost.ID() && s.Region == "eu" {
				return true
			}
		}
		return false
	}, "observer never saw the honest announcement")

	// The forger claims to be the honest server, with a different region.
	// It publishes until the observer has demonstrably seen and refused
	// one, so mesh timing cannot hide the message and pass the test for
	// the wrong reason.
	waitFor(t, 10*time.Second, func() bool {
		for _, p := range forNode.PubSub().ListPeers(AnnounceTopic) {
			if p == obsHost.ID() {
				return true
			}
		}
		return false
	}, "forger never saw the observer on the topic")

	forged := announcement(t, honHost.ID(), "forged")
	deadline := time.Now().Add(10 * time.Second)
	for observer.RejectedAnnouncements() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("forged announcement never reached the observer")
		}
		if err := forNode.Publish(ctx, AnnounceTopic, forged); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		for _, s := range observer.GetAvailableServers() {
			if s.Region == "forged" || s.ServerID == forHost.ID() {
				t.Fatalf("forged announcement was stored: %+v", s)
			}
		}
	}

	servers := observer.GetAvailableServers()
	if len(servers) != 1 || servers[0].ServerID != honHost.ID() || servers[0].Region != "eu" {
		t.Fatalf("servers = %+v, want only the honest entry in eu", servers)
	}
}

// scriptedCheck fails on the calls whose (1-based) number is listed and
// counts how often it was run.
type scriptedCheck struct {
	mu    sync.Mutex
	calls int
	fail  map[int]bool
	// failing, when set, fails every call regardless of the script.
	failing bool
}

func (c *scriptedCheck) run(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.failing || c.fail[c.calls] {
		return errors.New("postgres: connection refused")
	}
	return nil
}

func (c *scriptedCheck) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *scriptedCheck) setFailing(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failing = v
}

// The uptime score a server announces is the share of its recent self-checks
// that passed, and Healthy is the latest verdict. Before any check both say
// nothing has failed; the window forgets a failure after uptimeSamples
// further checks.
func TestUptimeScoreTracksFailedChecks(t *testing.T) {
	check := &scriptedCheck{fail: map[int]bool{2: true, 4: true}}
	r := NewRegistry(nil, core.DefaultConfig(), randomPeer(t), quiet()).WithHealthCheck(check.run)
	ctx := context.Background()

	if !r.Healthy() || r.UptimeScore() != 1.0 {
		t.Fatalf("before any check: healthy=%v score=%v, want true and 1.0", r.Healthy(), r.UptimeScore())
	}

	for i := 1; i <= 4; i++ {
		r.runCheck(ctx)
	}
	if r.Healthy() {
		t.Fatal("latest check failed but the registry reports healthy")
	}
	if got := r.UptimeScore(); got != 0.5 {
		t.Fatalf("score after 2 of 4 failed = %v, want 0.5", got)
	}

	r.runCheck(ctx)
	info := r.ownInfo()
	if !info.Healthy || info.UptimeScore != 0.6 {
		t.Fatalf("announcement after 3 of 5 passed: healthy=%v score=%v, want true and 0.6", info.Healthy, info.UptimeScore)
	}

	for i := 0; i < uptimeSamples; i++ {
		r.runCheck(ctx)
	}
	if got := r.UptimeScore(); got != 1.0 {
		t.Fatalf("score after %d clean checks = %v, want the failures forgotten", uptimeSamples, got)
	}
	if check.count() != 5+uptimeSamples {
		t.Fatalf("check ran %d times, want %d", check.count(), 5+uptimeSamples)
	}
}

func heartbeat(t *testing.T, serverID peer.ID, healthy bool, score float64, at time.Time) []byte {
	t.Helper()
	data, err := json.Marshal(&Heartbeat{ServerID: serverID, Healthy: healthy, UptimeScore: score, Timestamp: at})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A heartbeat refreshes the entry of the server that signed it and nothing
// else: one naming another server is refused, one from a server that never
// announced is dropped, and an unhealthy one takes the server out of the
// available list until a healthy one follows.
func TestHeartbeatMustBeSignedByItsServer(t *testing.T) {
	observer, honest, forger, stranger := randomPeer(t), randomPeer(t), randomPeer(t), randomPeer(t)
	r := NewRegistry(nil, core.DefaultConfig(), observer, quiet())
	if err := r.handleAnnouncement(honest, announcement(t, honest, "eu")); err != nil {
		t.Fatal(err)
	}
	announced := r.GetAvailableServers()[0].Timestamp

	if err := r.handleHeartbeat(forger, heartbeat(t, honest, false, 0, time.Now())); err == nil {
		t.Fatal("heartbeat naming another server was accepted")
	}
	if err := r.handleHeartbeat(stranger, heartbeat(t, stranger, true, 1, time.Now())); err != nil {
		t.Fatalf("heartbeat from an unannounced server is not an error: %v", err)
	}
	if s := r.GetAvailableServers(); len(s) != 1 || s[0].ServerID != honest || !s[0].Healthy || s[0].Timestamp != announced {
		t.Fatalf("servers after refused heartbeats = %+v, want the honest entry untouched", s)
	}

	if err := r.handleHeartbeat(honest, heartbeat(t, honest, false, 0.75, time.Now())); err != nil {
		t.Fatal(err)
	}
	if s := r.GetAvailableServers(); len(s) != 0 {
		t.Fatalf("a server whose last heartbeat was unhealthy is still available: %+v", s)
	}
	if stats := r.GetStats(); stats["total_known"] != 1 || stats["available"] != 0 {
		t.Fatalf("stats = %v, want 1 known and 0 available", stats)
	}

	recovered := time.Now().Add(time.Minute)
	if err := r.handleHeartbeat(honest, heartbeat(t, honest, true, 0.8, recovered)); err != nil {
		t.Fatal(err)
	}
	s := r.GetAvailableServers()
	if len(s) != 1 || s[0].UptimeScore != 0.8 || !s[0].Timestamp.Equal(recovered) || s[0].Region != "eu" {
		t.Fatalf("servers after recovery = %+v, want the honest entry with score 0.8, the heartbeat's timestamp and its announced region", s)
	}
}

// Through real GossipSub: heartbeats arrive every health_check interval and
// keep an entry fresh without any announcement, and the observer's view
// follows the server's self-check within one interval each way.
func TestHeartbeatsKeepAHealthyServerFresh(t *testing.T) {
	obsHost, obsNode := newTestNode(t)
	honHost, honNode := newTestNode(t)
	connect(t, obsHost, honHost)

	cfg := core.DefaultConfig()
	cfg.ServiceAnnouncementInterval = time.Hour // only the startup announcement, if the mesh is even up
	cfg.HealthCheckInterval = 100 * time.Millisecond
	cfg.ServerRegion = "eu"

	check := &scriptedCheck{}
	observer := NewRegistry(obsNode, cfg, obsHost.ID(), quiet())
	honest := NewRegistry(honNode, cfg, honHost.ID(), quiet()).WithHealthCheck(check.run)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := observer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := honest.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// The observer learns of the server by hand, dated well before any
	// heartbeat, so a refreshed timestamp can only have come from one.
	announced := time.Now().Add(-time.Minute)
	info := &SFServerInfo{ServerID: honHost.ID(), Region: "eu", Regions: []string{"eu"}, UptimeScore: 1, Healthy: true, Timestamp: announced}
	data, _ := json.Marshal(info)
	if err := observer.handleAnnouncement(honHost.ID(), data); err != nil {
		t.Fatal(err)
	}

	entry := func() SFServerInfo {
		observer.mu.RLock()
		defer observer.mu.RUnlock()
		return *observer.servers[honHost.ID().String()]
	}
	waitFor(t, 10*time.Second, func() bool { return entry().Timestamp.After(time.Now().Add(-10 * time.Second)) },
		"heartbeats never refreshed the entry")
	if check.count() < 2 {
		t.Fatalf("self-check ran %d times, want it paced by health_check_interval", check.count())
	}

	check.setFailing(true)
	waitFor(t, 10*time.Second, func() bool { return len(observer.GetAvailableServers()) == 0 },
		"a failing server stayed available")
	if e := entry(); e.Healthy || e.UptimeScore >= 1 {
		t.Fatalf("entry after failed checks = %+v, want unhealthy with a score below 1", e)
	}

	check.setFailing(false)
	waitFor(t, 10*time.Second, func() bool {
		s := observer.GetAvailableServers()
		return len(s) == 1 && s[0].ServerID == honHost.ID() && s[0].Region == "eu"
	}, "a recovered server never came back")
	if e := entry(); e.UptimeScore >= 1 || e.UptimeScore <= 0 {
		t.Fatalf("recovered entry carries score %v, want the failures still counted", e.UptimeScore)
	}
}
