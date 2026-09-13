package registry

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
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
		UptimeScore: 1.0, Timestamp: time.Now(),
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
