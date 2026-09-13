package presence

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// newQuietService starts a non-broadcasting service on h with a short
// timeout and a fast reconcile, so a test can watch the online set move
// without a node or pubsub.
func newQuietService(t *testing.T, h host.Host, cache *Cache) *Service {
	t.Helper()
	cfg := &PresenceConfig{
		HeartbeatInterval: time.Hour,
		TimeoutDuration:   100 * time.Millisecond,
		CheckInterval:     20 * time.Millisecond,
		BatchWindow:       time.Second,
		MaxBatchSize:      50,
		EnableBroadcast:   false,
	}
	svc := NewService(h, nil, cache, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("start service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })
	return svc
}

func isOnline(svc *Service, pid peer.ID) bool {
	for _, online := range svc.GetOnlinePeers() {
		if online == pid {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func connect(t *testing.T, from, to host.Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := from.Connect(ctx, peer.AddrInfo{ID: to.ID(), Addrs: to.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

// A peer is online for as long as it is connected, not for TimeoutDuration
// after it connected.
func TestLongConnectedPeerStaysOnline(t *testing.T) {
	server := newTestHost(t)
	client := newTestHost(t)
	cache := NewCache(time.Hour)
	svc := newQuietService(t, server, cache)

	connect(t, client, server)
	waitFor(t, 2*time.Second, func() bool { return isOnline(svc, client.ID()) },
		"connected peer was never marked online")

	// Stay connected for many timeouts and many reconcile sweeps.
	time.Sleep(5 * svc.config.TimeoutDuration)

	if !isOnline(svc, client.ID()) {
		t.Fatalf("peer still connected after %v was dropped from the online set",
			5*svc.config.TimeoutDuration)
	}
	if status := cache.Get(client.ID()); status == nil || status.State != Online {
		t.Fatalf("cache for a connected peer = %v, want Online", status)
	}

	// Once the connection goes, so does the peer.
	if err := client.Network().ClosePeer(server.ID()); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		status := cache.Get(client.ID())
		return !isOnline(svc, client.ID()) && status != nil && status.State == Offline
	}, "disconnected peer was never marked offline")
}

// The reconcile sweep repairs the online set when a connect or disconnect
// notification was missed.
func TestReconcileRepairsMissedNotifications(t *testing.T) {
	server := newTestHost(t)
	client := newTestHost(t)
	stranger := newTestHost(t) // never connects to server
	cache := NewCache(time.Hour)
	svc := newQuietService(t, server, cache)

	connect(t, client, server)
	waitFor(t, 2*time.Second, func() bool { return isOnline(svc, client.ID()) },
		"connected peer was never marked online")

	// Pretend the notifier lost the client's connect and reported a
	// connection to a peer the network never had.
	svc.mu.Lock()
	delete(svc.connectedPeers, client.ID())
	svc.connectedPeers[stranger.ID()] = time.Now()
	svc.mu.Unlock()

	svc.reconcile()

	if !isOnline(svc, client.ID()) {
		t.Fatal("reconcile did not restore a connected peer to the online set")
	}
	if isOnline(svc, stranger.ID()) {
		t.Fatal("reconcile kept a peer the network is not connected to")
	}
	if status := cache.Get(stranger.ID()); status == nil || status.State != Offline {
		t.Fatalf("cache for the unconnected peer = %v, want Offline", status)
	}
}

func randomPeerIDs(t *testing.T, n int) []peer.ID {
	t.Helper()
	ids := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			t.Fatalf("peer id: %v", err)
		}
		ids = append(ids, pid)
	}
	return ids
}

// The heartbeat is split into pages that each stay well under the pubsub
// message cap, and together carry the whole online set.
func TestHeartbeatPagesBoundMessageSize(t *testing.T) {
	ids := randomPeerIDs(t, 10)
	serverID := ids[0]
	at := time.Now()

	pages := heartbeatPages(serverID, 7, at, ids, 4)
	if len(pages) != 3 {
		t.Fatalf("10 ids at 4 per page = %d pages, want 3", len(pages))
	}
	var joined []peer.ID
	for i, hb := range pages {
		if hb.Page != i || hb.PageCount != 3 {
			t.Fatalf("page %d labelled %d/%d", i, hb.Page, hb.PageCount)
		}
		if hb.HeartbeatSequence != 7 || hb.OnlineCount != 10 || !hb.Timestamp.Equal(at) {
			t.Fatalf("page %d does not carry the sequence header: %+v", i, hb)
		}
		joined = append(joined, hb.OnlinePeerIDs...)
	}
	if len(joined) != len(ids) {
		t.Fatalf("pages carry %d ids, want %d", len(joined), len(ids))
	}
	for i := range ids {
		if joined[i] != ids[i] {
			t.Fatalf("id %d differs across paging", i)
		}
	}

	empty := heartbeatPages(serverID, 8, at, nil, 4)
	if len(empty) != 1 || empty[0].PageCount != 1 || len(empty[0].OnlinePeerIDs) != 0 {
		t.Fatalf("empty online set = %+v, want one empty page", empty)
	}

	// A full page at the production page size must leave the 1 MiB
	// GossipSub cap with room to spare.
	full := heartbeatPages(serverID, 9, at, randomPeerIDs(t, heartbeatPageSize), heartbeatPageSize)
	if len(full) != 1 {
		t.Fatalf("%d ids = %d pages, want 1", heartbeatPageSize, len(full))
	}
	data, err := full[0].Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if limit := 512 * 1024; len(data) > limit {
		t.Fatalf("a full heartbeat page encodes to %d bytes, want at most %d", len(data), limit)
	}
}
