package audit

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	multiaddr "github.com/multiformats/go-multiaddr"

	_ "github.com/stephanfeb/go-libp2p-udx-transport" // registers /udx

	"github.com/stephanfeb/go-ricochet/internal/storage"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestDescribeFindsTheIPAndTransport(t *testing.T) {
	relay := "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"
	for _, c := range []struct{ addr, ip, transport string }{
		{"/ip4/203.0.113.7/udp/55223/udx", "203.0.113.7", "udx"},
		{"/ip6/2001:db8::1/tcp/4001", "2001:db8::1", "tcp"},
		{"/ip4/203.0.113.7/udp/4001/quic-v1", "203.0.113.7", "quic-v1"},
		{"/ip4/203.0.113.7/udp/4001/quic-v1/webtransport", "203.0.113.7", "webtransport"},
		{"/ip4/203.0.113.7/tcp/443/ws", "203.0.113.7", "ws"},
		{"/ip4/203.0.113.7/tcp/443/tls/ws", "203.0.113.7", "wss"},
		{"/ip4/203.0.113.7/tcp/443/wss", "203.0.113.7", "wss"},
		{"/dns4/relay.example.com/tcp/443/ws", "relay.example.com", "ws"},
		{"/ip4/203.0.113.7/udp/4001/webrtc-direct", "203.0.113.7", "webrtc-direct"},
		// A relayed connection: the relay's IP, transport p2p-circuit.
		{"/ip4/198.51.100.2/udp/55223/udx/p2p/" + relay + "/p2p-circuit", "198.51.100.2", TransportCircuit},
		{"/p2p/" + relay + "/p2p-circuit", "", TransportCircuit},
	} {
		addr, err := multiaddr.NewMultiaddr(c.addr)
		if err != nil {
			t.Fatalf("%s: %v", c.addr, err)
		}
		ip, transport := Describe(addr)
		if ip != c.ip || transport != c.transport {
			t.Errorf("Describe(%s) = %q, %q; want %q, %q", c.addr, ip, transport, c.ip, c.transport)
		}
	}
	if ip, transport := Describe(nil); ip != "" || transport != "" {
		t.Errorf("Describe(nil) = %q, %q", ip, transport)
	}
}

// pair is a server host and a client host on a mock network.
func pair(t *testing.T) (mocknet.Mocknet, host.Host, host.Host) {
	t.Helper()
	mn := mocknet.New()
	t.Cleanup(func() { _ = mn.Close() })
	server, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	client, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return mn, server, client
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func rows(t *testing.T, store storage.Storage, id peer.ID) []*storage.PeerConnection {
	t.Helper()
	out, err := store.ListPeerConnections(context.Background(), id.String(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRecorderLogsAConnectionAndItsClose(t *testing.T) {
	mn, server, client := pair(t)
	store := storagetest.New()
	r := New(store, 90, quiet)
	r.Start(server.Network())
	t.Cleanup(r.Stop)

	if _, err := mn.ConnectPeers(client.ID(), server.ID()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the connection row", func() bool { return len(rows(t, store, client.ID())) == 1 })
	row := rows(t, store, client.ID())[0]
	wantIP, _ := Describe(client.Addrs()[0])
	if row.IP == "" || row.IP != wantIP || row.Transport != "tcp" || row.Direction != "inbound" {
		t.Errorf("row = %+v, want ip %s over tcp, inbound", row, wantIP)
	}
	if row.RemoteAddr == "" || row.ConnectedAt.IsZero() || row.DisconnectedAt != nil {
		t.Errorf("row = %+v, want the remote address, a connect time and no disconnect yet", row)
	}

	if err := server.Network().ClosePeer(client.ID()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the disconnect", func() bool { return rows(t, store, client.ID())[0].DisconnectedAt != nil })
	row = rows(t, store, client.ID())[0]
	if row.DisconnectedAt.Before(row.ConnectedAt) {
		t.Errorf("disconnected %v before connecting %v", row.DisconnectedAt, row.ConnectedAt)
	}
}

func TestRecorderLogsConnectionsAlreadyOpenAndClosesThemOnStop(t *testing.T) {
	mn, server, client := pair(t)
	if _, err := mn.ConnectPeers(server.ID(), client.ID()); err != nil {
		t.Fatal(err)
	}
	store := storagetest.New()
	r := New(store, 90, quiet)
	r.Start(server.Network())
	eventually(t, "the open connection's row", func() bool { return len(rows(t, store, client.ID())) == 1 })
	if d := rows(t, store, client.ID())[0].Direction; d != "outbound" {
		t.Errorf("direction = %q, want outbound", d)
	}

	r.Stop()
	r.Stop() // twice is fine
	if row := rows(t, store, client.ID())[0]; row.DisconnectedAt == nil {
		t.Error("a connection open at Stop was left open in the log")
	}
}

func TestRecorderPurgesRowsPastTheRetention(t *testing.T) {
	_, server, _ := pair(t)
	store := storagetest.New()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, at := range []time.Time{now.Add(-31 * 24 * time.Hour), now.Add(-29 * 24 * time.Hour)} {
		if _, err := store.InsertPeerConnection(ctx, &storage.PeerConnection{PeerID: "p", ConnectedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	r := New(store, 30, quiet)
	r.Start(server.Network())
	t.Cleanup(r.Stop)
	eventually(t, "the purge", func() bool {
		left, _ := store.ListPeerConnections(ctx, "p", nil, 10)
		return len(left) == 1
	})
}

func TestRecorderIsOffAtZeroDays(t *testing.T) {
	mn, server, client := pair(t)
	store := storagetest.New()
	r := New(store, 0, quiet)
	if r != nil {
		t.Fatal("a recorder at 0 days")
	}
	r.Start(server.Network()) // a nil recorder does nothing
	if _, err := mn.ConnectPeers(client.ID(), server.ID()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	r.Stop()
	if got := rows(t, store, client.ID()); len(got) != 0 {
		t.Errorf("logged %d rows with the log off", len(got))
	}
}

// fakeConn is enough of a network.Conn for the callbacks.
type fakeConn struct {
	network.Conn
	id string
}

func (c fakeConn) ID() string                           { return c.id }
func (c fakeConn) RemotePeer() peer.ID                  { return "" }
func (c fakeConn) RemoteMultiaddr() multiaddr.Multiaddr { return nil }
func (c fakeConn) Stat() network.ConnStats              { return network.ConnStats{} }
func (c fakeConn) LocalMultiaddr() multiaddr.Multiaddr  { return nil }

// With nothing draining the queue, the callbacks still return at once: the
// overflow is dropped and counted, never waited on.
func TestCallbacksNeverBlockOnAFullQueue(t *testing.T) {
	r := New(storagetest.New(), 90, quiet)
	n := (*notifiee)(r)
	done := make(chan struct{})
	go func() {
		for i := 0; i < queueSize+100; i++ {
			n.Connected(nil, fakeConn{id: "c"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a callback blocked on the full queue")
	}
	if got := r.dropped.Load(); got != 100 {
		t.Errorf("dropped = %d, want 100", got)
	}
}
