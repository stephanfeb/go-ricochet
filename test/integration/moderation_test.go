package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/stephanfeb/go-ricochet/internal/moderation"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

// A banned peer is taken off the server but for one thing: on a real host
// with the moderation gater and RestrictStreams, as the server installs
// them, its connection is closed (what the ban operation does), it may
// connect again but reach only the allowed protocols, and the server will
// not dial it; an unban gives it everything back.
func TestABannedPeerReachesOnlyTheAllowedProtocols(t *testing.T) {
	malloryKey, malloryID := keyAndID(t)
	_, moderatorID := keyAndID(t)
	serverKey, _ := keyAndID(t)
	const allowed, other = protocol.ID("/test/allowed/1.0.0"), protocol.ID("/test/other/1.0.0")

	bans := moderation.NewBans(storagetest.New())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := libp2p.New(
		libp2p.Identity(serverKey),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.ConnectionGater(moderation.NewGater(bans, nil, quiet)),
		libp2p.DisableRelay(),
	)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	echo := func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(s, buf); err == nil {
			_, _ = s.Write(buf)
		}
	}
	server.SetStreamHandler(allowed, echo)
	server.SetStreamHandler(other, echo)
	if err := moderation.RestrictStreams(server, bans, func(p protocol.ID) bool { return p == allowed }, quiet); err != nil {
		t.Fatal(err)
	}
	target := peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mallory := createHost(t, malloryKey)
	t.Cleanup(func() { mallory.Close() })
	reaches := func(p protocol.ID) bool {
		t.Helper()
		s, err := mallory.NewStream(ctx, server.ID(), p)
		if err != nil {
			return false
		}
		defer s.Close()
		if _, err := s.Write([]byte("ping")); err != nil {
			return false
		}
		buf := make([]byte, 4)
		_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(s, buf)
		return err == nil && string(buf) == "ping"
	}

	// apart waits until neither side holds a connection to the other.
	apart := func() {
		t.Helper()
		for i := 0; i < 50; i++ {
			if server.Network().Connectedness(malloryID) != network.Connected &&
				mallory.Network().Connectedness(server.ID()) != network.Connected {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("still connected")
	}

	if err := mallory.Connect(ctx, target); err != nil {
		t.Fatalf("before the ban: %v", err)
	}
	if !reaches(allowed) || !reaches(other) {
		t.Fatal("before the ban, both protocols should answer")
	}

	if _, err := bans.Ban(ctx, malloryID, moderatorID, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := server.Network().ClosePeer(malloryID); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = mallory.Network().ClosePeer(server.ID())
	apart()
	if err := mallory.Connect(ctx, target); err != nil {
		t.Fatalf("a banned peer could not connect to read its notice: %v", err)
	}
	if !reaches(allowed) {
		t.Error("a banned peer cannot reach the allowed protocol")
	}
	if reaches(other) {
		t.Error("a banned peer reached a protocol it should not")
	}
	_ = mallory.Network().ClosePeer(server.ID())
	_ = server.Network().ClosePeer(malloryID)
	apart()
	server.Peerstore().AddAddrs(malloryID, mallory.Addrs(), time.Minute)
	dial, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDial()
	if err := server.Connect(dial, peer.AddrInfo{ID: malloryID, Addrs: mallory.Addrs()}); err == nil {
		t.Error("the server dialled a banned peer")
	}

	if _, err := bans.Unban(ctx, malloryID); err != nil {
		t.Fatal(err)
	}
	if err := mallory.Connect(ctx, target); err != nil {
		t.Fatalf("after the unban: %v", err)
	}
	if !reaches(other) {
		t.Error("after the unban, the other protocol should answer again")
	}
}
