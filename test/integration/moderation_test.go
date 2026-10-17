package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/stephanfeb/go-ricochet/internal/moderation"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

// A ban takes a peer off the server at the connection, on a real host: the
// moderation gater the server installs, with no protocol involved. Its open
// connection is closed (what the ban operation does), it cannot connect
// again, and the server will not dial it; an unban lets it back.
func TestABannedPeerIsCutOffAndCannotReconnect(t *testing.T) {
	malloryKey, malloryID := keyAndID(t)
	_, moderatorID := keyAndID(t)
	serverKey, _ := keyAndID(t)

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
	target := peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	settle := func(h host.Host) network.Connectedness {
		time.Sleep(300 * time.Millisecond)
		return h.Network().Connectedness(server.ID())
	}

	mallory := createHost(t, malloryKey)
	t.Cleanup(func() { mallory.Close() })
	if err := mallory.Connect(ctx, target); err != nil || settle(mallory) != network.Connected {
		t.Fatalf("before the ban: %v", err)
	}

	if _, err := bans.Ban(ctx, malloryID, moderatorID, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := server.Network().ClosePeer(malloryID); err != nil {
		t.Fatalf("close: %v", err)
	}
	if settle(mallory) == network.Connected {
		t.Fatal("still connected after the ban")
	}

	retry, cancelRetry := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRetry()
	_ = mallory.Connect(retry, target)
	if settle(mallory) == network.Connected {
		t.Fatal("a banned peer connected again")
	}
	server.Peerstore().AddAddrs(malloryID, mallory.Addrs(), time.Minute)
	if err := server.Connect(retry, peer.AddrInfo{ID: malloryID, Addrs: mallory.Addrs()}); err == nil {
		t.Error("the server dialled a banned peer")
	}

	if _, err := bans.Unban(ctx, malloryID); err != nil {
		t.Fatal(err)
	}
	if err := mallory.Connect(ctx, target); err != nil || settle(mallory) != network.Connected {
		t.Fatalf("after the unban: %v", err)
	}
}
