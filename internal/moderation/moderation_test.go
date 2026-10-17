package moderation_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"github.com/stephanfeb/go-ricochet/internal/moderation"
	"github.com/stephanfeb/go-ricochet/internal/protocol/protocoltest"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

type addrs struct{}

func (addrs) LocalMultiaddr() multiaddr.Multiaddr {
	return multiaddr.StringCast("/ip4/127.0.0.1/tcp/1")
}
func (addrs) RemoteMultiaddr() multiaddr.Multiaddr {
	return multiaddr.StringCast("/ip4/127.0.0.1/tcp/2")
}

// refuseAll stands in for the trust gater with nobody trusted.
type refuseAll struct{}

var _ connmgr.ConnectionGater = refuseAll{}

func (refuseAll) InterceptPeerDial(peer.ID) bool                      { return true }
func (refuseAll) InterceptAddrDial(peer.ID, multiaddr.Multiaddr) bool { return true }
func (refuseAll) InterceptAccept(network.ConnMultiaddrs) bool         { return true }
func (refuseAll) InterceptSecured(network.Direction, peer.ID, network.ConnMultiaddrs) bool {
	return false
}
func (refuseAll) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestTheServerNeverDialsABannedPeerButLetsItConnect(t *testing.T) {
	ctx := context.Background()
	bans := moderation.NewBans(storagetest.New())
	mallory, bob, mod := protocoltest.PeerID(t), protocoltest.PeerID(t), protocoltest.PeerID(t)
	if _, err := bans.Ban(ctx, mallory, mod, "spam", time.Now()); err != nil {
		t.Fatal(err)
	}
	g := moderation.NewGater(bans, nil, quiet)

	// It may connect, to read its ban notice; RestrictStreams limits what it reaches.
	if !g.InterceptSecured(network.DirInbound, mallory, addrs{}) {
		t.Error("a banned peer cannot connect to read its notice")
	}
	if g.InterceptSecured(network.DirOutbound, mallory, addrs{}) {
		t.Error("an outbound connection to a banned peer was allowed")
	}
	if g.InterceptPeerDial(mallory) || g.InterceptAddrDial(mallory, addrs{}.RemoteMultiaddr()) {
		t.Error("the server would dial a banned peer")
	}
	if !g.InterceptPeerDial(bob) || !g.InterceptSecured(network.DirOutbound, bob, addrs{}) {
		t.Error("the server would not dial another peer")
	}

	if _, err := bans.Unban(ctx, mallory); err != nil {
		t.Fatal(err)
	}
	if !g.InterceptPeerDial(mallory) {
		t.Error("an unbanned peer is still not dialled")
	}
}

func TestTheTrustGaterStillDecidesForPeersNotBanned(t *testing.T) {
	g := moderation.NewGater(moderation.NewBans(storagetest.New()), refuseAll{}, quiet)
	if g.InterceptSecured(network.DirInbound, protocoltest.PeerID(t), addrs{}) {
		t.Error("the trust gater's refusal was overridden")
	}
}

func TestBansOutlastARestart(t *testing.T) {
	ctx := context.Background()
	store := storagetest.New()
	mallory, mod := protocoltest.PeerID(t), protocoltest.PeerID(t)
	if _, err := moderation.NewBans(store).Ban(ctx, mallory, mod, "", time.Now()); err != nil {
		t.Fatal(err)
	}

	again := moderation.NewBans(store)
	if again.IsBanned(mallory) {
		t.Fatal("banned before loading")
	}
	if _, err := again.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if !again.IsBanned(mallory) || again.Get(mallory).BannedBy != mod.String() {
		t.Errorf("after a restart: banned=%v ban=%+v", again.IsBanned(mallory), again.Get(mallory))
	}
	var none *moderation.Bans
	if none.IsBanned(mallory) {
		t.Error("a nil set banned someone")
	}
}

func TestModeratorsRefuseAMalformedPeerID(t *testing.T) {
	if _, err := moderation.ParseModerators([]string{"not-a-peer"}); err == nil {
		t.Error("a malformed moderator was accepted")
	}
}
