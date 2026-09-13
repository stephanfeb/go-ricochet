package trust

import (
	"io"
	"log/slog"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func testPeer(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type addrs struct{}

func (addrs) LocalMultiaddr() multiaddr.Multiaddr  { return nil }
func (addrs) RemoteMultiaddr() multiaddr.Multiaddr { return nil }

func TestParse_RejectsMalformedEntries(t *testing.T) {
	if _, err := Parse([]string{"not-a-peer-id"}); err == nil {
		t.Fatal("malformed entry accepted")
	}
	p, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 0 || p.Contains(testPeer(t)) {
		t.Fatal("empty set trusts someone")
	}
}

func TestGater_RefusesInboundFromUntrustedOnly(t *testing.T) {
	trusted, stranger := testPeer(t), testPeer(t)
	peers, err := Parse([]string{trusted.String()})
	if err != nil {
		t.Fatal(err)
	}
	g := NewGater(peers, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if !g.InterceptSecured(network.DirInbound, trusted, addrs{}) {
		t.Error("trusted inbound peer refused")
	}
	if g.InterceptSecured(network.DirInbound, stranger, addrs{}) {
		t.Error("untrusted inbound peer admitted")
	}
	if !g.InterceptSecured(network.DirOutbound, stranger, addrs{}) {
		t.Error("outbound connection to an untrusted peer refused; the server chose to dial it")
	}
	if !g.InterceptPeerDial(stranger) || !g.InterceptAccept(addrs{}) {
		t.Error("gater refused before the identity was known")
	}
}

// A nil set, which is what a server without trusted_peers has, refuses every
// inbound peer rather than admitting every one: the gater is only installed
// when authentication is on, and on with nobody listed must fail closed.
func TestGater_NilSetFailsClosed(t *testing.T) {
	g := NewGater(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if g.InterceptSecured(network.DirInbound, testPeer(t), addrs{}) {
		t.Fatal("nil trusted set admitted an inbound peer")
	}
}
