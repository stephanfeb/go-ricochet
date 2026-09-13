// Package trust holds the server's peer allow-list and the two things built
// on it: a connection gater for enable_authentication, and the forwarding
// check for enable_forwarding.
//
// The list is one list on purpose. "Peers this server trusts" means the same
// thing whichever feature consults it: with authentication on they are the
// only peers that may connect at all; with it off anyone may connect, and
// they are the only peers that may submit mail on someone else's behalf.
package trust

import (
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	forge "github.com/twostack/go-p2p-forge"
)

// RegistryKey is the forge registry key under which the Peers are provided.
const RegistryKey = "trust"

// Peers is an immutable set of trusted peer identities.
type Peers struct {
	set map[peer.ID]struct{}
}

// Parse builds the set from encoded peer IDs. A malformed entry is an error
// rather than a skipped line: an operator who mistyped the one peer they
// meant to admit should find out at startup, not from a client that cannot
// connect.
func Parse(ids []string) (*Peers, error) {
	p := &Peers{set: make(map[peer.ID]struct{}, len(ids))}
	for _, s := range ids {
		id, err := peer.Decode(s)
		if err != nil {
			return nil, fmt.Errorf("trusted_peers entry %q: %w", s, err)
		}
		p.set[id] = struct{}{}
	}
	return p, nil
}

// Contains reports whether id is trusted. A nil set trusts nobody.
func (p *Peers) Contains(id peer.ID) bool {
	if p == nil {
		return false
	}
	_, ok := p.set[id]
	return ok
}

// Len reports how many peers are trusted.
func (p *Peers) Len() int {
	if p == nil {
		return 0
	}
	return len(p.set)
}

// FromRegistry retrieves the configured set, or nil when none was provided.
func FromRegistry(reg *forge.Registry) *Peers {
	if reg == nil {
		return nil
	}
	p, _ := forge.Service[*Peers](reg, RegistryKey)
	return p
}

// Gater refuses inbound connections from peers outside the trusted set.
//
// It decides at InterceptSecured, the first point at which the remote
// identity is authenticated by the Noise handshake, so an untrusted peer
// costs one handshake and never reaches the multiplexer. Outbound
// connections are not gated: the server dials on its own initiative — DHT,
// relay, push notifications — and a peer it chose to dial is not the
// exposure this guards against.
type Gater struct {
	peers  *Peers
	logger *slog.Logger
}

var _ connmgr.ConnectionGater = (*Gater)(nil)

// NewGater builds a gater over peers.
func NewGater(peers *Peers, logger *slog.Logger) *Gater {
	return &Gater{peers: peers, logger: logger.With("component", "trust")}
}

func (g *Gater) InterceptPeerDial(peer.ID) bool                      { return true }
func (g *Gater) InterceptAddrDial(peer.ID, multiaddr.Multiaddr) bool { return true }
func (g *Gater) InterceptAccept(network.ConnMultiaddrs) bool         { return true }
func (g *Gater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}

func (g *Gater) InterceptSecured(dir network.Direction, id peer.ID, addrs network.ConnMultiaddrs) bool {
	if dir != network.DirInbound || g.peers.Contains(id) {
		return true
	}
	g.logger.Debug("refused untrusted peer", "peer", id.String(), "addr", addrs.RemoteMultiaddr())
	return false
}
