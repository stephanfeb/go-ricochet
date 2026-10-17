// Package moderation holds what a moderator's decisions change on this
// server: the peers banned, and the connection gater that refuses them.
//
// The moderators themselves are named in config (security.moderators) and
// act through the moderation protocol (internal/protocol/moa). A ban is
// kept in storage, so it outlasts a restart, and here in memory, where the
// gater and the read checks consult it on every connection and read.
//
// A ban is meant to take someone off this server, not to cap abuse:
// identities cost nothing to make (see README "Rate limiting"). A banned
// peer may connect, but only to read its own trust-safety folder, where
// Trust & Safety's ban notice is; everything else refuses it with a
// "banned" answer (wire.BanGate), so it knows why it is cut off. Its
// directory listing is removed and its documents, feeds and collections
// are hidden from everyone else. Nothing is deleted, so an unban puts it
// all back.
package moderation

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	multiaddr "github.com/multiformats/go-multiaddr"

	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// RegistryKey is the forge registry key under which the Bans are provided.
const RegistryKey = "bans"

// ModeratorsKey is the forge registry key of the Moderators.
const ModeratorsKey = "moderators"

// Moderators is the immutable set of peers that may use the moderation
// protocol.
type Moderators struct {
	set map[peer.ID]struct{}
}

// ParseModerators builds the set from encoded peer IDs; a malformed entry is
// an error, found at startup.
func ParseModerators(ids []string) (*Moderators, error) {
	m := &Moderators{set: make(map[peer.ID]struct{}, len(ids))}
	for _, s := range ids {
		id, err := peer.Decode(s)
		if err != nil {
			return nil, fmt.Errorf("moderators entry %q: %w", s, err)
		}
		m.set[id] = struct{}{}
	}
	return m, nil
}

// Contains reports whether id may moderate. A nil set holds nobody.
func (m *Moderators) Contains(id peer.ID) bool {
	if m == nil {
		return false
	}
	_, ok := m.set[id]
	return ok
}

// Len reports how many moderators there are.
func (m *Moderators) Len() int {
	if m == nil {
		return 0
	}
	return len(m.set)
}

// Bans is the set of banned peers, safe for concurrent use. Changes go to
// storage first, so the memory copy never holds a ban that was not kept.
type Bans struct {
	store storage.Storage

	mu  sync.RWMutex
	set map[peer.ID]*storage.PeerBan
}

// NewBans returns an empty set over store.
func NewBans(store storage.Storage) *Bans {
	return &Bans{store: store, set: map[peer.ID]*storage.PeerBan{}}
}

// Load reads the bans kept in storage, replacing those in memory. A stored
// peer ID that no longer parses is skipped and reported.
func (b *Bans) Load(ctx context.Context) (skipped []string, err error) {
	rows, err := b.store.ListPeerBans(ctx)
	if err != nil {
		return nil, err
	}
	set := make(map[peer.ID]*storage.PeerBan, len(rows))
	for _, r := range rows {
		id, err := peer.Decode(r.PeerID)
		if err != nil {
			skipped = append(skipped, r.PeerID)
			continue
		}
		set[id] = r
	}
	b.mu.Lock()
	b.set = set
	b.mu.Unlock()
	return skipped, nil
}

// IsBanned reports whether id is banned. A nil set bans nobody.
func (b *Bans) IsBanned(id peer.ID) bool {
	if b == nil {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.set[id]
	return ok
}

// IsBannedString is IsBanned for an encoded peer ID; one that does not
// parse is not banned.
func (b *Bans) IsBannedString(s string) bool {
	if b == nil {
		return false
	}
	id, err := peer.Decode(s)
	return err == nil && b.IsBanned(id)
}

// Get returns id's ban, or nil.
func (b *Bans) Get(id peer.ID) *storage.PeerBan {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if r, ok := b.set[id]; ok {
		c := *r
		return &c
	}
	return nil
}

// List returns every ban, oldest first.
func (b *Bans) List() []*storage.PeerBan {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*storage.PeerBan, 0, len(b.set))
	for _, r := range b.set {
		c := *r
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BannedAt.Before(out[j].BannedAt) })
	return out
}

// Ban bans id, keeping it in storage first. Banning a peer already banned
// updates the reason. Closing its open connections is the caller's part.
func (b *Bans) Ban(ctx context.Context, id, by peer.ID, reason string, at time.Time) (*storage.PeerBan, error) {
	ban := &storage.PeerBan{PeerID: id.String(), Reason: reason, BannedBy: by.String(), BannedAt: at.UTC()}
	if err := b.store.PutPeerBan(ctx, ban); err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.set[id] = ban
	b.mu.Unlock()
	c := *ban
	return &c, nil
}

// Unban lifts id's ban; false when it was not banned.
func (b *Bans) Unban(ctx context.Context, id peer.ID) (bool, error) {
	found, err := b.store.DeletePeerBan(ctx, id.String())
	if err != nil {
		return false, err
	}
	b.mu.Lock()
	_, inMemory := b.set[id]
	delete(b.set, id)
	b.mu.Unlock()
	return found || inMemory, nil
}

// FromRegistry retrieves the bans, or nil when none were provided.
func FromRegistry(reg *forge.Registry) *Bans {
	if reg == nil {
		return nil
	}
	b, _ := forge.Service[*Bans](reg, RegistryKey)
	return b
}

// FromStream retrieves the bans for a stream's handler, or nil.
func FromStream(sc *forge.StreamContext) *Bans {
	b, _ := forge.ServiceFrom[*Bans](sc, RegistryKey)
	return b
}

// ModeratorsFromStream retrieves the moderators for a stream's handler, or
// nil.
func ModeratorsFromStream(sc *forge.StreamContext) *Moderators {
	m, _ := forge.ServiceFrom[*Moderators](sc, ModeratorsKey)
	return m
}

// Gater keeps the server from dialling banned peers, then defers to inner
// (the trust gater under enable_authentication) when there is one.
//
// A banned peer may still connect: RestrictStreams lets it reach only the
// Ricochet protocols, which refuse it (wire.BanGate) except for reading its
// own trust-safety folder, where its ban notice is. That is how it learns it
// is banned, and why. The server never dials it, so the relay and the push
// notifier do not reach it on their own initiative.
type Gater struct {
	bans   *Bans
	inner  connmgr.ConnectionGater
	logger *slog.Logger
}

var _ connmgr.ConnectionGater = (*Gater)(nil)

// NewGater builds a gater over bans; inner may be nil.
func NewGater(bans *Bans, inner connmgr.ConnectionGater, logger *slog.Logger) *Gater {
	return &Gater{bans: bans, inner: inner, logger: logger.With("component", "moderation")}
}

func (g *Gater) InterceptPeerDial(id peer.ID) bool {
	if g.bans.IsBanned(id) {
		return false
	}
	return g.inner == nil || g.inner.InterceptPeerDial(id)
}

func (g *Gater) InterceptAddrDial(id peer.ID, addr multiaddr.Multiaddr) bool {
	if g.bans.IsBanned(id) {
		return false
	}
	return g.inner == nil || g.inner.InterceptAddrDial(id, addr)
}

func (g *Gater) InterceptAccept(addrs network.ConnMultiaddrs) bool {
	return g.inner == nil || g.inner.InterceptAccept(addrs)
}

func (g *Gater) InterceptSecured(dir network.Direction, id peer.ID, addrs network.ConnMultiaddrs) bool {
	if dir == network.DirOutbound && g.bans.IsBanned(id) {
		return false
	}
	return g.inner == nil || g.inner.InterceptSecured(dir, id, addrs)
}

// streamHandlerSource is the swarm's view of the handler it gives each new
// stream (basichost's protocol negotiation).
type streamHandlerSource interface {
	StreamHandler() network.StreamHandler
}

// RestrictStreams makes h's banned peers reach only the protocols allowed
// says yes to; any other stream of theirs is reset once its protocol is
// known. Others' streams go to h's own handling as before.
func RestrictStreams(h host.Host, bans *Bans, allowed func(protocol.ID) bool, logger *slog.Logger) error {
	src, ok := h.Network().(streamHandlerSource)
	if !ok {
		return fmt.Errorf("the host's network does not expose its stream handler")
	}
	normal := src.StreamHandler()
	if normal == nil {
		return fmt.Errorf("the host has no stream handler yet")
	}
	log := logger.With("component", "moderation")
	h.Network().SetStreamHandler(func(s network.Stream) {
		remote := s.Conn().RemotePeer()
		if !bans.IsBanned(remote) {
			normal(s)
			return
		}
		_ = s.SetDeadline(time.Now().Add(10 * time.Second))
		proto, handle, err := h.Mux().Negotiate(s)
		if err != nil {
			_ = s.Reset()
			return
		}
		if !allowed(protocol.ID(proto)) {
			log.Debug("banned peer's stream refused", "peer", remote.String(), "protocol", proto)
			_ = s.Reset()
			return
		}
		_ = s.SetDeadline(time.Time{})
		if err := s.SetProtocol(protocol.ID(proto)); err != nil {
			_ = s.Reset()
			return
		}
		handle(proto, s)
	})
	return nil
}

func (g *Gater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	if g.inner == nil {
		return true, 0
	}
	return g.inner.InterceptUpgraded(conn)
}
