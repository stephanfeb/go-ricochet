// Package moderation holds what a moderator's decisions change on this
// server: the peers banned, and the connection gater that refuses them.
//
// The moderators themselves are named in config (security.moderators) and
// act through the moderation protocol (internal/protocol/moa). A ban is
// kept in storage, so it outlasts a restart, and here in memory, where the
// gater and the read checks consult it on every connection and read.
//
// A ban is meant to take someone off this server, not to cap abuse:
// identities cost nothing to make (see README "Rate limiting"). It refuses
// the peer on every protocol, removes its directory listing, and hides its
// documents, feeds and collections from everyone else. Nothing is deleted,
// so an unban puts it all back.
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
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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

// Gater refuses banned peers on every connection, in and out, then defers
// to inner (the trust gater under enable_authentication) when there is one.
//
// Inbound, it decides at InterceptSecured, the first point the remote
// identity is proven, so a banned peer never reaches the multiplexer and
// so no protocol (DHT, GossipSub, relay, identify) serves it. Outbound, it
// refuses to dial a banned peer at all, so push notifications and the relay
// never reach it either.
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
	if g.bans.IsBanned(id) {
		g.logger.Debug("refused banned peer", "peer", id.String(), "addr", addrs.RemoteMultiaddr(), "direction", dir)
		return false
	}
	return g.inner == nil || g.inner.InterceptSecured(dir, id, addrs)
}

func (g *Gater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	if g.inner == nil {
		return true, 0
	}
	return g.inner.InterceptUpgraded(conn)
}
