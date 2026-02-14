package presence

import (
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PresenceState represents the online status of a peer.
type PresenceState int

const (
	// Online indicates the peer is currently connected and reachable.
	Online PresenceState = iota

	// ProbablyOnline indicates the peer was recently seen but connectivity
	// has not been confirmed.
	ProbablyOnline

	// Offline indicates the peer could not be reached.
	Offline

	// Unknown indicates no presence information is available.
	Unknown
)

// String returns a human-readable representation of the presence state.
func (s PresenceState) String() string {
	switch s {
	case Online:
		return "online"
	case ProbablyOnline:
		return "probably_online"
	case Offline:
		return "offline"
	case Unknown:
		return "unknown"
	default:
		return fmt.Sprintf("PresenceState(%d)", int(s))
	}
}

// PresenceStatus holds presence information for a single peer.
type PresenceStatus struct {
	PeerID    peer.ID
	State     PresenceState
	LastSeen  time.Time
	CheckedAt time.Time
}

// Cache is a TTL-based presence cache that stores and retrieves
// presence information for peers.
type Cache struct {
	entries map[peer.ID]*PresenceStatus
	ttl     time.Duration
	mu      sync.RWMutex
}

// NewCache creates a new presence cache with the given TTL for entries.
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries: make(map[peer.ID]*PresenceStatus),
		ttl:     ttl,
	}
}

// Get returns the cached presence status for a peer, or nil if the entry
// does not exist or has expired.
func (c *Cache) Get(id peer.ID) *PresenceStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[id]
	if !ok {
		return nil
	}

	if time.Since(entry.CheckedAt) > c.ttl {
		return nil
	}

	return entry
}

// Set stores or updates the presence status for a peer.
func (c *Cache) Set(id peer.ID, status *PresenceStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[id] = status
}

// Cleanup removes all expired entries from the cache.
func (c *Cache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for id, entry := range c.entries {
		if now.Sub(entry.CheckedAt) > c.ttl {
			delete(c.entries, id)
		}
	}
}

// GetStats returns cache statistics.
func (c *Cache) GetStats() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	online := 0
	probablyOnline := 0
	offline := 0
	unknown := 0
	expired := 0
	now := time.Now()

	for _, entry := range c.entries {
		if now.Sub(entry.CheckedAt) > c.ttl {
			expired++
			continue
		}
		switch entry.State {
		case Online:
			online++
		case ProbablyOnline:
			probablyOnline++
		case Offline:
			offline++
		case Unknown:
			unknown++
		}
	}

	return map[string]any{
		"total_entries":    len(c.entries),
		"online":          online,
		"probably_online": probablyOnline,
		"offline":         offline,
		"unknown":         unknown,
		"expired":         expired,
		"ttl":             c.ttl.String(),
	}
}
