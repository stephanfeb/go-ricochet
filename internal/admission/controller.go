// Package admission bounds how much work the server has in flight at once,
// rather than how many requests a peer may make per unit of time.
//
// The difference matters. A rate limit is a constant somebody guessed: set it
// low and it caps throughput far below what the hardware can do; set it high
// and it stops protecting anything. Either way the number has no relationship
// to what the database can actually commit right now.
//
// A concurrency bound has that relationship built in. Throughput is
// concurrency divided by latency, so holding concurrency fixed lets throughput
// rise as far as the hardware allows and fall on its own when the database
// slows down — no constant to tune, and no ceiling in documents per minute.
// It is also self-weighting: a batch write holds its slot for as long as it
// takes, so expensive work costs more capacity than cheap work without anyone
// having to write down a cost model.
//
// Two bounds are enforced. The global bound protects the server's own
// resources and is derived from the database pool size. The per-peer bound
// keeps one client from occupying every slot, which is what makes it safe to
// run with no per-peer rate limit at all.
package admission

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// ErrOverloaded is returned when a request could not be admitted before the
// acquire timeout. It means the server is saturated, not that the peer did
// anything wrong, so it maps to a 503 rather than a 429.
var ErrOverloaded = errors.New("server at capacity")

// OverloadedError is ErrOverloaded with a retry hint attached. It unwraps to
// the sentinel, so existing errors.Is checks are unaffected.
type OverloadedError struct {
	// RetryAfter is a floor to jitter around, not a schedule.
	//
	// Admission control already paces clients by blocking: a shed request has
	// waited the full acquire timeout, so a retry loop's natural period is
	// that timeout and there is no hot loop to prevent. What the hint is for
	// is desynchronising a fleet, which would otherwise retry in lockstep
	// after the same shed. A large value here would re-create the per-minute
	// ceiling that concurrency bounds exist to remove -- the exact failure the
	// sumi team hit when they were left to guess and settled on one pass per
	// 65 seconds.
	RetryAfter time.Duration

	// Reason says which bound was hit, for the log and the error text.
	Reason string
}

func (e *OverloadedError) Error() string {
	msg := ErrOverloaded.Error()
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	if e.RetryAfter > 0 {
		msg += fmt.Sprintf(", retry after %s", e.RetryAfter)
	}
	return msg
}

func (e *OverloadedError) Unwrap() error { return ErrOverloaded }

// retryHintFraction is how much of the acquire timeout a shed request is asked
// to wait. A quarter is short enough to keep a freed slot from sitting idle
// and long enough that a fleet retrying together spreads out once jittered.
const retryHintFraction = 4

// minRetryHint and maxRetryHint bound the hint however the timeout is
// configured. The floor keeps a very short timeout from producing a hint too
// small to separate anything; the ceiling keeps a long one from becoming the
// pacing constant this design exists to avoid.
const (
	minRetryHint = 50 * time.Millisecond
	maxRetryHint = 1 * time.Second
)

// retryHint derives the wait a shed request is asked to observe.
func (c *Controller) retryHint() time.Duration {
	if c == nil {
		return 0
	}
	hint := c.timeout / retryHintFraction
	if hint < minRetryHint {
		return minRetryHint
	}
	if hint > maxRetryHint {
		return maxRetryHint
	}
	return hint
}

// overloaded builds the shed error.
func (c *Controller) overloaded(reason string) error {
	return &OverloadedError{RetryAfter: c.retryHint(), Reason: reason}
}

const peerShardCount = 64

// peerSlots is one peer's concurrency allowance. refs counts how many requests
// hold or are waiting for a slot, so the entry can be dropped the moment the
// peer goes quiet. That is why there is no eviction goroutine here: state
// exists only while a peer has work in flight.
type peerSlots struct {
	ch   chan struct{}
	refs int
}

type peerShard struct {
	mu    sync.Mutex
	peers map[string]*peerSlots
}

// Controller admits requests up to a global and a per-peer concurrency bound.
// A nil Controller admits everything, so callers need not check for one.
type Controller struct {
	global     chan struct{}
	shards     []peerShard
	maxPerPeer int
	timeout    time.Duration

	inFlight atomic.Int64
	admitted atomic.Uint64
	shed     atomic.Uint64
	waited   atomic.Uint64
}

// New builds a controller from cfg. poolSize is the database connection pool
// size, used to derive the global bound when cfg does not set one explicitly.
//
// Returns nil when admission control is disabled, which callers treat as
// "admit everything".
func New(cfg core.AdmissionControl, poolSize int) *Controller {
	if !cfg.Enabled {
		return nil
	}

	c := &Controller{
		global:     make(chan struct{}, cfg.EffectiveMaxInFlight(poolSize)),
		shards:     make([]peerShard, peerShardCount),
		maxPerPeer: cfg.MaxInFlightPerPeer,
		timeout:    cfg.EffectiveAcquireTimeout(),
	}
	for i := range c.shards {
		c.shards[i].peers = make(map[string]*peerSlots)
	}
	return c
}

// Acquire admits one request, returning the function that releases its slot.
//
// It blocks until capacity is available, the context is cancelled, or the
// acquire timeout expires. Blocking is the point: a client that outruns the
// database is slowed to the database's pace rather than rejected, and only a
// genuinely saturated server sheds. The returned release function is safe to
// call more than once.
func (c *Controller) Acquire(ctx context.Context, peerID peer.ID) (func(), error) {
	if c == nil {
		return func() {}, nil
	}

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()

	// The per-peer bound is taken first. Taking the global slot first would
	// let a peer that is already at its own limit sit on server-wide capacity
	// while it waits.
	releasePeer := func() {}
	if c.maxPerPeer > 0 {
		slots, key := c.reservePeer(peerID)
		select {
		case slots.ch <- struct{}{}:
			releasePeer = func() {
				<-slots.ch
				c.releasePeer(key)
			}
		case <-timer.C:
			c.releasePeer(key)
			c.shed.Add(1)
			return nil, c.overloaded(fmt.Sprintf("peer already has %d requests in flight", c.maxPerPeer))
		case <-ctx.Done():
			c.releasePeer(key)
			return nil, ctx.Err()
		}
	}

	select {
	case c.global <- struct{}{}:
	default:
		// Only count a wait when the slot was not immediately available, so
		// the counter measures saturation rather than traffic.
		c.waited.Add(1)
		select {
		case c.global <- struct{}{}:
		case <-timer.C:
			releasePeer()
			c.shed.Add(1)
			return nil, c.overloaded(fmt.Sprintf("%d requests in flight", cap(c.global)))
		case <-ctx.Done():
			releasePeer()
			return nil, ctx.Err()
		}
	}

	c.inFlight.Add(1)
	c.admitted.Add(1)

	var once sync.Once
	return func() {
		once.Do(func() {
			c.inFlight.Add(-1)
			<-c.global
			releasePeer()
		})
	}, nil
}

// reservePeer returns the peer's slot channel, creating it if needed, and
// records that one more request is interested in it.
func (c *Controller) reservePeer(peerID peer.ID) (*peerSlots, string) {
	key := peerID.String()
	s := &c.shards[fnvHash(key)%peerShardCount]
	s.mu.Lock()
	defer s.mu.Unlock()

	slots, ok := s.peers[key]
	if !ok {
		slots = &peerSlots{ch: make(chan struct{}, c.maxPerPeer)}
		s.peers[key] = slots
	}
	slots.refs++
	return slots, key
}

// releasePeer drops one reference, forgetting the peer entirely once nothing
// holds or awaits a slot.
func (c *Controller) releasePeer(key string) {
	s := &c.shards[fnvHash(key)%peerShardCount]
	s.mu.Lock()
	defer s.mu.Unlock()

	slots, ok := s.peers[key]
	if !ok {
		return
	}
	slots.refs--
	if slots.refs <= 0 {
		delete(s.peers, key)
	}
}

// Stats reports the controller's counters, for logging and metrics.
func (c *Controller) Stats() map[string]any {
	if c == nil {
		return map[string]any{"enabled": false}
	}
	return map[string]any{
		"enabled":        true,
		"maxInFlight":    cap(c.global),
		"maxPerPeer":     c.maxPerPeer,
		"inFlight":       c.inFlight.Load(),
		"admittedTotal":  c.admitted.Load(),
		"shedTotal":      c.shed.Load(),
		"waitedTotal":    c.waited.Load(),
		"acquireTimeout": c.timeout.String(),
	}
}

// InFlight reports how many requests currently hold a slot.
func (c *Controller) InFlight() int {
	if c == nil {
		return 0
	}
	return int(c.inFlight.Load())
}

// TrackedPeers reports how many peers currently hold or await a slot. It
// should return to zero when the server is idle; a value that only grows is a
// leak.
func (c *Controller) TrackedPeers() int {
	if c == nil {
		return 0
	}
	total := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		total += len(s.peers)
		s.mu.Unlock()
	}
	return total
}

// Shed reports how many requests were rejected because the server was
// saturated. A non-zero value is the signal to add capacity — it is the
// measurement a guessed rate limit could never produce.
func (c *Controller) Shed() uint64 {
	if c == nil {
		return 0
	}
	return c.shed.Load()
}

func fnvHash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
