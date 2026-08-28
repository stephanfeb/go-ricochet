package admission_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/core"
)

func testPeerID(t *testing.T) peer.ID {
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

func cfg(maxInFlight, perPeer int, timeout time.Duration) core.AdmissionControl {
	return core.AdmissionControl{
		Enabled:            true,
		MaxInFlight:        maxInFlight,
		MaxInFlightPerPeer: perPeer,
		AcquireTimeout:     timeout,
	}
}

// TestThroughputIsNotCapped is the property this mechanism exists for. A rate
// limiter of n per minute permits n requests per minute no matter how fast
// they complete. A concurrency bound permits as many as the work allows, so
// with fast handlers the same bound yields far more than its own size.
func TestThroughputIsNotCapped(t *testing.T) {
	c := admission.New(cfg(4, 0, time.Second), 0)
	pid := testPeerID(t)
	ctx := context.Background()

	const requests = 2000
	var wg sync.WaitGroup
	var completed atomic.Int64

	start := time.Now()
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := c.Acquire(ctx, pid)
			if err != nil {
				return
			}
			completed.Add(1)
			release()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if got := completed.Load(); got != requests {
		t.Errorf("completed %d of %d requests; a concurrency bound should admit them all, just not at once", got, requests)
	}
	// The bound is 4. Had it behaved like a rate limit of 4 per anything,
	// this could not have finished promptly.
	if elapsed > 5*time.Second {
		t.Errorf("2000 requests through a bound of 4 took %s — throughput is being capped, not paced", elapsed)
	}
}

func TestGlobalBoundIsRespected(t *testing.T) {
	const bound = 5
	c := admission.New(cfg(bound, 0, 50*time.Millisecond), 0)
	ctx := context.Background()

	var live atomic.Int64
	var peak atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := c.Acquire(ctx, testPeerID(t))
			if err != nil {
				return
			}
			defer release()
			n := live.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			live.Add(-1)
		}()
	}
	wg.Wait()

	if got := peak.Load(); got > bound {
		t.Errorf("peak concurrency was %d, bound is %d", got, bound)
	}
}

func TestPerPeerBoundStopsMonopolisation(t *testing.T) {
	// The point of the per-peer bound: it is what makes it safe to run with
	// no per-peer rate limit. One peer must not be able to hold every slot.
	const global, perPeer = 20, 3
	c := admission.New(cfg(global, perPeer, 30*time.Millisecond), 0)
	hog := testPeerID(t)
	ctx := context.Background()

	var held []func()
	for i := 0; i < perPeer; i++ {
		release, err := c.Acquire(ctx, hog)
		if err != nil {
			t.Fatalf("hog request %d should be admitted: %v", i, err)
		}
		held = append(held, release)
	}

	// The hog is at its own limit even though 17 global slots are free.
	if _, err := c.Acquire(ctx, hog); !errors.Is(err, admission.ErrOverloaded) {
		t.Errorf("expected the hog to be shed at its per-peer bound, got %v", err)
	}

	// Another peer is unaffected.
	release, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Errorf("a different peer should still be admitted: %v", err)
	} else {
		release()
	}

	for _, r := range held {
		r()
	}

	// Once the hog releases, it is admitted again.
	if release, err := c.Acquire(ctx, hog); err != nil {
		t.Errorf("the hog should be admitted again after releasing: %v", err)
	} else {
		release()
	}
}

func TestShedsWhenSaturated(t *testing.T) {
	c := admission.New(cfg(1, 0, 20*time.Millisecond), 0)
	ctx := context.Background()

	release, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}

	_, err = c.Acquire(ctx, testPeerID(t))
	if !errors.Is(err, admission.ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded once saturated, got %v", err)
	}
	if c.Shed() != 1 {
		t.Errorf("shed counter is %d, want 1 — this is the signal that capacity is short", c.Shed())
	}

	release()
	if release, err := c.Acquire(ctx, testPeerID(t)); err != nil {
		t.Errorf("a slot freed up, so the next request should be admitted: %v", err)
	} else {
		release()
	}
}

func TestWaitsRatherThanSheddingWhenCapacityFreesUp(t *testing.T) {
	// Blocking is the point: a client that outruns the database is paced by
	// it, not rejected by it.
	c := admission.New(cfg(1, 0, 2*time.Second), 0)
	ctx := context.Background()

	release, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		release()
	}()

	start := time.Now()
	second, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("the second request should have waited and then been admitted: %v", err)
	}
	second()

	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("returned after %s without waiting for the slot", elapsed)
	}
	if c.Shed() != 0 {
		t.Errorf("nothing should have been shed, but the counter is %d", c.Shed())
	}
}

func TestContextCancellationIsNotCountedAsShedding(t *testing.T) {
	// A client that walks away is not evidence the server is short of
	// capacity, so it must not pollute the signal.
	c := admission.New(cfg(1, 0, time.Minute), 0)
	release, err := c.Acquire(context.Background(), testPeerID(t))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if _, err := c.Acquire(ctx, testPeerID(t)); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if c.Shed() != 0 {
		t.Errorf("a cancelled client should not count as shedding, but the counter is %d", c.Shed())
	}
}

func TestPeerStateIsReleasedWhenIdle(t *testing.T) {
	// Per-peer state exists only while a peer has work in flight, which is
	// why this needs no eviction goroutine. A leak here would be the same
	// unbounded map the old rate limiter had.
	c := admission.New(cfg(100, 4, time.Second), 0)
	ctx := context.Background()

	for i := 0; i < 500; i++ {
		release, err := c.Acquire(ctx, testPeerID(t))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		release()
	}

	if got := c.InFlight(); got != 0 {
		t.Errorf("in-flight count is %d after every request released, want 0", got)
	}
	if got := c.TrackedPeers(); got != 0 {
		t.Errorf("%d peers still tracked after all released, want 0", got)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	c := admission.New(cfg(2, 0, 50*time.Millisecond), 0)
	ctx := context.Background()

	release, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	release()

	if got := c.InFlight(); got != 0 {
		t.Errorf("in-flight count is %d after a double release, want 0", got)
	}
	// Capacity must not have been inflated by the second release.
	a, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	b, err := c.Acquire(ctx, testPeerID(t))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := c.Acquire(ctx, testPeerID(t)); !errors.Is(err, admission.ErrOverloaded) {
		t.Error("a double release inflated the bound")
	}
	a()
	b()
}

func TestDisabledAdmitsEverything(t *testing.T) {
	c := admission.New(core.AdmissionControl{Enabled: false}, 25)
	if c != nil {
		t.Fatal("a disabled controller should be nil")
	}
	// A nil controller must be usable without a guard at every call site.
	release, err := c.Acquire(context.Background(), testPeerID(t))
	if err != nil {
		t.Errorf("a nil controller should admit everything, got %v", err)
	}
	release()
	if c.InFlight() != 0 || c.Shed() != 0 {
		t.Error("a nil controller should report no activity")
	}
}

func TestMaxInFlightDerivesFromThePool(t *testing.T) {
	// The bound tracks the resource it protects rather than a guessed number.
	ac := core.DefaultAdmissionControl()
	small := ac.EffectiveMaxInFlight(25)
	large := ac.EffectiveMaxInFlight(50)

	if small <= 0 || large <= small {
		t.Errorf("a larger pool should derive a larger bound: pool 25 gave %d, pool 50 gave %d", small, large)
	}
	ac.MaxInFlight = 7
	if got := ac.EffectiveMaxInFlight(50); got != 7 {
		t.Errorf("an explicit bound should win, got %d", got)
	}
}
