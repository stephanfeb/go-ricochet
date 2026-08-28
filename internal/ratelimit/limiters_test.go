package ratelimit_test

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/twostack/go-p2p-forge"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/ratelimit"
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

// countAllowed spends against a single-bucket limiter until it refuses.
func countAllowed(limiter interface{ Allow(peer.ID) bool }, pid peer.ID, cap int) int {
	n := 0
	for i := 0; i < cap; i++ {
		if !limiter.Allow(pid) {
			break
		}
		n++
	}
	return n
}

func TestNew_HonoursConfiguredRateAndBurst(t *testing.T) {
	cfg := core.RateLimits{
		Window: time.Minute,
		Protocols: map[string]core.ProtocolLimits{
			core.RateLimitMMA: {Requests: core.Limit{Rate: 4, Burst: 9}},
			core.RateLimitSDA: {
				Read:  core.Limit{Rate: 2, Burst: 5},
				Write: core.Limit{Rate: 1, Burst: 2},
			},
		},
	}
	limiters := ratelimit.New(cfg)
	defer limiters.Close()
	pid := testPeerID(t)

	// Burst, not rate, is what a peer may spend at once.
	if got := countAllowed(limiters.MMA, pid, 100); got != 9 {
		t.Errorf("mma: allowed %d requests, want the configured burst of 9", got)
	}

	writes := 0
	for i := 0; i < 100; i++ {
		if !limiters.SDA.Allow(pid, true) {
			break
		}
		writes++
	}
	if writes != 2 {
		t.Errorf("sda writes: allowed %d, want the configured burst of 2", writes)
	}

	reads := 0
	for i := 0; i < 100; i++ {
		if !limiters.SDA.Allow(pid, false) {
			break
		}
		reads++
	}
	if reads != 5 {
		t.Errorf("sda reads: allowed %d, want the configured burst of 5", reads)
	}
}

func TestNew_UnlistedProtocolUsesItsDefault(t *testing.T) {
	// Configuring one protocol must not disturb another. The default is
	// unlimited, so MMA must stay unlimited here.
	cfg := core.RateLimits{
		Window:    time.Minute,
		Protocols: map[string]core.ProtocolLimits{core.RateLimitSDA: {Write: core.Limit{Rate: 1, Burst: 1}}},
	}
	limiters := ratelimit.New(cfg)
	defer limiters.Close()
	pid := testPeerID(t)

	if got := countAllowed(limiters.MMA, pid, 2000); got != 2000 {
		t.Errorf("mma should be unlimited by default, but stopped after %d", got)
	}
	// The one protocol that was configured must still be enforced.
	if !limiters.SDA.Allow(pid, true) {
		t.Fatal("the first sda write should be allowed")
	}
	if limiters.SDA.Allow(pid, true) {
		t.Error("the second sda write should have exhausted the configured burst of 1")
	}
}

func TestNew_NegativeRateDisablesTheBucket(t *testing.T) {
	// The documented way to turn a limit off entirely.
	cfg := core.RateLimits{
		Window:    time.Minute,
		Protocols: map[string]core.ProtocolLimits{core.RateLimitMMA: {Requests: core.Limit{Rate: -1}}},
	}
	limiters := ratelimit.New(cfg)
	defer limiters.Close()
	pid := testPeerID(t)

	if got := countAllowed(limiters.MMA, pid, 5000); got != 5000 {
		t.Errorf("a negative rate should disable limiting, but stopped after %d", got)
	}
}

func TestNew_WindowScalesTheRefill(t *testing.T) {
	// The same rate over a shorter window refills faster.
	cfg := core.RateLimits{
		Window:    100 * time.Millisecond,
		Protocols: map[string]core.ProtocolLimits{core.RateLimitMMA: {Requests: core.Limit{Rate: 10, Burst: 2}}},
	}
	limiters := ratelimit.New(cfg)
	defer limiters.Close()
	pid := testPeerID(t)

	if got := countAllowed(limiters.MMA, pid, 10); got != 2 {
		t.Fatalf("expected the burst of 2 first, got %d", got)
	}
	time.Sleep(120 * time.Millisecond)
	if !limiters.MMA.Allow(pid) {
		t.Error("a full window later, the bucket should have refilled")
	}
}

func TestFromRegistry_ReturnsTheProvidedLimiters(t *testing.T) {
	limiters := ratelimit.New(core.DefaultRateLimits())
	defer limiters.Close()

	reg := forge.NewRegistry()
	reg.Provide(ratelimit.RegistryKey, limiters)

	if got := ratelimit.FromRegistry(reg); got != limiters {
		t.Error("FromRegistry returned a different instance than was provided")
	}
}

func TestFromRegistry_FallsBackToTheDefaults(t *testing.T) {
	// A wiring mistake must yield a usable limiter rather than a nil one that
	// every call site would have to guard.
	for name, reg := range map[string]*forge.Registry{
		"nil registry": nil,
		"missing key":  forge.NewRegistry(),
	} {
		limiters := ratelimit.FromRegistry(reg)
		if limiters == nil {
			t.Fatalf("%s: got nil limiters", name)
		}
		if limiters.MMA == nil || limiters.SDA == nil {
			t.Errorf("%s: fallback limiters are incomplete", name)
		}
	}
}
