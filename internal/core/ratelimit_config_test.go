package core_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// writeConfig writes yaml to a temp file and returns its path.
func writeConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestRateLimits_DefaultsAreOffButExplicit(t *testing.T) {
	// Per-peer rate limits are off by default: throughput is governed by
	// admission control, which bounds concurrent work instead. Every protocol
	// must still be listed explicitly, so that "no limit here" is a recorded
	// decision rather than an entry somebody forgot to add.
	protocols := []string{
		core.RateLimitMSA, core.RateLimitMSABatch, core.RateLimitMAA,
		core.RateLimitMMA, core.RateLimitSDA, core.RateLimitSFA,
		core.RateLimitSCA, core.RateLimitMTA,
	}
	limits := core.DefaultRateLimits()
	if len(limits.Protocols) != len(protocols) {
		t.Errorf("defaults list %d protocols, expected %d", len(limits.Protocols), len(protocols))
	}
	for _, name := range protocols {
		p, ok := limits.Protocols[name]
		if !ok {
			t.Errorf("protocol %q has no default entry", name)
			continue
		}
		for bucket, limit := range map[string]core.Limit{
			"requests": p.Requests, "read": p.Read, "write": p.Write,
		} {
			if limit.Rate != core.RateUnlimited {
				t.Errorf("protocol %q %s: rate is %d, want RateUnlimited", name, bucket, limit.Rate)
			}
		}
	}
}

func TestRateLimits_ForFallsBackToDefault(t *testing.T) {
	// A config that lists only some protocols must not leave the rest
	// unlimited.
	limits := core.RateLimits{
		Window:    time.Minute,
		Protocols: map[string]core.ProtocolLimits{core.RateLimitSDA: {Write: core.Limit{Rate: 5}}},
	}

	sda := limits.For(core.RateLimitSDA)
	if sda.Write.Rate != 5 {
		t.Errorf("configured limit lost: got write rate %d, want 5", sda.Write.Rate)
	}

	mma := limits.For(core.RateLimitMMA)
	want := core.DefaultRateLimits().Protocols[core.RateLimitMMA]
	if mma != want {
		t.Errorf("unlisted protocol should fall back to its default: got %+v, want %+v", mma, want)
	}
}

func TestRateLimits_EffectiveWindowDefaults(t *testing.T) {
	if got := (core.RateLimits{}).EffectiveWindow(); got != time.Minute {
		t.Errorf("unset window should default to one minute, got %s", got)
	}
	if got := (core.RateLimits{Window: 30 * time.Second}).EffectiveWindow(); got != 30*time.Second {
		t.Errorf("configured window should be honoured, got %s", got)
	}
}

func TestLoadConfig_RateLimitPerProtocolOverride(t *testing.T) {
	path := writeConfig(t, `
rate_limiting:
  window: 30s
  protocols:
    sda:
      write:
        rate: 5
        burst: 9
    mma:
      requests:
        rate: 7
`)
	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.RateLimits.Window != 30*time.Second {
		t.Errorf("window: got %s, want 30s", cfg.RateLimits.Window)
	}

	sda := cfg.RateLimits.For(core.RateLimitSDA)
	if sda.Write.Rate != 5 || sda.Write.Burst != 9 {
		t.Errorf("sda write: got %+v, want rate 5 burst 9", sda.Write)
	}
	// The read bucket was not mentioned, so it must keep its default rather
	// than collapse to zero.
	wantRead := core.DefaultRateLimits().Protocols[core.RateLimitSDA].Read
	if sda.Read != wantRead {
		t.Errorf("sda read should be untouched: got %+v, want %+v", sda.Read, wantRead)
	}

	mma := cfg.RateLimits.For(core.RateLimitMMA)
	if mma.Requests.Rate != 7 {
		t.Errorf("mma rate: got %d, want 7", mma.Requests.Rate)
	}
	// Burst was not mentioned either.
	if mma.Requests.Burst != core.DefaultRateLimits().Protocols[core.RateLimitMMA].Requests.Burst {
		t.Errorf("mma burst should be untouched, got %d", mma.Requests.Burst)
	}

	// A protocol absent from the file keeps its default entirely.
	sfa := cfg.RateLimits.For(core.RateLimitSFA)
	if sfa != core.DefaultRateLimits().Protocols[core.RateLimitSFA] {
		t.Errorf("sfa should be untouched, got %+v", sfa)
	}
}

func TestLoadConfig_RateLimitUnknownProtocolIsAnError(t *testing.T) {
	// Silently ignoring a typo is the failure mode that made the hardcoded
	// limits so hard to diagnose: the operator changes a knob and nothing
	// happens.
	path := writeConfig(t, `
rate_limiting:
  protocols:
    sdaa:
      write:
        rate: 5
`)
	cfg := core.DefaultConfig()
	err := core.LoadConfigFromFile(path, cfg)
	if err == nil {
		t.Fatal("expected an error for an unknown protocol name, got nil")
	}
}

func TestLoadConfig_RateLimitLegacyKeysStillWork(t *testing.T) {
	// Existing deployments have these two keys and nothing else.
	path := writeConfig(t, `
rate_limiting:
  window_minutes: 2
  max_requests_per_window: 1000
`)
	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.RateLimits.Window != 2*time.Minute {
		t.Errorf("window_minutes: got %s, want 2m", cfg.RateLimits.Window)
	}
	// The legacy key only ever fed the MTA router, so it still only does.
	mta := cfg.RateLimits.For(core.RateLimitMTA)
	if mta.Requests.Rate != 1000 {
		t.Errorf("mta rate: got %d, want 1000", mta.Requests.Rate)
	}
	sda := cfg.RateLimits.For(core.RateLimitSDA)
	if sda != core.DefaultRateLimits().Protocols[core.RateLimitSDA] {
		t.Errorf("legacy key must not touch per-protocol limits, sda is %+v", sda)
	}
}

func TestLoadConfig_RateLimitBadWindowIsAnError(t *testing.T) {
	for _, window := range []string{"soon", "-5s", "0"} {
		path := writeConfig(t, "rate_limiting:\n  window: \""+window+"\"\n")
		cfg := core.DefaultConfig()
		if err := core.LoadConfigFromFile(path, cfg); err == nil {
			t.Errorf("window %q: expected an error, got nil", window)
		}
	}
}

func TestLoadConfig_RateLimitNoSectionKeepsDefaults(t *testing.T) {
	path := writeConfig(t, "server:\n  port: 4242\n")
	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load config: %v", err)
	}
	for name, want := range core.DefaultRateLimits().Protocols {
		if got := cfg.RateLimits.For(name); got != want {
			t.Errorf("protocol %q: got %+v, want %+v", name, got, want)
		}
	}
}

func TestValidate_RejectsNegativeBurst(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.RateLimits.Protocols[core.RateLimitSDA] = core.ProtocolLimits{
		Read:  core.Limit{Rate: 10, Burst: 10},
		Write: core.Limit{Rate: 10, Burst: -1},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected a negative burst to be rejected")
	}
}

// TestExampleConfigLoads guards against the shipped example drifting from the
// parser. An example that silently fails to apply is the exact trap the
// hardcoded limits set for operators.
func TestExampleConfigLoads(t *testing.T) {
	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile("../../config.example.yaml", cfg); err != nil {
		t.Fatalf("load config.example.yaml: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config.example.yaml does not validate: %v", err)
	}

	// The example's rate_limiting section is commented out because limiting is
	// off by default, so what this checks is that loading the file leaves the
	// built-in limits intact — an example that silently switched limiting on
	// would be worse than one that omitted it.
	for name, want := range core.DefaultRateLimits().Protocols {
		if got := cfg.RateLimits.For(name); got != want {
			t.Errorf("protocol %q: example config gives %+v, defaults are %+v", name, got, want)
		}
	}
}
