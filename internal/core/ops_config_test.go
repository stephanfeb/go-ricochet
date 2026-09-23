package core_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

func TestOpsConfig_Defaults(t *testing.T) {
	ops := core.DefaultOpsConfig()

	if !ops.Enabled {
		t.Error("operator surface off by default; an instance with no health check reads as healthy to anything that cannot reach it")
	}
	if ops.Bind != "127.0.0.1" {
		t.Errorf("bind = %q, want loopback: readiness details and profiles disclose internal state", ops.Bind)
	}
	if ops.EnablePprof {
		t.Error("pprof on by default; profiles include goroutine stacks and heap contents")
	}
	if ops.Port != 9090 {
		t.Errorf("port = %d, want 9090", ops.Port)
	}
	if got := core.DefaultConfig().Ops; got != ops {
		t.Errorf("DefaultConfig().Ops = %+v, want %+v", got, ops)
	}
}

func TestOpsConfig_EffectiveAccessors(t *testing.T) {
	var zero core.OpsConfig

	if got := zero.EffectiveBind(); got != "127.0.0.1" {
		t.Errorf("EffectiveBind() = %q, want loopback", got)
	}
	if got := zero.EffectiveReadinessTimeout(); got != 2*time.Second {
		t.Errorf("EffectiveReadinessTimeout() = %v, want 2s", got)
	}
	if got := zero.EffectiveShutdownTimeout(); got != 5*time.Second {
		t.Errorf("EffectiveShutdownTimeout() = %v, want 5s", got)
	}

	set := core.OpsConfig{Bind: "0.0.0.0", Port: 8080, ReadinessTimeout: time.Second, ShutdownTimeout: time.Minute}
	if got := set.Address(); got != "0.0.0.0:8080" {
		t.Errorf("Address() = %q, want 0.0.0.0:8080", got)
	}
	if got := set.EffectiveReadinessTimeout(); got != time.Second {
		t.Errorf("EffectiveReadinessTimeout() = %v, want 1s", got)
	}
	if got := set.EffectiveShutdownTimeout(); got != time.Minute {
		t.Errorf("EffectiveShutdownTimeout() = %v, want 1m", got)
	}
}

func TestLoadConfig_OpsOverrides(t *testing.T) {
	path := writeConfig(t, `
ops:
  enabled: true
  bind: 0.0.0.0
  port: 9999
  enable_pprof: true
  readiness_timeout: 500ms
  shutdown_timeout: 30s
  drain_delay: 10s
`)

	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Ops.Bind != "0.0.0.0" || cfg.Ops.Port != 9999 {
		t.Errorf("address = %s, want 0.0.0.0:9999", cfg.Ops.Address())
	}
	if !cfg.Ops.EnablePprof {
		t.Error("enable_pprof not applied")
	}
	if cfg.Ops.ReadinessTimeout != 500*time.Millisecond {
		t.Errorf("readiness_timeout = %v, want 500ms", cfg.Ops.ReadinessTimeout)
	}
	if cfg.Ops.ShutdownTimeout != 30*time.Second {
		t.Errorf("shutdown_timeout = %v, want 30s", cfg.Ops.ShutdownTimeout)
	}
	if cfg.Ops.DrainDelay != 10*time.Second {
		t.Errorf("drain_delay = %v, want 10s", cfg.Ops.DrainDelay)
	}
}

// The drain delay defaults to zero because it is only useful when something is
// probing /readyz. It must nonetheless be settable, since without it Drain
// announces a withdrawal nobody has time to observe.
func TestOpsConfig_DrainDelayDefaultsToZero(t *testing.T) {
	if got := core.DefaultOpsConfig().DrainDelay; got != 0 {
		t.Errorf("DrainDelay = %v, want 0", got)
	}

	path := writeConfig(t, "ops:\n  drain_delay: 0s\n")
	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Ops.DrainDelay != 0 {
		t.Errorf("DrainDelay = %v, want 0", cfg.Ops.DrainDelay)
	}
}

// Enabled, port and enable_pprof all have meaningful zero values, so the YAML
// struct holds pointers. Without that, "enabled: false" and "port: 0" would be
// indistinguishable from an absent key and impossible to express.
func TestLoadConfig_OpsZeroValuesAreDistinguishable(t *testing.T) {
	path := writeConfig(t, `
ops:
  enabled: false
  port: 0
`)

	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Ops.Enabled {
		t.Error("enabled: false was ignored — the surface cannot be switched off from a config file")
	}
	if cfg.Ops.Port != 0 {
		t.Errorf("port = %d, want 0 (ephemeral); an explicit zero was treated as unset", cfg.Ops.Port)
	}
}

func TestLoadConfig_OpsNoSectionKeepsDefaults(t *testing.T) {
	path := writeConfig(t, "server:\n  port: 1234\n")

	cfg := core.DefaultConfig()
	if err := core.LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Ops != core.DefaultOpsConfig() {
		t.Errorf("Ops = %+v, want the defaults untouched", cfg.Ops)
	}
}

func TestLoadConfig_OpsBadValuesAreErrors(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"port too high":     {"ops:\n  port: 70000\n", "ops.port"},
		"bad duration":      {"ops:\n  readiness_timeout: soon\n", "ops.readiness_timeout"},
		"negative duration": {"ops:\n  shutdown_timeout: -1s\n", "ops.shutdown_timeout"},
		"negative drain":    {"ops:\n  drain_delay: -1s\n", "ops.drain_delay"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := core.LoadConfigFromFile(writeConfig(t, tc.yaml), core.DefaultConfig())
			if err == nil {
				t.Fatalf("accepted %q", strings.TrimSpace(tc.yaml))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to name %s", err, tc.want)
			}
		})
	}
}

func TestValidate_RejectsBadOpsPort(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.Ops.Port = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted a negative ops port")
	}

	cfg = core.DefaultConfig()
	cfg.Ops.Port = 0 // ephemeral is legitimate
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate rejected an ephemeral ops port: %v", err)
	}
}

// A near-capacity ratio above 1 makes the gauge it drives unreachable for a
// mailbox that is merely full, which is the state it exists to warn about. A
// threshold that silently never fires is worse than no threshold.
func TestNearCapacityRatioIsBounded(t *testing.T) {
	for _, ratio := range []float64{-0.1, 1.5, 90} {
		cfg := core.DefaultConfig()
		cfg.NearCapacityRatio = ratio
		if err := cfg.Validate(); err == nil {
			t.Errorf("a ratio of %v was accepted", ratio)
		}
	}

	for _, ratio := range []float64{0, 0.5, 0.9, 1} {
		cfg := core.DefaultConfig()
		cfg.NearCapacityRatio = ratio
		if err := cfg.Validate(); err != nil {
			t.Errorf("a ratio of %v was rejected: %v", ratio, err)
		}
	}
}

// Zero means "use the default", not "warn about every mailbox". The sampler
// treats a non-positive ratio as unset, so the effective value has to be the
// one the storage layer would pick anyway.
func TestUnsetNearCapacityRatioResolvesToTheDefault(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.NearCapacityRatio = 0

	if got := cfg.EffectiveNearCapacityRatio(); got != 0.9 {
		t.Errorf("EffectiveNearCapacityRatio = %v, want 0.9", got)
	}
}

func TestOpsQueryTimeoutHasAnEffectiveValue(t *testing.T) {
	var ops core.OpsConfig
	if got := ops.EffectiveQueryTimeout(); got != 10*time.Second {
		t.Errorf("EffectiveQueryTimeout = %v on a zero config, want 10s", got)
	}

	ops.QueryTimeout = 45 * time.Second
	if got := ops.EffectiveQueryTimeout(); got != 45*time.Second {
		t.Errorf("EffectiveQueryTimeout = %v, want the configured 45s", got)
	}
}
