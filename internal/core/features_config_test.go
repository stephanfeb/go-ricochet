package core

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A file with no features section used to turn every feature off, because
// the parser copied eleven zero-valued booleans over the preset. An operator
// who wrote a file to change the port lost relay, metrics, push and presence.
func TestFeaturesOmittedKeepThePreset(t *testing.T) {
	cfg := DefaultConfig()
	want := *cfg
	path := writeConfig(t, "server:\n  port: 5000\n")
	if err := LoadConfigFromFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 5000 {
		t.Errorf("port not applied: %d", cfg.Port)
	}
	flags := map[string][2]bool{
		"relay":            {want.EnableRelay, cfg.EnableRelay},
		"relay_service":    {want.EnableRelayService, cfg.EnableRelayService},
		"autonat":          {want.EnableAutoNAT, cfg.EnableAutoNAT},
		"metrics":          {want.EnableMetrics, cfg.EnableMetrics},
		"push":             {want.EnablePushDelivery, cfg.EnablePushDelivery},
		"presence_monitor": {want.EnablePresenceMonitoring, cfg.EnablePresenceMonitoring},
		"presence_bcast":   {want.EnablePresenceBroadcast, cfg.EnablePresenceBroadcast},
		"hole_punching":    {want.EnableHolePunching, cfg.EnableHolePunching},
	}
	for name, v := range flags {
		if v[0] != v[1] {
			t.Errorf("%s: preset %v, after load %v", name, v[0], v[1])
		}
	}
}

// A flag that is set is applied whichever way it points; "false" is a
// choice, not an absence.
func TestFeaturesSetAreApplied(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.EnableRelay || cfg.EnableForwarding {
		t.Fatalf("test assumes relay on and forwarding off by default")
	}
	path := writeConfig(t, "features:\n  enable_relay: false\n  enable_forwarding: true\n")
	if err := LoadConfigFromFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.EnableRelay {
		t.Error("enable_relay: false was not applied")
	}
	if !cfg.EnableForwarding {
		t.Error("enable_forwarding: true was not applied")
	}
	if !cfg.EnableMetrics {
		t.Error("enable_metrics, not mentioned, was turned off")
	}
}

// A misspelt key is an error. Before, it parsed as nothing and the operator
// ran with the default they believed they had changed.
func TestUnknownKeysAreErrors(t *testing.T) {
	path := writeConfig(t, "features:\n  enable_relay_servce: false\n")
	err := LoadConfigFromFile(path, DefaultConfig())
	if err == nil {
		t.Fatal("unknown key accepted")
	}
	if !strings.Contains(err.Error(), "enable_relay_servce") {
		t.Errorf("error does not name the key: %v", err)
	}
}

func TestEmptyConfigFileIsNotAnError(t *testing.T) {
	path := writeConfig(t, "# nothing set\n")
	if err := LoadConfigFromFile(path, DefaultConfig()); err != nil {
		t.Fatalf("empty file: %v", err)
	}
}

// The example config must itself parse strictly, or the check above would
// reject the file every install starts from.
func TestExampleParsesStrictly(t *testing.T) {
	if err := LoadConfigFromFile(examplePath, DefaultConfig()); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionURIEscapesEveryField(t *testing.T) {
	cfg := &PostgresConfig{
		Host: "db.example.com", Port: 5432, Database: "ricochet",
		Username: "ricochet", Password: "p@ss:w/rd?&#", SSLMode: "require",
	}
	got := cfg.ConnectionURI()
	if strings.Contains(got, "p@ss:w/rd?&#") {
		t.Errorf("password not escaped: %s", got)
	}
	if !strings.HasPrefix(got, "postgresql://ricochet:") || !strings.HasSuffix(got, "@db.example.com:5432/ricochet?sslmode=require") {
		t.Errorf("unexpected shape: %s", got)
	}

	cfg.Password = ""
	got = cfg.ConnectionURI()
	if want := "postgresql://ricochet@db.example.com:5432/ricochet?sslmode=require"; got != want {
		t.Errorf("empty password: got %s, want %s", got, want)
	}
}
