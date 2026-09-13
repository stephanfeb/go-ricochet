package core

import (
	"strings"
	"testing"
)

// max_connections_per_ip defaults to no cap in every preset, is read from
// the file, and refuses a negative value.
func TestPerIPConnectionCapConfig(t *testing.T) {
	for name, cfg := range map[string]*ServerConfig{
		"default": DefaultConfig(), "development": DevelopmentConfig(),
		"production": ProductionConfig(), "high-capacity": HighCapacityConfig(),
	} {
		if cfg.MaxConnectionsPerIP != 0 {
			t.Errorf("%s preset caps connections per address at %d, want none", name, cfg.MaxConnectionsPerIP)
		}
	}

	cfg := DefaultConfig()
	if err := LoadConfigFromFile(writeConfig(t, "performance:\n  max_connections_per_ip: 32\n"), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConnectionsPerIP != 32 {
		t.Fatalf("max_connections_per_ip = %d after load, want 32", cfg.MaxConnectionsPerIP)
	}

	cfg.MaxConnectionsPerIP = -1
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_connections_per_ip") {
		t.Fatalf("negative cap validated as %v, want an error naming the key", err)
	}
}
