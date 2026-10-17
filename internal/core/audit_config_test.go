package core

import (
	"strings"
	"testing"
)

// audit.connection_log_days is 90 in every preset, is read from the file
// (0 included, which turns the log off), and refuses a negative value.
func TestConnectionLogDaysConfig(t *testing.T) {
	for name, cfg := range map[string]*ServerConfig{
		"default": DefaultConfig(), "development": DevelopmentConfig(),
		"production": ProductionConfig(), "high-capacity": HighCapacityConfig(),
	} {
		if cfg.ConnectionLogDays != 90 {
			t.Errorf("%s preset keeps the connection log %d days, want 90", name, cfg.ConnectionLogDays)
		}
	}

	cfg := DefaultConfig()
	if err := LoadConfigFromFile(writeConfig(t, "audit:\n  connection_log_days: 0\n"), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectionLogDays != 0 {
		t.Fatalf("connection_log_days = %d after loading 0, want 0 (off)", cfg.ConnectionLogDays)
	}
	if err := LoadConfigFromFile(writeConfig(t, "pubsub:\n  relay_topics: []\n"), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectionLogDays != 0 {
		t.Fatalf("a file without audit changed connection_log_days to %d", cfg.ConnectionLogDays)
	}

	cfg.ConnectionLogDays = -1
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "connection_log_days") {
		t.Fatalf("negative days validated as %v, want an error naming the key", err)
	}
}
