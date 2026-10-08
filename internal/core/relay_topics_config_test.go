package core

import (
	"strings"
	"testing"
)

// pubsub.relay_topics is empty by default, read from the file, and refuses
// empty or repeated topics.
func TestRelayTopicsConfig(t *testing.T) {
	for name, cfg := range map[string]*ServerConfig{
		"default": DefaultConfig(), "development": DevelopmentConfig(),
		"production": ProductionConfig(), "high-capacity": HighCapacityConfig(),
	} {
		if len(cfg.RelayTopics) != 0 {
			t.Errorf("%s preset relays %v, want nothing", name, cfg.RelayTopics)
		}
	}

	cfg := DefaultConfig()
	file := "pubsub:\n  relay_topics:\n    - /overmedia/service-announce\n"
	if err := LoadConfigFromFile(writeConfig(t, file), cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.RelayTopics) != 1 || cfg.RelayTopics[0] != "/overmedia/service-announce" {
		t.Fatalf("relay_topics = %v after load", cfg.RelayTopics)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid relay_topics refused: %v", err)
	}

	for _, topics := range [][]string{{" "}, {"/a", "/a"}} {
		cfg.RelayTopics = topics
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "relay_topics") {
			t.Errorf("relay_topics %q validated as %v, want an error naming the key", topics, err)
		}
	}
}
