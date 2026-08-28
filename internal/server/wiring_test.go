package server

import (
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
)

// The MDA has to be built from the server's configuration.
//
// This pins one line, deliberately. That line passed the literals 1000 and 30
// for the life of the codebase, so max_messages_per_mailbox and
// retention_policy applied only to mailboxes created explicitly through MMA.
// Nothing failed and nothing was logged; the settings were just ignored. A
// wiring mistake of that shape is invisible without something asserting the
// wiring.
func TestMailboxDefaultsComeFromTheServerConfig(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.MaxMessagesPerMailbox = 17
	cfg.RetentionPolicy = 3 * 24 * time.Hour

	got := NewServer(cfg, nil).mailboxDefaults()

	if got.MaxMessages != 17 {
		t.Errorf("MaxMessages = %d, want the configured 17", got.MaxMessages)
	}
	if got.RetentionDays != 3 {
		t.Errorf("RetentionDays = %d, want the configured 3", got.RetentionDays)
	}
}

// The capacity sampler has to be built from the configured threshold.
//
// Same blind spot as the mailbox defaults, and found the same way: the
// integration suite builds its own sampler mirroring this line rather than
// calling it, so hardcoding the constant back here failed no test at all.
// The ratio reaches the database only through a query, so a sampler built with
// the wrong one is indistinguishable from a correct one until an operator
// notices the threshold they set did nothing.
func TestCapacitySamplerUsesTheConfiguredThreshold(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.NearCapacityRatio = 0.6

	if got := NewServer(cfg, nil).newCapacitySampler().NearCapacityRatio(); got != 0.6 {
		t.Errorf("NearCapacityRatio = %v, want the configured 0.6", got)
	}
}

// An unset ratio must resolve to the default rather than to zero, which would
// count every mailbox as near capacity.
func TestCapacitySamplerFallsBackWhenUnset(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.NearCapacityRatio = 0

	if got := NewServer(cfg, nil).newCapacitySampler().NearCapacityRatio(); got != 0.9 {
		t.Errorf("NearCapacityRatio = %v, want the default 0.9", got)
	}
}
