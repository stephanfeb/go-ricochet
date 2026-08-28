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
