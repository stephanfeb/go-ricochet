package server

import (
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
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

// The relay must not cut a relayed connection unless the operator sets a
// limit. Zero used to fall through to go-libp2p's 2 minutes and 128 KiB,
// which ended every relayed connection between two clients behind NAT.
// go-libp2p limits a connection on both duration and data or on neither, so
// an operator who sets one must get only that one.
func TestRelayConnectionLimitComesFromTheServerConfig(t *testing.T) {
	for _, tc := range []struct {
		name          string
		duration      time.Duration
		data          int64
		wantUnlimited bool
		wantDuration  time.Duration
		wantData      int64
	}{
		{name: "unset is unlimited", wantUnlimited: true},
		{name: "duration only", duration: 10 * time.Minute,
			wantDuration: 10 * time.Minute, wantData: math.MaxInt64},
		{name: "data only", data: 64 << 20,
			wantDuration: time.Duration(math.MaxUint32) * time.Second, wantData: 64 << 20},
		{name: "both", duration: time.Hour, data: 1 << 30,
			wantDuration: time.Hour, wantData: 1 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := core.DefaultConfig()
			cfg.RelayLimits.ConnectionDuration = tc.duration
			cfg.RelayLimits.ConnectionData = tc.data

			got := NewServer(cfg, slog.New(slog.DiscardHandler)).buildForgeConfig().Host.RelayLimits

			if got.UnlimitedConnections != tc.wantUnlimited {
				t.Fatalf("UnlimitedConnections = %v, want %v", got.UnlimitedConnections, tc.wantUnlimited)
			}
			if tc.wantUnlimited {
				return
			}
			if got.ConnectionDuration != tc.wantDuration || got.ConnectionData != tc.wantData {
				t.Errorf("limit = %s / %d bytes, want %s / %d bytes",
					got.ConnectionDuration, got.ConnectionData, tc.wantDuration, tc.wantData)
			}
		})
	}
}
