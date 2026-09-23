package mda_test

import (
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/mda"
)

// The bug this replaces: getMailbox passed the literals 1000 and 30, so a
// mailbox created by delivery ignored the configuration entirely. An operator
// could lower the cap and watch delivery keep filling past it.
func TestDefaultsComeFromTheConfiguration(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.MaxMessagesPerMailbox = 42
	cfg.RetentionPolicy = 7 * 24 * time.Hour

	got := mda.DefaultsFromConfig(cfg)
	if got.MaxMessages != 42 {
		t.Errorf("MaxMessages = %d, want the configured 42", got.MaxMessages)
	}
	if got.RetentionDays != 7 {
		t.Errorf("RetentionDays = %d, want 7", got.RetentionDays)
	}
}

// A cap of zero is not "no limit" — the mailbox check is count >= max, so zero
// makes every mailbox full on its first message and nothing is ever delivered.
func TestZeroCapFallsBackRatherThanRejectingEverything(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.MaxMessagesPerMailbox = 0

	if got := mda.DefaultsFromConfig(cfg).MaxMessages; got <= 0 {
		t.Errorf("MaxMessages = %d; a cap of zero rejects every message", got)
	}
}

// Retention is enforced as "delete anything older than N days", so zero days
// deletes everything on the next sweep. A sub-day policy must not truncate
// into that.
func TestSubDayRetentionDoesNotBecomeZero(t *testing.T) {
	for _, policy := range []time.Duration{
		time.Hour,
		12 * time.Hour,
		23*time.Hour + 59*time.Minute,
	} {
		cfg := core.DefaultConfig()
		cfg.RetentionPolicy = policy

		got := mda.DefaultsFromConfig(cfg).RetentionDays
		if got < 1 {
			t.Errorf("a %v retention policy became %d days; that deletes every message",
				policy, got)
		}
	}
}

// A partial day rounds up rather than down, for the same reason: rounding down
// deletes messages the policy said to keep.
func TestRetentionRoundsUp(t *testing.T) {
	cases := []struct {
		policy time.Duration
		want   int
	}{
		{24 * time.Hour, 1},
		{25 * time.Hour, 2},
		{30 * 24 * time.Hour, 30},
		{30*24*time.Hour + time.Minute, 31},
	}

	for _, tc := range cases {
		cfg := core.DefaultConfig()
		cfg.RetentionPolicy = tc.policy

		if got := mda.DefaultsFromConfig(cfg).RetentionDays; got != tc.want {
			t.Errorf("%v = %d days, want %d", tc.policy, got, tc.want)
		}
	}
}

func TestUnsetRetentionFallsBack(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.RetentionPolicy = 0

	if got := mda.DefaultsFromConfig(cfg).RetentionDays; got <= 0 {
		t.Errorf("RetentionDays = %d with no policy set; that deletes everything", got)
	}
}

func TestNilConfigIsSafe(t *testing.T) {
	got := mda.DefaultsFromConfig(nil)
	if got.MaxMessages <= 0 || got.RetentionDays <= 0 {
		t.Errorf("DefaultsFromConfig(nil) = %+v, want usable fallbacks", got)
	}
}

// A server built with a zero-valued struct must still be safe, since the
// fallback is what stops a hand-assembled MDA from rejecting every message.
func TestServerAppliesFallbacksToZeroDefaults(t *testing.T) {
	srv := mda.NewMailboxServer(nil, mda.MailboxDefaults{}, nil)

	got := srv.MailboxDefaults()
	if got.MaxMessages <= 0 || got.RetentionDays <= 0 {
		t.Errorf("defaults = %+v, want the fallbacks applied", got)
	}
}

// The default configuration must keep producing what the old literals did, so
// this fix changes nothing for a deployment that never set the knobs.
func TestDefaultConfigMatchesTheOldLiterals(t *testing.T) {
	got := mda.DefaultsFromConfig(core.DefaultConfig())

	if got.MaxMessages != 1000 {
		t.Errorf("MaxMessages = %d, want the 1000 the literal used", got.MaxMessages)
	}
	if got.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want the 30 the literal used", got.RetentionDays)
	}
}
