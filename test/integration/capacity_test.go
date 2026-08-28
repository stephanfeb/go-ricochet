package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// fillMailbox delivers n messages to one folder and returns its record.
func fillMailbox(t *testing.T, server *testServer, folder string, n int) *storage.MailboxRecord {
	t.Helper()

	ctx := context.Background()
	owner := server.PeerID

	addr := &core.MailboxAddress{OwnerID: owner, FolderPath: folder}
	mailbox, err := server.Storage.GetOrCreateMailbox(ctx, addr,
		server.Config.MaxMessagesPerMailbox, 30, nil)
	if err != nil {
		t.Fatalf("create mailbox %s: %v", folder, err)
	}

	for i := 0; i < n; i++ {
		msg := core.NewMessageWithDefaultExpiry(owner, owner, []byte("0123456789"))
		msg.MessageID = fmt.Sprintf("%s-%d-%d", folder, time.Now().UnixNano(), i)
		msg.FolderPath = folder
		if _, err := server.Storage.StoreMessage(ctx, mailbox, msg); err != nil {
			t.Fatalf("store message %d in %s: %v", i, folder, err)
		}
	}
	return mailbox
}

// depthCount returns how many mailboxes fell in a named bucket.
func depthCount(stats *storage.ServerStats, label string) int {
	for _, b := range stats.Depth {
		if b.Label == label {
			return b.Mailboxes
		}
	}
	return -1
}

// The aggregate has to agree with the database, since the whole point is that
// the previous answer did not.
func TestServerStatsMatchTheDatabase(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()

	// The aggregate is server-wide by design, and the test database is shared
	// across a run, so absolute figures belong to every test at once. What
	// this checks is that the aggregate agrees with the database at the same
	// instant, which holds regardless of what else is stored.
	fillMailbox(t, server, "stats/empty", 0)
	fillMailbox(t, server, "stats/small", 5)
	fillMailbox(t, server, "stats/medium", 25)

	stats, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("server stats: %v", err)
	}

	var mailboxes, messages int
	var payloadBytes int64
	if err := server.Storage.Pool().QueryRow(ctx,
		`SELECT (SELECT COUNT(*) FROM mailboxes),
		        (SELECT COUNT(*) FROM stored_messages),
		        (SELECT COALESCE(SUM(octet_length(payload)), 0) FROM stored_messages)`).
		Scan(&mailboxes, &messages, &payloadBytes); err != nil {
		t.Fatalf("count directly: %v", err)
	}

	if stats.Mailboxes != mailboxes {
		t.Errorf("mailboxes = %d, database says %d", stats.Mailboxes, mailboxes)
	}
	if stats.Messages != int64(messages) {
		t.Errorf("messages = %d, database says %d", stats.Messages, messages)
	}
	if stats.MessageBytes != payloadBytes {
		t.Errorf("messageBytes = %d, database says %d", stats.MessageBytes, payloadBytes)
	}
	if stats.DatabaseBytes <= 0 {
		t.Errorf("databaseBytes = %d, want a real size", stats.DatabaseBytes)
	}
	if stats.SampledAt.IsZero() {
		t.Error("SampledAt is zero")
	}
}

// Mailbox depth is the distribution, not the mean. A server holding one
// enormous mailbox and a server holding a thousand small ones have the same
// average and need different responses.
func TestMailboxDepthBuckets(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()

	// Deltas, not absolutes: the aggregate covers every owner and the test
	// database is shared, so what this test can assert is the change its own
	// mailboxes caused.
	before, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("stats before: %v", err)
	}

	fillMailbox(t, server, "depth/a", 0)
	fillMailbox(t, server, "depth/b", 0)
	fillMailbox(t, server, "depth/c", 3)
	fillMailbox(t, server, "depth/d", 40)

	stats, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("server stats: %v", err)
	}

	if got := depthCount(stats, "0") - depthCount(before, "0"); got != 2 {
		t.Errorf("empty mailboxes rose by %d, want 2 (%+v)", got, stats.Depth)
	}
	if got := depthCount(stats, "1-9") - depthCount(before, "1-9"); got != 1 {
		t.Errorf("1-9 rose by %d, want 1 (%+v)", got, stats.Depth)
	}
	if got := depthCount(stats, "10-99") - depthCount(before, "10-99"); got != 1 {
		t.Errorf("10-99 rose by %d, want 1 (%+v)", got, stats.Depth)
	}

	// Every mailbox lands in exactly one bucket.
	total := 0
	for _, b := range stats.Depth {
		total += b.Mailboxes
	}
	if total != stats.Mailboxes {
		t.Errorf("buckets sum to %d but there are %d mailboxes", total, stats.Mailboxes)
	}
}

// The acceptance criterion for B2: filling a mailbox to its cap must move the
// near-capacity count. A mailbox at its cap starts evicting, so this is the
// number that predicts data loss.
func TestFillingAMailboxMovesNearCapacity(t *testing.T) {
	const cap = 10

	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.MaxMessagesPerMailbox = cap
	})
	ctx := context.Background()

	baseline, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("stats baseline: %v", err)
	}

	// A mailbox with room to spare must not move the count. Measured on its
	// own, because this is the assertion that catches a truncated ratio:
	// an integer-typed parameter turns 0.9 into 0 and counts every mailbox,
	// which is how this query first behaved.
	fillMailbox(t, server, "near/roomy", 1)

	before, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("stats before: %v", err)
	}
	if got := before.MailboxesNearCapacity - baseline.MailboxesNearCapacity; got != 0 {
		t.Errorf("a mailbox at 1 of 10 moved nearCapacity by %d, want 0", got)
	}

	// 9 of 10 is at the 90% threshold.
	fillMailbox(t, server, "near/full", 9)

	after, err := server.Storage.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("stats after: %v", err)
	}
	if got := after.MailboxesNearCapacity - before.MailboxesNearCapacity; got != 1 {
		t.Errorf("nearCapacity rose by %d after filling a mailbox to its cap, want 1", got)
	}
	if after.NearCapacityRatio != 0.9 {
		t.Errorf("ratio = %v, want the one that was asked for", after.NearCapacityRatio)
	}

	// A stricter threshold must not count the same mailbox.
	strict, err := server.Storage.ServerStats(ctx, 1.0)
	if err != nil {
		t.Fatalf("stats at 100%%: %v", err)
	}
	if strict.MailboxesNearCapacity > before.MailboxesNearCapacity {
		t.Errorf("at a 100%% threshold, nearCapacity = %d, want no more than %d",
			strict.MailboxesNearCapacity, before.MailboxesNearCapacity)
	}
	if strict.Mailboxes == strict.MailboxesNearCapacity && strict.Mailboxes > 0 {
		t.Error("every mailbox counted as near capacity; the ratio was truncated to zero")
	}
}

// queryCapacity used to report AvailableStorageBytes equal to the configured
// maximum with everything else zero — it said storage was 100% free no matter
// what was on disk. It must now agree with the database.
func TestQueryCapacityReportsRealUsage(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fillMailbox(t, server, "capacity/one", 12)

	if _, err := server.Capacity.Sample(ctx); err != nil {
		t.Fatalf("sample: %v", err)
	}

	// Over the wire, through the MMA handler — the path an operator uses.
	// Asserting against the sampler directly would leave the handler itself
	// free to go on fabricating.
	view, err := cl.QueryCapacity(ctx)
	if err != nil {
		t.Fatalf("query capacity: %v", err)
	}

	var mailboxes, messages int
	var dbBytes int64
	if err := server.Storage.Pool().QueryRow(ctx,
		`SELECT (SELECT COUNT(*) FROM mailboxes),
		        (SELECT COUNT(*) FROM stored_messages),
		        pg_database_size(current_database())`).
		Scan(&mailboxes, &messages, &dbBytes); err != nil {
		t.Fatalf("count directly: %v", err)
	}

	if view.MessageCount != messages {
		t.Errorf("messageCount = %d, database says %d", view.MessageCount, messages)
	}
	if view.ActiveMailboxes != mailboxes {
		t.Errorf("activeMailboxes = %d, database says %d", view.ActiveMailboxes, mailboxes)
	}
	if view.UsedStorageBytes == 0 {
		t.Error("usedStorageBytes = 0 with data in the database")
	}
	if view.AvailableStorageBytes == view.TotalStorageBytes {
		t.Error("available equals total with data stored — the old fabricated answer")
	}
	if view.UsedStorageBytes+view.AvailableStorageBytes != view.TotalStorageBytes {
		t.Errorf("used (%d) + available (%d) != total (%d)",
			view.UsedStorageBytes, view.AvailableStorageBytes, view.TotalStorageBytes)
	}
	if view.HealthScore <= 0 || view.HealthScore > 1 {
		t.Errorf("healthScore = %v, want a real fraction in (0,1]", view.HealthScore)
	}
	if view.SampledAt.IsZero() {
		t.Error("sampledAt is zero; the figure's age must travel with it")
	}
}

// The capacity series must reach a scrape, and must not appear at all before
// the first sample — zeroes would read as an empty server.
func TestCapacityMetricsAppearAfterSampling(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()

	base := scrapeCapacity(t, server)

	fillMailbox(t, server, "metrics/cap", 4)

	if _, err := server.Capacity.Sample(ctx); err != nil {
		t.Fatalf("sample: %v", err)
	}

	samples := scrape(t, server.Metrics)
	byName := map[string]float64{}
	depthByBucket := map[string]float64{}
	for _, s := range samples {
		if s.name == "ricochet_mailbox_depth" {
			depthByBucket[s.labels["bucket"]] = s.value
			continue
		}
		byName[s.name] = s.value
	}

	for _, want := range []string{
		"ricochet_mailboxes",
		"ricochet_messages_stored",
		"ricochet_message_bytes",
		"ricochet_database_bytes",
		"ricochet_storage_max_bytes",
		"ricochet_mailboxes_near_capacity",
		"ricochet_stats_age_seconds",
	} {
		if _, ok := byName[want]; !ok {
			t.Errorf("missing %s", want)
		}
	}

	if got := byName["ricochet_messages_stored"] - base.messages; got != 4 {
		t.Errorf("messages_stored rose by %v, want 4", got)
	}
	if got := depthByBucket["1-9"] - base.depth1to9; got != 1 {
		t.Errorf("depth{bucket=1-9} rose by %v, want 1", got)
	}
	if byName["ricochet_stats_age_seconds"] < 0 {
		t.Errorf("stats age = %v, want non-negative", byName["ricochet_stats_age_seconds"])
	}
}

// capacitySnapshot is the subset of capacity series a delta assertion needs.
type capacitySnapshot struct {
	messages  float64
	depth1to9 float64
}

func scrapeCapacity(t *testing.T, server *testServer) capacitySnapshot {
	t.Helper()

	var snap capacitySnapshot
	for _, s := range scrape(t, server.Metrics) {
		switch {
		case s.name == "ricochet_messages_stored":
			snap.messages = s.value
		case s.name == "ricochet_mailbox_depth" && s.labels["bucket"] == "1-9":
			snap.depth1to9 = s.value
		}
	}
	return snap
}

// The near-capacity threshold has to be the operator's, not the code's.
//
// It was a parameter at every layer — Storage.ServerStats takes it, the
// sampler takes it, /ops/storage reports it back — and then server.go passed
// a compiled-in constant, so there was no way to set it. That is the same
// shape as the hardcoded mailbox cap: plumbed for configuration right up to
// the one place that mattered.
func TestNearCapacityThresholdIsConfigurable(t *testing.T) {
	const cap = 10

	// At 0.5, a mailbox half full already counts; at the default 0.9 it does
	// not. One mailbox, two thresholds, opposite answers.
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.MaxMessagesPerMailbox = cap
		cfg.NearCapacityRatio = 0.5
	})
	ctx := context.Background()

	baseline, err := server.Storage.ServerStats(ctx, server.Config.EffectiveNearCapacityRatio())
	if err != nil {
		t.Fatalf("stats baseline: %v", err)
	}

	fillMailbox(t, server, "threshold/half", cap/2)

	lenient, err := server.Storage.ServerStats(ctx, server.Config.EffectiveNearCapacityRatio())
	if err != nil {
		t.Fatalf("stats at the configured ratio: %v", err)
	}
	if lenient.NearCapacityRatio != 0.5 {
		t.Fatalf("ratio = %v, want the configured 0.5", lenient.NearCapacityRatio)
	}
	if got := lenient.MailboxesNearCapacity - baseline.MailboxesNearCapacity; got != 1 {
		t.Errorf("a mailbox at 5 of 10 moved nearCapacity by %d at a 0.5 threshold, want 1", got)
	}

	// The sampler must use the configured value too, not re-derive a default.
	if _, err := server.Capacity.Sample(ctx); err != nil {
		t.Fatalf("sample: %v", err)
	}
	if got := server.Capacity.Latest().NearCapacityRatio; got != 0.5 {
		t.Errorf("sampler ratio = %v, want the configured 0.5", got)
	}
}
