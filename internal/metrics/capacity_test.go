package metrics_test

import (
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/metrics"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// stubStats stands in for capacity.Sampler.
type stubStats struct {
	stats *storage.ServerStats
	max   int64
}

func (s *stubStats) Latest() *storage.ServerStats { return s.stats }
func (s *stubStats) MaxStorageBytes() int64       { return s.max }

// Before the first sample the collector must publish nothing at all.
// Publishing zeroes would read as an empty server — the same fabrication that
// made handleQueryCapacity report storage as always free.
func TestCapacityCollectorPublishesNothingBeforeTheFirstSample(t *testing.T) {
	m := metrics.New()
	if err := m.Register(metrics.NewCapacityCollector(&stubStats{})); err != nil {
		t.Fatalf("register: %v", err)
	}

	for _, name := range gatherNames(t, m) {
		if name == "ricochet_mailboxes" || name == "ricochet_database_bytes" {
			t.Errorf("%s published with no sample taken", name)
		}
	}
}

func TestCapacityCollectorPublishesTheSample(t *testing.T) {
	src := &stubStats{
		max: 1000,
		stats: &storage.ServerStats{
			SampledAt:             time.Now().Add(-30 * time.Second),
			Mailboxes:             9,
			Messages:              120,
			MessageBytes:          4096,
			DatabaseBytes:         250,
			MailboxesNearCapacity: 2,
			NearCapacityRatio:     0.9,
			Depth: []storage.DepthBucket{
				{Label: "0", Min: 0, Max: 0, Mailboxes: 4},
				{Label: "1-9", Min: 1, Max: 9, Mailboxes: 5},
			},
		},
	}

	m := metrics.New()
	if err := m.Register(metrics.NewCapacityCollector(src)); err != nil {
		t.Fatalf("register: %v", err)
	}

	for name, want := range map[string]float64{
		"ricochet_mailboxes":               9,
		"ricochet_messages_stored":         120,
		"ricochet_message_bytes":           4096,
		"ricochet_database_bytes":          250,
		"ricochet_storage_max_bytes":       1000,
		"ricochet_mailboxes_near_capacity": 2,
	} {
		if got := gaugeValue(t, m, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	// The age is what stops a stalled sampler from looking like a quiet
	// server: the figures would stop changing either way, but the age climbs.
	if age := gaugeValue(t, m, "ricochet_stats_age_seconds"); age < 29 || age > 40 {
		t.Errorf("stats age = %v, want ~30", age)
	}

	// Depth is a labelled gauge, one series per bucket.
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	buckets := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "ricochet_mailbox_depth" {
			continue
		}
		for _, sample := range f.GetMetric() {
			buckets[labelMap(sample)["bucket"]] = sample.GetGauge().GetValue()
		}
	}
	if buckets["0"] != 4 || buckets["1-9"] != 5 {
		t.Errorf("depth buckets = %v, want {0:4, 1-9:5}", buckets)
	}
}

// The bucket label comes from a compiled-in list, never from data, so it
// cannot grow without a code change.
func TestDepthBucketLabelsAreBounded(t *testing.T) {
	src := &stubStats{stats: &storage.ServerStats{
		SampledAt: time.Now(),
		Depth: []storage.DepthBucket{
			{Label: "0"}, {Label: "1-9"}, {Label: "10-99"},
			{Label: "100-999"}, {Label: "1000-9999"}, {Label: "10000+"},
		},
	}}

	m := metrics.New()
	if err := m.Register(metrics.NewCapacityCollector(src)); err != nil {
		t.Fatalf("register: %v", err)
	}

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "ricochet_mailbox_depth" {
			continue
		}
		if got := len(f.GetMetric()); got != 6 {
			t.Errorf("%d depth series, want the 6 compiled-in buckets", got)
		}
	}
}
