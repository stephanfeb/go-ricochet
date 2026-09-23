package capacity_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/capacity"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// stubStorage supplies aggregates without a database. Only ServerStats is
// reachable from the sampler, so the rest of the interface is left embedded and
// unimplemented — calling any of it is a bug that should panic loudly.
type stubStorage struct {
	storage.Storage

	stats *storage.ServerStats
	err   error
	calls int
}

func (s *stubStorage) ServerStats(context.Context, float64) (*storage.ServerStats, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.stats, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func sampleStats(dbBytes int64) *storage.ServerStats {
	return &storage.ServerStats{
		SampledAt:             time.Now(),
		Mailboxes:             12,
		Messages:              340,
		MessageBytes:          4096,
		DatabaseBytes:         dbBytes,
		MailboxesNearCapacity: 2,
		NearCapacityRatio:     0.9,
	}
}

// Before the first sample, capacity must refuse to answer. The handler this
// replaces reported storage as 100% free regardless of what was on disk; a
// plausible wrong number does not get investigated, an error does.
func TestCapacityBeforeFirstSampleIsAnError(t *testing.T) {
	s := capacity.New(&stubStorage{stats: sampleStats(0)}, 1000, 0.9, discardLogger())

	view, err := s.Capacity()
	if !errors.Is(err, capacity.ErrNotSampled) {
		t.Fatalf("err = %v, want ErrNotSampled", err)
	}
	if view != nil {
		t.Errorf("capacity = %+v, want nil — zeroes would read as an empty server", view)
	}
	if _, ok := s.Age(time.Now()); ok {
		t.Error("Age reported a sample that does not exist")
	}
}

func TestCapacityReflectsTheSample(t *testing.T) {
	const budget = 1000
	s := capacity.New(&stubStorage{stats: sampleStats(250)}, budget, 0.9, discardLogger())

	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("sample: %v", err)
	}

	view, err := s.Capacity()
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}

	if view.TotalStorageBytes != budget {
		t.Errorf("total = %d, want %d", view.TotalStorageBytes, budget)
	}
	if view.UsedStorageBytes != 250 {
		t.Errorf("used = %d, want 250", view.UsedStorageBytes)
	}
	if view.AvailableStorageBytes != 750 {
		t.Errorf("available = %d, want 750", view.AvailableStorageBytes)
	}
	if view.MessageCount != 340 {
		t.Errorf("messages = %d, want 340", view.MessageCount)
	}
	if view.ActiveMailboxes != 12 {
		t.Errorf("mailboxes = %d, want 12", view.ActiveMailboxes)
	}
	if view.HealthScore != 0.75 {
		t.Errorf("health = %v, want 0.75 (the free fraction)", view.HealthScore)
	}
	if view.SampledAt.IsZero() {
		t.Error("SampledAt is zero; a cached figure must carry its age")
	}
}

// Over-budget must not produce negative headroom or a negative health score,
// which would read as nonsense on a dashboard.
func TestCapacityClampsWhenOverBudget(t *testing.T) {
	s := capacity.New(&stubStorage{stats: sampleStats(5000)}, 1000, 0.9, discardLogger())
	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("sample: %v", err)
	}

	view, err := s.Capacity()
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	if view.AvailableStorageBytes != 0 {
		t.Errorf("available = %d, want 0", view.AvailableStorageBytes)
	}
	if view.HealthScore != 0 {
		t.Errorf("health = %v, want 0", view.HealthScore)
	}
	if view.UsedStorageBytes != 5000 {
		t.Errorf("used = %d, want the real figure 5000, not a clamped one", view.UsedStorageBytes)
	}
}

// A failed refresh keeps the previous sample. Its age keeps climbing, so the
// gap is visible; discarding it would leave the operator with nothing at the
// moment the database is unhappy.
func TestFailedSampleKeepsThePreviousOne(t *testing.T) {
	store := &stubStorage{stats: sampleStats(250)}
	s := capacity.New(store, 1000, 0.9, discardLogger())

	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("first sample: %v", err)
	}

	store.err = errors.New("connection refused")
	if _, err := s.Sample(context.Background()); err == nil {
		t.Fatal("expected the second sample to fail")
	}

	view, err := s.Capacity()
	if err != nil {
		t.Fatalf("capacity after a failed refresh: %v", err)
	}
	if view.UsedStorageBytes != 250 {
		t.Errorf("used = %d, want the retained 250", view.UsedStorageBytes)
	}
}

func TestAgeGrowsWithTheSample(t *testing.T) {
	stats := sampleStats(100)
	stats.SampledAt = time.Now().Add(-90 * time.Second)

	s := capacity.New(&stubStorage{stats: stats}, 1000, 0.9, discardLogger())
	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("sample: %v", err)
	}

	age, ok := s.Age(time.Now())
	if !ok {
		t.Fatal("no sample recorded")
	}
	if age < 89*time.Second || age > 95*time.Second {
		t.Errorf("age = %v, want ~90s", age)
	}
}

// A budget of zero means unconfigured, not "everything is full".
func TestZeroBudgetDoesNotReportNegativeHeadroom(t *testing.T) {
	s := capacity.New(&stubStorage{stats: sampleStats(500)}, 0, 0.9, discardLogger())
	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("sample: %v", err)
	}

	view, err := s.Capacity()
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	if view.AvailableStorageBytes < 0 {
		t.Errorf("available = %d, want no negative headroom", view.AvailableStorageBytes)
	}
	if view.HealthScore < 0 || view.HealthScore > 1 {
		t.Errorf("health = %v, want it inside [0,1]", view.HealthScore)
	}
}

func TestNilSamplerIsSafe(t *testing.T) {
	var s *capacity.Sampler

	if got := s.Latest(); got != nil {
		t.Error("Latest on a nil sampler returned a value")
	}
	if got := s.MaxStorageBytes(); got != 0 {
		t.Errorf("MaxStorageBytes = %d, want 0", got)
	}
	if _, err := s.Capacity(); !errors.Is(err, capacity.ErrNotSampled) {
		t.Errorf("Capacity err = %v, want ErrNotSampled", err)
	}
	if _, err := s.Sample(context.Background()); !errors.Is(err, capacity.ErrNotSampled) {
		t.Errorf("Sample err = %v, want ErrNotSampled", err)
	}
	if _, ok := s.Age(time.Now()); ok {
		t.Error("Age on a nil sampler reported a sample")
	}
}

func TestFromRegistry(t *testing.T) {
	if got := capacity.FromRegistry(nil); got != nil {
		t.Error("FromRegistry(nil) returned non-nil")
	}
}
