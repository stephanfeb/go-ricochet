// Package capacity samples the server's aggregate storage state and answers
// capacity questions from that sample.
//
// It exists because those questions previously had a fabricated answer.
// handleQueryCapacity reported AvailableStorageBytes equal to the configured
// maximum with a "TODO: compute actual usage" beside it, and left the message
// count, mailbox count and health score at zero — so it told every caller that
// storage was 100% free regardless of what was on disk. A number that reads as
// working and is not is worse than no number at all, because nobody thinks to
// check it.
//
// The aggregates scan, so they are sampled on a timer rather than computed per
// request. Every answer therefore carries the time it was taken: a cached
// figure that has silently gone stale is the same trap in slower motion.
package capacity

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	forge "github.com/twostack/go-p2p-forge"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// RegistryKey is the forge registry key under which the sampler is provided to
// pipeline handlers.
const RegistryKey = "capacity"

// ErrNotSampled means no pass has completed yet. Callers must report this
// rather than substituting zeroes, which would read as an empty server.
var ErrNotSampled = errors.New("capacity has not been sampled yet")

// Sampler holds the most recent aggregate and refreshes it on demand.
type Sampler struct {
	store           storage.Storage
	maxStorageBytes int64
	nearRatio       float64
	logger          *slog.Logger

	mu     sync.RWMutex
	latest *storage.ServerStats
}

// New builds a sampler. maxStorageBytes is the configured budget capacity is
// measured against; nearRatio is the fill fraction at which a mailbox counts
// as near its own cap.
func New(store storage.Storage, maxStorageBytes int64, nearRatio float64, logger *slog.Logger) *Sampler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Sampler{
		store:           store,
		maxStorageBytes: maxStorageBytes,
		nearRatio:       nearRatio,
		logger:          logger,
	}
}

// Sample runs the aggregate query and stores the result. A failure leaves the
// previous sample in place: stale numbers marked with their age are more use
// than none, and the caller can see the gap widening.
func (s *Sampler) Sample(ctx context.Context) (*storage.ServerStats, error) {
	if s == nil {
		return nil, ErrNotSampled
	}

	stats, err := s.store.ServerStats(ctx, s.nearRatio)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.latest = stats
	s.mu.Unlock()

	return stats, nil
}

// Latest returns the most recent sample, or nil if none has completed.
func (s *Sampler) Latest() *storage.ServerStats {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latest
}

// NearCapacityRatio returns the threshold this sampler was built with.
//
// It is exposed so the wiring can be asserted without taking a sample: the
// ratio reaches the database only through a query, so a sampler built with the
// wrong one looks identical to a correct one until an operator notices their
// threshold did nothing.
func (s *Sampler) NearCapacityRatio() float64 {
	if s == nil {
		return 0
	}
	return s.nearRatio
}

// MaxStorageBytes returns the configured budget.
func (s *Sampler) MaxStorageBytes() int64 {
	if s == nil {
		return 0
	}
	return s.maxStorageBytes
}

// Capacity derives the operator-facing capacity view from the latest sample.
//
// Usage is measured as database size on disk rather than as the sum of stored
// payloads. Indexes and unreclaimed space are what actually fill a volume, and
// a server that reports 40% used while its disk is full has answered the wrong
// question.
func (s *Sampler) Capacity() (*core.ServerCapacity, error) {
	stats := s.Latest()
	if stats == nil {
		return nil, ErrNotSampled
	}

	used := stats.DatabaseBytes
	total := s.maxStorageBytes

	available := total - used
	if available < 0 {
		available = 0
	}

	return &core.ServerCapacity{
		TotalStorageBytes:     total,
		UsedStorageBytes:      used,
		AvailableStorageBytes: available,
		MessageCount:          int(stats.Messages),
		ActiveMailboxes:       stats.Mailboxes,
		HealthScore:           healthScore(used, total),
		SampledAt:             stats.SampledAt,
	}, nil
}

// healthScore is the fraction of the storage budget still free: 1.0 on an
// empty server, 0.0 when the budget is spent.
//
// It reports storage headroom and nothing else. That is narrow, and it is
// stated here rather than left to a caller's imagination, because the field
// name invites reading it as an overall verdict — which is how it came to be
// hardcoded to zero in the first place.
func healthScore(used, total int64) float64 {
	if total <= 0 {
		return 0
	}
	free := float64(total-used) / float64(total)
	if free < 0 {
		return 0
	}
	if free > 1 {
		return 1
	}
	return free
}

// Age returns how long ago the latest sample was taken, and whether there is
// one at all.
func (s *Sampler) Age(now time.Time) (time.Duration, bool) {
	stats := s.Latest()
	if stats == nil {
		return 0, false
	}
	return now.Sub(stats.SampledAt), true
}

// FromRegistry retrieves the sampler from a forge registry, returning nil when
// none was provided. Every method is nil-safe.
func FromRegistry(reg *forge.Registry) *Sampler {
	if reg == nil {
		return nil
	}
	s, ok := forge.Service[*Sampler](reg, RegistryKey)
	if !ok {
		return nil
	}
	return s
}
