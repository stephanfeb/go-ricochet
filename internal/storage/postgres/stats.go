package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// depthBucketEdges defines the mailbox-size histogram. The edges are fixed
// strings so they are safe as metric labels, and they are wide because the
// question they answer is coarse: is this server holding a million small
// mailboxes or a handful of enormous ones? Those need different responses and
// a mean hides which you have.
var depthBucketEdges = []struct {
	label    string
	min, max int
}{
	{"0", 0, 0},
	{"1-9", 1, 9},
	{"10-99", 10, 99},
	{"100-999", 100, 999},
	{"1000-9999", 1000, 9999},
	{"10000+", 10000, -1},
}

// DefaultNearCapacityRatio is the fill fraction at which a mailbox is counted
// as near capacity. A mailbox at its cap starts evicting, so 90% is late
// enough to be meaningful and early enough to act on.
const DefaultNearCapacityRatio = 0.9

// ServerStats aggregates across every owner in one pass over mailboxes.
//
// Every figure comes from the counters the store and delete paths maintain
// on the mailbox row (message_count, message_bytes), so the pass never
// touches stored_messages. It used to join and SUM(octet_length(payload))
// over the whole table every sampling interval, which is a full scan of the
// largest table on the server to answer a question about its size.
func (s *PostgresStorage) ServerStats(ctx context.Context, nearCapacityRatio float64) (*storage.ServerStats, error) {
	if nearCapacityRatio <= 0 {
		nearCapacityRatio = DefaultNearCapacityRatio
	}

	// Build the bucket projections. The edges are compiled in, never taken
	// from a caller, so there is nothing here to inject.
	buckets := ""
	for _, b := range depthBucketEdges {
		if b.max < 0 {
			buckets += fmt.Sprintf(",\n\t\t\tCOUNT(*) FILTER (WHERE n >= %d)", b.min)
			continue
		}
		buckets += fmt.Sprintf(",\n\t\t\tCOUNT(*) FILTER (WHERE n BETWEEN %d AND %d)", b.min, b.max)
	}

	query := fmt.Sprintf(`
		WITH per_mailbox AS (
			SELECT id, max_messages, message_count AS n, message_bytes AS bytes
			FROM mailboxes
		)
		SELECT
			COUNT(*),
			COALESCE(SUM(n), 0),
			COALESCE(SUM(bytes), 0),
			-- The cast is load-bearing. Without it Postgres infers $1 from the
			-- integer column it multiplies and truncates the ratio: 0.9
			-- becomes 0, and every mailbox counts as near capacity.
			COUNT(*) FILTER (WHERE max_messages > 0 AND n >= $1::double precision * max_messages),
			pg_database_size(current_database())%s
		FROM per_mailbox`, buckets)

	dest := make([]any, 0, 5+len(depthBucketEdges))
	var (
		mailboxes    int
		messages     int64
		messageBytes int64
		nearCapacity int
		dbBytes      int64
	)
	counts := make([]int, len(depthBucketEdges))

	dest = append(dest, &mailboxes, &messages, &messageBytes, &nearCapacity, &dbBytes)
	for i := range counts {
		dest = append(dest, &counts[i])
	}

	if err := s.pool.QueryRow(ctx, query, nearCapacityRatio).Scan(dest...); err != nil {
		return nil, fmt.Errorf("aggregate server stats: %w", err)
	}

	depth := make([]storage.DepthBucket, len(depthBucketEdges))
	for i, b := range depthBucketEdges {
		depth[i] = storage.DepthBucket{
			Label:     b.label,
			Min:       b.min,
			Max:       b.max,
			Mailboxes: counts[i],
		}
	}

	return &storage.ServerStats{
		SampledAt:             time.Now(),
		Mailboxes:             mailboxes,
		Messages:              messages,
		MessageBytes:          messageBytes,
		DatabaseBytes:         dbBytes,
		MailboxesNearCapacity: nearCapacity,
		NearCapacityRatio:     nearCapacityRatio,
		Depth:                 depth,
	}, nil
}
