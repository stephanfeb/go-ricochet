package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// =============================================================================
// Feed Operations
// =============================================================================

// CreateFeed creates a feed, or returns the existing one if (owner, path) is
// already taken. The upsert is what makes the SFA auto-create path safe: two
// concurrent non-owner APPENDs can both see no feed and both try to create it,
// and the loser of that race would otherwise get a unique violation. Title and
// description are only overwritten when non-empty, since the auto-create path
// passes "" for both and must not wipe an existing feed's metadata.
// collaborative_mode is deliberately left alone on conflict — a re-create must
// not silently flip an existing feed's access model.
// Visibility is set on creation only, like collaborative_mode; the ACCESS
// operation changes it afterwards.
func (s *PostgresStorage) CreateFeed(ctx context.Context, ownerID peer.ID, path, title, description string, collaborative bool, visibility core.Visibility) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := s.pool.QueryRow(ctx, `
		INSERT INTO feeds (owner_peer_id, path, title, description, collaborative_mode, visibility)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT ON CONSTRAINT uq_feed_owner_path DO UPDATE SET
			title = COALESCE(NULLIF(EXCLUDED.title, ''), feeds.title),
			description = COALESCE(NULLIF(EXCLUDED.description, ''), feeds.description)
		RETURNING id, owner_peer_id, path, title, description, entry_content_type,
				  created_at, last_entry_at, current_sequence, max_entries, max_age_days,
				  collaborative_mode, visibility`,
		ownerID.String(), path, title, description, collaborative, int(visibility),
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays, &r.CollaborativeMode, &r.Visibility)
	if err != nil {
		return nil, fmt.Errorf("create feed: %w", err)
	}

	s.logger.Debug("Created feed", "path", r.FullPath())
	return &r, nil
}

func (s *PostgresStorage) GetFeed(ctx context.Context, ownerID peer.ID, path string) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, path, title, description, entry_content_type,
			   created_at, last_entry_at, current_sequence, max_entries, max_age_days,
			   collaborative_mode, visibility
		FROM feeds
		WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays, &r.CollaborativeMode, &r.Visibility)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get feed: %w", err)
	}
	return &r, nil
}

func (s *PostgresStorage) DeleteFeed(ctx context.Context, ownerID peer.ID, path string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM feeds WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	)
	if err != nil {
		return false, fmt.Errorf("delete feed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PostgresStorage) ListFeeds(ctx context.Context, ownerID, readerID peer.ID) ([]*storage.FeedRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_peer_id, path, title, description, entry_content_type,
			   created_at, last_entry_at, current_sequence, max_entries, max_age_days,
			   collaborative_mode, visibility
		FROM feeds f WHERE owner_peer_id = $1
		  AND `+readableBy("f.id", "feed_acls", "feed_id", "$2")+`
		ORDER BY path`,
		ownerID.String(), readerID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("list feeds: %w", err)
	}
	defer rows.Close()

	var records []*storage.FeedRecord
	for rows.Next() {
		r, err := scanFeedRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// =============================================================================
// Feed Entry Operations
// =============================================================================

func (s *PostgresStorage) AppendFeedEntry(ctx context.Context, feedID int64, content []byte, createdBy peer.ID, entryType string) (*storage.FeedEntryRecord, error) {
	hash := sha256.Sum256(content)
	contentHash := fmt.Sprintf("sha256:%x", hash)
	now := time.Now()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Atomically increment the feed's current_sequence and get the new value
	var newSeq int
	err = tx.QueryRow(ctx, `
		UPDATE feeds
		SET current_sequence = current_sequence + 1, last_entry_at = $2
		WHERE id = $1
		RETURNING current_sequence`,
		feedID, now,
	).Scan(&newSeq)
	if err != nil {
		return nil, fmt.Errorf("increment feed sequence: %w", err)
	}

	// Insert the new entry
	var r storage.FeedEntryRecord
	err = tx.QueryRow(ctx, `
		INSERT INTO feed_entries (feed_id, sequence_number, content, content_hash,
								 created_at, created_by_peer_id, entry_type)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, feed_id, sequence_number, content, content_hash,
				  created_at, created_by_peer_id, entry_type`,
		feedID, newSeq, content, contentHash, now, createdBy.String(), entryType,
	).Scan(&r.ID, &r.FeedID, &r.SequenceNumber, &r.Content, &r.ContentHash,
		&r.CreatedAt, &r.CreatedByPeerID, &r.EntryType)
	if err != nil {
		return nil, fmt.Errorf("insert feed entry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	s.logger.Debug("Appended feed entry", "feedId", feedID, "sequence", newSeq)
	return &r, nil
}

func (s *PostgresStorage) GetFeedEntry(ctx context.Context, feedID int64, sequenceNumber int) (*storage.FeedEntryRecord, error) {
	var r storage.FeedEntryRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, feed_id, sequence_number, content, content_hash,
			   created_at, created_by_peer_id, entry_type
		FROM feed_entries
		WHERE feed_id = $1 AND sequence_number = $2`,
		feedID, sequenceNumber,
	).Scan(&r.ID, &r.FeedID, &r.SequenceNumber, &r.Content, &r.ContentHash,
		&r.CreatedAt, &r.CreatedByPeerID, &r.EntryType)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get feed entry: %w", err)
	}
	return &r, nil
}

func (s *PostgresStorage) GetFeedEntries(ctx context.Context, feedID int64, fromSeq, toSeq *int, entryType string, limit int) ([]*storage.FeedEntryRecord, bool, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	conditions := []string{"feed_id = $1"}
	args := []any{feedID}
	argIdx := 2

	if fromSeq != nil {
		conditions = append(conditions, fmt.Sprintf("sequence_number >= $%d", argIdx))
		args = append(args, *fromSeq)
		argIdx++
	}

	if toSeq != nil {
		conditions = append(conditions, fmt.Sprintf("sequence_number <= $%d", argIdx))
		args = append(args, *toSeq)
		argIdx++
	}

	if entryType != "" {
		conditions = append(conditions, fmt.Sprintf("entry_type = $%d", argIdx))
		args = append(args, entryType)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT id, feed_id, sequence_number, content, content_hash,
			   created_at, created_by_peer_id, entry_type
		FROM feed_entries
		WHERE %s
		ORDER BY sequence_number ASC
		LIMIT $%d`,
		strings.Join(conditions, " AND "), argIdx,
	)
	args = append(args, limit+1) // fetch one extra to detect hasMore

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("get feed entries: %w", err)
	}
	defer rows.Close()

	var records []*storage.FeedEntryRecord
	for rows.Next() {
		r, err := scanFeedEntryRecord(rows)
		if err != nil {
			return nil, false, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	return records, hasMore, nil
}

// =============================================================================
// Batch Feed Retrieval
// =============================================================================

// maxConcurrentFeedQueries bounds how many feeds in one batch are read from the
// database at once, so a large batch can't starve the rest of the pool.
const maxConcurrentFeedQueries = 10

// GetMultiFeedEntries retrieves entries from multiple feeds, fetching each
// feed's entries in parallel. Results are keyed by "ownerPeerID/path".
//
// A per-feed failure is reported in that feed's MultiFeedResult.Error rather
// than failing the whole batch, so one bad path can't sink the request.
func (s *PostgresStorage) GetMultiFeedEntries(ctx context.Context, queries []storage.MultiFeedQuery) (map[string]*storage.MultiFeedResult, error) {
	results := make(map[string]*storage.MultiFeedResult, len(queries))

	type feedRef struct {
		key   string
		id    int64
		query storage.MultiFeedQuery
	}

	// Resolve each (owner, path) to a feed ID, recording per-feed errors as we go.
	refs := make([]feedRef, 0, len(queries))
	for _, q := range queries {
		key := q.OwnerPeerID + "/" + q.Path
		ownerID, err := peer.Decode(q.OwnerPeerID)
		if err != nil {
			results[key] = &storage.MultiFeedResult{Error: "invalid ownerPeerId"}
			continue
		}

		feed, err := s.GetFeed(ctx, ownerID, q.Path)
		if err != nil {
			results[key] = &storage.MultiFeedResult{Error: err.Error()}
			continue
		}
		if feed == nil {
			results[key] = &storage.MultiFeedResult{Error: "feed not found"}
			continue
		}

		refs = append(refs, feedRef{key: key, id: feed.ID, query: q})
	}

	// Fetch each feed's entries concurrently, bounded by a semaphore so a large
	// batch can't monopolise the connection pool.
	type indexedResult struct {
		key    string
		result *storage.MultiFeedResult
	}
	ch := make(chan indexedResult, len(refs))

	sem := make(chan struct{}, maxConcurrentFeedQueries)
	var wg sync.WaitGroup

	for _, ref := range refs {
		wg.Add(1)
		go func(r feedRef) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			limit := r.query.Limit
			if limit <= 0 {
				limit = 50
			}

			entries, hasMore, err := s.GetFeedEntries(ctx, r.id, r.query.FromSequence, nil, "", limit)
			if err != nil {
				ch <- indexedResult{key: r.key, result: &storage.MultiFeedResult{Error: err.Error()}}
				return
			}
			ch <- indexedResult{key: r.key, result: &storage.MultiFeedResult{Entries: entries, HasMore: hasMore}}
		}(ref)
	}

	wg.Wait()
	close(ch)

	for ir := range ch {
		results[ir.key] = ir.result
	}

	return results, nil
}

// =============================================================================
// Feed Retention
// =============================================================================

func (s *PostgresStorage) EnforceFeedRetention(ctx context.Context, feed *storage.FeedRecord) (int, error) {
	totalDeleted := 0

	// Time-based retention
	if feed.MaxAgeDays != nil && *feed.MaxAgeDays > 0 {
		tag, err := s.pool.Exec(ctx, `
			DELETE FROM feed_entries
			WHERE feed_id = $1
			  AND created_at < NOW() - INTERVAL '1 day' * $2`,
			feed.ID, *feed.MaxAgeDays,
		)
		if err != nil {
			return 0, fmt.Errorf("enforce feed age retention: %w", err)
		}
		totalDeleted += int(tag.RowsAffected())
	}

	// Count-based retention
	if feed.MaxEntries != nil && *feed.MaxEntries > 0 {
		tag, err := s.pool.Exec(ctx, `
			DELETE FROM feed_entries
			WHERE id IN (
				SELECT id FROM feed_entries
				WHERE feed_id = $1
				ORDER BY sequence_number DESC
				OFFSET $2
			)`,
			feed.ID, *feed.MaxEntries,
		)
		if err != nil {
			return totalDeleted, fmt.Errorf("enforce feed count retention: %w", err)
		}
		totalDeleted += int(tag.RowsAffected())
	}

	if totalDeleted > 0 {
		s.logger.Info("Enforced feed retention", "feedId", feed.ID, "deleted", totalDeleted)
	}
	return totalDeleted, nil
}

// EnforceAllFeedRetention applies each feed's own limits in one pass. Until
// it existed, EnforceFeedRetention had no caller: a feed's max_entries and
// max_age_days were stored and never acted on.
func (s *PostgresStorage) EnforceAllFeedRetention(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM feed_entries fe
		USING feeds f
		WHERE fe.feed_id = f.id
		  AND f.max_age_days IS NOT NULL AND f.max_age_days > 0
		  AND fe.created_at < NOW() - INTERVAL '1 day' * f.max_age_days`)
	if err != nil {
		return 0, fmt.Errorf("feed age retention: %w", err)
	}
	deleted := int(tag.RowsAffected())

	tag, err = s.pool.Exec(ctx, `
		DELETE FROM feed_entries
		WHERE id IN (
			SELECT fe.id
			FROM feed_entries fe
			JOIN feeds f ON f.id = fe.feed_id
			WHERE f.max_entries IS NOT NULL AND f.max_entries > 0
			  AND fe.sequence_number <= (
				SELECT sequence_number FROM feed_entries x
				WHERE x.feed_id = f.id
				ORDER BY sequence_number DESC
				OFFSET f.max_entries LIMIT 1
			  )
		)`)
	if err != nil {
		return deleted, fmt.Errorf("feed count retention: %w", err)
	}
	deleted += int(tag.RowsAffected())
	if deleted > 0 {
		s.logger.Info("Feed retention sweep removed entries", "count", deleted)
	}
	return deleted, nil
}

func (s *PostgresStorage) CountFeedEntries(ctx context.Context, feedID int64) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM feed_entries WHERE feed_id = $1`, feedID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count feed entries: %w", err)
	}
	return n, nil
}

// =============================================================================
// Helpers
// =============================================================================

func scanFeedRecord(rows pgx.Rows) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := rows.Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays, &r.CollaborativeMode, &r.Visibility)
	if err != nil {
		return nil, fmt.Errorf("scan feed record: %w", err)
	}
	return &r, nil
}

func scanFeedEntryRecord(rows pgx.Rows) (*storage.FeedEntryRecord, error) {
	var r storage.FeedEntryRecord
	err := rows.Scan(&r.ID, &r.FeedID, &r.SequenceNumber, &r.Content, &r.ContentHash,
		&r.CreatedAt, &r.CreatedByPeerID, &r.EntryType)
	if err != nil {
		return nil, fmt.Errorf("scan feed entry record: %w", err)
	}
	return &r, nil
}
