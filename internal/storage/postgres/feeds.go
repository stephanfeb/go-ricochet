package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/storage"
)

// =============================================================================
// Feed Operations
// =============================================================================

func (s *PostgresStorage) CreateFeed(ctx context.Context, ownerID peer.ID, path, title, description string) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := s.pool.QueryRow(ctx, `
		INSERT INTO feeds (owner_peer_id, path, title, description)
		VALUES ($1, $2, $3, $4)
		RETURNING id, owner_peer_id, path, title, description, entry_content_type,
				  created_at, last_entry_at, current_sequence, max_entries, max_age_days`,
		ownerID.String(), path, title, description,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays)
	if err != nil {
		return nil, fmt.Errorf("create feed: %w", err)
	}

	s.logger.Info("Created feed", "path", r.FullPath())
	return &r, nil
}

func (s *PostgresStorage) GetFeed(ctx context.Context, ownerID peer.ID, path string) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, path, title, description, entry_content_type,
			   created_at, last_entry_at, current_sequence, max_entries, max_age_days
		FROM feeds
		WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays)

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

func (s *PostgresStorage) ListFeeds(ctx context.Context, ownerID peer.ID) ([]*storage.FeedRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_peer_id, path, title, description, entry_content_type,
			   created_at, last_entry_at, current_sequence, max_entries, max_age_days
		FROM feeds WHERE owner_peer_id = $1
		ORDER BY path`,
		ownerID.String(),
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

// =============================================================================
// Helpers
// =============================================================================

func scanFeedRecord(rows pgx.Rows) (*storage.FeedRecord, error) {
	var r storage.FeedRecord
	err := rows.Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Title, &r.Description,
		&r.EntryContentType, &r.CreatedAt, &r.LastEntryAt, &r.CurrentSequence,
		&r.MaxEntries, &r.MaxAgeDays)
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
