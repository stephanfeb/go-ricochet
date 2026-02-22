package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/storage"
)

// =============================================================================
// Collection Operations
// =============================================================================

func (s *PostgresStorage) CreateCollection(ctx context.Context, ownerID peer.ID, path, name string) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := s.pool.QueryRow(ctx, `
		INSERT INTO collections (owner_peer_id, path, name)
		VALUES ($1, $2, $3)
		RETURNING id, owner_peer_id, path, name, created_at, last_modified_at, record_count`,
		ownerID.String(), path, name,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name, &r.CreatedAt, &r.LastModifiedAt, &r.RecordCount)
	if err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	s.logger.Info("Created collection", "path", r.FullPath())
	return &r, nil
}

func (s *PostgresStorage) GetCollection(ctx context.Context, ownerID peer.ID, path string) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, path, name, created_at, last_modified_at, record_count
		FROM collections
		WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name, &r.CreatedAt, &r.LastModifiedAt, &r.RecordCount)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get collection: %w", err)
	}
	return &r, nil
}

func (s *PostgresStorage) DeleteCollection(ctx context.Context, ownerID peer.ID, path string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM collections WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	)
	if err != nil {
		return false, fmt.Errorf("delete collection: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PostgresStorage) ListCollections(ctx context.Context, ownerID peer.ID) ([]*storage.CollectionRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_peer_id, path, name, created_at, last_modified_at, record_count
		FROM collections WHERE owner_peer_id = $1
		ORDER BY path`,
		ownerID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	defer rows.Close()

	var records []*storage.CollectionRecord
	for rows.Next() {
		r, err := scanCollectionRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// =============================================================================
// Collection Item Operations
// =============================================================================

func (s *PostgresStorage) GetCollectionItem(ctx context.Context, collectionID int64, key string) (*storage.CollectionItemRecord, error) {
	var r storage.CollectionItemRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, collection_id, key, content, content_hash, version,
			   created_at, updated_at, updated_by_peer_id
		FROM collection_items
		WHERE collection_id = $1 AND key = $2`,
		collectionID, key,
	).Scan(&r.ID, &r.CollectionID, &r.Key, &r.Content, &r.ContentHash,
		&r.Version, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get collection item: %w", err)
	}
	return &r, nil
}

func (s *PostgresStorage) PutCollectionItem(ctx context.Context, collectionID int64, key string, content []byte, updatedBy peer.ID, ifMatch *string) (*storage.CollectionItemRecord, bool, error) {
	hash := sha256.Sum256(content)
	contentHash := fmt.Sprintf("sha256:%x", hash)
	now := time.Now()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if item exists
	var existingHash string
	var existingVersion int
	err = tx.QueryRow(ctx, `
		SELECT content_hash, version FROM collection_items
		WHERE collection_id = $1 AND key = $2
		FOR UPDATE`, collectionID, key).Scan(&existingHash, &existingVersion)

	var r storage.CollectionItemRecord
	created := false

	if err == pgx.ErrNoRows {
		// INSERT new item
		created = true
		err = tx.QueryRow(ctx, `
			INSERT INTO collection_items (collection_id, key, content, content_hash, version,
										  created_at, updated_at, updated_by_peer_id)
			VALUES ($1, $2, $3, $4, 1, $5, $5, $6)
			RETURNING id, collection_id, key, content, content_hash, version,
					  created_at, updated_at, updated_by_peer_id`,
			collectionID, key, content, contentHash, now, updatedBy.String(),
		).Scan(&r.ID, &r.CollectionID, &r.Key, &r.Content, &r.ContentHash,
			&r.Version, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID)
		if err != nil {
			return nil, false, fmt.Errorf("insert collection item: %w", err)
		}

		// Increment record_count
		_, err = tx.Exec(ctx, `
			UPDATE collections SET record_count = record_count + 1, last_modified_at = $2
			WHERE id = $1`, collectionID, now)
		if err != nil {
			return nil, false, fmt.Errorf("increment record count: %w", err)
		}
	} else if err != nil {
		return nil, false, fmt.Errorf("check existing item: %w", err)
	} else {
		// Item exists — check ifMatch
		if ifMatch != nil && *ifMatch != existingHash {
			return nil, false, &storage.CollectionItemConflictError{
				ExpectedHash: *ifMatch, ActualHash: existingHash,
			}
		}

		// UPDATE existing item
		newVersion := existingVersion + 1
		err = tx.QueryRow(ctx, `
			UPDATE collection_items
			SET content = $3, content_hash = $4, version = $5, updated_at = $6, updated_by_peer_id = $7
			WHERE collection_id = $1 AND key = $2
			RETURNING id, collection_id, key, content, content_hash, version,
					  created_at, updated_at, updated_by_peer_id`,
			collectionID, key, content, contentHash, newVersion, now, updatedBy.String(),
		).Scan(&r.ID, &r.CollectionID, &r.Key, &r.Content, &r.ContentHash,
			&r.Version, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID)
		if err != nil {
			return nil, false, fmt.Errorf("update collection item: %w", err)
		}

		// Update last_modified_at (no count change)
		_, err = tx.Exec(ctx, `
			UPDATE collections SET last_modified_at = $2 WHERE id = $1`, collectionID, now)
		if err != nil {
			return nil, false, fmt.Errorf("update collection timestamp: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit tx: %w", err)
	}

	s.logger.Debug("Put collection item", "collectionId", collectionID, "key", key, "created", created)
	return &r, created, nil
}

func (s *PostgresStorage) DeleteCollectionItem(ctx context.Context, collectionID int64, key string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		DELETE FROM collection_items WHERE collection_id = $1 AND key = $2`,
		collectionID, key,
	)
	if err != nil {
		return false, fmt.Errorf("delete collection item: %w", err)
	}

	deleted := tag.RowsAffected() > 0
	if deleted {
		_, err = tx.Exec(ctx, `
			UPDATE collections SET record_count = record_count - 1, last_modified_at = NOW()
			WHERE id = $1`, collectionID)
		if err != nil {
			return false, fmt.Errorf("decrement record count: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	return deleted, nil
}

func (s *PostgresStorage) ListCollectionKeys(ctx context.Context, collectionID int64, limit, offset int) ([]string, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	rows, err := s.pool.Query(ctx, `
		SELECT key, COUNT(*) OVER() AS total_count
		FROM collection_items
		WHERE collection_id = $1
		ORDER BY key
		LIMIT $2 OFFSET $3`,
		collectionID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list collection keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	var totalCount int
	for rows.Next() {
		var key string
		var tc int
		if err := rows.Scan(&key, &tc); err != nil {
			return nil, 0, fmt.Errorf("scan collection key: %w", err)
		}
		keys = append(keys, key)
		totalCount = tc
	}
	return keys, totalCount, rows.Err()
}

func (s *PostgresStorage) QueryCollection(ctx context.Context, collectionID int64, filter map[string]any, sortField string, sortAsc bool, limit, offset int) (*storage.CollectionQueryResult, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	// Build JSONB filter clause
	filterClause, filterArgs, err := BuildJSONBFilter(filter, 2) // $1 is collection_id
	if err != nil {
		return nil, fmt.Errorf("build filter: %w", err)
	}

	// Build the query
	args := []any{collectionID}
	args = append(args, filterArgs...)

	whereClause := fmt.Sprintf("collection_id = $1 AND %s", filterClause)

	// Sort
	orderClause := "key ASC"
	if sortField != "" {
		if !validFieldName.MatchString(sortField) {
			return nil, fmt.Errorf("invalid sort field name: %q", sortField)
		}
		dir := "ASC"
		if !sortAsc {
			dir = "DESC"
		}
		orderClause = fmt.Sprintf("content->>'%s' %s", sortField, dir)
	}

	// Count total matches
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM collection_items WHERE %s`, whereClause)
	var totalCount int
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("count collection items: %w", err)
	}

	// Fetch items
	nextArgIdx := len(args) + 1
	query := fmt.Sprintf(`
		SELECT id, collection_id, key, content, content_hash, version,
			   created_at, updated_at, updated_by_peer_id
		FROM collection_items
		WHERE %s
		ORDER BY %s
		LIMIT $%d OFFSET $%d`,
		whereClause, orderClause, nextArgIdx, nextArgIdx+1,
	)
	args = append(args, limit+1, offset) // limit+1 for hasMore detection

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query collection: %w", err)
	}
	defer rows.Close()

	var items []*storage.CollectionItemRecord
	for rows.Next() {
		r, err := scanCollectionItemRecord(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	return &storage.CollectionQueryResult{
		Items:      items,
		TotalCount: totalCount,
		HasMore:    hasMore,
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

func scanCollectionRecord(rows pgx.Rows) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := rows.Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name,
		&r.CreatedAt, &r.LastModifiedAt, &r.RecordCount)
	if err != nil {
		return nil, fmt.Errorf("scan collection record: %w", err)
	}
	return &r, nil
}

func scanCollectionItemRecord(rows pgx.Rows) (*storage.CollectionItemRecord, error) {
	var r storage.CollectionItemRecord
	err := rows.Scan(&r.ID, &r.CollectionID, &r.Key, &r.Content, &r.ContentHash,
		&r.Version, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID)
	if err != nil {
		return nil, fmt.Errorf("scan collection item record: %w", err)
	}
	return &r, nil
}

