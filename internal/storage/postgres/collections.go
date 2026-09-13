package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// =============================================================================
// Collection Operations
// =============================================================================

func (s *PostgresStorage) CreateCollection(ctx context.Context, ownerID peer.ID, path, name string, visibility core.Visibility) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := s.pool.QueryRow(ctx, `
		INSERT INTO collections (owner_peer_id, path, name, visibility)
		VALUES ($1, $2, $3, $4)
		RETURNING id, owner_peer_id, path, name, created_at, last_modified_at, record_count, visibility`,
		ownerID.String(), path, name, int(visibility),
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name, &r.CreatedAt, &r.LastModifiedAt, &r.RecordCount, &r.Visibility)
	if err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	s.logger.Debug("Created collection", "path", r.FullPath())
	return &r, nil
}

func (s *PostgresStorage) GetCollection(ctx context.Context, ownerID peer.ID, path string) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, path, name, created_at, last_modified_at, record_count, visibility
		FROM collections
		WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name, &r.CreatedAt, &r.LastModifiedAt, &r.RecordCount, &r.Visibility)

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

func (s *PostgresStorage) ListCollections(ctx context.Context, ownerID, readerID peer.ID) ([]*storage.CollectionRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_peer_id, path, name, created_at, last_modified_at, record_count, visibility
		FROM collections c WHERE owner_peer_id = $1
		  AND `+readableBy("c.id", "collection_acls", "collection_id", "$2")+`
		ORDER BY path`,
		ownerID.String(), readerID.String(),
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
			return nil, false, fmt.Errorf("insert collection item: %w", contentError(err))
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
			return nil, false, fmt.Errorf("update collection item: %w", contentError(err))
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

// Paging over collection items is keyset: a cursor names the last item seen
// (its sort value and key) and the next page starts strictly after it, so a
// page costs the rows it returns however deep into the collection it is.
// OFFSET paging is still honoured for callers that use it, but it re-reads
// and discards every skipped row, and a listing paged that way ran the JSONB
// filter twice per page: once to count and once to fetch. The count is now a
// window over the same pass, taken on a first page only.

// itemCursor is the opaque cursor a page hands back.
type itemCursor struct {
	Sort string `json:"s"`
	Key  string `json:"k"`
}

func encodeItemCursor(sort, key string) string {
	b, _ := json.Marshal(itemCursor{Sort: sort, Key: key})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeItemCursor(cursor string) (itemCursor, error) {
	var c itemCursor
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return c, fmt.Errorf("%w: %v", storage.ErrInvalidCursor, err)
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("%w: %v", storage.ErrInvalidCursor, err)
	}
	if c.Key == "" {
		return c, fmt.Errorf("%w: no key", storage.ErrInvalidCursor)
	}
	return c, nil
}

func clampCollectionLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (s *PostgresStorage) ListCollectionKeys(ctx context.Context, collectionID int64, limit, offset int, cursor string) ([]string, int, string, error) {
	limit = clampCollectionLimit(limit)

	conditions := []string{"collection_id = $1"}
	args := []any{collectionID}
	countExpr := "COUNT(*) OVER()"
	if cursor != "" {
		c, err := decodeItemCursor(cursor)
		if err != nil {
			return nil, 0, "", err
		}
		conditions = append(conditions, fmt.Sprintf("key > $%d", len(args)+1))
		args = append(args, c.Key)
		countExpr = "-1"
		offset = 0
	}
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit+1, offset)

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT key, %s AS total_count
		FROM collection_items
		WHERE %s
		ORDER BY key
		LIMIT $%d OFFSET $%d`, countExpr, strings.Join(conditions, " AND "), len(args)-1, len(args)),
		args...,
	)
	if err != nil {
		return nil, 0, "", fmt.Errorf("list collection keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	total := -1
	for rows.Next() {
		var key string
		var tc int
		if err := rows.Scan(&key, &tc); err != nil {
			return nil, 0, "", fmt.Errorf("scan collection key: %w", err)
		}
		keys = append(keys, key)
		total = tc
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}
	if cursor == "" && total < 0 {
		total = 0
	}
	next := ""
	if len(keys) > limit {
		keys = keys[:limit]
		next = encodeItemCursor("", keys[len(keys)-1])
	}
	return keys, total, next, nil
}

func (s *PostgresStorage) QueryCollection(ctx context.Context, collectionID int64, filter map[string]any, sortField string, sortAsc bool, limit, offset int, cursor string) (*storage.CollectionQueryResult, error) {
	limit = clampCollectionLimit(limit)

	filterClause, filterArgs, err := BuildJSONBFilter(filter, 2) // $1 is collection_id
	if err != nil {
		return nil, fmt.Errorf("build filter: %w", err)
	}
	args := []any{collectionID}
	args = append(args, filterArgs...)
	conditions := []string{"collection_id = $1", filterClause}

	// The sort expression is what the cursor carries, so a missing field
	// sorts as the empty string in both places rather than as NULL, whose
	// position depends on direction and cannot be named in a comparison.
	dir, cmp := "ASC", ">"
	if !sortAsc {
		dir, cmp = "DESC", "<"
	}
	sortExpr := "''"
	orderClause := "key " + dir
	if sortField != "" {
		if !validFieldName.MatchString(sortField) {
			return nil, fmt.Errorf("invalid sort field name: %q", sortField)
		}
		sortExpr = fmt.Sprintf("COALESCE(content->>'%s', '')", sortField)
		orderClause = fmt.Sprintf("%s %s, key %s", sortExpr, dir, dir)
	}

	countExpr := "COUNT(*) OVER()"
	if cursor != "" {
		c, err := decodeItemCursor(cursor)
		if err != nil {
			return nil, err
		}
		n := len(args) + 1
		if sortField != "" {
			conditions = append(conditions, fmt.Sprintf("(%s, key) %s ($%d, $%d)", sortExpr, cmp, n, n+1))
			args = append(args, c.Sort, c.Key)
		} else {
			conditions = append(conditions, fmt.Sprintf("key %s $%d", cmp, n))
			args = append(args, c.Key)
		}
		countExpr = "-1"
		offset = 0
	}
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit+1, offset) // limit+1 for hasMore detection

	query := fmt.Sprintf(`
		SELECT id, collection_id, key, content, content_hash, version,
			   created_at, updated_at, updated_by_peer_id,
			   %s AS sort_key, %s AS total_count
		FROM collection_items
		WHERE %s
		ORDER BY %s
		LIMIT $%d OFFSET $%d`,
		sortExpr, countExpr, strings.Join(conditions, " AND "), orderClause, len(args)-1, len(args),
	)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query collection: %w", err)
	}
	defer rows.Close()

	var (
		items    []*storage.CollectionItemRecord
		sortKeys []string
		total    = -1
	)
	for rows.Next() {
		var r storage.CollectionItemRecord
		var sortKey string
		var tc int
		if err := rows.Scan(&r.ID, &r.CollectionID, &r.Key, &r.Content, &r.ContentHash,
			&r.Version, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID, &sortKey, &tc); err != nil {
			return nil, fmt.Errorf("scan collection item: %w", err)
		}
		items = append(items, &r)
		sortKeys = append(sortKeys, sortKey)
		total = tc
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if cursor == "" && total < 0 {
		total = 0
	}

	result := &storage.CollectionQueryResult{TotalCount: total}
	if len(items) > limit {
		items = items[:limit]
		result.HasMore = true
		result.NextCursor = encodeItemCursor(sortKeys[limit-1], items[limit-1].Key)
	}
	result.Items = items
	return result, nil
}

// =============================================================================
// Helpers
// =============================================================================

func scanCollectionRecord(rows pgx.Rows) (*storage.CollectionRecord, error) {
	var r storage.CollectionRecord
	err := rows.Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Name,
		&r.CreatedAt, &r.LastModifiedAt, &r.RecordCount, &r.Visibility)
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

// contentError recognises the database refusing the content itself, as
// opposed to failing. jsonb has no representation for a NUL character, so a
// \u0000 escape in otherwise valid JSON fails at insert with SQLSTATE 22P05,
// and text outside the database encoding fails with 22021; both are the
// client's content and neither is a fault in the server, so they are wrapped
// in ErrInvalidContent with the database's own words. Anything else passes
// through unchanged.
func contentError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "22P05", "22021":
			return fmt.Errorf("%w: %s", storage.ErrInvalidContent, pgErr.Message)
		}
	}
	return err
}
