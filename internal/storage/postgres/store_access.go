package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// =============================================================================
// Read authorization for documents, feeds and collections
// =============================================================================

// storeTables maps a store to its table, its reader-list table and the
// reader list's foreign key column.
type storeTables struct {
	table, acls, fk string
}

func tablesFor(kind storage.StoreKind) (storeTables, error) {
	switch kind {
	case storage.StoreDocument:
		return storeTables{"documents", "document_acls", "document_id"}, nil
	case storage.StoreFeed:
		return storeTables{"feeds", "feed_acls", "feed_id"}, nil
	case storage.StoreCollection:
		return storeTables{"collections", "collection_acls", "collection_id"}, nil
	}
	return storeTables{}, fmt.Errorf("unknown store kind %q", kind)
}

// readableBy is the SQL predicate that admits a row of a store table to a
// reader: the owner sees everything, anyone sees public rows, and a shared
// row is seen by the peers on its reader list. idExpr names the row's id
// (with its table alias), reader the placeholder holding the reader's
// peer ID. The visibility column is assumed to be in scope unqualified.
func readableBy(idExpr, acls, fk, reader string) string {
	return fmt.Sprintf(`(owner_peer_id = %[4]s
		  OR visibility = %[5]d
		  OR (visibility = %[6]d AND EXISTS (
			  SELECT 1 FROM %[2]s a WHERE a.%[3]s = %[1]s AND a.peer_id = %[4]s)))`,
		idExpr, acls, fk, reader, int(core.VisibilityPublic), int(core.VisibilityShared))
}

func (s *PostgresStorage) SetStoreVisibility(ctx context.Context, kind storage.StoreKind, ownerID peer.ID, path string, visibility core.Visibility) (bool, error) {
	t, err := tablesFor(kind)
	if err != nil {
		return false, err
	}
	if !visibility.Valid() {
		return false, fmt.Errorf("invalid visibility %d", visibility)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE `+t.table+` SET visibility = $3 WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path, int(visibility))
	if err != nil {
		return false, fmt.Errorf("set %s visibility: %w", kind, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PostgresStorage) GrantStoreReader(ctx context.Context, kind storage.StoreKind, id int64, reader peer.ID) error {
	t, err := tablesFor(kind)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO `+t.acls+` (`+t.fk+`, peer_id) VALUES ($1, $2)
		 ON CONFLICT (`+t.fk+`, peer_id) DO NOTHING`,
		id, reader.String())
	if err != nil {
		return fmt.Errorf("grant %s reader: %w", kind, err)
	}
	return nil
}

func (s *PostgresStorage) RevokeStoreReader(ctx context.Context, kind storage.StoreKind, id int64, reader peer.ID) error {
	t, err := tablesFor(kind)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`DELETE FROM `+t.acls+` WHERE `+t.fk+` = $1 AND peer_id = $2`,
		id, reader.String())
	if err != nil {
		return fmt.Errorf("revoke %s reader: %w", kind, err)
	}
	return nil
}

func (s *PostgresStorage) ListStoreReaders(ctx context.Context, kind storage.StoreKind, id int64) ([]*storage.StoreReader, error) {
	t, err := tablesFor(kind)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT peer_id, granted_at FROM `+t.acls+` WHERE `+t.fk+` = $1 ORDER BY granted_at, peer_id`,
		id)
	if err != nil {
		return nil, fmt.Errorf("list %s readers: %w", kind, err)
	}
	defer rows.Close()

	readers := []*storage.StoreReader{}
	for rows.Next() {
		var r storage.StoreReader
		if err := rows.Scan(&r.PeerID, &r.GrantedAt); err != nil {
			return nil, err
		}
		readers = append(readers, &r)
	}
	return readers, rows.Err()
}

func (s *PostgresStorage) IsStoreReader(ctx context.Context, kind storage.StoreKind, id int64, reader peer.ID) (bool, error) {
	t, err := tablesFor(kind)
	if err != nil {
		return false, err
	}
	var one int
	err = s.pool.QueryRow(ctx,
		`SELECT 1 FROM `+t.acls+` WHERE `+t.fk+` = $1 AND peer_id = $2`,
		id, reader.String()).Scan(&one)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check %s reader: %w", kind, err)
	}
	return true, nil
}
