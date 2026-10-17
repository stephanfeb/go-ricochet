package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// =============================================================================
// Moderation
// =============================================================================

func (s *PostgresStorage) PutPeerBan(ctx context.Context, ban *storage.PeerBan) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO peer_bans (peer_id, reason, banned_by, banned_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (peer_id) DO UPDATE
		SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by, banned_at = EXCLUDED.banned_at`,
		ban.PeerID, ban.Reason, ban.BannedBy, ban.BannedAt,
	)
	if err != nil {
		return fmt.Errorf("put peer ban: %w", err)
	}
	return nil
}

func (s *PostgresStorage) DeletePeerBan(ctx context.Context, peerID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM peer_bans WHERE peer_id = $1`, peerID)
	if err != nil {
		return false, fmt.Errorf("delete peer ban: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PostgresStorage) ListPeerBans(ctx context.Context) ([]*storage.PeerBan, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT peer_id, reason, banned_by, banned_at FROM peer_bans ORDER BY banned_at`)
	if err != nil {
		return nil, fmt.Errorf("list peer bans: %w", err)
	}
	defer rows.Close()
	var out []*storage.PeerBan
	for rows.Next() {
		b := &storage.PeerBan{}
		if err := rows.Scan(&b.PeerID, &b.Reason, &b.BannedBy, &b.BannedAt); err != nil {
			return nil, fmt.Errorf("scan peer ban: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *PostgresStorage) FeedEntriesContaining(ctx context.Context, peerID peer.ID, pathPrefix string, needle []byte) ([]*storage.FeedEntryMatch, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT fe.id, fe.feed_id, fe.sequence_number, fe.content, fe.content_hash,
			   fe.created_at, fe.created_by_peer_id, fe.entry_type, f.owner_peer_id, f.path
		FROM feed_entries fe JOIN feeds f ON f.id = fe.feed_id
		WHERE (f.path = $2 OR f.path LIKE $3 ESCAPE '\')
		  AND (f.owner_peer_id = $1 OR fe.created_by_peer_id = $1)
		  AND position($4::bytea IN fe.content) > 0
		ORDER BY fe.feed_id, fe.sequence_number`,
		peerID.String(), pathPrefix, likePrefix(pathPrefix+"/"), needle,
	)
	if err != nil {
		return nil, fmt.Errorf("find feed entries: %w", err)
	}
	defer rows.Close()
	var out []*storage.FeedEntryMatch
	for rows.Next() {
		m := &storage.FeedEntryMatch{}
		if err := rows.Scan(&m.ID, &m.FeedID, &m.SequenceNumber, &m.Content, &m.ContentHash,
			&m.CreatedAt, &m.CreatedByPeerID, &m.EntryType, &m.FeedOwnerPeerID, &m.FeedPath); err != nil {
			return nil, fmt.Errorf("scan feed entry: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *PostgresStorage) DeleteFeedEntries(ctx context.Context, ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM feed_entries WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, fmt.Errorf("delete feed entries: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStorage) DeleteDocumentsWithPrefix(ctx context.Context, ownerID peer.ID, prefix string) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM documents WHERE owner_peer_id = $1 AND path LIKE $2 ESCAPE '\'`,
		ownerID.String(), likePrefix(prefix),
	)
	if err != nil {
		return 0, fmt.Errorf("delete documents: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// likePrefix is a LIKE pattern matching strings that start with prefix.
func likePrefix(prefix string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(prefix) + "%"
}
