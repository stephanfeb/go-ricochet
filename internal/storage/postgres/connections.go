package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// =============================================================================
// Connection log
// =============================================================================

func (s *PostgresStorage) InsertPeerConnection(ctx context.Context, c *storage.PeerConnection) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO peer_connections (peer_id, ip, remote_addr, transport, direction, connected_at, disconnected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		c.PeerID, c.IP, c.RemoteAddr, c.Transport, c.Direction, c.ConnectedAt, c.DisconnectedAt,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert peer connection: %w", err)
	}
	return id, nil
}

func (s *PostgresStorage) MarkPeerDisconnected(ctx context.Context, id int64, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE peer_connections SET disconnected_at = $2
		WHERE id = $1 AND disconnected_at IS NULL`, id, at)
	if err != nil {
		return fmt.Errorf("mark peer disconnected: %w", err)
	}
	return nil
}

func (s *PostgresStorage) ListPeerConnections(ctx context.Context, peerID string, since *time.Time, limit int) ([]*storage.PeerConnection, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, peer_id, ip, remote_addr, transport, direction, connected_at, disconnected_at
		FROM peer_connections
		WHERE peer_id = $1 AND ($2::timestamptz IS NULL OR connected_at >= $2)
		ORDER BY connected_at DESC, id DESC
		LIMIT $3`,
		peerID, since, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list peer connections: %w", err)
	}
	defer rows.Close()
	var out []*storage.PeerConnection
	for rows.Next() {
		c := &storage.PeerConnection{}
		if err := rows.Scan(&c.ID, &c.PeerID, &c.IP, &c.RemoteAddr, &c.Transport, &c.Direction,
			&c.ConnectedAt, &c.DisconnectedAt); err != nil {
			return nil, fmt.Errorf("scan peer connection: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *PostgresStorage) PurgePeerConnections(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM peer_connections WHERE connected_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("purge peer connections: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
