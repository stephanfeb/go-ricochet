package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/twostack/go-ricochet/internal/storage"
)

// This file holds the cross-owner listings behind the operator surface.
//
// Every query here derives the message count from stored_messages rather than
// reading a counter off mailboxes, because there is no such counter and a
// cached one would be the thing most likely to be wrong at the moment somebody
// is investigating. The cost is a scan, which is why these are bounded and
// kept off the request path.

// ListMailboxUsage lists mailboxes with their live counts.
//
// The fill ratio is computed in SQL rather than in Go so that the ordering and
// the reported figure come from the same expression. Sorting on one and
// reporting the other is how a "top 20 fullest" list ends up not being sorted
// by fullness.
func (s *PostgresStorage) ListMailboxUsage(ctx context.Context, q storage.MailboxUsageQuery) ([]*storage.MailboxUsage, error) {
	limit := storage.ClampPageSize(q.Limit)
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}

	// The ORDER BY is chosen from a fixed set, never interpolated from input.
	order := "fill DESC, n DESC, id"
	if q.Sort == storage.SortByCount {
		order = "n DESC, id"
	}

	query := fmt.Sprintf(`
		WITH per_mailbox AS (
			SELECT
				m.id,
				m.owner_peer_id,
				m.folder_path,
				m.max_messages,
				COUNT(sm.id) AS n,
				COALESCE(SUM(octet_length(sm.payload)), 0) AS bytes,
				MAX(sm.created_at) AS last_message_at
			FROM mailboxes m
			LEFT JOIN stored_messages sm ON sm.mailbox_id = m.id
			WHERE $1::text = '' OR m.owner_peer_id = $1::text
			GROUP BY m.id
		)
		SELECT
			id, owner_peer_id, folder_path, max_messages, n, bytes, last_message_at,
			-- An uncapped mailbox has no fill ratio, so it sorts last however
			-- much it holds. Reporting one against a cap of zero would invent
			-- a division that does not exist.
			CASE WHEN max_messages > 0
				 THEN n::double precision / max_messages
				 ELSE 0 END AS fill
		FROM per_mailbox
		ORDER BY %s
		LIMIT $2 OFFSET $3`, order)

	rows, err := s.pool.Query(ctx, query, q.Owner, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list mailbox usage: %w", err)
	}
	defer rows.Close()

	var out []*storage.MailboxUsage
	for rows.Next() {
		var (
			u    storage.MailboxUsage
			last *time.Time
		)
		if err := rows.Scan(&u.MailboxID, &u.OwnerPeerID, &u.FolderPath,
			&u.MaxMessages, &u.MessageCount, &u.MessageBytes, &last,
			&u.FillRatio); err != nil {
			return nil, fmt.Errorf("scan mailbox usage: %w", err)
		}
		u.LastMessageAt = last
		out = append(out, &u)
	}
	return out, rows.Err()
}

// ListOwnerUsage totals storage per owner, largest first.
//
// The aggregation is two-stage — per mailbox, then per owner — because summing
// payload bytes directly over the join would need the mailbox count from a
// separate pass. One scan answers both.
func (s *PostgresStorage) ListOwnerUsage(ctx context.Context, limit, offset int) ([]*storage.OwnerUsage, error) {
	limit = storage.ClampPageSize(limit)
	if offset < 0 {
		offset = 0
	}

	rows, err := s.pool.Query(ctx, `
		WITH per_mailbox AS (
			SELECT
				m.id,
				m.owner_peer_id,
				COUNT(sm.id) AS n,
				COALESCE(SUM(octet_length(sm.payload)), 0) AS bytes
			FROM mailboxes m
			LEFT JOIN stored_messages sm ON sm.mailbox_id = m.id
			GROUP BY m.id
		)
		SELECT
			owner_peer_id,
			COUNT(*) AS mailboxes,
			COALESCE(SUM(n), 0) AS messages,
			COALESCE(SUM(bytes), 0) AS bytes
		FROM per_mailbox
		GROUP BY owner_peer_id
		ORDER BY bytes DESC, messages DESC, owner_peer_id
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list owner usage: %w", err)
	}
	defer rows.Close()

	var out []*storage.OwnerUsage
	for rows.Next() {
		var o storage.OwnerUsage
		if err := rows.Scan(&o.OwnerPeerID, &o.Mailboxes, &o.Messages, &o.MessageBytes); err != nil {
			return nil, fmt.Errorf("scan owner usage: %w", err)
		}
		out = append(out, &o)
	}
	return out, rows.Err()
}
