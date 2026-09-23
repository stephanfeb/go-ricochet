package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// This file holds the cross-owner listings behind the operator surface.
//
// Every figure comes from the counters on the mailbox row (message_count,
// message_bytes), which the store path and the delete trigger maintain and
// the maintenance sweep reconciles. They used to be derived by joining
// stored_messages and summing payload lengths on every call, a full scan of
// the largest table to answer "who is using the space".

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
				id, owner_peer_id, folder_path, max_messages,
				message_count AS n,
				message_bytes AS bytes,
				-- An uncapped mailbox has no fill ratio, so it sorts last
				-- however much it holds. Reporting one against a cap of zero
				-- would invent a division that does not exist.
				CASE WHEN max_messages > 0
					 THEN message_count::double precision / max_messages
					 ELSE 0 END AS fill
			FROM mailboxes
			WHERE $1::text = '' OR owner_peer_id = $1::text
		),
		page AS (
			SELECT * FROM per_mailbox
			ORDER BY %s
			LIMIT $2 OFFSET $3
		)
		-- Only the page pays for the newest-message lookup.
		SELECT
			p.id, p.owner_peer_id, p.folder_path, p.max_messages, p.n, p.bytes,
			(SELECT MAX(created_at) FROM stored_messages sm WHERE sm.mailbox_id = p.id),
			p.fill
		FROM page p
		ORDER BY %s`, order, order)

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
		SELECT
			owner_peer_id,
			COUNT(*) AS mailboxes,
			COALESCE(SUM(message_count), 0) AS messages,
			COALESCE(SUM(message_bytes), 0) AS bytes
		FROM mailboxes
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
