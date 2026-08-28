package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

const (
	// expiryDeleteBatchSize bounds each DELETE in the expired-message sweep.
	expiryDeleteBatchSize = 5000

	// maxExpiryDeleteBatches caps one sweep so maintenance cannot run
	// unboundedly if messages expire as fast as they are deleted.
	maxExpiryDeleteBatches = 200
)

// PostgresStorage implements the storage.Storage interface using PostgreSQL.
type PostgresStorage struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresStorage creates a new PostgreSQL storage backend.
func NewPostgresStorage(cfg *core.PostgresConfig, logger *slog.Logger) (*PostgresStorage, error) {
	if logger == nil {
		logger = slog.Default()
	}
	return &PostgresStorage{
		logger: logger.With("component", "postgres"),
	}, nil
}

// Initialize creates the connection pool and verifies connectivity.
func (s *PostgresStorage) Initialize(ctx context.Context) error {
	return fmt.Errorf("Initialize must be called with InitializeWithConfig")
}

// InitializeWithConfig creates the connection pool using the provided config.
func (s *PostgresStorage) InitializeWithConfig(ctx context.Context, cfg *core.PostgresConfig) error {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s pool_max_conns=%d",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password, cfg.SSLMode, cfg.PoolSize)

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("parse postgres config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}

	s.pool = pool
	s.logger.Info("PostgreSQL connection pool initialized",
		"host", cfg.Host, "port", cfg.Port, "database", cfg.Database)
	return nil
}

// Close closes the connection pool.
func (s *PostgresStorage) Close() error {
	if s.pool != nil {
		s.pool.Close()
		s.logger.Info("PostgreSQL connection pool closed")
	}
	return nil
}

// Pool returns the underlying connection pool.
func (s *PostgresStorage) Pool() *pgxpool.Pool {
	return s.pool
}

// =============================================================================
// Mailbox Operations
// =============================================================================

func (s *PostgresStorage) GetOrCreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (*storage.MailboxRecord, error) {
	// Try to find existing
	mailbox, err := s.FindMailbox(ctx, addr.OwnerID, addr.FolderPath)
	if err != nil {
		return nil, err
	}
	if mailbox != nil {
		return mailbox, nil
	}

	// Create new
	row := s.pool.QueryRow(ctx, `
		INSERT INTO mailboxes (
			owner_peer_id, folder_path, mailbox_type, max_messages,
			retention_days, retention_count
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, owner_peer_id, folder_path, mailbox_type,
				  created_at, last_access_at, max_messages,
				  retention_days, retention_count`,
		addr.OwnerID.String(), addr.FolderPath, int(addr.Type),
		maxMessages, retentionDays, retentionCount,
	)

	record, err := scanMailboxRecord(row)
	if err != nil {
		return nil, fmt.Errorf("create mailbox: %w", err)
	}

	s.logger.Info("Created mailbox", "path", addr.FullPath(), "type", addr.Type)
	return record, nil
}

func (s *PostgresStorage) FindMailbox(ctx context.Context, ownerID peer.ID, folderPath string) (*storage.MailboxRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, folder_path, mailbox_type,
			   created_at, last_access_at, max_messages,
			   retention_days, retention_count
		FROM mailboxes
		WHERE owner_peer_id = $1 AND folder_path = $2`,
		ownerID.String(), folderPath,
	)

	record, err := scanMailboxRecord(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find mailbox: %w", err)
	}
	return record, nil
}

func (s *PostgresStorage) ListMailboxes(ctx context.Context, ownerID peer.ID) ([]*storage.MailboxRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, owner_peer_id, folder_path, mailbox_type,
			   created_at, last_access_at, max_messages,
			   retention_days, retention_count
		FROM mailboxes
		WHERE owner_peer_id = $1`,
		ownerID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("list mailboxes: %w", err)
	}
	defer rows.Close()

	var records []*storage.MailboxRecord
	for rows.Next() {
		r, err := scanMailboxRecordFromRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *PostgresStorage) UpdateMailbox(ctx context.Context, mailbox *storage.MailboxRecord) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE mailboxes
		SET max_messages = $1, retention_days = $2, retention_count = $3, last_access_at = $4
		WHERE id = $5`,
		mailbox.MaxMessages, mailbox.RetentionDays, mailbox.RetentionCount, mailbox.LastAccessAt, mailbox.ID,
	)
	return err
}

func (s *PostgresStorage) DeleteMailbox(ctx context.Context, mailboxID int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM mailboxes WHERE id = $1`, mailboxID)
	return err
}

func (s *PostgresStorage) UpdateMailboxAccess(ctx context.Context, mailboxID int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE mailboxes SET last_access_at = NOW() WHERE id = $1`, mailboxID)
	return err
}

// =============================================================================
// Message Operations
// =============================================================================

// StoreMessage assigns the next sequence number and inserts the message in one
// transaction. The counter is incremented with UPDATE ... RETURNING, which locks
// the mailbox row for the rest of the transaction, so two concurrent deliveries
// to the same mailbox serialise rather than both reading the same value.
//
// The previous implementation read SELECT MAX(sequence_number) + 1 in a separate
// statement, which two deliveries could both execute before either inserted.
// Nothing enforces uniqueness on (mailbox_id, sequence_number), so the duplicate
// was accepted -- it corrupted retrieval order and reader cursors instead of
// raising an error.
func (s *PostgresStorage) StoreMessage(ctx context.Context, mailbox *storage.MailboxRecord, msg *core.Message) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin store message: %w", err)
	}
	defer tx.Rollback(ctx)

	var seq int
	err = tx.QueryRow(ctx, `
		UPDATE mailboxes
		SET current_sequence = current_sequence + 1
		WHERE id = $1
		RETURNING current_sequence`,
		mailbox.ID,
	).Scan(&seq)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, storage.ErrMailboxNotFound
		}
		return 0, fmt.Errorf("increment mailbox sequence: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO stored_messages (
			mailbox_id, sequence_number, message_id, recipient_peer_id,
			sender_peer_id, payload, priority, created_at, expires_at,
			hop_count, flags_bitmap, persistent, folder_path
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		mailbox.ID, seq, msg.MessageID, msg.RecipientPeerID,
		msg.SenderPeerID, msg.Payload, int(msg.Priority),
		time.UnixMilli(msg.CreatedTimestamp),
		time.UnixMilli(msg.ExpiryTimestamp),
		msg.HopCount, uint32(msg.MsgFlags), msg.Persistent, msg.FolderPath,
	)
	if err != nil {
		return 0, fmt.Errorf("store message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit store message: %w", err)
	}

	s.logger.Debug("Stored message", "messageId", msg.MessageID, "sequence", seq)
	return seq, nil
}

func (s *PostgresStorage) RetrieveMessages(ctx context.Context, mailbox *storage.MailboxRecord, fromSequence *int, maxMessages *int, minPriority *core.MessagePriority) ([]*core.Message, error) {
	conditions := []string{"mailbox_id = $1", "expires_at > NOW()"}
	args := []any{mailbox.ID}
	argIdx := 2

	if fromSequence != nil {
		conditions = append(conditions, fmt.Sprintf("sequence_number >= $%d", argIdx))
		args = append(args, *fromSequence)
		argIdx++
	}

	if minPriority != nil {
		conditions = append(conditions, fmt.Sprintf("priority > $%d", argIdx))
		args = append(args, int(*minPriority))
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT id, mailbox_id, sequence_number, message_id, recipient_peer_id,
			   sender_peer_id, payload, priority, created_at, expires_at,
			   hop_count, flags_bitmap, persistent, folder_path
		FROM stored_messages
		WHERE %s
		ORDER BY priority DESC, sequence_number ASC`,
		strings.Join(conditions, " AND "),
	)

	if maxMessages != nil {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, *maxMessages)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("retrieve messages: %w", err)
	}
	defer rows.Close()

	var messages []*core.Message
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}

func (s *PostgresStorage) DeleteMessage(ctx context.Context, messageID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM stored_messages WHERE message_id = $1`, messageID)
	return err
}

func (s *PostgresStorage) DeleteMessages(ctx context.Context, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM stored_messages WHERE message_id = ANY($1)`, messageIDs)
	return err
}

func (s *PostgresStorage) GetMessageCount(ctx context.Context, mailboxID int64) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM stored_messages WHERE mailbox_id = $1`, mailboxID).Scan(&count)
	return count, err
}

// =============================================================================
// Flag Operations
// =============================================================================

func (s *PostgresStorage) UpdateMessageFlags(ctx context.Context, messageID string, addFlags, removeFlags uint32) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE stored_messages
		SET flags_bitmap = (flags_bitmap | $1) & ~$2::integer
		WHERE message_id = $3`,
		addFlags, removeFlags, messageID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PostgresStorage) GetMessageFlags(ctx context.Context, messageID string) (*uint32, error) {
	var flags uint32
	err := s.pool.QueryRow(ctx, `SELECT flags_bitmap FROM stored_messages WHERE message_id = $1`, messageID).Scan(&flags)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &flags, nil
}

func (s *PostgresStorage) ExpungeMailbox(ctx context.Context, mailboxID int64) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM stored_messages
		WHERE mailbox_id = $1 AND (flags_bitmap & $2) != 0`,
		mailboxID, uint32(core.MsgFlagDeleted),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStorage) ExpungeAllMailboxes(ctx context.Context, ownerID peer.ID) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM stored_messages
		WHERE mailbox_id IN (
			SELECT id FROM mailboxes WHERE owner_peer_id = $1
		) AND (flags_bitmap & $2) != 0`,
		ownerID.String(), uint32(core.MsgFlagDeleted),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStorage) MarkMessagesDelivered(ctx context.Context, messageIDs []string) (int, error) {
	if len(messageIDs) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE stored_messages
		SET flags_bitmap = flags_bitmap | $1
		WHERE message_id = ANY($2)`,
		uint32(core.MsgFlagSeen), messageIDs,
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// =============================================================================
// ACL Operations
// =============================================================================

func (s *PostgresStorage) GrantAccess(ctx context.Context, mailboxID int64, peerID peer.ID, mode core.AccessMode) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO mailbox_acls (mailbox_id, peer_id, access_mode)
		VALUES ($1, $2, $3)
		ON CONFLICT (mailbox_id, peer_id)
		DO UPDATE SET access_mode = $3, granted_at = NOW()`,
		mailboxID, peerID.String(), int(mode),
	)
	return err
}

func (s *PostgresStorage) RevokeAccess(ctx context.Context, mailboxID int64, peerID peer.ID) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM mailbox_acls
		WHERE mailbox_id = $1 AND peer_id = $2`,
		mailboxID, peerID.String(),
	)
	return err
}

func (s *PostgresStorage) CheckAccess(ctx context.Context, mailboxID int64, peerID peer.ID, requiredMode core.AccessMode) (bool, error) {
	var grantedMode int
	err := s.pool.QueryRow(ctx, `
		SELECT access_mode FROM mailbox_acls
		WHERE mailbox_id = $1 AND peer_id = $2`,
		mailboxID, peerID.String(),
	).Scan(&grantedMode)

	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	granted := core.AccessMode(grantedMode)
	switch requiredMode {
	case core.AccessReadOnly:
		return granted == core.AccessReadOnly || granted == core.AccessReadWrite, nil
	case core.AccessWriteOnly:
		return granted == core.AccessWriteOnly || granted == core.AccessReadWrite, nil
	case core.AccessReadWrite:
		return granted == core.AccessReadWrite, nil
	}
	return false, nil
}

func (s *PostgresStorage) ListACL(ctx context.Context, mailboxID int64) ([]*storage.AclRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, mailbox_id, peer_id, access_mode, granted_at
		FROM mailbox_acls WHERE mailbox_id = $1`,
		mailboxID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*storage.AclRecord
	for rows.Next() {
		var r storage.AclRecord
		var mode int
		if err := rows.Scan(&r.ID, &r.MailboxID, &r.PeerIDBase58, &mode, &r.GrantedAt); err != nil {
			return nil, err
		}
		r.AccessMode = core.AccessMode(mode)
		records = append(records, &r)
	}
	return records, rows.Err()
}

// =============================================================================
// Reader Cursor Operations
// =============================================================================

func (s *PostgresStorage) GetCursor(ctx context.Context, mailboxID int64, readerID peer.ID) (int, error) {
	var seq int
	err := s.pool.QueryRow(ctx, `
		SELECT last_sequence_read FROM reader_cursors
		WHERE mailbox_id = $1 AND reader_peer_id = $2`,
		mailboxID, readerID.String(),
	).Scan(&seq)

	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

func (s *PostgresStorage) UpdateCursor(ctx context.Context, mailboxID int64, readerID peer.ID, sequence int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO reader_cursors (mailbox_id, reader_peer_id, last_sequence_read)
		VALUES ($1, $2, $3)
		ON CONFLICT (mailbox_id, reader_peer_id)
		DO UPDATE SET last_sequence_read = $3, last_access_at = NOW()`,
		mailboxID, readerID.String(), sequence,
	)
	return err
}

// =============================================================================
// Document Operations
// =============================================================================

func (s *PostgresStorage) GetDocument(ctx context.Context, ownerID peer.ID, path string) (*storage.DocumentRecord, error) {
	var r storage.DocumentRecord
	var typ int
	_ = typ // mailbox_type not in documents table
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, path, content, content_type, content_hash,
			   created_at, updated_at, updated_by_peer_id, version_number,
			   history_enabled, max_history_versions, version_vector
		FROM documents
		WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	).Scan(&r.ID, &r.OwnerPeerID, &r.Path, &r.Content, &r.ContentType,
		&r.ContentHash, &r.CreatedAt, &r.UpdatedAt, &r.UpdatedByPeerID,
		&r.VersionNumber, &r.HistoryEnabled, &r.MaxHistoryVersions, &r.VersionVector)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get document: %w", err)
	}
	return &r, nil
}

// PutDocument writes a document, archiving the previous version when history is
// enabled. The whole operation runs in one transaction with the existing row
// locked, so an If-Match precondition is compared and swapped atomically --
// evaluating it outside a transaction let two concurrent conditional writes
// both pass the check and both write, silently losing the first.
//
// The lock query deliberately does not select `content`. The body is never
// needed here: the caller supplies the new one, and archiving the old one is a
// server-side copy that never round-trips through this process.
func (s *PostgresStorage) PutDocument(ctx context.Context, ownerID peer.ID, path string, content []byte, contentType string, updatedBy peer.ID, ifMatch *string) (*storage.DocumentPutResult, error) {
	if len(content) > storage.MaxDocumentSize {
		return nil, &storage.DocumentSizeExceededError{ActualSize: len(content), MaxSize: storage.MaxDocumentSize}
	}

	contentHash := computeContentHash(content)
	now := time.Now()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin put document: %w", err)
	}
	defer tx.Rollback(ctx)

	var (
		existingID         int64
		existingHash       string
		existingVersion    int
		historyEnabled     bool
		maxHistoryVersions *int
		found              bool
	)
	err = tx.QueryRow(ctx, `
		SELECT id, content_hash, version_number, history_enabled, max_history_versions
		FROM documents
		WHERE owner_peer_id = $1 AND path = $2
		FOR UPDATE`,
		ownerID.String(), path,
	).Scan(&existingID, &existingHash, &existingVersion, &historyEnabled, &maxHistoryVersions)
	switch {
	case err == nil:
		found = true
	case errors.Is(err, pgx.ErrNoRows):
		found = false
	default:
		return nil, fmt.Errorf("lock document: %w", err)
	}

	// Evaluated under the row lock. As before, an If-Match against a path that
	// does not exist yet is treated as satisfied.
	if ifMatch != nil && found && existingHash != *ifMatch {
		return nil, &storage.DocumentConflictError{ExpectedHash: *ifMatch, ActualHash: existingHash}
	}

	newVersion := 1
	if found {
		newVersion = existingVersion + 1

		if historyEnabled {
			if err := archiveDocumentVersion(ctx, tx, existingID); err != nil {
				return nil, err
			}
			if maxHistoryVersions != nil {
				if err := pruneDocumentVersions(ctx, tx, existingID, *maxHistoryVersions); err != nil {
					return nil, err
				}
			}
		}
	}

	var created bool
	err = tx.QueryRow(ctx, `
		INSERT INTO documents (
			owner_peer_id, path, content, content_type, content_hash,
			created_at, updated_at, updated_by_peer_id, version_number
		) VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8)
		ON CONFLICT (owner_peer_id, path)
		DO UPDATE SET
			content = $3, content_type = $4, content_hash = $5,
			updated_at = $6, updated_by_peer_id = $7, version_number = $8
		RETURNING (xmax = 0)`,
		ownerID.String(), path, content, contentType, contentHash,
		now, updatedBy.String(), newVersion,
	).Scan(&created)
	if err != nil {
		return nil, fmt.Errorf("put document: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit put document: %w", err)
	}

	return &storage.DocumentPutResult{
		ContentHash: contentHash,
		Created:     created,
		UpdatedAt:   now,
	}, nil
}

func (s *PostgresStorage) PatchDocument(ctx context.Context, ownerID peer.ID, path string, patch map[string]any, updatedBy peer.ID, ifMatch *string) (*storage.DocumentPutResult, error) {
	doc, err := s.GetDocument(ctx, ownerID, path)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, storage.ErrDocumentNotFound
	}

	if ifMatch != nil && doc.ContentHash != *ifMatch {
		return nil, &storage.DocumentConflictError{ExpectedHash: *ifMatch, ActualHash: doc.ContentHash}
	}

	// Parse existing content
	var existing map[string]any
	if err := json.Unmarshal(doc.Content, &existing); err != nil {
		return nil, fmt.Errorf("parse existing document: %w", err)
	}

	// Apply JSON Merge Patch (RFC 7396)
	merged := applyMergePatch(existing, patch)

	newContent, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("marshal merged document: %w", err)
	}

	return s.PutDocument(ctx, ownerID, path, newContent, "application/json", updatedBy, nil)
}

func (s *PostgresStorage) DeleteDocument(ctx context.Context, ownerID peer.ID, path string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM documents WHERE owner_peer_id = $1 AND path = $2`,
		ownerID.String(), path,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ListDocuments returns one page of document metadata, ordered by path.
//
// octet_length is computed in the database rather than selecting the body and
// measuring it here. The previous implementation selected `content` for every
// row so the handler could call len() on it, which made a single LIST transfer
// the caller's entire document set out of Postgres and into this process only
// to discard it.
func (s *PostgresStorage) ListDocuments(ctx context.Context, ownerID peer.ID, afterPath string, limit int) ([]*storage.DocumentSummary, bool, error) {
	if limit <= 0 {
		limit = storage.DefaultDocumentListLimit
	}
	if limit > storage.MaxDocumentListLimit {
		limit = storage.MaxDocumentListLimit
	}

	// Fetch one extra row to detect whether a further page exists without a
	// second count query.
	rows, err := s.pool.Query(ctx, `
		SELECT path, content_type, content_hash, octet_length(content),
			   updated_at, version_number
		FROM documents
		WHERE owner_peer_id = $1 AND ($2 = '' OR path > $2)
		ORDER BY path
		LIMIT $3`,
		ownerID.String(), afterPath, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	records := make([]*storage.DocumentSummary, 0, limit)
	for rows.Next() {
		var r storage.DocumentSummary
		if err := rows.Scan(&r.Path, &r.ContentType, &r.ContentHash, &r.Size,
			&r.UpdatedAt, &r.VersionNumber); err != nil {
			return nil, false, err
		}
		records = append(records, &r)
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

func (s *PostgresStorage) GetDocumentHistory(ctx context.Context, ownerID peer.ID, path string, maxVersions *int) ([]*storage.DocumentVersionRecord, error) {
	doc, err := s.GetDocument(ctx, ownerID, path)
	if err != nil || doc == nil {
		return nil, err
	}

	query := `
		SELECT id, document_id, version_number, content, content_hash,
			   content_type, created_at, created_by_peer_id
		FROM document_versions
		WHERE document_id = $1
		ORDER BY version_number DESC`
	args := []any{doc.ID}

	if maxVersions != nil {
		query += " LIMIT $2"
		args = append(args, *maxVersions)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*storage.DocumentVersionRecord
	for rows.Next() {
		var r storage.DocumentVersionRecord
		if err := rows.Scan(&r.ID, &r.DocumentID, &r.VersionNumber, &r.Content,
			&r.ContentHash, &r.ContentType, &r.CreatedAt, &r.CreatedByPeerID); err != nil {
			return nil, err
		}
		records = append(records, &r)
	}
	return records, rows.Err()
}

func (s *PostgresStorage) GetDocumentAtVersion(ctx context.Context, ownerID peer.ID, path string, versionNumber int) (*storage.DocumentVersionRecord, error) {
	doc, err := s.GetDocument(ctx, ownerID, path)
	if err != nil || doc == nil {
		return nil, err
	}

	var r storage.DocumentVersionRecord
	err = s.pool.QueryRow(ctx, `
		SELECT id, document_id, version_number, content, content_hash,
			   content_type, created_at, created_by_peer_id
		FROM document_versions
		WHERE document_id = $1 AND version_number = $2`,
		doc.ID, versionNumber,
	).Scan(&r.ID, &r.DocumentID, &r.VersionNumber, &r.Content,
		&r.ContentHash, &r.ContentType, &r.CreatedAt, &r.CreatedByPeerID)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// =============================================================================
// Directory Operations
// =============================================================================

func (s *PostgresStorage) UpsertDirectoryEntry(ctx context.Context, entry *storage.DirectoryEntry) error {
	var extrasJSON []byte
	var err error
	if entry.Extras != nil {
		extrasJSON, err = json.Marshal(entry.Extras)
		if err != nil {
			return fmt.Errorf("marshal extras: %w", err)
		}
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO directory_listings (
			owner_peer_id, display_name, bio, avatar_hash, extras
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (owner_peer_id)
		DO UPDATE SET
			display_name = $2, bio = $3, avatar_hash = $4,
			extras = $5, updated_at = NOW()`,
		entry.OwnerPeerID, entry.DisplayName, entry.Bio, entry.AvatarHash, extrasJSON,
	)
	if err != nil {
		return fmt.Errorf("upsert directory entry: %w", err)
	}

	s.logger.Debug("Upserted directory entry", "owner", entry.OwnerPeerID, "displayName", entry.DisplayName)
	return nil
}

func (s *PostgresStorage) RemoveDirectoryEntry(ctx context.Context, ownerPeerID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM directory_listings WHERE owner_peer_id = $1`, ownerPeerID)
	return err
}

func (s *PostgresStorage) GetDirectoryEntry(ctx context.Context, ownerPeerID string) (*storage.DirectoryEntry, error) {
	var entry storage.DirectoryEntry
	var extrasJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_peer_id, display_name, bio, avatar_hash,
			   listed_at, updated_at, extras
		FROM directory_listings
		WHERE owner_peer_id = $1`,
		ownerPeerID,
	).Scan(&entry.ID, &entry.OwnerPeerID, &entry.DisplayName, &entry.Bio,
		&entry.AvatarHash, &entry.ListedAt, &entry.UpdatedAt, &extrasJSON)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get directory entry: %w", err)
	}

	if extrasJSON != nil {
		if err := json.Unmarshal(extrasJSON, &entry.Extras); err != nil {
			s.logger.Warn("failed to unmarshal directory extras", "error", err)
		}
	}

	return &entry, nil
}

func (s *PostgresStorage) BrowseDirectory(ctx context.Context, query string, cursor string, limit int) (*storage.DirectoryPage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	conditions := []string{}
	args := []any{}
	argIdx := 1

	// Case-insensitive substring search on display_name and bio
	if query != "" {
		conditions = append(conditions, fmt.Sprintf(
			"(coalesce(display_name, '') ILIKE $%d OR coalesce(bio, '') ILIKE $%d)", argIdx, argIdx+1))
		likePattern := "%" + query + "%"
		args = append(args, likePattern, likePattern)
		argIdx += 2
	}

	// Cursor-based pagination.
	//
	// Cursor format: "{RFC3339Nano}:{peerId}". The timestamp contains colons of
	// its own -- in the time and in a numeric zone offset -- so the separator
	// is the LAST one, not the first. Splitting on the first colon parsed
	// "2026-08-28T14" as the whole timestamp, which failed, and the failure was
	// swallowed: every page then ran with no cursor at all, returned the same
	// first page, and reported hasMore forever. A client looping until hasMore
	// went false never terminated.
	//
	// A peer ID is base58 and never contains a colon, so the last colon is
	// unambiguously the separator.
	if cursor != "" {
		sepIdx := strings.LastIndex(cursor, ":")
		if sepIdx <= 0 {
			return nil, fmt.Errorf("%w: no separator", storage.ErrInvalidCursor)
		}
		cursorTime, err := time.Parse(time.RFC3339Nano, cursor[:sepIdx])
		if err != nil {
			// Reported rather than ignored. A cursor the server cannot read is
			// a request it cannot answer correctly, and silently answering the
			// first page instead is how this went unnoticed.
			return nil, fmt.Errorf("%w: %v", storage.ErrInvalidCursor, err)
		}
		cursorPeerID := cursor[sepIdx+1:]
		if cursorPeerID == "" {
			return nil, fmt.Errorf("%w: no peer id", storage.ErrInvalidCursor)
		}
		conditions = append(conditions, fmt.Sprintf(
			"(updated_at, owner_peer_id) < ($%d, $%d)", argIdx, argIdx+1))
		args = append(args, cursorTime, cursorPeerID)
		argIdx += 2
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Fetch limit+1 to detect hasMore
	sqlQuery := fmt.Sprintf(`
		SELECT id, owner_peer_id, display_name, bio, avatar_hash,
			   listed_at, updated_at, extras
		FROM directory_listings
		%s
		ORDER BY updated_at DESC, owner_peer_id DESC
		LIMIT $%d`, whereClause, argIdx)
	args = append(args, limit+1)

	s.logger.Info("browse directory SQL", "query", sqlQuery, "args", args)

	rows, err := s.pool.Query(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("browse directory: %w", err)
	}
	defer rows.Close()

	var entries []*storage.DirectoryEntry
	for rows.Next() {
		var entry storage.DirectoryEntry
		var extrasJSON []byte
		if err := rows.Scan(&entry.ID, &entry.OwnerPeerID, &entry.DisplayName,
			&entry.Bio, &entry.AvatarHash, &entry.ListedAt, &entry.UpdatedAt, &extrasJSON); err != nil {
			return nil, fmt.Errorf("scan directory entry: %w", err)
		}
		if extrasJSON != nil {
			if err := json.Unmarshal(extrasJSON, &entry.Extras); err != nil {
				s.logger.Warn("failed to unmarshal directory extras", "error", err)
			}
		}
		entries = append(entries, &entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	page := &storage.DirectoryPage{}

	if len(entries) > limit {
		page.HasMore = true
		entries = entries[:limit]
	}

	page.Entries = entries

	// Build next cursor from last entry
	if page.HasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		page.NextCursor = last.UpdatedAt.Format(time.RFC3339Nano) + ":" + last.OwnerPeerID
	}

	return page, nil
}

// =============================================================================
// Cleanup Operations
// =============================================================================

// DeleteExpiredMessages removes expired messages in bounded batches.
//
// A single unbounded DELETE over a large table holds one long transaction: a
// wide lock footprint, a WAL spike, and bloat for autovacuum to chase
// afterwards. Batching keeps each transaction short and lets maintenance be
// cancelled between batches.
func (s *PostgresStorage) DeleteExpiredMessages(ctx context.Context) (int, error) {
	total := 0
	for batch := 0; batch < maxExpiryDeleteBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		tag, err := s.pool.Exec(ctx, `
			DELETE FROM stored_messages
			WHERE id IN (
				SELECT id FROM stored_messages
				WHERE expires_at < NOW()
				LIMIT $1
			)`,
			expiryDeleteBatchSize,
		)
		if err != nil {
			return total, err
		}

		deleted := int(tag.RowsAffected())
		total += deleted
		if deleted < expiryDeleteBatchSize {
			if total > 0 {
				s.logger.Info("Deleted expired messages", "count", total)
			}
			return total, nil
		}
	}

	// Hit the batch ceiling with work still outstanding. Say so rather than
	// reporting a clean sweep -- the next maintenance tick picks up the rest.
	s.logger.Warn("Expired message sweep hit its batch ceiling; more remain",
		"deleted", total, "max_batches", maxExpiryDeleteBatches,
		"batch_size", expiryDeleteBatchSize)
	return total, nil
}

func (s *PostgresStorage) EnforceRetentionPolicy(ctx context.Context, mailbox *storage.MailboxRecord) error {
	// Time-based retention
	_, err := s.pool.Exec(ctx, `
		DELETE FROM stored_messages
		WHERE mailbox_id = $1
		  AND created_at < NOW() - INTERVAL '1 day' * $2`,
		mailbox.ID, mailbox.RetentionDays,
	)
	if err != nil {
		return err
	}

	// Count-based retention
	if mailbox.RetentionCount != nil {
		_, err = s.pool.Exec(ctx, `
			DELETE FROM stored_messages
			WHERE id IN (
				SELECT id FROM stored_messages
				WHERE mailbox_id = $1
				ORDER BY sequence_number DESC
				OFFSET $2
			)`,
			mailbox.ID, *mailbox.RetentionCount,
		)
	}
	return err
}

// =============================================================================
// Helpers
// =============================================================================

func scanMailboxRecord(row pgx.Row) (*storage.MailboxRecord, error) {
	var r storage.MailboxRecord
	var typ int
	err := row.Scan(&r.ID, &r.OwnerPeerID, &r.FolderPath, &typ,
		&r.CreatedAt, &r.LastAccessAt, &r.MaxMessages,
		&r.RetentionDays, &r.RetentionCount)
	if err != nil {
		return nil, err
	}
	r.Type = core.MailboxType(typ)
	return &r, nil
}

func scanMailboxRecordFromRows(rows pgx.Rows) (*storage.MailboxRecord, error) {
	var r storage.MailboxRecord
	var typ int
	err := rows.Scan(&r.ID, &r.OwnerPeerID, &r.FolderPath, &typ,
		&r.CreatedAt, &r.LastAccessAt, &r.MaxMessages,
		&r.RetentionDays, &r.RetentionCount)
	if err != nil {
		return nil, err
	}
	r.Type = core.MailboxType(typ)
	return &r, nil
}

func scanMessage(rows pgx.Rows) (*core.Message, error) {
	var (
		id, mailboxID int64
		seqNum        int
		msgID         string
		recipientID   string
		senderID      string
		payload       []byte
		priority      int
		createdAt     time.Time
		expiresAt     time.Time
		hopCount      int
		flagsBitmap   uint32
		persistent    bool
		folderPath    *string
	)

	err := rows.Scan(&id, &mailboxID, &seqNum, &msgID, &recipientID,
		&senderID, &payload, &priority, &createdAt, &expiresAt,
		&hopCount, &flagsBitmap, &persistent, &folderPath)
	if err != nil {
		return nil, fmt.Errorf("scan message: %w", err)
	}

	msg := &core.Message{
		MessageID:        msgID,
		RecipientPeerID:  recipientID,
		SenderPeerID:     senderID,
		Payload:          payload,
		Priority:         core.MessagePriority(priority),
		ExpiryTimestamp:  expiresAt.UnixMilli(),
		HopCount:         hopCount,
		Flags:            core.FlagNone,
		CreatedTimestamp: createdAt.UnixMilli(),
		SequenceNumber:   uint64(seqNum),
		MsgFlags:         core.MessageFlags(flagsBitmap),
		Persistent:       persistent,
	}
	if folderPath != nil {
		msg.FolderPath = *folderPath
	}
	return msg, nil
}

// archiveDocumentVersion copies a document's current row into document_versions
// before it is overwritten. The copy happens entirely inside the database --
// pulling the body out to Go only to send it straight back doubled the transfer
// cost of every write to a history-enabled document.
//
// Both helpers take the transaction rather than the pool, and return their
// errors rather than logging them: once inside a transaction a failed statement
// aborts the whole thing, so swallowing the error would commit a version bump
// whose history entry was never written.
func archiveDocumentVersion(ctx context.Context, tx pgx.Tx, documentID int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO document_versions (
			document_id, version_number, content, content_hash, content_type,
			created_at, created_by_peer_id
		)
		SELECT id, version_number, content, content_hash, content_type,
			   updated_at, updated_by_peer_id
		FROM documents
		WHERE id = $1
		ON CONFLICT (document_id, version_number) DO NOTHING`,
		documentID,
	)
	if err != nil {
		return fmt.Errorf("archive document version: %w", err)
	}
	return nil
}

func pruneDocumentVersions(ctx context.Context, tx pgx.Tx, documentID int64, maxVersions int) error {
	_, err := tx.Exec(ctx, `
		DELETE FROM document_versions
		WHERE id IN (
			SELECT id FROM document_versions
			WHERE document_id = $1
			ORDER BY version_number DESC
			OFFSET $2
		)`,
		documentID, maxVersions,
	)
	if err != nil {
		return fmt.Errorf("prune document versions: %w", err)
	}
	return nil
}

func computeContentHash(content []byte) string {
	hash := sha256.Sum256(content)
	return fmt.Sprintf("sha256:%x", hash)
}

func applyMergePatch(target, patch map[string]any) map[string]any {
	result := make(map[string]any, len(target))
	for k, v := range target {
		result[k] = v
	}
	for k, v := range patch {
		if v == nil {
			delete(result, k)
		} else if patchMap, ok := v.(map[string]any); ok {
			if targetMap, ok := result[k].(map[string]any); ok {
				result[k] = applyMergePatch(targetMap, patchMap)
			} else {
				result[k] = v
			}
		} else {
			result[k] = v
		}
	}
	return result
}

// Compile-time interface check
var _ storage.Storage = (*PostgresStorage)(nil)
