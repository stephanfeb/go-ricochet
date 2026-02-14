package mailboxes

import (
	"context"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// PrivateMailbox is a single-owner mailbox.
// Messages are deleted after retrieval unless marked as persistent.
type PrivateMailbox struct {
	record  *storage.MailboxRecord
	storage storage.Storage
	logger  *slog.Logger
}

// NewPrivateMailbox creates a new private mailbox.
func NewPrivateMailbox(record *storage.MailboxRecord, store storage.Storage, logger *slog.Logger) *PrivateMailbox {
	return &PrivateMailbox{
		record:  record,
		storage: store,
		logger:  logger,
	}
}

func (m *PrivateMailbox) Record() *storage.MailboxRecord {
	return m.record
}

func (m *PrivateMailbox) StoreMessage(ctx context.Context, msg *core.Message) error {
	// Verify recipient is mailbox owner
	if msg.RecipientPeerID != m.record.OwnerPeerID {
		return &UnauthorizedError{Message: "not mailbox owner"}
	}

	// Check capacity
	count, err := m.storage.GetMessageCount(ctx, m.record.ID)
	if err != nil {
		return err
	}
	if count >= m.record.MaxMessages {
		return &MailboxFullError{Current: count, Max: m.record.MaxMessages}
	}

	if _, err := m.storage.StoreMessage(ctx, m.record, msg); err != nil {
		return err
	}

	m.logger.Debug("stored message in private mailbox",
		"message_id", msg.MessageID,
		"mailbox", m.record.FullPath(),
	)
	return nil
}

func (m *PrivateMailbox) RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, error) {
	// Verify reader is owner
	if readerID != nil && readerID.String() != m.record.OwnerPeerID {
		return nil, &UnauthorizedError{Message: "not mailbox owner"}
	}

	messages, err := m.storage.RetrieveMessages(ctx, m.record, opts.FromSequence, opts.MaxMessages, opts.MinPriority)
	if err != nil {
		return nil, err
	}

	// Delete non-persistent messages after retrieval
	var toDelete []string
	for _, msg := range messages {
		if !msg.Persistent {
			toDelete = append(toDelete, msg.MessageID)
		}
	}

	if len(toDelete) > 0 {
		if err := m.storage.DeleteMessages(ctx, toDelete); err != nil {
			m.logger.Warn("failed to delete non-persistent messages", "error", err)
		} else {
			m.logger.Debug("deleted non-persistent messages",
				"count", len(toDelete),
				"mailbox", m.record.FullPath(),
			)
		}
	}

	return messages, nil
}
