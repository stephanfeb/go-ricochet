package mailboxes

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// PublicMailbox allows anyone to read but only authorized users to write.
type PublicMailbox struct {
	record  *storage.MailboxRecord
	storage storage.Storage
	logger  *slog.Logger
}

// NewPublicMailbox creates a new public mailbox.
func NewPublicMailbox(record *storage.MailboxRecord, store storage.Storage, logger *slog.Logger) *PublicMailbox {
	return &PublicMailbox{
		record:  record,
		storage: store,
		logger:  logger,
	}
}

func (m *PublicMailbox) Record() *storage.MailboxRecord {
	return m.record
}

func (m *PublicMailbox) StoreMessage(ctx context.Context, msg *core.Message) error {
	isOwner := msg.SenderPeerID == m.record.OwnerPeerID

	if !isOwner {
		senderID, err := peer.Decode(msg.SenderPeerID)
		if err != nil {
			return fmt.Errorf("decode sender peer ID: %w", err)
		}
		canWrite, err := m.storage.CheckAccess(ctx, m.record.ID, senderID, core.AccessWriteOnly)
		if err != nil {
			return err
		}
		if !canWrite {
			return &UnauthorizedError{Message: "not authorized to publish to public mailbox"}
		}
	}

	if _, err := m.storage.StoreMessage(ctx, m.record, msg); err != nil {
		return err
	}

	m.logger.Debug("stored message in public mailbox",
		"message_id", msg.MessageID,
		"mailbox", m.record.FullPath(),
	)

	if err := m.storage.EnforceRetentionPolicy(ctx, m.record); err != nil {
		m.logger.Warn("failed to enforce retention policy", "error", err)
	}

	return nil
}

func (m *PublicMailbox) RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, error) {
	startSequence := opts.FromSequence
	if startSequence == nil && readerID != nil {
		cursor, err := m.storage.GetCursor(ctx, m.record.ID, *readerID)
		if err != nil {
			return nil, err
		}
		if cursor > 0 {
			next := cursor + 1
			startSequence = &next
		} else {
			one := 1
			startSequence = &one
		}
	}
	if startSequence == nil {
		one := 1
		startSequence = &one
	}

	messages, err := m.storage.RetrieveMessages(ctx, m.record, startSequence, opts.MaxMessages, opts.MinPriority)
	if err != nil {
		return nil, err
	}

	if readerID != nil && len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		if lastMsg.SequenceNumber > 0 {
			if err := m.storage.UpdateCursor(ctx, m.record.ID, *readerID, int(lastMsg.SequenceNumber)); err != nil {
				m.logger.Warn("failed to update cursor", "error", err)
			}
		}
	}

	return messages, nil
}
