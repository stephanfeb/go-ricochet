package mailboxes

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// SharedMailbox supports multiple authorized readers with independent cursors.
type SharedMailbox struct {
	record  *storage.MailboxRecord
	storage storage.Storage
	logger  *slog.Logger
}

// NewSharedMailbox creates a new shared mailbox.
func NewSharedMailbox(record *storage.MailboxRecord, store storage.Storage, logger *slog.Logger) *SharedMailbox {
	return &SharedMailbox{
		record:  record,
		storage: store,
		logger:  logger,
	}
}

func (m *SharedMailbox) Record() *storage.MailboxRecord {
	return m.record
}

func (m *SharedMailbox) StoreMessage(ctx context.Context, msg *core.Message) error {
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
			return &UnauthorizedError{Message: "no write access to shared mailbox"}
		}
	}

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

	m.logger.Debug("stored message in shared mailbox",
		"message_id", msg.MessageID,
		"mailbox", m.record.FullPath(),
	)
	return nil
}

func (m *SharedMailbox) RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, bool, error) {
	if readerID == nil {
		return nil, false, fmt.Errorf("readerId required for shared mailbox")
	}

	isOwner := readerID.String() == m.record.OwnerPeerID

	if !isOwner {
		canRead, err := m.storage.CheckAccess(ctx, m.record.ID, *readerID, core.AccessReadOnly)
		if err != nil {
			return nil, false, err
		}
		if !canRead {
			return nil, false, &UnauthorizedError{Message: "no read access to shared mailbox"}
		}
	}

	startSequence := opts.FromSequence
	if startSequence == nil {
		cursor, err := m.storage.GetCursor(ctx, m.record.ID, *readerID)
		if err != nil {
			return nil, false, err
		}
		if cursor > 0 {
			next := cursor + 1
			startSequence = &next
		} else {
			one := 1
			startSequence = &one
		}
	}

	messages, hasMore, err := fetchPage(ctx, m.storage, m.record, startSequence, opts)
	if err != nil {
		return nil, false, err
	}

	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		if lastMsg.SequenceNumber > 0 {
			if err := m.storage.UpdateCursor(ctx, m.record.ID, *readerID, int(lastMsg.SequenceNumber)); err != nil {
				m.logger.Warn("failed to update cursor", "error", err)
			}
		}
	}

	return messages, hasMore, nil
}
