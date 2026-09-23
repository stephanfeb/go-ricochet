package mailboxes

import (
	"context"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// PrivateMailbox is a single-owner mailbox.
//
// Reading it changes nothing. Messages that were not sent as persistent are
// removed when the owner marks them delivered; persistent ones stay until
// deleted or expunged. Retrieval used to delete non-persistent messages
// before the response was even written, so a client that disconnected
// mid-read, or whose page did not fit in a frame, lost them for good.
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

	// Capacity is checked by storage, under the mailbox row lock.
	if err := storeMessage(ctx, m.storage, m.record, msg); err != nil {
		return err
	}

	m.logger.Debug("stored message in private mailbox",
		"message_id", msg.MessageID,
		"mailbox", m.record.FullPath(),
	)
	return nil
}

func (m *PrivateMailbox) RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, bool, error) {
	// Verify reader is owner
	if readerID != nil && readerID.String() != m.record.OwnerPeerID {
		return nil, false, &UnauthorizedError{Message: "not mailbox owner"}
	}

	return fetchPage(ctx, m.storage, m.record, opts.FromSequence, opts)
}
