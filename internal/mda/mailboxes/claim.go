package mailboxes

import (
	"context"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// ClaimMailbox holds items its owner hands out one at a time, such as Signal
// one-time prekeys. Only the owner stores into it or lists it. Anyone may
// claim from it (MailboxServer.Claim), which takes the oldest unexpired
// message and deletes it in one statement, so no two claimers get the same
// one.
//
// Listing is the owner's alone so that a reader cannot see every item at
// once: a public mailbox of prekeys let each sender pick any of them, and
// two senders who picked the same one shared a key the protocol assumes is
// used once.
type ClaimMailbox struct {
	record  *storage.MailboxRecord
	storage storage.Storage
	logger  *slog.Logger
}

// NewClaimMailbox creates a new claim mailbox.
func NewClaimMailbox(record *storage.MailboxRecord, store storage.Storage, logger *slog.Logger) *ClaimMailbox {
	return &ClaimMailbox{
		record:  record,
		storage: store,
		logger:  logger,
	}
}

func (m *ClaimMailbox) Record() *storage.MailboxRecord {
	return m.record
}

func (m *ClaimMailbox) StoreMessage(ctx context.Context, msg *core.Message) error {
	if msg.SenderPeerID != m.record.OwnerPeerID || msg.RecipientPeerID != m.record.OwnerPeerID {
		return &UnauthorizedError{Message: "only the owner stores into a claim mailbox"}
	}

	if err := storeMessage(ctx, m.storage, m.record, msg); err != nil {
		return err
	}

	m.logger.Debug("stored message in claim mailbox",
		"message_id", msg.MessageID,
		"mailbox", m.record.FullPath(),
	)
	return nil
}

func (m *ClaimMailbox) RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, bool, error) {
	if readerID != nil && readerID.String() != m.record.OwnerPeerID {
		return nil, false, &UnauthorizedError{Message: "a claim mailbox is listed by its owner only"}
	}

	return fetchPage(ctx, m.storage, m.record, opts.FromSequence, opts)
}
