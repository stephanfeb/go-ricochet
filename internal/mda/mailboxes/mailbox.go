package mailboxes

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// RetrieveOpts holds options for message retrieval.
type RetrieveOpts struct {
	FromSequence *int
	MaxMessages  *int
	MinPriority  *core.MessagePriority
}

// Mailbox defines the interface for all mailbox types.
type Mailbox interface {
	// StoreMessage stores a message in this mailbox.
	StoreMessage(ctx context.Context, msg *core.Message) error

	// RetrieveMessages retrieves messages from this mailbox.
	RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) ([]*core.Message, error)

	// Record returns the underlying mailbox record.
	Record() *storage.MailboxRecord
}

// UnauthorizedError indicates an access control failure.
type UnauthorizedError struct {
	Message string
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("unauthorized: %s", e.Message)
}

// NotFoundError indicates the addressed mailbox does not exist.
//
// It is distinct from UnauthorizedError on purpose: a reader who is refused
// an existing mailbox and a reader who names one that was never created get
// different codes, but neither learns anything about the other's contents.
type NotFoundError struct {
	Path string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("mailbox not found: %s", e.Path)
}

// MailboxFullError indicates a mailbox has reached capacity.
type MailboxFullError struct {
	Current int
	Max     int
}

func (e *MailboxFullError) Error() string {
	return fmt.Sprintf("mailbox full: %d/%d messages", e.Current, e.Max)
}
