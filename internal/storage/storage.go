package storage

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

const (
	// MaxDocumentSize is the maximum document size (10MB).
	MaxDocumentSize = 10 * 1024 * 1024
)

// Storage defines the interface for Ricochet's storage backends.
type Storage interface {
	// Lifecycle
	Initialize(ctx context.Context) error
	Close() error

	// Mailbox operations
	GetOrCreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (*MailboxRecord, error)
	FindMailbox(ctx context.Context, ownerID peer.ID, folderPath string) (*MailboxRecord, error)
	ListMailboxes(ctx context.Context, ownerID peer.ID) ([]*MailboxRecord, error)
	UpdateMailbox(ctx context.Context, mailbox *MailboxRecord) error
	DeleteMailbox(ctx context.Context, mailboxID int64) error
	UpdateMailboxAccess(ctx context.Context, mailboxID int64) error

	// Message operations
	StoreMessage(ctx context.Context, mailbox *MailboxRecord, msg *core.Message) (int, error)
	RetrieveMessages(ctx context.Context, mailbox *MailboxRecord, fromSequence *int, maxMessages *int, minPriority *core.MessagePriority) ([]*core.Message, error)
	DeleteMessage(ctx context.Context, messageID string) error
	DeleteMessages(ctx context.Context, messageIDs []string) error
	GetNextSequence(ctx context.Context, mailboxID int64) (int, error)
	GetMessageCount(ctx context.Context, mailboxID int64) (int, error)

	// Flag operations (IMAP-style)
	UpdateMessageFlags(ctx context.Context, messageID string, addFlags, removeFlags uint32) (bool, error)
	GetMessageFlags(ctx context.Context, messageID string) (*uint32, error)
	ExpungeMailbox(ctx context.Context, mailboxID int64) (int, error)
	ExpungeAllMailboxes(ctx context.Context, ownerID peer.ID) (int, error)
	MarkMessagesDelivered(ctx context.Context, messageIDs []string) (int, error)

	// ACL operations
	GrantAccess(ctx context.Context, mailboxID int64, peerID peer.ID, mode core.AccessMode) error
	RevokeAccess(ctx context.Context, mailboxID int64, peerID peer.ID) error
	CheckAccess(ctx context.Context, mailboxID int64, peerID peer.ID, requiredMode core.AccessMode) (bool, error)
	ListACL(ctx context.Context, mailboxID int64) ([]*AclRecord, error)

	// Reader cursor operations
	GetCursor(ctx context.Context, mailboxID int64, readerID peer.ID) (int, error)
	UpdateCursor(ctx context.Context, mailboxID int64, readerID peer.ID, sequence int) error

	// Document operations
	GetDocument(ctx context.Context, ownerID peer.ID, path string) (*DocumentRecord, error)
	PutDocument(ctx context.Context, ownerID peer.ID, path string, content []byte, contentType string, updatedBy peer.ID, ifMatch *string) (*DocumentPutResult, error)
	PatchDocument(ctx context.Context, ownerID peer.ID, path string, patch map[string]any, updatedBy peer.ID, ifMatch *string) (*DocumentPutResult, error)
	DeleteDocument(ctx context.Context, ownerID peer.ID, path string) (bool, error)
	ListDocuments(ctx context.Context, ownerID peer.ID) ([]*DocumentRecord, error)
	GetDocumentHistory(ctx context.Context, ownerID peer.ID, path string, maxVersions *int) ([]*DocumentVersionRecord, error)
	GetDocumentAtVersion(ctx context.Context, ownerID peer.ID, path string, versionNumber int) (*DocumentVersionRecord, error)

	// Cleanup operations
	DeleteExpiredMessages(ctx context.Context) (int, error)
	EnforceRetentionPolicy(ctx context.Context, mailbox *MailboxRecord) error
}
