package storage

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

const (
	// MaxDocumentSize is the maximum document size (10MB).
	MaxDocumentSize = 10 * 1024 * 1024

	// DefaultDocumentListLimit bounds a LIST that does not ask for a page size.
	// Listing was previously unbounded; callers that need everything page with
	// the cursor rather than relying on one unbounded response.
	DefaultDocumentListLimit = 1000

	// MaxDocumentListLimit caps what a caller may request per page.
	MaxDocumentListLimit = 5000
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
	// DeleteMessages removes messages by ID with no ownership check. It is
	// for the mailbox types, which only ever pass IDs they just read from
	// their own record; a caller-facing path must use DeleteOwnedMessages.
	DeleteMessages(ctx context.Context, messageIDs []string) error
	GetMessageCount(ctx context.Context, mailboxID int64) (int, error)

	// Caller-facing mutations. Each is scoped to mailboxes ownerID owns, in
	// the statement itself, so a message ID that belongs to someone else is
	// simply not matched: message IDs are chosen by the sender and echoed in
	// every acknowledgement, so knowing one proves nothing about who may act
	// on it. Each reports how many rows it touched so an acknowledgement can
	// say what actually happened rather than what was asked.
	DeleteOwnedMessages(ctx context.Context, ownerID peer.ID, messageIDs []string) (int, error)
	// MarkMessagesDelivered removes the non-persistent messages among
	// messageIDs and sets \Seen on the persistent ones, atomically.
	MarkMessagesDelivered(ctx context.Context, ownerID peer.ID, messageIDs []string) (int, error)
	// UpdateMessageFlags returns the resulting flags, or nil when no message
	// with that ID exists in a mailbox ownerID owns.
	UpdateMessageFlags(ctx context.Context, ownerID peer.ID, messageID string, addFlags, removeFlags uint32) (*uint32, error)

	ExpungeMailbox(ctx context.Context, mailboxID int64) (int, error)
	ExpungeAllMailboxes(ctx context.Context, ownerID peer.ID) (int, error)

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
	// ListDocuments returns one page of document metadata for an owner, ordered
	// by path. afterPath is a keyset cursor ("" for the first page); the bool
	// reports whether more rows follow the returned page.
	ListDocuments(ctx context.Context, ownerID peer.ID, afterPath string, limit int) ([]*DocumentSummary, bool, error)
	GetDocumentHistory(ctx context.Context, ownerID peer.ID, path string, maxVersions *int) ([]*DocumentVersionRecord, error)
	GetDocumentAtVersion(ctx context.Context, ownerID peer.ID, path string, versionNumber int) (*DocumentVersionRecord, error)

	// Feed operations
	CreateFeed(ctx context.Context, ownerID peer.ID, path, title, description string, collaborative bool) (*FeedRecord, error)
	GetFeed(ctx context.Context, ownerID peer.ID, path string) (*FeedRecord, error)
	DeleteFeed(ctx context.Context, ownerID peer.ID, path string) (bool, error)
	ListFeeds(ctx context.Context, ownerID peer.ID) ([]*FeedRecord, error)
	AppendFeedEntry(ctx context.Context, feedID int64, content []byte, createdBy peer.ID, entryType string) (*FeedEntryRecord, error)
	GetFeedEntry(ctx context.Context, feedID int64, sequenceNumber int) (*FeedEntryRecord, error)
	GetFeedEntries(ctx context.Context, feedID int64, fromSeq, toSeq *int, entryType string, limit int) ([]*FeedEntryRecord, bool, error)
	EnforceFeedRetention(ctx context.Context, feed *FeedRecord) (int, error)
	GetMultiFeedEntries(ctx context.Context, queries []MultiFeedQuery) (map[string]*MultiFeedResult, error)

	// Collection operations
	CreateCollection(ctx context.Context, ownerID peer.ID, path, name string) (*CollectionRecord, error)
	GetCollection(ctx context.Context, ownerID peer.ID, path string) (*CollectionRecord, error)
	DeleteCollection(ctx context.Context, ownerID peer.ID, path string) (bool, error)
	ListCollections(ctx context.Context, ownerID peer.ID) ([]*CollectionRecord, error)
	GetCollectionItem(ctx context.Context, collectionID int64, key string) (*CollectionItemRecord, error)
	PutCollectionItem(ctx context.Context, collectionID int64, key string, content []byte, updatedBy peer.ID, ifMatch *string) (*CollectionItemRecord, bool, error)
	DeleteCollectionItem(ctx context.Context, collectionID int64, key string) (bool, error)
	ListCollectionKeys(ctx context.Context, collectionID int64, limit, offset int) ([]string, int, error)
	QueryCollection(ctx context.Context, collectionID int64, filter map[string]any, sortField string, sortAsc bool, limit, offset int) (*CollectionQueryResult, error)

	// Directory operations
	UpsertDirectoryEntry(ctx context.Context, entry *DirectoryEntry) error
	RemoveDirectoryEntry(ctx context.Context, ownerPeerID string) error
	GetDirectoryEntry(ctx context.Context, ownerPeerID string) (*DirectoryEntry, error)
	BrowseDirectory(ctx context.Context, query string, cursor string, limit int) (*DirectoryPage, error)

	// Cleanup operations
	DeleteExpiredMessages(ctx context.Context) (int, error)
	EnforceRetentionPolicy(ctx context.Context, mailbox *MailboxRecord) error

	// Operator views
	//
	// These are the only methods here that are not owner- or mailbox-scoped.
	// They exist to answer operator questions ("how full is this server",
	// "which mailboxes are about to overflow", "who is using the space")
	// that per-owner methods cannot: a protocol handler always knows whose
	// data it is looking at, and an operator investigating a problem does
	// not. They all scan, so none belongs on a request path — ServerStats is
	// sampled on a timer, and the listings are bounded by construction.

	// ServerStats aggregates across every owner in one pass.
	ServerStats(ctx context.Context, nearCapacityRatio float64) (*ServerStats, error)

	// ListMailboxUsage lists mailboxes with their live message counts, either
	// across all owners or within one. The result is capped by
	// ClampPageSize regardless of what the query asks for.
	ListMailboxUsage(ctx context.Context, q MailboxUsageQuery) ([]*MailboxUsage, error)

	// ListOwnerUsage totals storage per owner, largest first. Also capped.
	ListOwnerUsage(ctx context.Context, limit, offset int) ([]*OwnerUsage, error)
}
