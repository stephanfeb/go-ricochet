package client

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

// MessagePriority is the priority level for a message.
type MessagePriority = core.MessagePriority

const (
	PriorityLow    = core.PriorityLow
	PriorityNormal = core.PriorityNormal
	PriorityHigh   = core.PriorityHigh
	PriorityUrgent = core.PriorityUrgent
)

// SendOption configures SendMessage behavior.
type SendOption func(*sendConfig)

type sendConfig struct {
	FolderPath           string
	Priority             MessagePriority
	Expiry               time.Duration
	Persistent           bool
	Compress             bool
	CompressionThreshold int
	Encrypt              bool
}

// WithFolderPath sets the target folder path for the message.
func WithFolderPath(path string) SendOption {
	return func(c *sendConfig) {
		c.FolderPath = path
	}
}

// WithPriority sets the message priority level.
func WithPriority(p MessagePriority) SendOption {
	return func(c *sendConfig) {
		c.Priority = p
	}
}

// WithExpiry sets the message expiry duration from now.
func WithExpiry(d time.Duration) SendOption {
	return func(c *sendConfig) {
		c.Expiry = d
	}
}

// WithPersistent sets whether the message should persist after delivery.
func WithPersistent(p bool) SendOption {
	return func(c *sendConfig) {
		c.Persistent = p
	}
}

// WithCompression enables LZ4 compression for the message payload.
// An optional threshold parameter specifies the minimum payload size to compress.
func WithCompression(threshold ...int) SendOption {
	return func(c *sendConfig) {
		c.Compress = true
		if len(threshold) > 0 && threshold[0] > 0 {
			c.CompressionThreshold = threshold[0]
		} else {
			c.CompressionThreshold = DefaultCompressionThreshold
		}
	}
}

// WithEncryption enables end-to-end encryption for the message using NaCl box
// (X25519 key agreement + XSalsa20-Poly1305). The sender's Ed25519 private key
// and the recipient's public key (derived from their peer ID) are used.
func WithEncryption() SendOption {
	return func(c *sendConfig) {
		c.Encrypt = true
	}
}

// RetrieveOption configures RetrieveMessages behavior.
type RetrieveOption func(*retrieveConfig)

type retrieveConfig struct {
	FolderPath   string
	FromSequence *uint64
	MaxMessages  *int
	MinPriority  *MessagePriority
	TargetPeerID *peer.ID
	ServerPeerID *peer.ID
}

// WithRetrieveFolderPath sets the folder path to retrieve messages from.
func WithRetrieveFolderPath(path string) RetrieveOption {
	return func(c *retrieveConfig) {
		c.FolderPath = path
	}
}

// WithFromSequence sets the starting sequence number for retrieval.
func WithFromSequence(seq uint64) RetrieveOption {
	return func(c *retrieveConfig) {
		c.FromSequence = &seq
	}
}

// WithMaxMessages sets the maximum number of messages to retrieve.
func WithMaxMessages(n int) RetrieveOption {
	return func(c *retrieveConfig) {
		c.MaxMessages = &n
	}
}

// WithMinPriority sets the minimum priority filter for retrieval.
func WithMinPriority(p MessagePriority) RetrieveOption {
	return func(c *retrieveConfig) {
		c.MinPriority = &p
	}
}

// WithTargetPeer sets the target peer whose mailbox to retrieve from.
// If not set, the client's own peer ID is used.
func WithTargetPeer(id peer.ID) RetrieveOption {
	return func(c *retrieveConfig) {
		c.TargetPeerID = &id
	}
}

// ExpungeOption configures Expunge behavior.
type ExpungeOption func(*expungeConfig)

type expungeConfig struct {
	FolderPath string
}

// WithExpungeFolderPath sets the folder path to expunge.
func WithExpungeFolderPath(path string) ExpungeOption {
	return func(c *expungeConfig) {
		c.FolderPath = path
	}
}

// DocOption configures document operations.
type DocOption func(*docConfig)

type docConfig struct {
	IfNoneMatch  string
	IfMatch      string
	ContentType  string
	ServerPeerID *peer.ID
}

// WithIfNoneMatch sets the If-None-Match header for conditional GET/HEAD.
func WithIfNoneMatch(etag string) DocOption {
	return func(c *docConfig) {
		c.IfNoneMatch = etag
	}
}

// WithIfMatch sets the If-Match header for conditional PUT/PATCH/DELETE.
func WithIfMatch(etag string) DocOption {
	return func(c *docConfig) {
		c.IfMatch = etag
	}
}

// WithContentType sets the Content-Type header for document writes.
func WithContentType(ct string) DocOption {
	return func(c *docConfig) {
		c.ContentType = ct
	}
}

// WithDocServer sets a specific server peer ID for the document operation.
func WithDocServer(id peer.ID) DocOption {
	return func(c *docConfig) {
		c.ServerPeerID = &id
	}
}

// DirectoryBrowseOption configures BrowseDirectory behavior.
type DirectoryBrowseOption func(*directoryBrowseConfig)

type directoryBrowseConfig struct {
	Query        string
	Cursor       string
	Limit        int
	ServerPeerID *peer.ID
}

// WithDirectoryQuery sets the search query for directory browsing.
func WithDirectoryQuery(q string) DirectoryBrowseOption {
	return func(c *directoryBrowseConfig) { c.Query = q }
}

// WithDirectoryCursor sets the pagination cursor for directory browsing.
func WithDirectoryCursor(cursor string) DirectoryBrowseOption {
	return func(c *directoryBrowseConfig) { c.Cursor = cursor }
}

// WithDirectoryLimit sets the maximum number of entries per page.
func WithDirectoryLimit(limit int) DirectoryBrowseOption {
	return func(c *directoryBrowseConfig) { c.Limit = limit }
}

// WithDirectoryServer sets a specific server peer ID for directory operations.
func WithDirectoryServer(id peer.ID) DirectoryBrowseOption {
	return func(c *directoryBrowseConfig) { c.ServerPeerID = &id }
}

// MailboxOption configures CreateMailbox behavior.
type MailboxOption func(*mailboxConfig)

type mailboxConfig struct {
	MaxMessages    *int
	RetentionDays  *int
	RetentionCount *int
}

// WithMailboxMaxMessages sets the maximum number of messages for a mailbox.
func WithMailboxMaxMessages(n int) MailboxOption {
	return func(c *mailboxConfig) {
		c.MaxMessages = &n
	}
}

// WithRetentionDays sets the retention period in days for a mailbox.
func WithRetentionDays(d int) MailboxOption {
	return func(c *mailboxConfig) {
		c.RetentionDays = &d
	}
}

// WithRetentionCount sets the retention count for a mailbox.
func WithRetentionCount(cnt int) MailboxOption {
	return func(c *mailboxConfig) {
		c.RetentionCount = &cnt
	}
}

// FeedOption configures feed operations.
type FeedOption func(*feedConfig)

type feedConfig struct {
	ServerPeerID  *peer.ID
	Collaborative bool
}

// WithFeedServer sets a specific server peer ID for the feed operation.
func WithFeedServer(id peer.ID) FeedOption {
	return func(c *feedConfig) {
		c.ServerPeerID = &id
	}
}

// WithCollaborative sets the feed as collaborative, allowing non-owner appends.
func WithCollaborative() FeedOption {
	return func(c *feedConfig) {
		c.Collaborative = true
	}
}

// FeedEntryOption configures feed entry retrieval.
type FeedEntryOption func(*feedEntryConfig)

type feedEntryConfig struct {
	ServerPeerID *peer.ID
	FromSequence *int
	ToSequence   *int
	Limit        *int
	EntryType    string
}

// WithFeedEntryFrom sets the starting sequence number.
func WithFeedEntryFrom(seq int) FeedEntryOption {
	return func(c *feedEntryConfig) {
		c.FromSequence = &seq
	}
}

// WithFeedEntryTo sets the ending sequence number.
func WithFeedEntryTo(seq int) FeedEntryOption {
	return func(c *feedEntryConfig) {
		c.ToSequence = &seq
	}
}

// WithFeedEntryLimit sets the maximum number of entries to return.
func WithFeedEntryLimit(n int) FeedEntryOption {
	return func(c *feedEntryConfig) {
		c.Limit = &n
	}
}

// WithFeedEntryType filters entries by type.
func WithFeedEntryType(t string) FeedEntryOption {
	return func(c *feedEntryConfig) {
		c.EntryType = t
	}
}

// WithFeedEntryServer sets a specific server peer ID for entry retrieval.
func WithFeedEntryServer(id peer.ID) FeedEntryOption {
	return func(c *feedEntryConfig) {
		c.ServerPeerID = &id
	}
}

// CollectionOption configures collection operations.
type CollectionOption func(*collectionConfig)

type collectionConfig struct {
	ServerPeerID *peer.ID
	IfMatch      string
}

// WithCollectionServer sets a specific server peer ID for the collection operation.
func WithCollectionServer(id peer.ID) CollectionOption {
	return func(c *collectionConfig) {
		c.ServerPeerID = &id
	}
}

// WithCollectionIfMatch sets the If-Match header for optimistic locking on PUT.
func WithCollectionIfMatch(etag string) CollectionOption {
	return func(c *collectionConfig) {
		c.IfMatch = etag
	}
}

// CollectionQueryOption configures collection query and list operations.
type CollectionQueryOption func(*collectionQueryConfig)

type collectionQueryConfig struct {
	ServerPeerID *peer.ID
	SortField    string
	SortAsc      *bool
	Limit        *int
	Offset       *int
}

// WithQuerySort sets the sort field and direction for query results.
func WithQuerySort(field string, asc bool) CollectionQueryOption {
	return func(c *collectionQueryConfig) {
		c.SortField = field
		c.SortAsc = &asc
	}
}

// WithQueryLimit sets the maximum number of items to return.
func WithQueryLimit(n int) CollectionQueryOption {
	return func(c *collectionQueryConfig) {
		c.Limit = &n
	}
}

// WithQueryOffset sets the offset for pagination.
func WithQueryOffset(n int) CollectionQueryOption {
	return func(c *collectionQueryConfig) {
		c.Offset = &n
	}
}

// WithQueryServer sets a specific server peer ID for query operations.
func WithQueryServer(id peer.ID) CollectionQueryOption {
	return func(c *collectionQueryConfig) {
		c.ServerPeerID = &id
	}
}
