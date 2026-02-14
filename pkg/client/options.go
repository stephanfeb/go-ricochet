package client

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

// SendOption configures SendMessage behavior.
type SendOption func(*sendConfig)

type sendConfig struct {
	FolderPath string
	Priority   core.MessagePriority
	Expiry     time.Duration
	Persistent bool
}

// WithFolderPath sets the target folder path for the message.
func WithFolderPath(path string) SendOption {
	return func(c *sendConfig) {
		c.FolderPath = path
	}
}

// WithPriority sets the message priority level.
func WithPriority(p core.MessagePriority) SendOption {
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

// RetrieveOption configures RetrieveMessages behavior.
type RetrieveOption func(*retrieveConfig)

type retrieveConfig struct {
	FolderPath   string
	FromSequence *uint64
	MaxMessages  *int
	MinPriority  *core.MessagePriority
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
func WithMinPriority(p core.MessagePriority) RetrieveOption {
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
