package storage

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-ricochet/internal/core"
)

// MailboxRecord represents a stored mailbox.
type MailboxRecord struct {
	ID             int64          `json:"id"`
	OwnerPeerID    string         `json:"ownerPeerId"`
	FolderPath     string         `json:"folderPath"`
	Type           core.MailboxType `json:"type"`
	CreatedAt      time.Time      `json:"createdAt"`
	LastAccessAt   time.Time      `json:"lastAccessAt"`
	MaxMessages    int            `json:"maxMessages"`
	RetentionDays  int            `json:"retentionDays"`
	RetentionCount *int           `json:"retentionCount,omitempty"`
}

// FullPath returns "ownerPeerId/folderPath".
func (r *MailboxRecord) FullPath() string {
	return r.OwnerPeerID + "/" + r.FolderPath
}

// OwnerPeerIDObj parses the owner peer ID.
func (r *MailboxRecord) OwnerPeerIDObj() (peer.ID, error) {
	return peer.Decode(r.OwnerPeerID)
}

// StoredMessageRecord represents a stored message.
type StoredMessageRecord struct {
	ID              int64               `json:"id"`
	MailboxID       int64               `json:"mailboxId"`
	SequenceNumber  int                 `json:"sequenceNumber"`
	MessageID       string              `json:"messageId"`
	RecipientPeerID string              `json:"recipientPeerId"`
	SenderPeerID    string              `json:"senderPeerId"`
	Payload         []byte              `json:"payload"`
	Priority        core.MessagePriority `json:"priority"`
	CreatedAt       time.Time           `json:"createdAt"`
	ExpiresAt       time.Time           `json:"expiresAt"`
	HopCount        int                 `json:"hopCount"`
	FlagsBitmap     uint32              `json:"flagsBitmap"`
	Persistent      bool                `json:"persistent"`
	FolderPath      *string             `json:"folderPath,omitempty"`
}

// AclRecord represents an access control entry.
type AclRecord struct {
	ID           int64           `json:"id"`
	MailboxID    int64           `json:"mailboxId"`
	PeerIDBase58 string          `json:"peerIdBase58"`
	AccessMode   core.AccessMode `json:"accessMode"`
	GrantedAt    time.Time       `json:"grantedAt"`
}

// PeerIDObj parses the peer ID.
func (r *AclRecord) PeerIDObj() (peer.ID, error) {
	return peer.Decode(r.PeerIDBase58)
}

// ReaderCursorRecord tracks per-reader position in a mailbox.
type ReaderCursorRecord struct {
	ID               int64     `json:"id"`
	MailboxID        int64     `json:"mailboxId"`
	ReaderPeerID     string    `json:"readerPeerId"`
	LastSequenceRead int       `json:"lastSequenceRead"`
	LastAccessAt     time.Time `json:"lastAccessAt"`
}

// BlockRecord represents a CRDT content-addressed block.
type BlockRecord struct {
	ID           int64              `json:"id"`
	CID          string             `json:"cid"`
	PayloadBytes []byte             `json:"payloadBytes"`
	Collection   *string            `json:"collection,omitempty"`
	Metadata     map[string]any     `json:"metadata,omitempty"`
	Tombstoned   bool               `json:"tombstoned"`
	Timestamp    time.Time          `json:"timestamp"`
}

// DocumentRecord represents a stored document.
type DocumentRecord struct {
	ID                 int64     `json:"id"`
	OwnerPeerID        string    `json:"ownerPeerId"`
	Path               string    `json:"path"`
	Content            []byte    `json:"content"`
	ContentType        string    `json:"contentType"`
	ContentHash        string    `json:"contentHash"` // ETag
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
	UpdatedByPeerID    string    `json:"updatedByPeerId"`
	VersionNumber      int       `json:"versionNumber"`
	HistoryEnabled     bool      `json:"historyEnabled"`
	MaxHistoryVersions *int      `json:"maxHistoryVersions,omitempty"`
	VersionVector      *string   `json:"versionVector,omitempty"`
}

// DocumentSummary is document metadata without the body. Listing returns these
// rather than DocumentRecord: the body is never part of a listing response, and
// selecting it meant every LIST pulled the caller's entire document set out of
// the database purely so the handler could take its length.
type DocumentSummary struct {
	Path          string    `json:"path"`
	ContentType   string    `json:"contentType"`
	ContentHash   string    `json:"contentHash"`
	Size          int       `json:"size"`
	UpdatedAt     time.Time `json:"updatedAt"`
	VersionNumber int       `json:"versionNumber"`
}

// FullPath returns "ownerPeerId/doc/path".
func (r *DocumentRecord) FullPath() string {
	return r.OwnerPeerID + "/doc/" + r.Path
}

// DocumentVersionRecord represents a historical document version.
type DocumentVersionRecord struct {
	ID              int64     `json:"id"`
	DocumentID      int64     `json:"documentId"`
	VersionNumber   int       `json:"versionNumber"`
	Content         []byte    `json:"content"`
	ContentHash     string    `json:"contentHash"`
	ContentType     string    `json:"contentType"`
	CreatedAt       time.Time `json:"createdAt"`
	CreatedByPeerID string    `json:"createdByPeerId"`
}

// DocumentPutResult is the result of a document put operation.
type DocumentPutResult struct {
	ContentHash string    `json:"contentHash"`
	Created     bool      `json:"created"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// DirectoryEntry represents a user's listing in the public directory.
type DirectoryEntry struct {
	ID          int64          `json:"id"`
	OwnerPeerID string         `json:"ownerPeerId"`
	DisplayName string         `json:"displayName"`
	Bio         string         `json:"bio"`
	AvatarHash  string         `json:"avatarHash"`
	ListedAt    time.Time      `json:"listedAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
	Extras      map[string]any `json:"extras,omitempty"`
}

// DirectoryPage represents a paginated directory browse result.
type DirectoryPage struct {
	Entries    []*DirectoryEntry `json:"entries"`
	NextCursor string            `json:"nextCursor,omitempty"`
	HasMore    bool              `json:"hasMore"`
}

// FeedRecord represents a stored feed.
type FeedRecord struct {
	ID                int64     `json:"id"`
	OwnerPeerID       string    `json:"ownerPeerId"`
	Path              string    `json:"path"`
	Title             string    `json:"title"`
	Description       string    `json:"description"`
	EntryContentType  string    `json:"entryContentType"`
	CreatedAt         time.Time `json:"createdAt"`
	LastEntryAt       time.Time `json:"lastEntryAt"`
	CurrentSequence   int       `json:"currentSequence"`
	MaxEntries        *int      `json:"maxEntries,omitempty"`
	MaxAgeDays        *int      `json:"maxAgeDays,omitempty"`
	CollaborativeMode bool      `json:"collaborativeMode"`
}

// FullPath returns "ownerPeerId/feed/path".
func (r *FeedRecord) FullPath() string {
	return r.OwnerPeerID + "/feed/" + r.Path
}

// FeedEntryRecord represents a single entry in a feed.
type FeedEntryRecord struct {
	ID              int64     `json:"id"`
	FeedID          int64     `json:"feedId"`
	SequenceNumber  int       `json:"sequenceNumber"`
	Content         []byte    `json:"content"`
	ContentHash     string    `json:"contentHash"`
	CreatedAt       time.Time `json:"createdAt"`
	CreatedByPeerID string    `json:"createdByPeerId"`
	EntryType       string    `json:"entryType,omitempty"`
}

// MultiFeedQuery describes a single feed to retrieve in a batch request.
type MultiFeedQuery struct {
	OwnerPeerID  string
	Path         string
	FromSequence *int
	Limit        int
}

// MultiFeedResult holds the entries returned for one feed in a batch request.
type MultiFeedResult struct {
	Entries []*FeedEntryRecord
	HasMore bool
	Error   string // per-feed error (non-fatal)
}

// CollectionRecord represents a stored collection.
type CollectionRecord struct {
	ID             int64     `json:"id"`
	OwnerPeerID    string    `json:"ownerPeerId"`
	Path           string    `json:"path"`
	Name           string    `json:"name,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedAt time.Time `json:"lastModifiedAt"`
	RecordCount    int       `json:"recordCount"`
}

// FullPath returns "ownerPeerId/collection/path".
func (r *CollectionRecord) FullPath() string {
	return r.OwnerPeerID + "/collection/" + r.Path
}

// CollectionItemRecord represents a single record in a collection.
type CollectionItemRecord struct {
	ID              int64     `json:"id"`
	CollectionID    int64     `json:"collectionId"`
	Key             string    `json:"key"`
	Content         []byte    `json:"content"`
	ContentHash     string    `json:"contentHash"`
	Version         int       `json:"version"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	UpdatedByPeerID string    `json:"updatedByPeerId"`
}

// CollectionQueryResult wraps a paged query response.
type CollectionQueryResult struct {
	Items      []*CollectionItemRecord `json:"items"`
	TotalCount int                     `json:"totalCount"`
	HasMore    bool                    `json:"hasMore"`
}

// Storage errors.
var (
	ErrDocumentNotFound       = fmt.Errorf("document not found")
	ErrDirectoryEntryNotFound = fmt.Errorf("directory entry not found")
	ErrFeedNotFound           = fmt.Errorf("feed not found")
	ErrCollectionNotFound     = fmt.Errorf("collection not found")
	ErrMailboxNotFound        = fmt.Errorf("mailbox not found")

	// ErrInvalidCursor means a pagination cursor could not be read. It is a
	// distinct error rather than a silent fall back to the first page: a
	// listing that quietly restarts looks like a listing that never ends, and
	// a client looping until "no more" never stops.
	ErrInvalidCursor = fmt.Errorf("invalid pagination cursor")
)

// DocumentSizeExceededError indicates a document exceeds the size limit.
type DocumentSizeExceededError struct {
	ActualSize int
	MaxSize    int
}

func (e *DocumentSizeExceededError) Error() string {
	return fmt.Sprintf("document size %d exceeds maximum %d", e.ActualSize, e.MaxSize)
}

// DocumentConflictError indicates an ETag mismatch (optimistic locking).
type DocumentConflictError struct {
	ExpectedHash string
	ActualHash   string
}

func (e *DocumentConflictError) Error() string {
	return fmt.Sprintf("document conflict: expected hash %s, got %s", e.ExpectedHash, e.ActualHash)
}

// CollectionItemConflictError indicates a version mismatch (optimistic locking).
type CollectionItemConflictError struct {
	ExpectedHash string
	ActualHash   string
}

func (e *CollectionItemConflictError) Error() string {
	return fmt.Sprintf("collection item conflict: expected hash %s, got %s", e.ExpectedHash, e.ActualHash)
}

// ServerStats is a point-in-time aggregate across every owner, for operator
// views and metrics.
//
// Everything else on the Storage interface is scoped to one owner or one
// mailbox, which is right for serving requests and useless for answering "how
// full is this server". These figures come from a single pass and are sampled
// on a timer rather than computed per request.
type ServerStats struct {
	// SampledAt is when the pass ran. It travels with the numbers because a
	// cached figure that has silently gone stale is worse than no figure.
	SampledAt time.Time

	// Mailboxes is the total across all owners.
	Mailboxes int

	// Messages is the total stored message count.
	Messages int64

	// MessageBytes is the summed payload length. This is what users stored,
	// not what it costs on disk.
	MessageBytes int64

	// DatabaseBytes is the size of the database on disk, including indexes
	// and unreclaimed space. This is the figure that runs a server out of
	// room, so it is what capacity is measured against.
	DatabaseBytes int64

	// MailboxesNearCapacity counts mailboxes at or above NearCapacityRatio of
	// their own max_messages. A mailbox at its cap silently drops or evicts,
	// so this is the number that predicts data loss.
	MailboxesNearCapacity int

	// NearCapacityRatio is the fraction used for the count above.
	NearCapacityRatio float64

	// Depth is the distribution of mailbox sizes. A single enormous mailbox
	// and a million small ones need different responses, and a mean hides
	// which one you have.
	Depth []DepthBucket
}

// DepthBucket counts mailboxes whose message count falls within [Min, Max].
type DepthBucket struct {
	// Label names the range, e.g. "10-99". It is a fixed string so it is safe
	// as a metric label.
	Label string

	// Min and Max bound the bucket inclusively. Max is -1 when unbounded.
	Min int
	Max int

	// Mailboxes is how many fall in this range.
	Mailboxes int
}

// Operator page-size bounds. Every cross-owner query is capped here rather
// than at the caller, because a query that is not owner-scoped has no natural
// bound and one unbounded scan on a busy server is enough to matter.
const (
	// DefaultOperatorPageSize is what a request that names no limit gets.
	DefaultOperatorPageSize = 20

	// MaxOperatorPageSize is the ceiling, whatever a caller asks for.
	MaxOperatorPageSize = 500
)

// ClampPageSize bounds a requested page size to something a cross-owner query
// may safely return.
func ClampPageSize(n int) int {
	if n <= 0 {
		return DefaultOperatorPageSize
	}
	if n > MaxOperatorPageSize {
		return MaxOperatorPageSize
	}
	return n
}

// MailboxSort selects the order of a MailboxUsage listing.
type MailboxSort string

const (
	// SortByFill orders by how close a mailbox is to its cap, which is the
	// order that answers "what is about to start evicting". A mailbox with no
	// cap has no fill ratio and sorts last regardless of size.
	SortByFill MailboxSort = "fill"

	// SortByCount orders by raw message count, which is the order that
	// answers "who is holding the most". An uncapped mailbox can dominate
	// this list without being in any danger.
	SortByCount MailboxSort = "count"
)

// MailboxUsageQuery selects and bounds a cross-owner mailbox listing.
type MailboxUsageQuery struct {
	// Owner restricts the listing to one peer. Empty means every owner.
	Owner string

	// Sort defaults to SortByFill.
	Sort MailboxSort

	// Limit is clamped by ClampPageSize; Offset pages through the result.
	Limit  int
	Offset int
}

// MailboxUsage is one mailbox as an operator needs to see it: enough to tell
// whether it is full, who owns it, and how much it is holding.
//
// It is deliberately not a MailboxRecord. A record describes configuration —
// caps and retention — and says nothing about what is actually stored, which
// is the entire question being asked here.
type MailboxUsage struct {
	MailboxID    int64  `json:"id"`
	OwnerPeerID  string `json:"owner"`
	FolderPath   string `json:"folderPath"`
	MessageCount int    `json:"messageCount"`

	// MaxMessages is the cap. Zero or less means uncapped, in which case
	// FillRatio is zero and Full is false however much is stored.
	MaxMessages int `json:"maxMessages"`

	MessageBytes int64 `json:"messageBytes"`

	// FillRatio is MessageCount/MaxMessages, computed by the database so the
	// ordering and the reported figure cannot disagree.
	FillRatio float64 `json:"fillRatio"`

	// LastMessageAt is nil for a mailbox that has never received one.
	LastMessageAt *time.Time `json:"lastMessageAt,omitempty"`
}

// Full reports whether the mailbox has reached its cap, which is the point at
// which it starts evicting.
func (u *MailboxUsage) Full() bool {
	return u.MaxMessages > 0 && u.MessageCount >= u.MaxMessages
}

// OwnerUsage is one peer's total footprint across all of its mailboxes.
type OwnerUsage struct {
	OwnerPeerID  string `json:"owner"`
	Mailboxes    int    `json:"mailboxes"`
	Messages     int64  `json:"messages"`
	MessageBytes int64  `json:"messageBytes"`
}
