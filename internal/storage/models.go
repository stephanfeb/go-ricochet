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
