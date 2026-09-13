// Package storagetest holds an in-memory storage.Storage for unit tests.
//
// It models enough of each store — mailboxes, messages, ACLs, cursors,
// documents, feeds, collections, the directory — for the handlers and the
// mailbox types to be exercised without PostgreSQL. It is deliberately not a
// second implementation to keep in step with the SQL: where a behaviour is
// specific to the database (JSONB operators, keyset cursors, statistics) the
// fake either models the simple case or refuses with ErrNotFaked, so a test
// that wanders past what it covers fails loudly rather than passing on a
// shortcut.
package storagetest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// ErrNotFaked is returned by the operations the fake does not model.
var ErrNotFaked = errors.New("storagetest: operation not modelled by the fake")

// Fake is an in-memory storage.Storage. It is safe for concurrent use.
type Fake struct {
	mu sync.Mutex

	nextID int64

	mailboxes map[int64]*storage.MailboxRecord
	sequences map[int64]int
	messages  map[int64][]*core.Message // by mailbox ID, in store order
	acls      map[int64]map[string]*storage.AclRecord
	cursors   map[int64]map[string]int

	documents map[string]*storage.DocumentRecord          // owner/path
	versions  map[string][]*storage.DocumentVersionRecord // owner/path, ascending

	feeds   map[string]*storage.FeedRecord // owner/path
	entries map[int64][]*storage.FeedEntryRecord

	collections map[string]*storage.CollectionRecord // owner/path
	items       map[int64]map[string]*storage.CollectionItemRecord

	directory map[string]*storage.DirectoryEntry

	readers map[storage.StoreKind]map[int64]map[string]time.Time // reader lists by store and row id
}

// New returns an empty fake.
func New() *Fake {
	return &Fake{
		mailboxes:   map[int64]*storage.MailboxRecord{},
		sequences:   map[int64]int{},
		messages:    map[int64][]*core.Message{},
		acls:        map[int64]map[string]*storage.AclRecord{},
		cursors:     map[int64]map[string]int{},
		documents:   map[string]*storage.DocumentRecord{},
		versions:    map[string][]*storage.DocumentVersionRecord{},
		feeds:       map[string]*storage.FeedRecord{},
		entries:     map[int64][]*storage.FeedEntryRecord{},
		collections: map[string]*storage.CollectionRecord{},
		items:       map[int64]map[string]*storage.CollectionItemRecord{},
		directory:   map[string]*storage.DirectoryEntry{},
		readers:     map[storage.StoreKind]map[int64]map[string]time.Time{},
	}
}

var _ storage.Storage = (*Fake)(nil)

func (f *Fake) id() int64 {
	f.nextID++
	return f.nextID
}

func hash(content []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(content))
}

func key(owner peer.ID, path string) string { return owner.String() + "/" + path }

// ---------------------------------------------------------------------------
// Inspection helpers for tests
// ---------------------------------------------------------------------------

// AllMessages returns every stored message across all mailboxes, in store
// order within each mailbox and mailbox ID order across them.
func (f *Fake) AllMessages() []*core.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.messages))
	for id := range f.messages {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []*core.Message
	for _, id := range ids {
		out = append(out, f.messages[id]...)
	}
	return out
}

// Messages returns the messages stored in one mailbox, in store order.
func (f *Fake) Messages(mailboxID int64) []*core.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*core.Message(nil), f.messages[mailboxID]...)
}

// MailboxCount reports how many mailboxes exist.
func (f *Fake) MailboxCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mailboxes)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (f *Fake) Initialize(context.Context) error { return nil }
func (f *Fake) Close() error                     { return nil }

// ---------------------------------------------------------------------------
// Mailboxes
// ---------------------------------------------------------------------------

func (f *Fake) findMailbox(owner peer.ID, folder string) *storage.MailboxRecord {
	for _, mb := range f.mailboxes {
		if mb.OwnerPeerID == owner.String() && mb.FolderPath == folder {
			return mb
		}
	}
	return nil
}

func (f *Fake) GetOrCreateMailbox(_ context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (*storage.MailboxRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if mb := f.findMailbox(addr.OwnerID, addr.FolderPath); mb != nil {
		return mb, nil
	}
	now := time.Now()
	mb := &storage.MailboxRecord{
		ID:             f.id(),
		OwnerPeerID:    addr.OwnerID.String(),
		FolderPath:     addr.FolderPath,
		Type:           addr.Type,
		CreatedAt:      now,
		LastAccessAt:   now,
		MaxMessages:    maxMessages,
		RetentionDays:  retentionDays,
		RetentionCount: retentionCount,
	}
	f.mailboxes[mb.ID] = mb
	return mb, nil
}

func (f *Fake) FindMailbox(_ context.Context, owner peer.ID, folder string) (*storage.MailboxRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.findMailbox(owner, folder), nil
}

func (f *Fake) ListMailboxes(_ context.Context, owner peer.ID) ([]*storage.MailboxRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*storage.MailboxRecord
	for _, mb := range f.mailboxes {
		if mb.OwnerPeerID == owner.String() {
			out = append(out, mb)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FolderPath < out[j].FolderPath })
	return out, nil
}

func (f *Fake) UpdateMailbox(_ context.Context, rec *storage.MailboxRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	mb, ok := f.mailboxes[rec.ID]
	if !ok {
		return storage.ErrMailboxNotFound
	}
	mb.MaxMessages = rec.MaxMessages
	mb.RetentionDays = rec.RetentionDays
	mb.RetentionCount = rec.RetentionCount
	mb.LastAccessAt = time.Now()
	return nil
}

func (f *Fake) DeleteMailbox(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.mailboxes[id]; !ok {
		return storage.ErrMailboxNotFound
	}
	delete(f.mailboxes, id)
	delete(f.messages, id)
	delete(f.acls, id)
	delete(f.cursors, id)
	delete(f.sequences, id)
	return nil
}

func (f *Fake) UpdateMailboxAccess(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if mb, ok := f.mailboxes[id]; ok {
		mb.LastAccessAt = time.Now()
	}
	return nil
}

func (f *Fake) CountMailboxes(_ context.Context, owner peer.ID) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mine := 0
	for _, mb := range f.mailboxes {
		if mb.OwnerPeerID == owner.String() {
			mine++
		}
	}
	return mine, len(f.mailboxes), nil
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func (f *Fake) StoreMessage(_ context.Context, mailbox *storage.MailboxRecord, msg *core.Message) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mb, ok := f.mailboxes[mailbox.ID]
	if !ok {
		return 0, storage.ErrMailboxNotFound
	}
	if mb.MaxMessages > 0 && len(f.messages[mb.ID]) >= mb.MaxMessages {
		return 0, &storage.MailboxFullError{Current: len(f.messages[mb.ID]), Max: mb.MaxMessages}
	}
	f.sequences[mb.ID]++
	seq := f.sequences[mb.ID]
	msg.SequenceNumber = uint64(seq)
	f.messages[mb.ID] = append(f.messages[mb.ID], msg)
	mb.MessageCount = len(f.messages[mb.ID])
	mb.LastAccessAt = time.Now()
	return seq, nil
}

func (f *Fake) RetrieveMessages(_ context.Context, mailbox *storage.MailboxRecord, fromSequence, maxMessages *int, minPriority *core.MessagePriority) ([]*core.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*core.Message
	for _, m := range f.messages[mailbox.ID] {
		if fromSequence != nil && int(m.SequenceNumber) < *fromSequence {
			continue
		}
		if minPriority != nil && m.Priority < *minPriority {
			continue
		}
		out = append(out, m)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].SequenceNumber < out[j].SequenceNumber
	})
	if maxMessages != nil && *maxMessages > 0 && len(out) > *maxMessages {
		out = out[:*maxMessages]
	}
	return out, nil
}

// remove deletes messages matching keep==false and fixes the counters.
func (f *Fake) remove(mailboxID int64, drop func(*core.Message) bool) int {
	kept := f.messages[mailboxID][:0]
	n := 0
	for _, m := range f.messages[mailboxID] {
		if drop(m) {
			n++
			continue
		}
		kept = append(kept, m)
	}
	f.messages[mailboxID] = kept
	if mb, ok := f.mailboxes[mailboxID]; ok {
		mb.MessageCount = len(kept)
	}
	return n
}

func (f *Fake) ownedMailboxes(owner peer.ID) []int64 {
	var ids []int64
	for id, mb := range f.mailboxes {
		if mb.OwnerPeerID == owner.String() {
			ids = append(ids, id)
		}
	}
	return ids
}

func inSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func (f *Fake) DeleteMessage(ctx context.Context, messageID string) error {
	return f.DeleteMessages(ctx, []string{messageID})
}

func (f *Fake) DeleteMessages(_ context.Context, messageIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := inSet(messageIDs)
	for id := range f.messages {
		f.remove(id, func(m *core.Message) bool { return set[m.MessageID] })
	}
	return nil
}

func (f *Fake) GetMessageCount(_ context.Context, mailboxID int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages[mailboxID]), nil
}

func (f *Fake) DeleteOwnedMessages(_ context.Context, owner peer.ID, messageIDs []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := inSet(messageIDs)
	n := 0
	for _, id := range f.ownedMailboxes(owner) {
		n += f.remove(id, func(m *core.Message) bool { return set[m.MessageID] })
	}
	return n, nil
}

func (f *Fake) MarkMessagesDelivered(_ context.Context, owner peer.ID, messageIDs []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := inSet(messageIDs)
	n := 0
	for _, id := range f.ownedMailboxes(owner) {
		for _, m := range f.messages[id] {
			if set[m.MessageID] && m.Persistent {
				m.MsgFlags |= core.MsgFlagSeen
				n++
			}
		}
		n += f.remove(id, func(m *core.Message) bool { return set[m.MessageID] && !m.Persistent })
	}
	return n, nil
}

func (f *Fake) UpdateMessageFlags(_ context.Context, owner peer.ID, messageID string, add, remove uint32) (*uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.ownedMailboxes(owner) {
		for _, m := range f.messages[id] {
			if m.MessageID == messageID {
				m.MsgFlags = core.MessageFlags((uint32(m.MsgFlags) | add) &^ remove)
				flags := uint32(m.MsgFlags)
				return &flags, nil
			}
		}
	}
	return nil, nil
}

func (f *Fake) ExpungeMailbox(_ context.Context, mailboxID int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remove(mailboxID, func(m *core.Message) bool { return m.MsgFlags.HasFlag(core.MsgFlagDeleted) }), nil
}

func (f *Fake) ExpungeAllMailboxes(_ context.Context, owner peer.ID) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, id := range f.ownedMailboxes(owner) {
		n += f.remove(id, func(m *core.Message) bool { return m.MsgFlags.HasFlag(core.MsgFlagDeleted) })
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// ACLs and cursors
// ---------------------------------------------------------------------------

func (f *Fake) GrantAccess(_ context.Context, mailboxID int64, peerID peer.ID, mode core.AccessMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.mailboxes[mailboxID]; !ok {
		return storage.ErrMailboxNotFound
	}
	if f.acls[mailboxID] == nil {
		f.acls[mailboxID] = map[string]*storage.AclRecord{}
	}
	f.acls[mailboxID][peerID.String()] = &storage.AclRecord{
		ID: f.id(), MailboxID: mailboxID, PeerIDBase58: peerID.String(), AccessMode: mode, GrantedAt: time.Now(),
	}
	return nil
}

func (f *Fake) RevokeAccess(_ context.Context, mailboxID int64, peerID peer.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.acls[mailboxID], peerID.String())
	return nil
}

func (f *Fake) CheckAccess(_ context.Context, mailboxID int64, peerID peer.ID, required core.AccessMode) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.acls[mailboxID][peerID.String()]
	if !ok {
		return false, nil
	}
	switch required {
	case core.AccessReadOnly:
		return rec.AccessMode == core.AccessReadOnly || rec.AccessMode == core.AccessReadWrite, nil
	case core.AccessWriteOnly:
		return rec.AccessMode == core.AccessWriteOnly || rec.AccessMode == core.AccessReadWrite, nil
	case core.AccessReadWrite:
		return rec.AccessMode == core.AccessReadWrite, nil
	}
	return false, nil
}

func (f *Fake) ListACL(_ context.Context, mailboxID int64) ([]*storage.AclRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*storage.AclRecord
	for _, rec := range f.acls[mailboxID] {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerIDBase58 < out[j].PeerIDBase58 })
	return out, nil
}

func (f *Fake) GetCursor(_ context.Context, mailboxID int64, reader peer.ID) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursors[mailboxID][reader.String()], nil
}

func (f *Fake) UpdateCursor(_ context.Context, mailboxID int64, reader peer.ID, sequence int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cursors[mailboxID] == nil {
		f.cursors[mailboxID] = map[string]int{}
	}
	f.cursors[mailboxID][reader.String()] = sequence
	return nil
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

func (f *Fake) GetDocument(_ context.Context, owner peer.ID, path string) (*storage.DocumentRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	doc, ok := f.documents[key(owner, path)]
	if !ok {
		return nil, nil
	}
	cp := *doc
	return &cp, nil
}

func (f *Fake) HeadDocument(_ context.Context, owner peer.ID, path string) (*storage.DocumentRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	doc, ok := f.documents[key(owner, path)]
	if !ok {
		return nil, nil
	}
	cp := *doc
	cp.Content = nil
	return &cp, nil
}

func (f *Fake) putDocument(owner peer.ID, path string, content []byte, contentType string, updatedBy peer.ID, ifMatch *string, visibility *core.Visibility) (*storage.DocumentPutResult, error) {
	if len(content) > storage.MaxDocumentSize {
		return nil, &storage.DocumentSizeExceededError{ActualSize: len(content), MaxSize: storage.MaxDocumentSize}
	}
	k := key(owner, path)
	now := time.Now()
	existing, found := f.documents[k]
	if ifMatch != nil && found && existing.ContentHash != *ifMatch {
		return nil, &storage.DocumentConflictError{ExpectedHash: *ifMatch, ActualHash: existing.ContentHash}
	}
	if !found {
		vis := core.VisibilityPrivate
		if visibility != nil {
			vis = *visibility
		}
		f.documents[k] = &storage.DocumentRecord{
			ID: f.id(), OwnerPeerID: owner.String(), Path: path, Content: content, ContentHash: hash(content),
			ContentLength: len(content), ContentType: contentType, CreatedAt: now, UpdatedAt: now,
			UpdatedByPeerID: updatedBy.String(), VersionNumber: 1, HistoryEnabled: true, Visibility: vis,
		}
		return &storage.DocumentPutResult{ContentHash: hash(content), Created: true, UpdatedAt: now}, nil
	}
	if visibility != nil {
		existing.Visibility = *visibility
	}
	f.versions[k] = append(f.versions[k], &storage.DocumentVersionRecord{
		ID: f.id(), DocumentID: existing.ID, VersionNumber: existing.VersionNumber, Content: existing.Content,
		ContentLength: existing.ContentLength, ContentHash: existing.ContentHash, ContentType: existing.ContentType,
		CreatedAt: existing.UpdatedAt, CreatedByPeerID: existing.UpdatedByPeerID,
	})
	existing.Content = content
	existing.ContentHash = hash(content)
	existing.ContentLength = len(content)
	if contentType != "" {
		existing.ContentType = contentType
	}
	existing.UpdatedAt = now
	existing.UpdatedByPeerID = updatedBy.String()
	existing.VersionNumber++
	return &storage.DocumentPutResult{ContentHash: existing.ContentHash, Created: false, UpdatedAt: now}, nil
}

func (f *Fake) PutDocument(_ context.Context, owner peer.ID, path string, content []byte, contentType string, updatedBy peer.ID, ifMatch *string, visibility *core.Visibility) (*storage.DocumentPutResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putDocument(owner, path, content, contentType, updatedBy, ifMatch, visibility)
}

func (f *Fake) PatchDocument(_ context.Context, owner peer.ID, path string, patch map[string]any, updatedBy peer.ID, ifMatch *string) (*storage.DocumentPutResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	existing, ok := f.documents[key(owner, path)]
	if !ok {
		return nil, storage.ErrDocumentNotFound
	}
	if ifMatch != nil && existing.ContentHash != *ifMatch {
		return nil, &storage.DocumentConflictError{ExpectedHash: *ifMatch, ActualHash: existing.ContentHash}
	}
	var current map[string]any
	if err := json.Unmarshal(existing.Content, &current); err != nil {
		return nil, fmt.Errorf("parse existing document: %w", err)
	}
	merged := mergePatch(current, patch)
	content, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}
	return f.putDocument(owner, path, content, existing.ContentType, updatedBy, nil, nil)
}

// mergePatch applies an RFC 7386 merge patch.
func mergePatch(target, patch map[string]any) map[string]any {
	out := make(map[string]any, len(target))
	for k, v := range target {
		out[k] = v
	}
	for k, v := range patch {
		if v == nil {
			delete(out, k)
			continue
		}
		if pm, ok := v.(map[string]any); ok {
			if tm, ok := out[k].(map[string]any); ok {
				out[k] = mergePatch(tm, pm)
				continue
			}
			out[k] = mergePatch(map[string]any{}, pm)
			continue
		}
		out[k] = v
	}
	return out
}

func (f *Fake) DeleteDocument(_ context.Context, owner peer.ID, path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	doc, ok := f.documents[k]
	if !ok {
		return false, nil
	}
	delete(f.documents, k)
	delete(f.versions, k)
	delete(f.readers[storage.StoreDocument], doc.ID)
	return true, nil
}

func (f *Fake) ListDocuments(_ context.Context, owner, reader peer.ID, afterPath string, limit int) ([]*storage.DocumentSummary, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 || limit > storage.MaxDocumentListLimit {
		limit = storage.MaxDocumentListLimit
	}
	var out []*storage.DocumentSummary
	for _, d := range f.documents {
		if d.OwnerPeerID != owner.String() || d.Path <= afterPath {
			continue
		}
		if !f.readable(storage.StoreDocument, d.ID, d.OwnerPeerID, d.Visibility, reader) {
			continue
		}
		out = append(out, &storage.DocumentSummary{
			Path: d.Path, ContentType: d.ContentType, ContentHash: d.ContentHash, Size: d.ContentLength,
			UpdatedAt: d.UpdatedAt, VersionNumber: d.VersionNumber, Visibility: d.Visibility,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	return out, hasMore, nil
}

func (f *Fake) GetDocumentHistory(_ context.Context, owner peer.ID, path string, maxVersions *int) ([]*storage.DocumentVersionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	if _, ok := f.documents[k]; !ok {
		return nil, storage.ErrDocumentNotFound
	}
	var out []*storage.DocumentVersionRecord
	vs := f.versions[k]
	for i := len(vs) - 1; i >= 0; i-- {
		cp := *vs[i]
		cp.Content = nil
		out = append(out, &cp)
		if maxVersions != nil && *maxVersions > 0 && len(out) >= *maxVersions {
			break
		}
	}
	return out, nil
}

func (f *Fake) GetDocumentAtVersion(_ context.Context, owner peer.ID, path string, version int) (*storage.DocumentVersionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.versions[key(owner, path)] {
		if v.VersionNumber == version {
			cp := *v
			return &cp, nil
		}
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Feeds
// ---------------------------------------------------------------------------

func (f *Fake) CreateFeed(_ context.Context, owner peer.ID, path, title, description string, collaborative bool, visibility core.Visibility) (*storage.FeedRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	if feed, ok := f.feeds[k]; ok {
		// As in SQL: a re-create refreshes the text and leaves the access
		// model (collaborative, visibility) alone.
		if title != "" {
			feed.Title = title
		}
		if description != "" {
			feed.Description = description
		}
		return feed, nil
	}
	feed := &storage.FeedRecord{
		ID: f.id(), OwnerPeerID: owner.String(), Path: path, Title: title, Description: description,
		EntryContentType: "application/json", CreatedAt: time.Now(), CollaborativeMode: collaborative,
		Visibility: visibility,
	}
	f.feeds[k] = feed
	return feed, nil
}

func (f *Fake) GetFeed(_ context.Context, owner peer.ID, path string) (*storage.FeedRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	feed, ok := f.feeds[key(owner, path)]
	if !ok {
		return nil, nil
	}
	return feed, nil
}

func (f *Fake) DeleteFeed(_ context.Context, owner peer.ID, path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	feed, ok := f.feeds[k]
	if !ok {
		return false, nil
	}
	delete(f.feeds, k)
	delete(f.entries, feed.ID)
	delete(f.readers[storage.StoreFeed], feed.ID)
	return true, nil
}

func (f *Fake) ListFeeds(_ context.Context, owner, reader peer.ID) ([]*storage.FeedRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*storage.FeedRecord
	for _, feed := range f.feeds {
		if feed.OwnerPeerID == owner.String() &&
			f.readable(storage.StoreFeed, feed.ID, feed.OwnerPeerID, feed.Visibility, reader) {
			out = append(out, feed)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func (f *Fake) feedByID(id int64) *storage.FeedRecord {
	for _, feed := range f.feeds {
		if feed.ID == id {
			return feed
		}
	}
	return nil
}

func (f *Fake) AppendFeedEntry(_ context.Context, feedID int64, content []byte, createdBy peer.ID, entryType string) (*storage.FeedEntryRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	feed := f.feedByID(feedID)
	if feed == nil {
		return nil, storage.ErrFeedNotFound
	}
	feed.CurrentSequence++
	feed.LastEntryAt = time.Now()
	entry := &storage.FeedEntryRecord{
		ID: f.id(), FeedID: feedID, SequenceNumber: feed.CurrentSequence, Content: content, ContentHash: hash(content),
		CreatedAt: feed.LastEntryAt, CreatedByPeerID: createdBy.String(), EntryType: entryType,
	}
	f.entries[feedID] = append(f.entries[feedID], entry)
	return entry, nil
}

func (f *Fake) GetFeedEntry(_ context.Context, feedID int64, seq int) (*storage.FeedEntryRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries[feedID] {
		if e.SequenceNumber == seq {
			return e, nil
		}
	}
	return nil, nil
}

func (f *Fake) GetFeedEntries(_ context.Context, feedID int64, fromSeq, toSeq *int, entryType string, limit int) ([]*storage.FeedEntryRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*storage.FeedEntryRecord
	for _, e := range f.entries[feedID] {
		if fromSeq != nil && e.SequenceNumber < *fromSeq {
			continue
		}
		if toSeq != nil && e.SequenceNumber > *toSeq {
			continue
		}
		if entryType != "" && e.EntryType != entryType {
			continue
		}
		out = append(out, e)
	}
	hasMore := limit > 0 && len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	return out, hasMore, nil
}

func (f *Fake) EnforceFeedRetention(_ context.Context, feed *storage.FeedRecord) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pruneFeed(feed), nil
}

func (f *Fake) pruneFeed(feed *storage.FeedRecord) int {
	if feed.MaxEntries == nil || *feed.MaxEntries <= 0 {
		return 0
	}
	es := f.entries[feed.ID]
	if len(es) <= *feed.MaxEntries {
		return 0
	}
	drop := len(es) - *feed.MaxEntries
	f.entries[feed.ID] = es[drop:]
	return drop
}

func (f *Fake) EnforceAllFeedRetention(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, feed := range f.feeds {
		n += f.pruneFeed(feed)
	}
	return n, nil
}

func (f *Fake) CountFeedEntries(_ context.Context, feedID int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries[feedID]), nil
}

// GetMultiFeedEntries resolves each query as the SQL implementation does: a
// bad owner or a missing feed is that feed's error, not the batch's.
func (f *Fake) GetMultiFeedEntries(ctx context.Context, queries []storage.MultiFeedQuery) (map[string]*storage.MultiFeedResult, error) {
	results := make(map[string]*storage.MultiFeedResult, len(queries))
	for _, q := range queries {
		k := q.OwnerPeerID + "/" + q.Path
		owner, err := peer.Decode(q.OwnerPeerID)
		if err != nil {
			results[k] = &storage.MultiFeedResult{Error: "invalid ownerPeerId"}
			continue
		}
		feed, _ := f.GetFeed(ctx, owner, q.Path)
		if feed == nil {
			results[k] = &storage.MultiFeedResult{Error: "feed not found"}
			continue
		}
		limit := q.Limit
		if limit <= 0 {
			limit = 50
		}
		entries, hasMore, err := f.GetFeedEntries(ctx, feed.ID, q.FromSequence, nil, "", limit)
		if err != nil {
			results[k] = &storage.MultiFeedResult{Error: err.Error()}
			continue
		}
		results[k] = &storage.MultiFeedResult{Entries: entries, HasMore: hasMore}
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

func (f *Fake) CreateCollection(_ context.Context, owner peer.ID, path, name string, visibility core.Visibility) (*storage.CollectionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	if _, ok := f.collections[k]; ok {
		return nil, fmt.Errorf("create collection: duplicate key value violates unique constraint")
	}
	now := time.Now()
	coll := &storage.CollectionRecord{ID: f.id(), OwnerPeerID: owner.String(), Path: path, Name: name, CreatedAt: now, LastModifiedAt: now, Visibility: visibility}
	f.collections[k] = coll
	f.items[coll.ID] = map[string]*storage.CollectionItemRecord{}
	return coll, nil
}

func (f *Fake) GetCollection(_ context.Context, owner peer.ID, path string) (*storage.CollectionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	coll, ok := f.collections[key(owner, path)]
	if !ok {
		return nil, nil
	}
	return coll, nil
}

func (f *Fake) DeleteCollection(_ context.Context, owner peer.ID, path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(owner, path)
	coll, ok := f.collections[k]
	if !ok {
		return false, nil
	}
	delete(f.collections, k)
	delete(f.items, coll.ID)
	delete(f.readers[storage.StoreCollection], coll.ID)
	return true, nil
}

func (f *Fake) ListCollections(_ context.Context, owner, reader peer.ID) ([]*storage.CollectionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*storage.CollectionRecord
	for _, c := range f.collections {
		if c.OwnerPeerID == owner.String() &&
			f.readable(storage.StoreCollection, c.ID, c.OwnerPeerID, c.Visibility, reader) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func (f *Fake) collectionByID(id int64) *storage.CollectionRecord {
	for _, c := range f.collections {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (f *Fake) GetCollectionItem(_ context.Context, collectionID int64, itemKey string) (*storage.CollectionItemRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.items[collectionID][itemKey]
	if !ok {
		return nil, nil
	}
	return item, nil
}

func (f *Fake) PutCollectionItem(_ context.Context, collectionID int64, itemKey string, content []byte, updatedBy peer.ID, ifMatch *string) (*storage.CollectionItemRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	coll := f.collectionByID(collectionID)
	if coll == nil {
		return nil, false, storage.ErrCollectionNotFound
	}
	now := time.Now()
	existing, ok := f.items[collectionID][itemKey]
	if !ok {
		item := &storage.CollectionItemRecord{
			ID: f.id(), CollectionID: collectionID, Key: itemKey, Content: content, ContentHash: hash(content),
			Version: 1, CreatedAt: now, UpdatedAt: now, UpdatedByPeerID: updatedBy.String(),
		}
		f.items[collectionID][itemKey] = item
		coll.RecordCount++
		coll.LastModifiedAt = now
		return item, true, nil
	}
	if ifMatch != nil && *ifMatch != existing.ContentHash {
		return nil, false, &storage.CollectionItemConflictError{ExpectedHash: *ifMatch, ActualHash: existing.ContentHash}
	}
	existing.Content = content
	existing.ContentHash = hash(content)
	existing.Version++
	existing.UpdatedAt = now
	existing.UpdatedByPeerID = updatedBy.String()
	coll.LastModifiedAt = now
	return existing, false, nil
}

func (f *Fake) DeleteCollectionItem(_ context.Context, collectionID int64, itemKey string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.items[collectionID][itemKey]; !ok {
		return false, nil
	}
	delete(f.items[collectionID], itemKey)
	if coll := f.collectionByID(collectionID); coll != nil {
		coll.RecordCount--
	}
	return true, nil
}

func (f *Fake) sortedKeys(collectionID int64) []string {
	keys := make([]string, 0, len(f.items[collectionID]))
	for k := range f.items[collectionID] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ListCollectionKeys pages by offset. A cursor is the last key of the
// previous page, which is the shape the SQL cursor takes without the sort
// component.
func (f *Fake) ListCollectionKeys(_ context.Context, collectionID int64, limit, offset int, cursor string) ([]string, int, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	keys := f.sortedKeys(collectionID)
	total := len(keys)
	if cursor != "" {
		i := sort.SearchStrings(keys, cursor)
		if i < len(keys) && keys[i] == cursor {
			i++
		}
		keys = keys[i:]
		total = -1
	} else if offset > 0 {
		if offset > len(keys) {
			offset = len(keys)
		}
		keys = keys[offset:]
	}
	next := ""
	if len(keys) > limit {
		keys = keys[:limit]
		next = keys[len(keys)-1]
	}
	return keys, total, next, nil
}

// QueryCollection models equality, the four comparison operators, $and and
// $or, and a single sort field, compared as text the way the SQL does. A
// cursor is the last key of the previous page and only works unsorted;
// $ilike and $contains are JSONB matters. Anything past that is ErrNotFaked.
func (f *Fake) QueryCollection(_ context.Context, collectionID int64, filter map[string]any, sortField string, sortAsc bool, limit, offset int, cursor string) (*storage.CollectionQueryResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cursor != "" && sortField != "" {
		return nil, ErrNotFaked
	}
	if limit <= 0 {
		limit = 100
	}
	var matched []*storage.CollectionItemRecord
	for _, k := range f.sortedKeys(collectionID) {
		if cursor != "" && k <= cursor {
			continue
		}
		item := f.items[collectionID][k]
		var doc map[string]any
		if err := json.Unmarshal(item.Content, &doc); err != nil {
			continue
		}
		ok, err := matches(doc, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, item)
		}
	}
	if sortField != "" {
		sort.SliceStable(matched, func(i, j int) bool {
			a, b := fieldString(matched[i].Content, sortField), fieldString(matched[j].Content, sortField)
			if sortAsc {
				return a < b
			}
			return a > b
		})
	}
	total := len(matched)
	if cursor != "" {
		total = -1
	} else if offset > 0 {
		if offset > len(matched) {
			offset = len(matched)
		}
		matched = matched[offset:]
	}
	result := &storage.CollectionQueryResult{TotalCount: total}
	if len(matched) > limit {
		matched = matched[:limit]
		result.HasMore = true
		result.NextCursor = matched[len(matched)-1].Key
	}
	result.Items = matched
	return result, nil
}

func fieldString(content []byte, field string) string {
	var doc map[string]any
	_ = json.Unmarshal(content, &doc)
	return fmt.Sprint(doc[field])
}

func matches(doc map[string]any, filter map[string]any) (bool, error) {
	for k, v := range filter {
		switch k {
		case "$and", "$or":
			clauses, ok := v.([]any)
			if !ok {
				return false, fmt.Errorf("%w: operator %s requires an array", storage.ErrInvalidFilter, k)
			}
			some, all := false, true
			for _, c := range clauses {
				cm, ok := c.(map[string]any)
				if !ok {
					return false, fmt.Errorf("%w: operator %s array items must be objects", storage.ErrInvalidFilter, k)
				}
				m, err := matches(doc, cm)
				if err != nil {
					return false, err
				}
				some, all = some || m, all && m
			}
			if (k == "$and" && !all) || (k == "$or" && !some) {
				return false, nil
			}
		default:
			if ops, ok := v.(map[string]any); ok {
				for op, want := range ops {
					switch op {
					case "$lt", "$lte", "$gt", "$gte":
						have, ok1 := toFloat(doc[k])
						target, ok2 := toFloat(want)
						if !ok1 || !ok2 {
							return false, nil
						}
						if !compare(op, have, target) {
							return false, nil
						}
					case "$ilike", "$contains":
						return false, ErrNotFaked
					default:
						return false, fmt.Errorf("%w: unknown operator %s", storage.ErrInvalidFilter, op)
					}
				}
				continue
			}
			if fmt.Sprint(doc[k]) != fmt.Sprint(v) {
				return false, nil
			}
		}
	}
	return true, nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func compare(op string, a, b float64) bool {
	switch op {
	case "$lt":
		return a < b
	case "$lte":
		return a <= b
	case "$gt":
		return a > b
	case "$gte":
		return a >= b
	}
	return false
}

// ---------------------------------------------------------------------------
// Directory
// ---------------------------------------------------------------------------

func (f *Fake) UpsertDirectoryEntry(_ context.Context, entry *storage.DirectoryEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *entry
	if existing, ok := f.directory[entry.OwnerPeerID]; ok {
		cp.ID, cp.ListedAt = existing.ID, existing.ListedAt
	} else {
		cp.ID, cp.ListedAt = f.id(), time.Now()
	}
	cp.UpdatedAt = time.Now()
	f.directory[entry.OwnerPeerID] = &cp
	return nil
}

func (f *Fake) RemoveDirectoryEntry(_ context.Context, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.directory, owner)
	return nil
}

func (f *Fake) GetDirectoryEntry(_ context.Context, owner string) (*storage.DirectoryEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.directory[owner]
	if !ok {
		return nil, storage.ErrDirectoryEntryNotFound
	}
	return entry, nil
}

func (f *Fake) BrowseDirectory(_ context.Context, query, cursor string, limit int) (*storage.DirectoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(query) > storage.MaxDirectoryQueryLength {
		return nil, storage.ErrDirectoryQueryTooLong
	}
	if cursor != "" {
		return nil, ErrNotFaked
	}
	if limit <= 0 {
		limit = 50
	}
	var out []*storage.DirectoryEntry
	q := strings.ToLower(query)
	for _, e := range f.directory {
		if q == "" || strings.Contains(strings.ToLower(e.DisplayName), q) || strings.Contains(strings.ToLower(e.Bio), q) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OwnerPeerID < out[j].OwnerPeerID })
	page := &storage.DirectoryPage{Entries: out}
	if len(out) > limit {
		page.Entries = out[:limit]
		page.HasMore = true
	}
	return page, nil
}

// ---------------------------------------------------------------------------
// Maintenance and statistics
// ---------------------------------------------------------------------------

func (f *Fake) DeleteExpiredMessages(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UnixMilli()
	n := 0
	for id := range f.messages {
		n += f.remove(id, func(m *core.Message) bool { return m.ExpiryTimestamp > 0 && m.ExpiryTimestamp < now })
	}
	return n, nil
}

// EnforceRetentionPolicy keeps the newest retention_count messages.
func (f *Fake) EnforceRetentionPolicy(_ context.Context, mailbox *storage.MailboxRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneMailbox(mailbox.ID)
	return nil
}

func (f *Fake) pruneMailbox(id int64) int {
	mb, ok := f.mailboxes[id]
	if !ok || mb.RetentionCount == nil || *mb.RetentionCount <= 0 {
		return 0
	}
	msgs := f.messages[id]
	if len(msgs) <= *mb.RetentionCount {
		return 0
	}
	drop := len(msgs) - *mb.RetentionCount
	f.messages[id] = msgs[drop:]
	mb.MessageCount = len(f.messages[id])
	return drop
}

func (f *Fake) EnforceAllRetention(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for id := range f.mailboxes {
		n += f.pruneMailbox(id)
	}
	return n, nil
}

func (f *Fake) ReconcileMessageCounts(context.Context) (int, error) { return 0, nil }

func (f *Fake) ServerStats(_ context.Context, nearCapacityRatio float64) (*storage.ServerStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stats := &storage.ServerStats{SampledAt: time.Now(), Mailboxes: len(f.mailboxes), NearCapacityRatio: nearCapacityRatio}
	for _, msgs := range f.messages {
		stats.Messages += int64(len(msgs))
		for _, m := range msgs {
			stats.MessageBytes += int64(len(m.Payload))
		}
	}
	return stats, nil
}

func (f *Fake) ListMailboxUsage(context.Context, storage.MailboxUsageQuery) ([]*storage.MailboxUsage, error) {
	return nil, ErrNotFaked
}

func (f *Fake) ListOwnerUsage(context.Context, int, int) ([]*storage.OwnerUsage, error) {
	return nil, ErrNotFaked
}

// ---------------------------------------------------------------------------
// Read authorization for documents, feeds and collections
// ---------------------------------------------------------------------------

// readable is the fake's copy of the SQL predicate: owner, or public, or
// shared with the reader on the list. Called with the lock held.
func (f *Fake) readable(kind storage.StoreKind, id int64, owner string, vis core.Visibility, reader peer.ID) bool {
	if reader.String() == owner || vis == core.VisibilityPublic {
		return true
	}
	if vis != core.VisibilityShared {
		return false
	}
	_, ok := f.readers[kind][id][reader.String()]
	return ok
}

// storeByPath finds a resource's row id and visibility field. Called with
// the lock held.
func (f *Fake) storeByPath(kind storage.StoreKind, owner peer.ID, path string) (id int64, vis *core.Visibility) {
	k := key(owner, path)
	switch kind {
	case storage.StoreDocument:
		if d, ok := f.documents[k]; ok {
			return d.ID, &d.Visibility
		}
	case storage.StoreFeed:
		if fd, ok := f.feeds[k]; ok {
			return fd.ID, &fd.Visibility
		}
	case storage.StoreCollection:
		if c, ok := f.collections[k]; ok {
			return c.ID, &c.Visibility
		}
	}
	return 0, nil
}

func (f *Fake) SetStoreVisibility(_ context.Context, kind storage.StoreKind, owner peer.ID, path string, visibility core.Visibility) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !visibility.Valid() {
		return false, fmt.Errorf("invalid visibility %d", visibility)
	}
	_, vis := f.storeByPath(kind, owner, path)
	if vis == nil {
		return false, nil
	}
	*vis = visibility
	return true, nil
}

func (f *Fake) GrantStoreReader(_ context.Context, kind storage.StoreKind, id int64, reader peer.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readers[kind] == nil {
		f.readers[kind] = map[int64]map[string]time.Time{}
	}
	if f.readers[kind][id] == nil {
		f.readers[kind][id] = map[string]time.Time{}
	}
	if _, ok := f.readers[kind][id][reader.String()]; !ok {
		f.readers[kind][id][reader.String()] = time.Now()
	}
	return nil
}

func (f *Fake) RevokeStoreReader(_ context.Context, kind storage.StoreKind, id int64, reader peer.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.readers[kind][id], reader.String())
	return nil
}

func (f *Fake) ListStoreReaders(_ context.Context, kind storage.StoreKind, id int64) ([]*storage.StoreReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []*storage.StoreReader{}
	for p, at := range f.readers[kind][id] {
		out = append(out, &storage.StoreReader{PeerID: p, GrantedAt: at})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].GrantedAt.Equal(out[j].GrantedAt) {
			return out[i].GrantedAt.Before(out[j].GrantedAt)
		}
		return out[i].PeerID < out[j].PeerID
	})
	return out, nil
}

func (f *Fake) IsStoreReader(_ context.Context, kind storage.StoreKind, id int64, reader peer.ID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.readers[kind][id][reader.String()]
	return ok, nil
}
