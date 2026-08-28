package mta_test

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/storage"
)

// ---------------------------------------------------------------------------
// mockStorage implements storage.Storage with only the methods needed by
// MailboxServer.DeliverLocal (via PrivateMailbox.StoreMessage).
// All other methods panic with "not implemented".
// ---------------------------------------------------------------------------

type mockStorage struct {
	mailboxes map[string]*storage.MailboxRecord
	messages  []*core.Message
	nextSeq   int
	nextID    int64
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		mailboxes: make(map[string]*storage.MailboxRecord),
		nextSeq:   1,
		nextID:    1,
	}
}

// --- Methods actually used by the DeliverLocal / PrivateMailbox path ---

func (m *mockStorage) GetOrCreateMailbox(_ context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (*storage.MailboxRecord, error) {
	key := addr.FullPath()
	if rec, ok := m.mailboxes[key]; ok {
		return rec, nil
	}
	rec := &storage.MailboxRecord{
		ID:             m.nextID,
		OwnerPeerID:    addr.OwnerID.String(),
		FolderPath:     addr.FolderPath,
		Type:           addr.Type,
		CreatedAt:      time.Now(),
		LastAccessAt:   time.Now(),
		MaxMessages:    maxMessages,
		RetentionDays:  retentionDays,
		RetentionCount: retentionCount,
	}
	m.nextID++
	m.mailboxes[key] = rec
	return rec, nil
}

func (m *mockStorage) StoreMessage(_ context.Context, _ *storage.MailboxRecord, msg *core.Message) (int, error) {
	seq := m.nextSeq
	m.nextSeq++
	msg.SequenceNumber = uint64(seq)
	m.messages = append(m.messages, msg)
	return seq, nil
}

func (m *mockStorage) GetMessageCount(_ context.Context, _ int64) (int, error) {
	return len(m.messages), nil
}

func (m *mockStorage) RetrieveMessages(_ context.Context, _ *storage.MailboxRecord, _ *int, _ *int, _ *core.MessagePriority) ([]*core.Message, error) {
	return m.messages, nil
}

// --- Lifecycle ---

func (m *mockStorage) Initialize(_ context.Context) error { return nil }
func (m *mockStorage) Close() error                       { return nil }

// --- Mailbox operations (not used by tests) ---

func (m *mockStorage) FindMailbox(_ context.Context, _ peer.ID, _ string) (*storage.MailboxRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) ListMailboxes(_ context.Context, _ peer.ID) ([]*storage.MailboxRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) UpdateMailbox(_ context.Context, _ *storage.MailboxRecord) error {
	panic("not implemented")
}

func (m *mockStorage) DeleteMailbox(_ context.Context, _ int64) error {
	panic("not implemented")
}

func (m *mockStorage) UpdateMailboxAccess(_ context.Context, _ int64) error {
	panic("not implemented")
}

// --- Message operations (not used by tests) ---

func (m *mockStorage) DeleteMessage(_ context.Context, _ string) error {
	panic("not implemented")
}

func (m *mockStorage) DeleteMessages(_ context.Context, _ []string) error {
	panic("not implemented")
}

// --- Flag operations ---

func (m *mockStorage) UpdateMessageFlags(_ context.Context, _ string, _, _ uint32) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) GetMessageFlags(_ context.Context, _ string) (*uint32, error) {
	panic("not implemented")
}

func (m *mockStorage) ExpungeMailbox(_ context.Context, _ int64) (int, error) {
	panic("not implemented")
}

func (m *mockStorage) ExpungeAllMailboxes(_ context.Context, _ peer.ID) (int, error) {
	panic("not implemented")
}

func (m *mockStorage) MarkMessagesDelivered(_ context.Context, _ []string) (int, error) {
	panic("not implemented")
}

// --- ACL operations ---

func (m *mockStorage) GrantAccess(_ context.Context, _ int64, _ peer.ID, _ core.AccessMode) error {
	panic("not implemented")
}

func (m *mockStorage) RevokeAccess(_ context.Context, _ int64, _ peer.ID) error {
	panic("not implemented")
}

func (m *mockStorage) CheckAccess(_ context.Context, _ int64, _ peer.ID, _ core.AccessMode) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) ListACL(_ context.Context, _ int64) ([]*storage.AclRecord, error) {
	panic("not implemented")
}

// --- Reader cursor operations ---

func (m *mockStorage) GetCursor(_ context.Context, _ int64, _ peer.ID) (int, error) {
	panic("not implemented")
}

func (m *mockStorage) UpdateCursor(_ context.Context, _ int64, _ peer.ID, _ int) error {
	panic("not implemented")
}

// --- Document operations ---

func (m *mockStorage) GetDocument(_ context.Context, _ peer.ID, _ string) (*storage.DocumentRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) PutDocument(_ context.Context, _ peer.ID, _ string, _ []byte, _ string, _ peer.ID, _ *string) (*storage.DocumentPutResult, error) {
	panic("not implemented")
}

func (m *mockStorage) PatchDocument(_ context.Context, _ peer.ID, _ string, _ map[string]any, _ peer.ID, _ *string) (*storage.DocumentPutResult, error) {
	panic("not implemented")
}

func (m *mockStorage) DeleteDocument(_ context.Context, _ peer.ID, _ string) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) ListDocuments(_ context.Context, _ peer.ID, _ string, _ int) ([]*storage.DocumentSummary, bool, error) {
	panic("not implemented")
}

func (m *mockStorage) GetDocumentHistory(_ context.Context, _ peer.ID, _ string, _ *int) ([]*storage.DocumentVersionRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetDocumentAtVersion(_ context.Context, _ peer.ID, _ string, _ int) (*storage.DocumentVersionRecord, error) {
	panic("not implemented")
}

// --- Directory operations ---

func (m *mockStorage) UpsertDirectoryEntry(_ context.Context, _ *storage.DirectoryEntry) error {
	panic("not implemented")
}

func (m *mockStorage) RemoveDirectoryEntry(_ context.Context, _ string) error {
	panic("not implemented")
}

func (m *mockStorage) GetDirectoryEntry(_ context.Context, _ string) (*storage.DirectoryEntry, error) {
	panic("not implemented")
}

func (m *mockStorage) BrowseDirectory(_ context.Context, _ string, _ string, _ int) (*storage.DirectoryPage, error) {
	panic("not implemented")
}

// --- Feed operations ---

func (m *mockStorage) CreateFeed(_ context.Context, _ peer.ID, _, _, _ string, _ bool) (*storage.FeedRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetFeed(_ context.Context, _ peer.ID, _ string) (*storage.FeedRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) DeleteFeed(_ context.Context, _ peer.ID, _ string) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) ListFeeds(_ context.Context, _ peer.ID) ([]*storage.FeedRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) AppendFeedEntry(_ context.Context, _ int64, _ []byte, _ peer.ID, _ string) (*storage.FeedEntryRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetFeedEntry(_ context.Context, _ int64, _ int) (*storage.FeedEntryRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetFeedEntries(_ context.Context, _ int64, _, _ *int, _ string, _ int) ([]*storage.FeedEntryRecord, bool, error) {
	panic("not implemented")
}

func (m *mockStorage) EnforceFeedRetention(_ context.Context, _ *storage.FeedRecord) (int, error) {
	panic("not implemented")
}

func (m *mockStorage) GetMultiFeedEntries(_ context.Context, _ []storage.MultiFeedQuery) (map[string]*storage.MultiFeedResult, error) {
	panic("not implemented")
}

// --- Cleanup operations ---

func (m *mockStorage) DeleteExpiredMessages(_ context.Context) (int, error) {
	panic("not implemented")
}

func (m *mockStorage) EnforceRetentionPolicy(_ context.Context, _ *storage.MailboxRecord) error {
	panic("not implemented")
}

func (m *mockStorage) ServerStats(_ context.Context, _ float64) (*storage.ServerStats, error) {
	panic("not implemented")
}

func (m *mockStorage) ListMailboxUsage(_ context.Context, _ storage.MailboxUsageQuery) ([]*storage.MailboxUsage, error) {
	panic("not implemented")
}

func (m *mockStorage) ListOwnerUsage(_ context.Context, _, _ int) ([]*storage.OwnerUsage, error) {
	panic("not implemented")
}

// --- Collection operations ---

func (m *mockStorage) CreateCollection(_ context.Context, _ peer.ID, _, _ string) (*storage.CollectionRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetCollection(_ context.Context, _ peer.ID, _ string) (*storage.CollectionRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) DeleteCollection(_ context.Context, _ peer.ID, _ string) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) ListCollections(_ context.Context, _ peer.ID) ([]*storage.CollectionRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) GetCollectionItem(_ context.Context, _ int64, _ string) (*storage.CollectionItemRecord, error) {
	panic("not implemented")
}

func (m *mockStorage) PutCollectionItem(_ context.Context, _ int64, _ string, _ []byte, _ peer.ID, _ *string) (*storage.CollectionItemRecord, bool, error) {
	panic("not implemented")
}

func (m *mockStorage) DeleteCollectionItem(_ context.Context, _ int64, _ string) (bool, error) {
	panic("not implemented")
}

func (m *mockStorage) ListCollectionKeys(_ context.Context, _ int64, _, _ int) ([]string, int, error) {
	panic("not implemented")
}

func (m *mockStorage) QueryCollection(_ context.Context, _ int64, _ map[string]any, _ string, _ bool, _, _ int) (*storage.CollectionQueryResult, error) {
	panic("not implemented")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// generatePeerID creates a random peer.ID for testing.
func generatePeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer ID from key: %v", err)
	}
	return id
}

// newTestRouter creates a Router backed by a mockStorage.
func newTestRouter(t *testing.T, maxRequests int) (*mta.Router, *mockStorage) {
	t.Helper()
	store := newMockStorage()
	logger := slog.Default()
	mailboxServer := mda.NewMailboxServer(store, logger)
	limiter := middleware.NewTokenBucket(1*time.Minute, maxRequests, maxRequests)
	t.Cleanup(limiter.Close)
	router := mta.NewRouter(mailboxServer, limiter, logger)
	return router, store
}

// validMessage creates a valid message from sender to recipient with a 7-day expiry.
func validMessage(senderID, recipientID peer.ID) *core.Message {
	return core.NewMessageWithDefaultExpiry(senderID, recipientID, []byte("hello"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestAcceptMessage_Success(t *testing.T) {
	router, store := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	ctx := context.Background()

	id, err := router.AcceptMessage(ctx, msg, sender)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if id != msg.MessageID {
		t.Fatalf("expected message ID %q, got %q", msg.MessageID, id)
	}
	if len(store.messages) != 1 {
		t.Fatalf("expected 1 stored message, got %d", len(store.messages))
	}
	if store.messages[0].MessageID != msg.MessageID {
		t.Fatalf("stored message ID mismatch: %q vs %q", store.messages[0].MessageID, msg.MessageID)
	}
}

func TestAcceptMessage_SenderMismatch(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)
	imposter := generatePeerID(t)

	msg := validMessage(sender, recipient)
	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, imposter)
	if err == nil {
		t.Fatal("expected error for sender mismatch, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_ExpiredMessage(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	// Set expiry to 1 second in the past
	msg.ExpiryTimestamp = time.Now().Add(-1 * time.Second).UnixMilli()

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for expired message, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_HopCountExceeded(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	msg.HopCount = core.MaxHopCount + 1

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for hop count exceeded, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_PayloadTooLarge(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	msg.Payload = make([]byte, core.MaxPayloadSize+1)

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for payload too large, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_RateLimit(t *testing.T) {
	maxRequests := 3
	router, _ := newTestRouter(t, maxRequests)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	ctx := context.Background()

	// Send maxRequests messages successfully
	for i := 0; i < maxRequests; i++ {
		msg := validMessage(sender, recipient)
		_, err := router.AcceptMessage(ctx, msg, sender)
		if err != nil {
			t.Fatalf("message %d: expected no error, got: %v", i+1, err)
		}
	}

	// The next message should be rate limited
	msg := validMessage(sender, recipient)
	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected rate limit error on 4th message, got nil")
	}

	var rlErr *mta.RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected RateLimitError, got %T: %v", err, err)
	}
}
