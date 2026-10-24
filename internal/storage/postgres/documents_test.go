package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// newTestStorage connects to the database named by RICOCHET_TEST_POSTGRES_DSN,
// the same variable the integration suite uses. Tests skip when it is unset so
// the package still passes without a database.
func newTestStorage(t *testing.T) *PostgresStorage {
	t.Helper()

	dsn := os.Getenv("RICOCHET_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RICOCHET_TEST_POSTGRES_DSN not set — skipping database test")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	port := 5432
	if u.Port() != "" {
		if port, err = strconv.Atoi(u.Port()); err != nil {
			t.Fatalf("parse port: %v", err)
		}
	}
	password, _ := u.User.Password()
	sslMode := "disable"
	if sm := u.Query().Get("sslmode"); sm != "" {
		sslMode = sm
	}

	cfg := &core.PostgresConfig{
		Host:           u.Hostname(),
		Port:           port,
		Database:       u.Path[1:],
		Username:       u.User.Username(),
		Password:       password,
		SSLMode:        sslMode,
		PoolSize:       10,
		ConnectTimeout: 10 * time.Second,
	}

	store, err := NewPostgresStorage(cfg, nil)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	if err := store.InitializeWithConfig(context.Background(), cfg); err != nil {
		t.Fatalf("initialize storage: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// newTestPeer returns a fresh peer ID so each test owns a disjoint set of rows
// and tests do not interfere when run against a shared database.
func newTestPeer(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := libp2pcrypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer ID: %v", err)
	}
	return id
}

// TestListDocumentsPaging walks the keyset cursor across a document set larger
// than one page and checks that every document is returned exactly once, in
// path order, with no gaps or repeats at the page boundaries.
func TestListDocumentsPaging(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	const total = 250
	const pageSize = 40

	for i := 0; i < total; i++ {
		path := fmt.Sprintf("vault/doc-%03d", i)
		content := []byte(fmt.Sprintf(`{"i":%d}`, i))
		if _, err := store.PutDocument(ctx, owner, path, content, "application/json", owner, nil, storage.DocumentWriteOptions{}); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
	}

	var (
		seen   []string
		cursor string
		pages  int
	)
	for {
		page, hasMore, err := store.ListDocuments(ctx, owner, owner, cursor, pageSize)
		if err != nil {
			t.Fatalf("list page %d: %v", pages, err)
		}
		pages++

		if hasMore && len(page) != pageSize {
			t.Fatalf("page %d: hasMore is true but page holds %d of %d", pages, len(page), pageSize)
		}
		for _, d := range page {
			seen = append(seen, d.Path)
		}
		if !hasMore {
			break
		}
		cursor = page[len(page)-1].Path

		if pages > total {
			t.Fatal("cursor is not advancing — paging would not terminate")
		}
	}

	if len(seen) != total {
		t.Fatalf("walked %d documents across %d pages, want %d", len(seen), pages, total)
	}
	for i, path := range seen {
		want := fmt.Sprintf("vault/doc-%03d", i)
		if path != want {
			t.Fatalf("position %d: got %q, want %q", i, path, want)
		}
	}
}

// TestListDocumentsReportsSizeWithoutBody checks that Size is the true body
// length even though the listing query never selects the body.
func TestListDocumentsReportsSizeWithoutBody(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	sizes := map[string]int{
		"a-empty":  0,
		"b-small":  17,
		"c-larger": 9000,
	}
	for path, size := range sizes {
		if _, err := store.PutDocument(ctx, owner, path, make([]byte, size), "application/octet-stream", owner, nil, storage.DocumentWriteOptions{}); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
	}

	page, _, err := store.ListDocuments(ctx, owner, owner, "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != len(sizes) {
		t.Fatalf("listed %d documents, want %d", len(page), len(sizes))
	}
	for _, d := range page {
		if want := sizes[d.Path]; d.Size != want {
			t.Errorf("%s: size %d, want %d", d.Path, d.Size, want)
		}
	}
}

// TestPutDocumentConditionalIsAtomic is the regression test for the lost-update
// bug: If-Match was compared in one statement and the row written in another,
// with no transaction between them, so concurrent writers presenting the same
// precondition all passed the check and all wrote. Exactly one must win.
func TestPutDocumentConditionalIsAtomic(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "vault/contended"

	base, err := store.PutDocument(ctx, owner, path, []byte("original"), "text/plain", owner, nil, storage.DocumentWriteOptions{})
	if err != nil {
		t.Fatalf("seed put: %v", err)
	}
	etag := base.ContentHash

	const writers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded []string
		conflicts int
	)
	start := make(chan struct{})

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			body := []byte(fmt.Sprintf("writer-%d", i))
			ifMatch := etag
			_, err := store.PutDocument(ctx, owner, path, body, "text/plain", owner, &ifMatch, storage.DocumentWriteOptions{})

			mu.Lock()
			defer mu.Unlock()
			switch e := err.(type) {
			case nil:
				succeeded = append(succeeded, string(body))
			case *storage.DocumentConflictError:
				conflicts++
			default:
				t.Errorf("writer %d: unexpected error %T: %v", i, e, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if len(succeeded) != 1 {
		t.Fatalf("%d of %d conditional writes succeeded against the same ETag, want exactly 1 (%v)",
			len(succeeded), writers, succeeded)
	}
	if conflicts != writers-1 {
		t.Fatalf("got %d conflicts, want %d", conflicts, writers-1)
	}

	// The surviving document must be the one whose write reported success.
	final, err := store.GetDocument(ctx, owner, path)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if string(final.Content) != succeeded[0] {
		t.Fatalf("stored body is %q but the winning writer wrote %q", final.Content, succeeded[0])
	}
	if final.VersionNumber != 2 {
		t.Fatalf("version is %d after one successful conditional write, want 2", final.VersionNumber)
	}
}

// TestPutDocumentArchivesPreviousVersion covers the history path, where the old
// body is copied into document_versions inside the database rather than being
// read into this process and written back.
func TestPutDocumentArchivesPreviousVersion(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "vault/versioned"

	if _, err := store.PutDocument(ctx, owner, path, []byte("v1-body"), "text/plain", owner, nil, storage.DocumentWriteOptions{}); err != nil {
		t.Fatalf("first put: %v", err)
	}
	// History is a column with no setter on the storage interface, so enable it
	// directly for this document.
	if _, err := store.pool.Exec(ctx,
		`UPDATE documents SET history_enabled = TRUE WHERE owner_peer_id = $1 AND path = $2`,
		owner.String(), path); err != nil {
		t.Fatalf("enable history: %v", err)
	}

	if _, err := store.PutDocument(ctx, owner, path, []byte("v2-body"), "text/plain", owner, nil, storage.DocumentWriteOptions{}); err != nil {
		t.Fatalf("second put: %v", err)
	}

	versions, err := store.GetDocumentHistory(ctx, owner, path, nil)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("got %d archived versions, want 1", len(versions))
	}
	if versions[0].ContentLength != len("v1-body") || versions[0].Content != nil {
		t.Fatalf("history entry = %d bytes, body %q; want the length only", versions[0].ContentLength, versions[0].Content)
	}
	if versions[0].VersionNumber != 1 {
		t.Fatalf("archived version number is %d, want 1", versions[0].VersionNumber)
	}
	archived, err := store.GetDocumentAtVersion(ctx, owner, path, 1)
	if err != nil || archived == nil {
		t.Fatalf("get version 1: %v", err)
	}
	if string(archived.Content) != "v1-body" || archived.ContentLength != len("v1-body") {
		t.Fatalf("archived body is %q (%d bytes), want %q", archived.Content, archived.ContentLength, "v1-body")
	}

	current, err := store.GetDocument(ctx, owner, path)
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if string(current.Content) != "v2-body" {
		t.Fatalf("current body is %q, want %q", current.Content, "v2-body")
	}
}

// TestDocumentHistoryNotFound pins the not-found contract the SDA handler
// maps to 404: a version that is not stored, and a document that does not
// exist, are ErrDocumentNotFound. A nil record with no error once reached the
// HISTORY handler, which dereferenced it and closed the stream with no reply.
func TestDocumentHistoryNotFound(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "vault/unversioned"

	if _, err := store.PutDocument(ctx, owner, path, []byte("body"), "text/plain", owner, nil, storage.DocumentWriteOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, err := store.GetDocumentAtVersion(ctx, owner, path, 7); !errors.Is(err, storage.ErrDocumentNotFound) {
		t.Errorf("unstored version = %+v, %v; want ErrDocumentNotFound", v, err)
	}
	if v, err := store.GetDocumentAtVersion(ctx, owner, "vault/missing", 1); !errors.Is(err, storage.ErrDocumentNotFound) {
		t.Errorf("version of a missing document = %+v, %v; want ErrDocumentNotFound", v, err)
	}
	if vs, err := store.GetDocumentHistory(ctx, owner, "vault/missing", nil); !errors.Is(err, storage.ErrDocumentNotFound) {
		t.Errorf("history of a missing document = %+v, %v; want ErrDocumentNotFound", vs, err)
	}
}

// TestDocumentHistorySetting is N19 in SQL: history is off until a write
// turns it on, the setting applies to that write, the ceiling bounds every
// prune (also for a document switched on by hand with no maximum), turning
// it off deletes the kept versions, and a restore is a new version.
func TestDocumentHistorySetting(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)
	const path = "vault/history"
	put := func(body string, opts storage.DocumentWriteOptions) {
		t.Helper()
		if _, err := store.PutDocument(ctx, owner, path, []byte(body), "text/plain", owner, nil, opts); err != nil {
			t.Fatalf("put %s: %v", body, err)
		}
	}
	kept := func() []int {
		t.Helper()
		vs, err := store.GetDocumentHistory(ctx, owner, path, nil)
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		out := []int{}
		for _, v := range vs {
			out = append(out, v.VersionNumber)
		}
		return out
	}
	ceiling := storage.DocumentWriteOptions{MaxHistory: 2}
	ten, zero := 10, 0

	put("v1", ceiling)
	put("v2", ceiling)
	if got := kept(); len(got) != 0 {
		t.Fatalf("history off by default: kept %v", got)
	}
	put("v3", storage.DocumentWriteOptions{History: &ten, MaxHistory: 2})
	if got := kept(); len(got) != 1 || got[0] != 2 {
		t.Errorf("turning history on keeps the version it replaces: kept %v, want [2]", got)
	}
	doc, err := store.HeadDocument(ctx, owner, path)
	if err != nil || !doc.HistoryEnabled || doc.MaxHistoryVersions == nil || *doc.MaxHistoryVersions != 2 {
		t.Errorf("stored setting = %+v (err %v), want enabled with 10 clamped to 2", doc, err)
	}
	put("v4", ceiling)
	put("v5", ceiling)
	if got := kept(); len(got) != 2 || got[0] != 4 || got[1] != 3 {
		t.Errorf("pruned to the ceiling: kept %v, want [4 3]", got)
	}

	restored, err := store.RestoreDocumentVersion(ctx, owner, path, 3, owner, nil, ceiling)
	if err != nil || restored.VersionNumber != 6 {
		t.Fatalf("restore v3 = %+v, %v; want version 6", restored, err)
	}
	if cur, _ := store.GetDocument(ctx, owner, path); string(cur.Content) != "v3" {
		t.Errorf("after restore the body is %q, want v3", cur.Content)
	}
	if got := kept(); len(got) != 2 || got[0] != 5 {
		t.Errorf("after restore: kept %v, want v5 first", got)
	}
	if _, err := store.RestoreDocumentVersion(ctx, owner, path, 1, owner, nil, ceiling); !errors.Is(err, storage.ErrDocumentNotFound) {
		t.Errorf("restore of a pruned version: %v, want ErrDocumentNotFound", err)
	}
	stale := "sha256:stale"
	var conflict *storage.DocumentConflictError
	if _, err := store.RestoreDocumentVersion(ctx, owner, path, 5, owner, &stale, ceiling); !errors.As(err, &conflict) {
		t.Errorf("restore with a stale If-Match: %v, want a conflict", err)
	}

	put("v7", storage.DocumentWriteOptions{History: &zero, MaxHistory: 2})
	if got := kept(); len(got) != 0 {
		t.Errorf("turning history off: kept %v, want none", got)
	}

	// Switched on by hand with no maximum, as before history had a setter:
	// the ceiling still bounds it.
	if _, err := store.pool.Exec(ctx,
		`UPDATE documents SET history_enabled = TRUE, max_history_versions = NULL WHERE owner_peer_id = $1 AND path = $2`,
		owner.String(), path); err != nil {
		t.Fatalf("enable by hand: %v", err)
	}
	for _, b := range []string{"v8", "v9", "v10", "v11"} {
		put(b, ceiling)
	}
	if got := kept(); len(got) != 2 {
		t.Errorf("no maximum of its own: kept %v, want 2 (the ceiling)", got)
	}
}

// TestStoreMessageSequencesAreUnique is the regression test for the sequence
// race: the next sequence number was read with SELECT MAX(...) + 1 in a separate
// statement from the insert, so concurrent deliveries to one mailbox could all
// read the same value. Nothing enforces uniqueness on (mailbox_id,
// sequence_number), so the duplicates were accepted silently.
func TestStoreMessageSequencesAreUnique(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	addr := &core.MailboxAddress{
		OwnerID:    owner,
		FolderPath: "inbox",
		Type:       core.MailboxPrivate,
	}
	mailbox, err := store.GetOrCreateMailbox(ctx, addr, 1000, 30, nil)
	if err != nil {
		t.Fatalf("create mailbox: %v", err)
	}

	const senders = 16
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seqs []int
	)
	start := make(chan struct{})

	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			msg := &core.Message{
				// message_id is globally unique, so scope it to this run's mailbox.
				MessageID:        fmt.Sprintf("msg-%d-%d", mailbox.ID, i),
				RecipientPeerID:  owner.String(),
				SenderPeerID:     owner.String(),
				Payload:          []byte(fmt.Sprintf("payload-%d", i)),
				Priority:         core.PriorityNormal,
				CreatedTimestamp: time.Now().UnixMilli(),
				ExpiryTimestamp:  time.Now().Add(time.Hour).UnixMilli(),
			}
			seq, err := store.StoreMessage(ctx, mailbox, msg)
			if err != nil {
				t.Errorf("sender %d: %v", i, err)
				return
			}
			mu.Lock()
			seqs = append(seqs, seq)
			mu.Unlock()
		}(i)
	}

	close(start)
	wg.Wait()

	if len(seqs) != senders {
		t.Fatalf("stored %d messages, want %d", len(seqs), senders)
	}

	seen := make(map[int]int, len(seqs))
	for _, seq := range seqs {
		seen[seq]++
	}
	if len(seen) != senders {
		t.Fatalf("%d concurrent deliveries produced %d distinct sequence numbers: %v",
			senders, len(seen), seen)
	}

	// The assigned numbers must be exactly 1..senders with no gaps, so reader
	// cursors can rely on them.
	for want := 1; want <= senders; want++ {
		if seen[want] != 1 {
			t.Fatalf("sequence %d was assigned %d times, want exactly 1", want, seen[want])
		}
	}
}

// TestDeleteExpiredMessagesBatches checks the batched sweep removes everything
// expired across more than one batch while leaving live messages alone.
func TestDeleteExpiredMessagesBatches(t *testing.T) {
	store := newTestStorage(t)
	ctx := context.Background()
	owner := newTestPeer(t)

	addr := &core.MailboxAddress{
		OwnerID:    owner,
		FolderPath: "sweep",
		Type:       core.MailboxPrivate,
	}
	mailbox, err := store.GetOrCreateMailbox(ctx, addr, 10000, 30, nil)
	if err != nil {
		t.Fatalf("create mailbox: %v", err)
	}

	const expired = 40
	const live = 5
	now := time.Now()

	for i := 0; i < expired+live; i++ {
		expiry := now.Add(time.Hour)
		if i < expired {
			expiry = now.Add(-time.Hour)
		}
		msg := &core.Message{
			MessageID:        fmt.Sprintf("sweep-%d-%d", mailbox.ID, i),
			RecipientPeerID:  owner.String(),
			SenderPeerID:     owner.String(),
			Payload:          []byte("x"),
			Priority:         core.PriorityNormal,
			CreatedTimestamp: now.UnixMilli(),
			ExpiryTimestamp:  expiry.UnixMilli(),
		}
		if _, err := store.StoreMessage(ctx, mailbox, msg); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	deleted, err := store.DeleteExpiredMessages(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if deleted < expired {
		t.Fatalf("swept %d messages, want at least %d", deleted, expired)
	}

	remaining, err := store.GetMessageCount(ctx, mailbox.ID)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != live {
		t.Fatalf("%d messages remain in the mailbox, want %d", remaining, live)
	}
}
