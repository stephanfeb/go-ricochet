package postgres

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
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
		if _, err := store.PutDocument(ctx, owner, path, content, "application/json", owner, nil); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
	}

	var (
		seen   []string
		cursor string
		pages  int
	)
	for {
		page, hasMore, err := store.ListDocuments(ctx, owner, cursor, pageSize)
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
		if _, err := store.PutDocument(ctx, owner, path, make([]byte, size), "application/octet-stream", owner, nil); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
	}

	page, _, err := store.ListDocuments(ctx, owner, "", 0)
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

	base, err := store.PutDocument(ctx, owner, path, []byte("original"), "text/plain", owner, nil)
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
			_, err := store.PutDocument(ctx, owner, path, body, "text/plain", owner, &ifMatch)

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

	if _, err := store.PutDocument(ctx, owner, path, []byte("v1-body"), "text/plain", owner, nil); err != nil {
		t.Fatalf("first put: %v", err)
	}
	// History is a column with no setter on the storage interface, so enable it
	// directly for this document.
	if _, err := store.pool.Exec(ctx,
		`UPDATE documents SET history_enabled = TRUE WHERE owner_peer_id = $1 AND path = $2`,
		owner.String(), path); err != nil {
		t.Fatalf("enable history: %v", err)
	}

	if _, err := store.PutDocument(ctx, owner, path, []byte("v2-body"), "text/plain", owner, nil); err != nil {
		t.Fatalf("second put: %v", err)
	}

	versions, err := store.GetDocumentHistory(ctx, owner, path, nil)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("got %d archived versions, want 1", len(versions))
	}
	if string(versions[0].Content) != "v1-body" {
		t.Fatalf("archived body is %q, want %q", versions[0].Content, "v1-body")
	}
	if versions[0].VersionNumber != 1 {
		t.Fatalf("archived version number is %d, want 1", versions[0].VersionNumber)
	}

	current, err := store.GetDocument(ctx, owner, path)
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if string(current.Content) != "v2-body" {
		t.Fatalf("current body is %q, want %q", current.Content, "v2-body")
	}
}
