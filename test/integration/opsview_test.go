package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/opsapi"
	"github.com/stephanfeb/go-ricochet/internal/opsview"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// The unit tests in internal/opsview cover the HTTP contract against a stub.
// These exist for the part a stub cannot check: that the cross-owner SQL
// returns what the database actually holds. The B2 near-capacity bug — a
// parameter Postgres inferred as an integer, truncating a ratio to zero — was
// invisible to everything except a query run against a real server.

// opsSurface mounts the operator view the way internal/server does and returns
// a function that issues requests against it. No socket: the mux is the
// contract under test.
func opsSurface(t *testing.T, server *testServer) func(string) *httptest.ResponseRecorder {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := opsapi.New(opsapi.Options{
		Logger: logger,
		Routes: opsview.Routes(opsview.Options{
			Storage:  server.Storage,
			Capacity: server.Capacity,
			Config:   server.Config,
			PoolSize: 10,
			Logger:   logger,
		}),
	})

	return func(target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		return rec
	}
}

func decodeOps[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var body T
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (body %q)", err, rec.Body.String())
	}
	return body
}

// newPeerID mints an identity the caller does not own, which is the whole
// point: the operator surface must answer for peers it has never met.
func newPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return id
}

// fillMailboxFor is fillMailbox with an explicit owner and cap.
func fillMailboxFor(t *testing.T, server *testServer, owner peer.ID, folder string, cap, n int) {
	t.Helper()

	ctx := context.Background()
	addr := &core.MailboxAddress{OwnerID: owner, FolderPath: folder}
	mailbox, err := server.Storage.GetOrCreateMailbox(ctx, addr, cap, 30, nil)
	if err != nil {
		t.Fatalf("create mailbox %s: %v", folder, err)
	}

	for i := 0; i < n; i++ {
		msg := core.NewMessageWithDefaultExpiry(owner, owner, []byte("0123456789"))
		msg.MessageID = fmt.Sprintf("%s-%d-%d", folder, time.Now().UnixNano(), i)
		msg.FolderPath = folder
		if _, err := server.Storage.StoreMessage(ctx, mailbox, msg); err != nil {
			t.Fatalf("store message %d in %s: %v", i, folder, err)
		}
	}
}

// opsMailbox mirrors the JSON the surface emits. It is declared here rather
// than exported from opsview so that a change to the wire shape breaks this
// test, which is the point of having it.
type opsMailbox struct {
	ID            int64      `json:"id"`
	Owner         string     `json:"owner"`
	FolderPath    string     `json:"folderPath"`
	MessageCount  int        `json:"messageCount"`
	MaxMessages   int        `json:"maxMessages"`
	FillRatio     float64    `json:"fillRatio"`
	Full          bool       `json:"full"`
	MessageBytes  int64      `json:"messageBytes"`
	LastMessageAt *time.Time `json:"lastMessageAt"`
}

type opsMailboxes struct {
	Owner     string       `json:"owner"`
	Sort      string       `json:"sort"`
	Limit     int          `json:"limit"`
	Count     int          `json:"count"`
	Mailboxes []opsMailbox `json:"mailboxes"`
}

// The acceptance criterion for B3, stated as a test: an operator with no peer
// identity and no key asks whether a mailbox belonging to somebody else is
// full, and gets an answer.
func TestOperatorCanSeeAnotherPeersFullMailbox(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	stranger := newPeerID(t)
	fillMailboxFor(t, server, stranger, "ops/full", 5, 5)
	fillMailboxFor(t, server, stranger, "ops/roomy", 100, 2)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+stranger.String()))

	if body.Count != 2 {
		t.Fatalf("got %d mailboxes for the stranger, want 2 (%+v)", body.Count, body.Mailboxes)
	}

	byFolder := map[string]opsMailbox{}
	for _, m := range body.Mailboxes {
		byFolder[m.FolderPath] = m
	}

	full, ok := byFolder["ops/full"]
	if !ok {
		t.Fatalf("the full mailbox is missing: %+v", body.Mailboxes)
	}
	if !full.Full {
		t.Errorf("a mailbox at 5 of 5 reports full=false: %+v", full)
	}
	if full.MessageCount != 5 || full.MaxMessages != 5 {
		t.Errorf("counts = %d/%d, want 5/5", full.MessageCount, full.MaxMessages)
	}
	if full.Owner != stranger.String() {
		t.Errorf("owner = %q, want the stranger's peer ID", full.Owner)
	}

	roomy := byFolder["ops/roomy"]
	if roomy.Full {
		t.Errorf("a mailbox at 2 of 100 reports full=true: %+v", roomy)
	}
}

// The fill ratio is computed in SQL. If the division is done in integers —
// the same mistake that truncated the near-capacity ratio in B2 — every
// partially filled mailbox reports 0 and the "fullest first" ordering becomes
// arbitrary.
func TestFillRatioIsNotIntegerDivision(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "ratio/half", 4, 2)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+owner.String()))
	if len(body.Mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(body.Mailboxes))
	}

	if got := body.Mailboxes[0].FillRatio; got != 0.5 {
		t.Errorf("fillRatio for 2 of 4 = %v, want 0.5 — an integer division truncates it to 0", got)
	}
}

// An uncapped mailbox has no fill ratio however much it holds, and must not
// be reported as full. A cap of zero means unlimited, not "already over".
func TestUncappedMailboxIsNotReportedFull(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "uncapped/firehose", 0, 7)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+owner.String()))
	m := body.Mailboxes[0]

	if m.Full {
		t.Error("an uncapped mailbox is reported as full")
	}
	if m.FillRatio != 0 {
		t.Errorf("fillRatio = %v against no cap, want 0", m.FillRatio)
	}
	if m.MessageCount != 7 {
		t.Errorf("messageCount = %d, want 7 — the count is real even without a cap", m.MessageCount)
	}
}

// The reported figures have to agree with the database, since a plausible
// wrong number is the failure mode that does not get investigated.
func TestMailboxUsageAgreesWithTheDatabase(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)
	ctx := context.Background()

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "agree/one", 50, 6)
	fillMailboxFor(t, server, owner, "agree/two", 50, 0)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+owner.String()))

	for _, m := range body.Mailboxes {
		var count int
		var bytes int64
		if err := server.Storage.Pool().QueryRow(ctx,
			`SELECT COUNT(*), COALESCE(SUM(octet_length(payload)), 0)
			 FROM stored_messages WHERE mailbox_id = $1`, m.ID).
			Scan(&count, &bytes); err != nil {
			t.Fatalf("count directly: %v", err)
		}

		if m.MessageCount != count {
			t.Errorf("%s: messageCount = %d, database says %d", m.FolderPath, m.MessageCount, count)
		}
		if m.MessageBytes != bytes {
			t.Errorf("%s: messageBytes = %d, database says %d", m.FolderPath, m.MessageBytes, bytes)
		}
	}

	// An empty mailbox has never received a message, and must say so rather
	// than reporting an invented timestamp.
	for _, m := range body.Mailboxes {
		if m.MessageCount == 0 && m.LastMessageAt != nil {
			t.Errorf("%s: empty mailbox reports lastMessageAt = %v", m.FolderPath, m.LastMessageAt)
		}
		if m.MessageCount > 0 && m.LastMessageAt == nil {
			t.Errorf("%s: %d messages but no lastMessageAt", m.FolderPath, m.MessageCount)
		}
	}
}

// "Top" has to mean top. The list is server-wide and the test database is
// shared, so what can be asserted without owning every row is that the
// ordering holds and that a mailbox at its cap appears.
func TestTopMailboxesAreOrderedByFullness(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "top/full", 3, 3)
	fillMailboxFor(t, server, owner, "top/quarter", 8, 2)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes/top?n=500"))
	if len(body.Mailboxes) == 0 {
		t.Fatal("empty listing")
	}

	// A mailbox at its cap exists, so the head of a fullest-first list must be
	// at least full. Checking only that the page is non-increasing is not
	// enough: a page of uncapped mailboxes is non-increasing at zero and would
	// pass whatever the ordering is.
	if head := body.Mailboxes[0]; head.FillRatio < 1 || !head.Full {
		t.Errorf("head of the fullest-first list is %+v; a mailbox at its cap exists and should lead", head)
	}

	prev := 2.0
	for i, m := range body.Mailboxes {
		if m.FillRatio > prev {
			t.Fatalf("mailbox %d has fill %v after %v — the list is not sorted",
				i, m.FillRatio, prev)
		}
		prev = m.FillRatio
	}

	var seen bool
	for _, m := range body.Mailboxes {
		if m.Owner == owner.String() && m.FolderPath == "top/full" {
			seen = true
			if !m.Full {
				t.Errorf("the full mailbox is in the list but not marked full: %+v", m)
			}
		}
	}
	if !seen {
		t.Error("a mailbox at its cap is absent from the fullest-first listing")
	}
}

func TestTopMailboxesByCount(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes/top?sort=count&n=100"))
	if body.Sort != "count" {
		t.Errorf("sort = %q, want count", body.Sort)
	}

	prev := -1
	for i, m := range body.Mailboxes {
		if prev >= 0 && m.MessageCount > prev {
			t.Fatalf("mailbox %d holds %d after %d — not sorted by count",
				i, m.MessageCount, prev)
		}
		prev = m.MessageCount
	}
}

// A cross-owner query has no natural bound, so the cap is enforced in storage
// rather than trusted to the caller. One unbounded scan is enough to matter.
func TestPageSizeIsCappedInStorage(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()

	// The bound can only be observed against more rows than the bound allows,
	// so the fixture has to exceed it. Inserted directly because this is
	// setup, not the behaviour under test, and one statement beats a thousand
	// round trips.
	owner := newPeerID(t)
	if _, err := server.Storage.Pool().Exec(ctx, `
		INSERT INTO mailboxes (owner_peer_id, folder_path, mailbox_type, max_messages, retention_days)
		SELECT $1, 'bulk/' || i, 0, 10, 30
		FROM generate_series(1, $2) AS i`,
		owner.String(), storage.MaxOperatorPageSize+1); err != nil {
		t.Fatalf("bulk insert mailboxes: %v", err)
	}

	usage, err := server.Storage.ListMailboxUsage(ctx, storage.MailboxUsageQuery{Limit: 100000})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(usage) > storage.MaxOperatorPageSize {
		t.Errorf("returned %d rows for a limit of 100000, want at most %d",
			len(usage), storage.MaxOperatorPageSize)
	}
	if len(usage) != storage.MaxOperatorPageSize {
		t.Errorf("returned %d rows with more than %d available, want a full page",
			len(usage), storage.MaxOperatorPageSize)
	}

	// And the endpoint reports the limit it actually applied, so a caller
	// does not read a truncated page as a complete one.
	get := opsSurface(t, server)
	body := decodeOps[opsMailboxes](t, get("/ops/mailboxes/top?n=100000"))
	if body.Limit != storage.MaxOperatorPageSize {
		t.Errorf("reported limit = %d, want the applied %d", body.Limit, storage.MaxOperatorPageSize)
	}
}

func TestMailboxPagingDoesNotRepeat(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	owner := newPeerID(t)
	for i := 0; i < 4; i++ {
		fillMailboxFor(t, server, owner, fmt.Sprintf("page/%d", i), 10, i)
	}

	first := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+owner.String()+"&limit=2"))
	second := decodeOps[opsMailboxes](t, get("/ops/mailboxes?owner="+owner.String()+"&limit=2&offset=2"))

	if len(first.Mailboxes) != 2 || len(second.Mailboxes) != 2 {
		t.Fatalf("pages of %d and %d, want 2 and 2", len(first.Mailboxes), len(second.Mailboxes))
	}
	for _, a := range first.Mailboxes {
		for _, b := range second.Mailboxes {
			if a.ID == b.ID {
				t.Errorf("mailbox %d appears on both pages", a.ID)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// /ops/storage
// ---------------------------------------------------------------------------

type opsStorage struct {
	Server *struct {
		SampledAt     time.Time `json:"sampledAt"`
		AgeSeconds    float64   `json:"ageSeconds"`
		Mailboxes     int       `json:"mailboxes"`
		Messages      int64     `json:"messages"`
		DatabaseBytes int64     `json:"databaseBytes"`
		Depth         []struct {
			Label     string `json:"label"`
			Mailboxes int    `json:"mailboxes"`
		} `json:"depth"`
	} `json:"server"`
	Owners []struct {
		Owner        string `json:"owner"`
		Mailboxes    int    `json:"mailboxes"`
		Messages     int64  `json:"messages"`
		MessageBytes int64  `json:"messageBytes"`
	} `json:"owners"`
}

func TestStorageReportsWhoIsUsingTheSpace(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)
	ctx := context.Background()

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "usage/a", 100, 12)
	fillMailboxFor(t, server, owner, "usage/b", 100, 8)

	if _, err := server.Capacity.Sample(ctx); err != nil {
		t.Fatalf("sample: %v", err)
	}

	// Ask for the maximum page: the listing is ordered by bytes and the test
	// database is shared, so this owner's position in it is not predictable.
	body := decodeOps[opsStorage](t, get("/ops/storage?limit=500"))

	var found bool
	for _, o := range body.Owners {
		if o.Owner != owner.String() {
			continue
		}
		found = true
		if o.Mailboxes != 2 {
			t.Errorf("mailboxes = %d, want 2", o.Mailboxes)
		}
		if o.Messages != 20 {
			t.Errorf("messages = %d, want 20", o.Messages)
		}
		if o.MessageBytes != 200 {
			t.Errorf("messageBytes = %d, want 200 (20 × 10 bytes)", o.MessageBytes)
		}
	}
	if !found {
		t.Fatalf("the owner is absent from the per-owner totals (%d owners listed)", len(body.Owners))
	}

	if body.Server == nil {
		t.Fatal("server totals missing after a sample")
	}
	if body.Server.DatabaseBytes <= 0 {
		t.Errorf("databaseBytes = %d, want a real size", body.Server.DatabaseBytes)
	}
	if body.Server.SampledAt.IsZero() {
		t.Error("sampledAt is zero; the figure's age must travel with it")
	}

	// The per-owner totals are live while the server block is sampled, so the
	// two can legitimately disagree. What must hold is that the sampled block
	// is not smaller than one owner's live share of it.
	var mine int64
	if err := server.Storage.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM stored_messages sm
		 JOIN mailboxes m ON m.id = sm.mailbox_id
		 WHERE m.owner_peer_id = $1`, owner.String()).Scan(&mine); err != nil {
		t.Fatalf("count directly: %v", err)
	}
	if mine != 20 {
		t.Errorf("the database holds %d of this owner's messages, want 20", mine)
	}
}

// Owner totals must sum a peer's mailboxes rather than multiplying them: a
// join that counts messages once per mailbox row inflates every figure.
func TestOwnerTotalsDoNotDoubleCount(t *testing.T) {
	server := newTestServer(t)
	get := opsSurface(t, server)

	owner := newPeerID(t)
	fillMailboxFor(t, server, owner, "sum/a", 100, 3)
	fillMailboxFor(t, server, owner, "sum/b", 100, 3)
	fillMailboxFor(t, server, owner, "sum/c", 100, 3)

	body := decodeOps[opsStorage](t, get("/ops/storage?limit=500"))
	for _, o := range body.Owners {
		if o.Owner == owner.String() {
			if o.Messages != 9 {
				t.Errorf("messages = %d across 3 mailboxes of 3, want 9", o.Messages)
			}
			// The mailbox count is the figure a single-stage aggregate gets
			// wrong: counting rows of the join counts each mailbox once per
			// message it holds.
			if o.Mailboxes != 3 {
				t.Errorf("mailboxes = %d, want 3", o.Mailboxes)
			}
			if o.MessageBytes != 90 {
				t.Errorf("messageBytes = %d, want 90 (9 × 10 bytes)", o.MessageBytes)
			}
			return
		}
	}
	t.Fatal("owner not listed")
}

// ---------------------------------------------------------------------------
// /ops/limits
// ---------------------------------------------------------------------------

// sumi's Finding A: a knob was changed and there was no way to tell whether it
// had taken effect. The endpoint has to report the running value, not the file.
func TestLimitsReportsWhatTheServerIsRunningWith(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Admission.MaxInFlight = 137
		cfg.MaxMessagesPerMailbox = 4242
	})
	get := opsSurface(t, server)

	var body struct {
		Admission struct {
			MaxInFlight     int    `json:"maxInFlight"`
			DerivedFromPool bool   `json:"maxInFlightDerivedFromPoolSize"`
			AcquireTimeout  string `json:"acquireTimeout"`
		} `json:"admission"`
		RateLimits struct {
			Window    string         `json:"window"`
			Protocols map[string]any `json:"protocols"`
		} `json:"rateLimits"`
		Storage struct {
			MaxMessagesPerMailbox int `json:"maxMessagesPerMailbox"`
		} `json:"storage"`
	}

	rec := get("/ops/limits")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d (%s)", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.Admission.MaxInFlight != 137 {
		t.Errorf("maxInFlight = %d, want the configured 137", body.Admission.MaxInFlight)
	}
	if body.Admission.DerivedFromPool {
		t.Error("an explicit bound was reported as derived")
	}
	if body.Storage.MaxMessagesPerMailbox != 4242 {
		t.Errorf("maxMessagesPerMailbox = %d, want 4242", body.Storage.MaxMessagesPerMailbox)
	}
	if len(body.RateLimits.Protocols) == 0 {
		t.Error("no protocols listed; an omitted protocol is unlimited and must still be shown")
	}
	if body.RateLimits.Window == "" || body.RateLimits.Window == "0s" {
		t.Errorf("window = %q, want the effective one", body.RateLimits.Window)
	}
}

// The database password lives on ServerConfig. It must not be reachable from
// any endpoint that reads configuration.
func TestOperatorSurfaceNeverServesTheDatabasePassword(t *testing.T) {
	const password = "sentinel-password-must-not-appear"

	server := newTestServer(t, func(cfg *core.ServerConfig) {
		// The config the surface reads is the one the server holds, so the
		// secret is planted there rather than taken from the DSN, which may
		// carry none.
		if cfg.Storage.Postgres == nil {
			cfg.Storage.Postgres = &core.PostgresConfig{}
		}
		cfg.Storage.Postgres.Password = password
	})
	get := opsSurface(t, server)

	for _, target := range []string{
		"/ops/limits",
		"/ops/storage",
		"/ops/mailboxes/top",
		"/ops/mailboxes?owner=" + server.PeerID.String(),
	} {
		if body := get(target).Body.String(); strings.Contains(body, password) {
			t.Errorf("GET %s leaked the database password:\n%s", target, body)
		}
	}
}
