package opsview

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/capacity"
	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// A real peer ID, so the owner validation is exercised against something that
// actually decodes rather than against a shape that happens to pass.
const testPeerID = "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"

// stubStorage answers only the operator listings. Everything else panics: if a
// handler reaches for it, that is a bug worth failing loudly on.
type stubStorage struct {
	storage.Storage

	mailboxes []*storage.MailboxUsage
	owners    []*storage.OwnerUsage
	err       error

	lastQuery storage.MailboxUsageQuery
	lastLimit int
}

func (s *stubStorage) ListMailboxUsage(_ context.Context, q storage.MailboxUsageQuery) ([]*storage.MailboxUsage, error) {
	s.lastQuery = q
	if s.err != nil {
		return nil, s.err
	}
	return s.mailboxes, nil
}

func (s *stubStorage) ListOwnerUsage(_ context.Context, limit, _ int) ([]*storage.OwnerUsage, error) {
	s.lastLimit = limit
	if s.err != nil {
		return nil, s.err
	}
	return s.owners, nil
}

// ServerStats is reached only through the sampler.
func (s *stubStorage) ServerStats(context.Context, float64) (*storage.ServerStats, error) {
	return &storage.ServerStats{
		SampledAt:             time.Now(),
		Mailboxes:             3,
		Messages:              42,
		MessageBytes:          4096,
		DatabaseBytes:         250,
		MailboxesNearCapacity: 1,
		NearCapacityRatio:     0.9,
		Depth: []storage.DepthBucket{
			{Label: "0", Min: 0, Max: 0, Mailboxes: 1},
			{Label: "1-9", Min: 1, Max: 9, Mailboxes: 2},
		},
	}, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() *core.ServerConfig {
	cfg := core.DefaultConfig()
	cfg.Storage.Backend = "postgres"
	cfg.Storage.Postgres = &core.PostgresConfig{
		Host:     "localhost",
		Database: "ricochet",
		Username: "ricochet",
		Password: "hunter2-do-not-leak",
		PoolSize: 25,
	}
	return cfg
}

// call routes a request through the handler map, which is the same map the ops
// server mounts.
func call(t *testing.T, opts Options, target string) *httptest.ResponseRecorder {
	t.Helper()

	if opts.Logger == nil {
		opts.Logger = discardLogger()
	}
	if opts.Config == nil {
		opts.Config = testConfig()
	}

	routes := Routes(opts)
	path, _, _ := strings.Cut(target, "?")

	h, ok := routes[path]
	if !ok {
		t.Fatalf("no route for %s (have %v)", path, keysOf(routes))
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func keysOf(m map[string]http.Handler) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var body T
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (body %q)", err, rec.Body.String())
	}
	return body
}

func sampledSampler(t *testing.T, store storage.Storage, maxBytes int64) *capacity.Sampler {
	t.Helper()
	s := capacity.New(store, maxBytes, 0.9, discardLogger())
	if _, err := s.Sample(context.Background()); err != nil {
		t.Fatalf("sample: %v", err)
	}
	return s
}

// ---------------------------------------------------------------------------
// /ops/mailboxes/top
// ---------------------------------------------------------------------------

func fullMailbox() *storage.MailboxUsage {
	return &storage.MailboxUsage{
		MailboxID: 1, OwnerPeerID: testPeerID, FolderPath: "inbox",
		MessageCount: 10, MaxMessages: 10, MessageBytes: 100, FillRatio: 1,
	}
}

func roomyMailbox() *storage.MailboxUsage {
	return &storage.MailboxUsage{
		MailboxID: 2, OwnerPeerID: testPeerID, FolderPath: "archive",
		MessageCount: 1, MaxMessages: 10, MessageBytes: 10, FillRatio: 0.1,
	}
}

// The acceptance criterion in one assertion: a full mailbox has to say so,
// without the reader doing the division themselves.
func TestTopMailboxesReportsFullness(t *testing.T) {
	store := &stubStorage{mailboxes: []*storage.MailboxUsage{fullMailbox(), roomyMailbox()}}

	rec := call(t, Options{Storage: store}, "/ops/mailboxes/top")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}

	body := decode[mailboxesResponse](t, rec)
	if len(body.Mailboxes) != 2 {
		t.Fatalf("got %d mailboxes, want 2", len(body.Mailboxes))
	}
	if !body.Mailboxes[0].Full {
		t.Error("a mailbox at 10 of 10 is not reported as full")
	}
	if body.Mailboxes[1].Full {
		t.Error("a mailbox at 1 of 10 is reported as full")
	}
	if body.Mailboxes[0].Owner != testPeerID {
		t.Errorf("owner = %q, want the peer ID", body.Mailboxes[0].Owner)
	}
	if body.Sort != "fill" {
		t.Errorf("sort = %q, want fill by default", body.Sort)
	}
}

// An uncapped mailbox is not full however much it holds. Reporting otherwise
// would send an operator after a mailbox that is behaving exactly as
// configured.
func TestUncappedMailboxIsNeverFull(t *testing.T) {
	store := &stubStorage{mailboxes: []*storage.MailboxUsage{{
		MailboxID: 3, OwnerPeerID: testPeerID, FolderPath: "firehose",
		MessageCount: 1_000_000, MaxMessages: 0,
	}}}

	body := decode[mailboxesResponse](t, call(t, Options{Storage: store}, "/ops/mailboxes/top"))
	if body.Mailboxes[0].Full {
		t.Error("an uncapped mailbox is reported as full")
	}
	if body.Mailboxes[0].FillRatio != 0 {
		t.Errorf("fillRatio = %v against no cap, want 0", body.Mailboxes[0].FillRatio)
	}
}

func TestTopMailboxesPaging(t *testing.T) {
	store := &stubStorage{}

	call(t, Options{Storage: store}, "/ops/mailboxes/top")
	if store.lastQuery.Limit != storage.DefaultOperatorPageSize {
		t.Errorf("default limit = %d, want %d", store.lastQuery.Limit, storage.DefaultOperatorPageSize)
	}

	call(t, Options{Storage: store}, "/ops/mailboxes/top?n=5&offset=10")
	if store.lastQuery.Limit != 5 || store.lastQuery.Offset != 10 {
		t.Errorf("limit/offset = %d/%d, want 5/10", store.lastQuery.Limit, store.lastQuery.Offset)
	}

	// limit is accepted as a synonym, because every other endpoint here uses
	// it and guessing wrong should not hand back a differently sized page.
	call(t, Options{Storage: store}, "/ops/mailboxes/top?limit=7")
	if store.lastQuery.Limit != 7 {
		t.Errorf("limit = %d, want 7", store.lastQuery.Limit)
	}
}

func TestTopMailboxesSortSelection(t *testing.T) {
	store := &stubStorage{}

	call(t, Options{Storage: store}, "/ops/mailboxes/top?sort=count")
	if store.lastQuery.Sort != storage.SortByCount {
		t.Errorf("sort = %q, want count", store.lastQuery.Sort)
	}

	rec := call(t, Options{Storage: store}, "/ops/mailboxes/top?sort=sideways")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("unknown sort = %d, want 400", rec.Code)
	}
}

// A malformed page size is an error, not a silent default. An operator who
// typed n=twenty and got twenty results back would never learn otherwise.
func TestMalformedParametersAreRejected(t *testing.T) {
	store := &stubStorage{}
	for _, target := range []string{
		"/ops/mailboxes/top?n=twenty",
		"/ops/mailboxes/top?n=-1",
		"/ops/mailboxes/top?offset=x",
		"/ops/storage?limit=lots",
	} {
		if rec := call(t, Options{Storage: store, Capacity: sampledSampler(t, store, 1000)}, target); rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s = %d, want 400", target, rec.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// /ops/mailboxes
// ---------------------------------------------------------------------------

func TestOwnerMailboxesRequiresAValidOwner(t *testing.T) {
	store := &stubStorage{}

	rec := call(t, Options{Storage: store}, "/ops/mailboxes")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing owner = %d, want 400", rec.Code)
	}

	rec = call(t, Options{Storage: store}, "/ops/mailboxes?owner=not-a-peer-id")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("malformed owner = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "peer ID") {
		t.Errorf("error does not say what was wrong: %s", rec.Body.String())
	}
}

func TestOwnerMailboxesScopesTheQuery(t *testing.T) {
	store := &stubStorage{mailboxes: []*storage.MailboxUsage{fullMailbox()}}

	rec := call(t, Options{Storage: store}, "/ops/mailboxes?owner="+testPeerID)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d (%s)", rec.Code, rec.Body.String())
	}
	if store.lastQuery.Owner != testPeerID {
		t.Errorf("query owner = %q, want the requested peer", store.lastQuery.Owner)
	}

	body := decode[mailboxesResponse](t, rec)
	if body.Owner != testPeerID {
		t.Errorf("response owner = %q", body.Owner)
	}
}

// An empty result is an empty list, not a null one. A caller that has to
// distinguish the two will get it wrong eventually.
func TestEmptyListingIsAnEmptyArray(t *testing.T) {
	rec := call(t, Options{Storage: &stubStorage{}}, "/ops/mailboxes/top")
	if !strings.Contains(rec.Body.String(), `"mailboxes":[]`) {
		t.Errorf("empty listing serialized as %s", rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// /ops/storage
// ---------------------------------------------------------------------------

func TestStorageReportsOwnersAndServerTotals(t *testing.T) {
	store := &stubStorage{owners: []*storage.OwnerUsage{
		{OwnerPeerID: testPeerID, Mailboxes: 2, Messages: 30, MessageBytes: 300},
	}}

	rec := call(t, Options{
		Storage:  store,
		Capacity: sampledSampler(t, store, 1000),
	}, "/ops/storage")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d (%s)", rec.Code, rec.Body.String())
	}

	body := decode[storageResponse](t, rec)
	if len(body.Owners) != 1 || body.Owners[0].MessageBytes != 300 {
		t.Fatalf("owners = %+v", body.Owners)
	}
	if body.Server == nil {
		t.Fatal("server totals missing after a sample")
	}
	if body.Server.DatabaseBytes != 250 {
		t.Errorf("databaseBytes = %d, want 250", body.Server.DatabaseBytes)
	}
	if body.Server.MaxStorageBytes != 1000 || body.Server.AvailableBytes != 750 {
		t.Errorf("max/available = %d/%d, want 1000/750",
			body.Server.MaxStorageBytes, body.Server.AvailableBytes)
	}
	if body.Server.SampledAt.IsZero() {
		t.Error("sampledAt is zero; a cached figure must carry its age")
	}
	if len(body.Server.Depth) != 2 {
		t.Errorf("depth buckets = %d, want 2", len(body.Server.Depth))
	}
}

// Before the first sample the server block is null with a note, not zeroes.
// Zeroes would read as an empty server, which is exactly the wrong conclusion
// to draw during an incident.
func TestStorageBeforeFirstSampleIsExplicit(t *testing.T) {
	store := &stubStorage{owners: []*storage.OwnerUsage{
		{OwnerPeerID: testPeerID, Mailboxes: 1, Messages: 5, MessageBytes: 50},
	}}

	rec := call(t, Options{
		Storage:  store,
		Capacity: capacity.New(store, 1000, 0.9, discardLogger()), // never sampled
	}, "/ops/storage")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d (%s)", rec.Code, rec.Body.String())
	}

	body := decode[storageResponse](t, rec)
	if body.Server != nil {
		t.Errorf("server totals = %+v before any sample, want null", body.Server)
	}
	if body.Note == "" {
		t.Error("a null server block with no explanation")
	}
	if len(body.Owners) != 1 {
		t.Error("per-owner totals are live and must be reported regardless")
	}
}

// Over-budget headroom must not go negative, which would read as nonsense.
func TestStorageClampsHeadroom(t *testing.T) {
	store := &stubStorage{}
	body := decode[storageResponse](t, call(t, Options{
		Storage:  store,
		Capacity: sampledSampler(t, store, 100), // budget below the 250 in use
	}, "/ops/storage"))

	if body.Server.AvailableBytes != 0 {
		t.Errorf("available = %d, want 0", body.Server.AvailableBytes)
	}
	if body.Server.DatabaseBytes != 250 {
		t.Errorf("used = %d, want the real 250 rather than a clamped one", body.Server.DatabaseBytes)
	}
}

// ---------------------------------------------------------------------------
// /ops/limits
// ---------------------------------------------------------------------------

// The whole point of this endpoint: report the number the server is running
// with, not the unresolved zero somebody left in the file.
func TestLimitsReportsEffectiveValues(t *testing.T) {
	cfg := testConfig()
	cfg.Admission.MaxInFlight = 0 // derived from the pool
	cfg.Admission.AcquireTimeout = 0

	body := decode[limitsResponse](t, call(t, Options{
		Storage: &stubStorage{}, Config: cfg, PoolSize: 25,
	}, "/ops/limits"))

	if body.Admission.MaxInFlight != cfg.Admission.EffectiveMaxInFlight(25) {
		t.Errorf("maxInFlight = %d, want the derived %d",
			body.Admission.MaxInFlight, cfg.Admission.EffectiveMaxInFlight(25))
	}
	if body.Admission.MaxInFlight == 0 {
		t.Error("maxInFlight reported as the unresolved zero")
	}
	if !body.Admission.DerivedFromPool {
		t.Error("a derived bound must say it was derived")
	}
	if body.Admission.PoolSize != 25 {
		t.Errorf("poolSize = %d, want 25", body.Admission.PoolSize)
	}
	if body.Admission.AcquireTimeout == "0s" || body.Admission.AcquireTimeout == "" {
		t.Errorf("acquireTimeout = %q, want the effective default", body.Admission.AcquireTimeout)
	}
}

func TestLimitsReportsAnExplicitBoundAsExplicit(t *testing.T) {
	cfg := testConfig()
	cfg.Admission.MaxInFlight = 77

	body := decode[limitsResponse](t, call(t, Options{
		Storage: &stubStorage{}, Config: cfg, PoolSize: 25,
	}, "/ops/limits"))

	if body.Admission.MaxInFlight != 77 {
		t.Errorf("maxInFlight = %d, want the configured 77", body.Admission.MaxInFlight)
	}
	if body.Admission.DerivedFromPool {
		t.Error("an explicitly configured bound was reported as derived")
	}
}

// Every protocol is listed, including ones the configuration omits. A missing
// entry means unlimited, and hiding it would conceal the protocol somebody
// meant to limit and misspelled.
func TestLimitsListsEveryProtocol(t *testing.T) {
	cfg := testConfig()
	cfg.RateLimits.Protocols = map[string]core.ProtocolLimits{
		core.RateLimitSDA: {Read: core.Limit{Rate: 100, Burst: 20}},
	}

	body := decode[limitsResponse](t, call(t, Options{
		Storage: &stubStorage{}, Config: cfg,
	}, "/ops/limits"))

	for _, name := range rateLimitedProtocols {
		if _, ok := body.RateLimits.Protocols[name]; !ok {
			t.Errorf("protocol %s absent from the report", name)
		}
	}

	sda := body.RateLimits.Protocols[core.RateLimitSDA]
	if !sda.Read.Enabled || sda.Read.Rate != 100 {
		t.Errorf("sda read = %+v, want an enabled rate of 100", sda.Read)
	}
	if sda.Write.Enabled {
		t.Error("an unconfigured bucket is reported as enabled")
	}
	if body.RateLimits.EnabledProtocols != 1 {
		t.Errorf("enabledProtocols = %d, want 1", body.RateLimits.EnabledProtocols)
	}
	if sda.Read.Per == "" {
		t.Error("a rate with no window is not a rate")
	}
}

func TestLimitsReportsDisabledRateLimitsAsDisabled(t *testing.T) {
	body := decode[limitsResponse](t, call(t, Options{
		Storage: &stubStorage{}, Config: testConfig(),
	}, "/ops/limits"))

	if body.RateLimits.EnabledProtocols != 0 {
		t.Errorf("enabledProtocols = %d on the defaults, want 0", body.RateLimits.EnabledProtocols)
	}
	for name, p := range body.RateLimits.Protocols {
		if p.Requests.Enabled || p.Read.Enabled || p.Write.Enabled {
			t.Errorf("%s reported as rate limited by default", name)
		}
	}
}

// ServerConfig carries the database password. Nothing on this surface may
// marshal it whole, and this is the assertion that stops a later "just add
// the config" from being quietly correct-looking.
func TestLimitsNeverLeaksTheDatabasePassword(t *testing.T) {
	cfg := testConfig()

	for _, target := range []string{"/ops/limits", "/ops/mailboxes/top"} {
		rec := call(t, Options{Storage: &stubStorage{}, Config: cfg}, target)
		body := rec.Body.String()
		if strings.Contains(body, cfg.Storage.Postgres.Password) {
			t.Errorf("GET %s leaked the database password:\n%s", target, body)
		}
		if strings.Contains(body, "postgres") && strings.Contains(body, "username") {
			t.Errorf("GET %s leaked connection details:\n%s", target, body)
		}
	}
}

// ---------------------------------------------------------------------------
// Failure paths
// ---------------------------------------------------------------------------

// A database error string can name schemas, hosts and roles. The operator gets
// a 500 and a pointer to the log, not the driver's sentence.
func TestQueryFailuresDoNotEchoTheDatabaseError(t *testing.T) {
	store := &stubStorage{err: errors.New("FATAL: role \"ricochet_secret\" does not exist")}

	for _, target := range []string{"/ops/mailboxes/top", "/ops/storage"} {
		rec := call(t, Options{
			Storage: store, Capacity: capacity.New(store, 1000, 0.9, discardLogger()),
		}, target)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("GET %s = %d, want 500", target, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "ricochet_secret") {
			t.Errorf("GET %s echoed the driver error: %s", target, rec.Body.String())
		}
	}
}

func TestSlowQueryTimesOut(t *testing.T) {
	store := &slowStorage{}

	rec := call(t, Options{Storage: store, Timeout: 20 * time.Millisecond},
		"/ops/mailboxes/top")

	if rec.Code != http.StatusGatewayTimeout {
		t.Errorf("code = %d, want 504 (%s)", rec.Code, rec.Body.String())
	}
}

type slowStorage struct{ storage.Storage }

func (s *slowStorage) ListMailboxUsage(ctx context.Context, _ storage.MailboxUsageQuery) ([]*storage.MailboxUsage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
