// Package opsview serves the operator's read-only view of what this server is
// actually holding.
//
// It exists because every question an operator has about storage was
// previously answerable only by the owner of the data. A peer could ask about
// its own mailboxes over MMA; nobody could ask "which mailbox on this server
// is about to start evicting", which is the question that precedes every
// storage incident. These endpoints answer it without a peer identity, a
// handshake or a client library — the operator already has curl.
//
// The handlers are here rather than in opsapi so that package stays purely a
// transport: it knows about listeners, readiness and draining, and nothing
// about mailboxes. The same separation puts the readiness ping in the server
// package rather than in opsapi.
//
// Nothing here mutates. The surface is loopback-bound by default, so peer IDs
// in a response body are fine — they are the whole point of the view — but
// they must never reach a metric label, where they would be unbounded
// cardinality on a scrape anyone can collect.
package opsview

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// defaultTimeout bounds each query. These endpoints scan, and an operator
// surface that can be wedged by its own diagnostic query is worse than one
// that admits it timed out.
const defaultTimeout = 10 * time.Second

// Options are the dependencies the operator view reads. Everything is
// read-only; nothing here holds state of its own.
type Options struct {
	Storage  storage.Storage
	Capacity *capacity.Sampler

	// Config supplies the effective limits reported by /ops/limits. It is
	// read field by field and never marshalled whole: ServerConfig carries
	// the database password.
	Config *core.ServerConfig

	// PoolSize is the database pool size the admission bound was derived
	// from. It is passed in because it is a property of the storage backend,
	// not of the configuration.
	PoolSize int

	// Timeout bounds each query. Zero means defaultTimeout.
	Timeout time.Duration

	Logger *slog.Logger
}

type view struct {
	opts Options
	log  *slog.Logger
}

// Routes returns the operator endpoints keyed by ServeMux pattern, ready to
// hand to opsapi.Options.Routes.
func Routes(opts Options) map[string]http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	v := &view{opts: opts, log: opts.Logger}

	return map[string]http.Handler{
		"/ops/mailboxes/top": http.HandlerFunc(v.handleTopMailboxes),
		"/ops/mailboxes":     http.HandlerFunc(v.handleOwnerMailboxes),
		"/ops/storage":       http.HandlerFunc(v.handleStorage),
		"/ops/limits":        http.HandlerFunc(v.handleLimits),
	}
}

// ---------------------------------------------------------------------------
// /ops/mailboxes/top and /ops/mailboxes
// ---------------------------------------------------------------------------

// mailboxView is one mailbox as reported. It restates the storage record's
// figures plus the verdict, so an operator reading the response does not have
// to divide two numbers to answer the question they came with.
type mailboxView struct {
	ID            int64      `json:"id"`
	Owner         string     `json:"owner"`
	FolderPath    string     `json:"folderPath"`
	MessageCount  int        `json:"messageCount"`
	MaxMessages   int        `json:"maxMessages"`
	FillRatio     float64    `json:"fillRatio"`
	Full          bool       `json:"full"`
	MessageBytes  int64      `json:"messageBytes"`
	LastMessageAt *time.Time `json:"lastMessageAt,omitempty"`
}

func toMailboxViews(in []*storage.MailboxUsage) []mailboxView {
	out := make([]mailboxView, 0, len(in))
	for _, u := range in {
		out = append(out, mailboxView{
			ID:            u.MailboxID,
			Owner:         u.OwnerPeerID,
			FolderPath:    u.FolderPath,
			MessageCount:  u.MessageCount,
			MaxMessages:   u.MaxMessages,
			FillRatio:     u.FillRatio,
			Full:          u.Full(),
			MessageBytes:  u.MessageBytes,
			LastMessageAt: u.LastMessageAt,
		})
	}
	return out
}

type mailboxesResponse struct {
	Owner     string        `json:"owner,omitempty"`
	Sort      string        `json:"sort"`
	Limit     int           `json:"limit"`
	Offset    int           `json:"offset"`
	Count     int           `json:"count"`
	Mailboxes []mailboxView `json:"mailboxes"`
}

// handleTopMailboxes answers "which mailboxes are closest to full", across
// every owner. This is the endpoint the plan's acceptance criterion is about:
// one curl, no peer identity, any owner's mailbox.
func (v *view) handleTopMailboxes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// n is the operator-facing spelling; limit is accepted too, because every
	// other paging endpoint here uses it and guessing wrong should not be a
	// silent empty page.
	n, err := intParam(q, "n", storage.DefaultOperatorPageSize)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if q.Has("limit") {
		if n, err = intParam(q, "limit", n); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	offset, err := intParam(q, "offset", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sort := storage.SortByFill
	switch q.Get("sort") {
	case "", "fill":
	case "count":
		sort = storage.SortByCount
	default:
		writeError(w, http.StatusBadRequest, "sort must be 'fill' or 'count'")
		return
	}

	v.listMailboxes(w, r, storage.MailboxUsageQuery{
		Sort:   sort,
		Limit:  n,
		Offset: offset,
	})
}

// handleOwnerMailboxes answers the same question for one owner, without
// needing to be that owner. An operator investigating a complaint has a peer
// ID and no key.
func (v *view) handleOwnerMailboxes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	owner := q.Get("owner")
	if owner == "" {
		writeError(w, http.StatusBadRequest,
			"owner is required; use /ops/mailboxes/top for a cross-owner listing")
		return
	}
	// Reject a malformed peer ID here rather than scanning for something that
	// cannot exist. A typo should come back as a typo, not an empty list.
	if _, err := peer.Decode(owner); err != nil {
		writeError(w, http.StatusBadRequest, "owner is not a valid peer ID: "+err.Error())
		return
	}

	limit, err := intParam(q, "limit", storage.DefaultOperatorPageSize)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := intParam(q, "offset", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	v.listMailboxes(w, r, storage.MailboxUsageQuery{
		Owner:  owner,
		Sort:   storage.SortByFill,
		Limit:  limit,
		Offset: offset,
	})
}

func (v *view) listMailboxes(w http.ResponseWriter, r *http.Request, q storage.MailboxUsageQuery) {
	ctx, cancel := context.WithTimeout(r.Context(), v.opts.Timeout)
	defer cancel()

	usage, err := v.opts.Storage.ListMailboxUsage(ctx, q)
	if err != nil {
		v.fail(w, "list mailbox usage", err)
		return
	}

	sort := q.Sort
	if sort == "" {
		sort = storage.SortByFill
	}

	writeJSON(w, http.StatusOK, mailboxesResponse{
		Owner:  q.Owner,
		Sort:   string(sort),
		Limit:  storage.ClampPageSize(q.Limit),
		Offset: q.Offset,
		Count:  len(usage),
		// Reported even when empty, so an absent list reads as "none" rather
		// than as a field the caller forgot to look for.
		Mailboxes: toMailboxViews(usage),
	})
}

// ---------------------------------------------------------------------------
// /ops/storage
// ---------------------------------------------------------------------------

type depthView struct {
	Label     string `json:"label"`
	Min       int    `json:"min"`
	Max       int    `json:"max"`
	Mailboxes int    `json:"mailboxes"`
}

// serverStorageView is the sampled server-wide picture. It is a pointer in the
// response so that "not sampled yet" is null rather than a set of zeroes,
// which would read as an empty server.
type serverStorageView struct {
	SampledAt             time.Time   `json:"sampledAt"`
	AgeSeconds            float64     `json:"ageSeconds"`
	Mailboxes             int         `json:"mailboxes"`
	Messages              int64       `json:"messages"`
	MessageBytes          int64       `json:"messageBytes"`
	DatabaseBytes         int64       `json:"databaseBytes"`
	MaxStorageBytes       int64       `json:"maxStorageBytes"`
	AvailableBytes        int64       `json:"availableBytes"`
	MailboxesNearCapacity int         `json:"mailboxesNearCapacity"`
	NearCapacityRatio     float64     `json:"nearCapacityRatio"`
	Depth                 []depthView `json:"depth"`
}

type storageResponse struct {
	// Server is null until the first sample completes. The figures scan, so
	// they are taken on a timer; a cached number whose age is invisible is
	// worse than an admitted absence.
	Server *serverStorageView `json:"server"`

	// Note explains a null Server rather than leaving the caller to guess.
	Note string `json:"note,omitempty"`

	Limit  int                   `json:"limit"`
	Offset int                   `json:"offset"`
	Owners []*storage.OwnerUsage `json:"owners"`
}

// handleStorage answers "how much room is left, and who is using it". The
// per-owner totals are live; the server-wide figures come from the sampler,
// because they include a database size that is expensive to compute.
func (v *view) handleStorage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, err := intParam(q, "limit", storage.DefaultOperatorPageSize)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := intParam(q, "offset", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), v.opts.Timeout)
	defer cancel()

	owners, err := v.opts.Storage.ListOwnerUsage(ctx, limit, offset)
	if err != nil {
		v.fail(w, "list owner usage", err)
		return
	}
	if owners == nil {
		owners = []*storage.OwnerUsage{}
	}

	resp := storageResponse{
		Limit:  storage.ClampPageSize(limit),
		Offset: offset,
		Owners: owners,
	}

	if stats := v.opts.Capacity.Latest(); stats != nil {
		max := v.opts.Capacity.MaxStorageBytes()
		available := max - stats.DatabaseBytes
		if available < 0 {
			available = 0
		}

		depth := make([]depthView, 0, len(stats.Depth))
		for _, b := range stats.Depth {
			depth = append(depth, depthView{
				Label:     b.Label,
				Min:       b.Min,
				Max:       b.Max,
				Mailboxes: b.Mailboxes,
			})
		}

		age, _ := v.opts.Capacity.Age(time.Now())
		resp.Server = &serverStorageView{
			SampledAt:             stats.SampledAt,
			AgeSeconds:            age.Seconds(),
			Mailboxes:             stats.Mailboxes,
			Messages:              stats.Messages,
			MessageBytes:          stats.MessageBytes,
			DatabaseBytes:         stats.DatabaseBytes,
			MaxStorageBytes:       max,
			AvailableBytes:        available,
			MailboxesNearCapacity: stats.MailboxesNearCapacity,
			NearCapacityRatio:     stats.NearCapacityRatio,
			Depth:                 depth,
		}
	} else {
		resp.Note = capacity.ErrNotSampled.Error() +
			"; per-owner totals below are live"
	}

	writeJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// /ops/limits
// ---------------------------------------------------------------------------

// limitView reports one rate-limit bucket in resolved form. Enabled is stated
// rather than left to be inferred from a negative rate, which is the encoding
// that made "did my change take effect" unanswerable in the first place.
type limitView struct {
	Rate    int    `json:"rate"`
	Burst   int    `json:"burst"`
	Enabled bool   `json:"enabled"`
	Per     string `json:"per"`
}

type protocolLimitsView struct {
	Requests limitView `json:"requests"`
	Read     limitView `json:"read"`
	Write    limitView `json:"write"`
}

type admissionView struct {
	Enabled            bool   `json:"enabled"`
	MaxInFlight        int    `json:"maxInFlight"`
	DerivedFromPool    bool   `json:"maxInFlightDerivedFromPoolSize"`
	PoolSize           int    `json:"databasePoolSize"`
	MaxInFlightPerPeer int    `json:"maxInFlightPerPeer"`
	AcquireTimeout     string `json:"acquireTimeout"`
}

type rateLimitsView struct {
	Window           string                        `json:"window"`
	EnabledProtocols int                           `json:"enabledProtocols"`
	Protocols        map[string]protocolLimitsView `json:"protocols"`
}

type storageLimitsView struct {
	MaxStorageBytes       int64  `json:"maxStorageBytes"`
	MaxMessagesPerMailbox int    `json:"maxMessagesPerMailbox"`
	MaxMailboxes          int    `json:"maxMailboxes"`
	RetentionPolicy       string `json:"retentionPolicy"`
}

type limitsResponse struct {
	Admission  admissionView     `json:"admission"`
	RateLimits rateLimitsView    `json:"rateLimits"`
	Storage    storageLimitsView `json:"storage"`
}

// rateLimitedProtocols is the fixed list reported by /ops/limits.
//
// It is the constant list rather than the keys of the configured map on
// purpose: a protocol missing from the map is unlimited, and reporting only
// what was configured would hide exactly the protocol somebody meant to limit
// and misspelled.
var rateLimitedProtocols = []string{
	core.RateLimitMSA,
	core.RateLimitMSABatch,
	core.RateLimitMAA,
	core.RateLimitMMA,
	core.RateLimitSDA,
	core.RateLimitSFA,
	core.RateLimitSCA,
	core.RateLimitMTA,
}

// handleLimits reports what the server actually parsed.
//
// Every value here is the effective one — the number the running server uses,
// after defaults and derivations — because the question this answers is "did
// the knob I changed take effect", and echoing the unresolved configuration
// answers a different one.
func (v *view) handleLimits(w http.ResponseWriter, _ *http.Request) {
	cfg := v.opts.Config
	if cfg == nil {
		writeError(w, http.StatusInternalServerError, "configuration unavailable")
		return
	}

	window := cfg.RateLimits.EffectiveWindow()
	protocols := make(map[string]protocolLimitsView, len(rateLimitedProtocols))
	enabled := 0
	for _, name := range rateLimitedProtocols {
		limits := cfg.RateLimits.For(name)
		pv := protocolLimitsView{
			Requests: toLimitView(limits.Requests, window),
			Read:     toLimitView(limits.Read, window),
			Write:    toLimitView(limits.Write, window),
		}
		if pv.Requests.Enabled || pv.Read.Enabled || pv.Write.Enabled {
			enabled++
		}
		protocols[name] = pv
	}

	writeJSON(w, http.StatusOK, limitsResponse{
		Admission: admissionView{
			Enabled:            cfg.Admission.Enabled,
			MaxInFlight:        cfg.Admission.EffectiveMaxInFlight(v.opts.PoolSize),
			DerivedFromPool:    cfg.Admission.MaxInFlight <= 0,
			PoolSize:           v.opts.PoolSize,
			MaxInFlightPerPeer: cfg.Admission.MaxInFlightPerPeer,
			AcquireTimeout:     cfg.Admission.EffectiveAcquireTimeout().String(),
		},
		RateLimits: rateLimitsView{
			Window:           window.String(),
			EnabledProtocols: enabled,
			Protocols:        protocols,
		},
		Storage: storageLimitsView{
			MaxStorageBytes:       cfg.MaxStorageBytes,
			MaxMessagesPerMailbox: cfg.MaxMessagesPerMailbox,
			MaxMailboxes:          cfg.MaxMailboxes,
			RetentionPolicy:       cfg.RetentionPolicy.String(),
		},
	})
}

func toLimitView(l core.Limit, window time.Duration) limitView {
	return limitView{
		Rate:    l.Rate,
		Burst:   l.Burst,
		Enabled: l.Rate > 0,
		Per:     window.String(),
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// intParam parses a non-negative integer parameter. A malformed value is an
// error rather than a silent fallback to the default: an operator who typed
// "n=twenty" should be told so, not handed a page of a size they did not ask
// for and may not notice.
func intParam(q map[string][]string, name string, def int) (int, error) {
	values, ok := q[name]
	if !ok || len(values) == 0 || values[0] == "" {
		return def, nil
	}
	n, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, errors.New(name + " must be an integer")
	}
	if n < 0 {
		return 0, errors.New(name + " must not be negative")
	}
	return n, nil
}

// fail logs the cause and reports a generic failure. The detail goes to the
// log rather than the body because a database error string can carry schema
// and connection detail, and this surface is not always loopback-bound.
func (v *view) fail(w http.ResponseWriter, what string, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		v.log.Warn("ops query timed out", "query", what)
		writeError(w, http.StatusGatewayTimeout, what+" timed out")
		return
	}
	v.log.Error("ops query failed", "query", what, "error", err)
	writeError(w, http.StatusInternalServerError, what+" failed; see server log")
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
