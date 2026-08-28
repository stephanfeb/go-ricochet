// Package metrics exposes the server's behaviour as Prometheus series.
//
// It exists because the previous answer to "what is this server doing" was to
// read the logs of one instance and guess. The sumi team spent days inferring a
// rate limit from client-side 429s; every number that investigation needed is
// now a series.
//
// # Cardinality
//
// Peer identity never appears in a label. forge hands middleware a peer ID and
// the temptation is to use it, but with thousands of mobile clients that is an
// unbounded label set: it exhausts the scraper's memory long before it
// exhausts ours, and takes monitoring down at exactly the moment monitoring is
// needed. Peer IDs belong in logs, and in top-N views computed on demand.
//
// Every label here is drawn from a fixed set: `protocol` from the seven
// pipelines, `operation` from each protocol's routing table (forge records it
// only after a route matches, so a caller cannot invent one), and `outcome`
// from the six values below.
package metrics

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	forge "github.com/twostack/go-p2p-forge"

	"github.com/twostack/go-ricochet/internal/admission"
)

// RegistryKey is the forge registry key under which the metrics are provided
// to pipeline constructors.
const RegistryKey = "metrics"

// Request outcomes. These are the whole label set for `outcome`.
//
// The distinction between OutcomeRateLimited and OutcomeOverloaded is the one
// sumi could not see and is the reason both exist: a rate-limited request is a
// client exceeding a configured per-peer budget, and the fix is to raise the
// budget; an overloaded request is the server shedding work it has no capacity
// for, and the fix is to add capacity. Conflating them tells an operator to
// change the wrong thing.
const (
	// OutcomeOK is a request that completed and reported success.
	OutcomeOK = "ok"

	// OutcomeClientError is a request the server handled correctly and
	// refused: a 4xx in the response. Not a server fault, but a spike is
	// still worth seeing.
	OutcomeClientError = "client_error"

	// OutcomeServerError is a 5xx carried in the response body.
	OutcomeServerError = "server_error"

	// OutcomeRateLimited is a request rejected by a per-peer rate limit.
	OutcomeRateLimited = "rate_limited"

	// OutcomeOverloaded is a request shed by admission control because the
	// server was at capacity. This is the signal to add capacity.
	OutcomeOverloaded = "overloaded"

	// OutcomeError is a pipeline failure: a decode error, an unroutable
	// operation, a panic.
	OutcomeError = "error"
)

// operationUnrouted labels requests that never reached a handler, because they
// named an operation that does not exist or carried none at all. Using a fixed
// placeholder rather than the value the caller sent is what keeps the label
// set bounded.
const operationUnrouted = "unrouted"

// statusCoder is implemented by the response types that carry an HTTP-style
// status. It lets the middleware tell a handled-and-refused request from a
// successful one, which sc.Err alone cannot: a handler that answers 404 or 409
// returns no pipeline error, so without this every such request would be
// counted as ok.
type statusCoder interface {
	StatusCode() int
}

// Metrics holds the server's Prometheus registry and its metric families.
type Metrics struct {
	registry *prometheus.Registry

	requests  *prometheus.CounterVec
	durations *prometheus.HistogramVec
}

// New builds a metrics registry with the request families registered.
//
// The registry is private to this server rather than Prometheus's global
// default. That keeps the exposed series a deliberate list — libp2p registers
// a large number of collectors into the default registry as a side effect of
// being imported — and it lets a test enumerate exactly what is published.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ricochet_requests_total",
			Help: "Protocol requests by outcome.",
		}, []string{"protocol", "operation", "outcome"}),
		durations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ricochet_request_duration_seconds",
			Help: "Protocol request latency.",
			// Spans a sub-millisecond cache hit to a multi-second batch
			// write. The default buckets top out at 10s, which is past the
			// point where anything here is still interesting.
			Buckets: []float64{
				0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
			},
		}, []string{"protocol", "operation", "outcome"}),
	}

	reg.MustRegister(m.requests, m.durations)

	// Go runtime and process metrics. Heap growth and goroutine counts are
	// the first thing to check when latency climbs without load climbing.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Registry returns the Prometheus registry, for the /metrics handler and for
// tests that want to gather.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// Register adds a collector to the registry. It is how the admission,
// connection-pool and buffer-pool collectors are attached once the things they
// observe exist.
func (m *Metrics) Register(c prometheus.Collector) error {
	if m == nil {
		return nil
	}
	return m.registry.Register(c)
}

// Observe records one completed request.
func (m *Metrics) Observe(protocol, operation, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(protocol, operation, outcome).Inc()
	m.durations.WithLabelValues(protocol, operation, outcome).Observe(d.Seconds())
}

// FromRegistry retrieves the metrics from a forge registry, returning nil when
// none was provided. Every method is nil-safe, so a pipeline built without
// metrics simply records nothing.
func FromRegistry(reg *forge.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	m, ok := forge.Service[*Metrics](reg, RegistryKey)
	if !ok {
		return nil
	}
	return m
}

// Middleware records the outcome and duration of every request through a
// pipeline.
//
// Install it outermost, ahead of Recovery: a panicking handler is exactly the
// request an operator most needs counted, and Recovery converts the panic into
// sc.Err only once it has returned. fallbackOperation names the operation for
// pipelines that have no OperationRouter and therefore serve exactly one.
func Middleware(m *Metrics, protocol, fallbackOperation string) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		if m == nil {
			next()
			return
		}

		start := time.Now()

		// Deferred so that a panic escaping Recovery — or any future
		// middleware that returns early — is still counted rather than
		// silently vanishing from the totals.
		defer func() {
			m.Observe(protocol, Operation(sc, fallbackOperation), classify(sc), time.Since(start))
		}()

		next()
	}
}

// classify turns a finished request into one of the six outcomes.
//
// Pipeline errors are checked before response status because a rate-limited or
// shed request is also given a 429 or 503 in its response body; reading the
// status first would file both under client_error and lose the distinction
// that motivated the labels.
func classify(sc *forge.StreamContext) string {
	switch {
	case sc.Err == nil:
		// Fall through to the response status.
	case errors.Is(sc.Err, forge.ErrRateLimited):
		return OutcomeRateLimited
	case errors.Is(sc.Err, admission.ErrOverloaded):
		return OutcomeOverloaded
	default:
		return OutcomeError
	}

	resp, ok := sc.Response.(statusCoder)
	if !ok {
		return OutcomeOK
	}

	switch status := resp.StatusCode(); {
	case status >= 500:
		return OutcomeServerError
	case status >= 400:
		return OutcomeClientError
	default:
		return OutcomeOK
	}
}

// Operation returns the operation label a request would be recorded under.
// Exported for tests that assert the label set is bounded.
func Operation(sc *forge.StreamContext, fallback string) string {
	if op := sc.Operation(); op != "" {
		return op
	}
	if fallback != "" {
		return fallback
	}
	return operationUnrouted
}
