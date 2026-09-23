package metrics_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	forge "github.com/stephanfeb/go-p2p-forge"
	forgemw "github.com/stephanfeb/go-p2p-forge/middleware"

	"github.com/stephanfeb/go-ricochet/internal/admission"
	"github.com/stephanfeb/go-ricochet/internal/metrics"
)

// statusResponse stands in for the protocol response types, which carry an
// HTTP-style status the middleware reads.
type statusResponse struct{ status int }

func (r *statusResponse) StatusCode() int { return r.status }

// run drives one request through the middleware, letting the inner function
// set whatever a real pipeline would have set.
func run(m *metrics.Metrics, protocol, fallback string, inner func(sc *forge.StreamContext)) {
	sc := &forge.StreamContext{}
	metrics.Middleware(m, protocol, fallback)(sc, func() { inner(sc) })
}

// counterValue returns the value of ricochet_requests_total for one label set,
// and whether that series exists at all.
func counterValue(t *testing.T, m *metrics.Metrics, protocol, operation, outcome string) (float64, bool) {
	t.Helper()

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() != "ricochet_requests_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			labels := labelMap(metric)
			if labels["protocol"] == protocol && labels["operation"] == operation && labels["outcome"] == outcome {
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func labelMap(m *dto.Metric) map[string]string {
	out := make(map[string]string, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

// assertCounted fails unless exactly one request was recorded under the given
// labels.
func assertCounted(t *testing.T, m *metrics.Metrics, protocol, operation, outcome string) {
	t.Helper()
	got, ok := counterValue(t, m, protocol, operation, outcome)
	if !ok {
		t.Fatalf("no series for {protocol=%q, operation=%q, outcome=%q}; got %v",
			protocol, operation, outcome, seriesLabels(t, m))
	}
	if got != 1 {
		t.Errorf("count = %v for outcome %q, want 1", got, outcome)
	}
}

// seriesLabels lists every request series, for failure messages.
func seriesLabels(t *testing.T, m *metrics.Metrics) []string {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var out []string
	for _, f := range families {
		if f.GetName() != "ricochet_requests_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			l := labelMap(metric)
			out = append(out, fmt.Sprintf("%s/%s/%s", l["protocol"], l["operation"], l["outcome"]))
		}
	}
	return out
}

// The four outcomes an operator acts on differently must be distinguishable.
// Conflating rate_limited with overloaded is the specific mistake this guards:
// one says raise a client's budget, the other says add capacity.
func TestOutcomesAreDistinguishable(t *testing.T) {
	cases := []struct {
		name  string
		setup func(sc *forge.StreamContext)
		want  string
	}{
		{"success", func(sc *forge.StreamContext) {
			sc.Response = &statusResponse{status: 200}
		}, metrics.OutcomeOK},

		{"handler refused", func(sc *forge.StreamContext) {
			sc.Response = &statusResponse{status: 404}
		}, metrics.OutcomeClientError},

		{"handler failed", func(sc *forge.StreamContext) {
			sc.Response = &statusResponse{status: 500}
		}, metrics.OutcomeServerError},

		{"rate limited", func(sc *forge.StreamContext) {
			sc.Err = fmt.Errorf("submit: %w", forge.ErrRateLimited)
		}, metrics.OutcomeRateLimited},

		{"shed", func(sc *forge.StreamContext) {
			sc.Err = fmt.Errorf("acquire: %w", admission.ErrOverloaded)
		}, metrics.OutcomeOverloaded},

		{"pipeline error", func(sc *forge.StreamContext) {
			sc.Err = errors.New("frame decode failed")
		}, metrics.OutcomeError},

		{"no response type", func(sc *forge.StreamContext) {
			sc.Response = struct{ Acks int }{Acks: 3}
		}, metrics.OutcomeOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New()
			run(m, "sda", "", func(sc *forge.StreamContext) {
				sc.SetOperation("GET")
				tc.setup(sc)
			})
			assertCounted(t, m, "sda", "GET", tc.want)
		})
	}
}

// A rejection carries both a pipeline error and a 429/503 in its response. The
// error must win, or both would be filed as client_error and the distinction
// that motivates the labels would be lost.
func TestRejectionOutcomeBeatsResponseStatus(t *testing.T) {
	m := metrics.New()

	run(m, "sda", "", func(sc *forge.StreamContext) {
		sc.SetOperation("PUT")
		sc.Err = fmt.Errorf("wrapped: %w", admission.ErrOverloaded)
		sc.Response = &statusResponse{status: 503}
	})

	assertCounted(t, m, "sda", "PUT", metrics.OutcomeOverloaded)
	if _, ok := counterValue(t, m, "sda", "PUT", metrics.OutcomeServerError); ok {
		t.Error("a shed request was also counted as a server error")
	}
}

// The operation label must come from the routing table, never from the
// request. Otherwise a caller could mint an unbounded number of series and
// take down the scraper.
func TestOperationLabelIsBounded(t *testing.T) {
	m := metrics.New()

	// What an unroutable request looks like once forge's router has refused
	// it: no operation recorded, ErrUnknownOperation set.
	run(m, "sda", "", func(sc *forge.StreamContext) {
		sc.Err = forgemw.ErrUnknownOperation
	})

	assertCounted(t, m, "sda", "unrouted", metrics.OutcomeError)

	for _, label := range seriesLabels(t, m) {
		if strings.Contains(label, "DROP") || strings.Contains(label, "12D3Koo") {
			t.Errorf("caller-supplied text reached a label: %s", label)
		}
	}
}

// A protocol with a single operation and no router still needs a meaningful
// label, so the pipeline supplies one.
func TestFallbackOperationIsUsedWhenThereIsNoRouter(t *testing.T) {
	m := metrics.New()

	run(m, "msa", "submit", func(sc *forge.StreamContext) {
		sc.Response = struct{}{}
	})

	assertCounted(t, m, "msa", "submit", metrics.OutcomeOK)
}

// A panicking handler is the request an operator most needs counted. Recovery
// sits inside this middleware and converts the panic to sc.Err, but the
// recording is deferred so that even a panic escaping Recovery is counted.
func TestPanicsAreCounted(t *testing.T) {
	m := metrics.New()

	sc := &forge.StreamContext{Logger: discardLogger()}
	chain := metrics.Middleware(m, "sda", "")
	chain(sc, func() {
		forgemw.Recovery()(sc, func() {
			sc.SetOperation("PUT")
			panic("handler exploded")
		})
	})

	assertCounted(t, m, "sda", "PUT", metrics.OutcomeError)
}

func TestPanicEscapingRecoveryIsStillCounted(t *testing.T) {
	m := metrics.New()

	sc := &forge.StreamContext{}
	func() {
		defer func() { _ = recover() }()
		metrics.Middleware(m, "sda", "")(sc, func() {
			sc.SetOperation("PUT")
			panic("nothing recovered this")
		})
	}()

	// No sc.Err was ever set, so it lands under ok — but it is counted, and
	// the latency histogram will show it. A request that vanishes entirely is
	// the failure this guards against.
	if _, ok := counterValue(t, m, "sda", "PUT", metrics.OutcomeOK); !ok {
		t.Errorf("an unrecovered panic was not counted at all; series: %v", seriesLabels(t, m))
	}
}

// Metrics being off must not change behaviour. Every method is nil-safe so a
// pipeline built without them simply records nothing.
func TestNilMetricsIsInert(t *testing.T) {
	var m *metrics.Metrics

	called := false
	run(m, "sda", "", func(sc *forge.StreamContext) { called = true })

	if !called {
		t.Error("the pipeline did not run with metrics disabled")
	}
	if m.Registry() != nil {
		t.Error("nil metrics returned a registry")
	}
	if err := m.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "x"})); err != nil {
		t.Errorf("Register on nil metrics: %v", err)
	}
	m.Observe("sda", "GET", metrics.OutcomeOK, 0)
}

func TestFromRegistry(t *testing.T) {
	if got := metrics.FromRegistry(nil); got != nil {
		t.Error("FromRegistry(nil) returned non-nil")
	}

	reg := forge.NewRegistry()
	if got := metrics.FromRegistry(reg); got != nil {
		t.Error("FromRegistry returned non-nil for an empty registry")
	}

	m := metrics.New()
	reg.Provide(metrics.RegistryKey, m)
	if got := metrics.FromRegistry(reg); got != m {
		t.Errorf("FromRegistry = %v, want the provided metrics", got)
	}
}

// Durations are recorded alongside counts, under the same labels, so latency
// can be broken down by outcome — shed requests are fast and would otherwise
// flatter the average.
func TestDurationsAreRecorded(t *testing.T) {
	m := metrics.New()

	run(m, "sda", "", func(sc *forge.StreamContext) {
		sc.SetOperation("LIST")
		sc.Response = &statusResponse{status: 200}
	})

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() != "ricochet_request_duration_seconds" {
			continue
		}
		for _, metric := range f.GetMetric() {
			l := labelMap(metric)
			if l["operation"] != "LIST" {
				continue
			}
			if got := metric.GetHistogram().GetSampleCount(); got != 1 {
				t.Errorf("sample count = %d, want 1", got)
			}
			return
		}
	}
	t.Error("no duration histogram recorded for LIST")
}
