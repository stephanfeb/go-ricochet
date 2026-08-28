package integration_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/metrics"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// series is one gathered sample, flattened for assertions.
type series struct {
	name   string
	labels map[string]string
	value  float64
}

// scrape gathers the server's registry, flattening counters and gauges the way
// a Prometheus scrape would see them.
func scrape(t *testing.T, m *metrics.Metrics) []series {
	t.Helper()

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var out []series
	for _, f := range families {
		for _, sample := range f.GetMetric() {
			labels := make(map[string]string, len(sample.GetLabel()))
			for _, l := range sample.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			var v float64
			switch {
			case sample.GetCounter() != nil:
				v = sample.GetCounter().GetValue()
			case sample.GetGauge() != nil:
				v = sample.GetGauge().GetValue()
			case sample.GetHistogram() != nil:
				v = float64(sample.GetHistogram().GetSampleCount())
			}
			out = append(out, series{name: f.GetName(), labels: labels, value: v})
		}
	}
	return out
}

// requestCount sums ricochet_requests_total over the given label filter. An
// empty filter value matches any value for that label.
func requestCount(samples []series, protocol, operation, outcome string) float64 {
	total := 0.0
	for _, s := range samples {
		if s.name != "ricochet_requests_total" {
			continue
		}
		if protocol != "" && s.labels["protocol"] != protocol {
			continue
		}
		if operation != "" && s.labels["operation"] != operation {
			continue
		}
		if outcome != "" && s.labels["outcome"] != outcome {
			continue
		}
		total += s.value
	}
	return total
}

func describe(samples []series) string {
	var b strings.Builder
	for _, s := range samples {
		if s.name != "ricochet_requests_total" {
			continue
		}
		fmt.Fprintf(&b, "\n  %s{protocol=%s,operation=%s,outcome=%s} = %v",
			s.name, s.labels["protocol"], s.labels["operation"], s.labels["outcome"], s.value)
	}
	return b.String()
}

// The acceptance criterion for B1: real traffic through the real pipelines
// produces series, labelled by the operation that actually ran.
func TestMetricsRecordRealTraffic(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()

	if _, err := cl.PutDocument(ctx, ownerID, "metrics/doc", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := cl.GetDocument(ctx, ownerID, "metrics/doc"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := cl.GetDocument(ctx, ownerID, "metrics/doc"); err != nil {
		t.Fatalf("second get: %v", err)
	}

	samples := scrape(t, server.Metrics)

	if got := requestCount(samples, "sda", "PUT", metrics.OutcomeOK); got != 1 {
		t.Errorf("sda/PUT/ok = %v, want 1%s", got, describe(samples))
	}
	if got := requestCount(samples, "sda", "GET", metrics.OutcomeOK); got != 2 {
		t.Errorf("sda/GET/ok = %v, want 2%s", got, describe(samples))
	}

	// Latency must be recorded under the same labels, so a slow operation can
	// be told from a slow protocol.
	var sawDuration bool
	for _, s := range samples {
		if s.name == "ricochet_request_duration_seconds" && s.labels["operation"] == "PUT" {
			sawDuration = true
			if s.value != 1 {
				t.Errorf("PUT duration samples = %v, want 1", s.value)
			}
		}
	}
	if !sawDuration {
		t.Error("no duration histogram for sda/PUT")
	}
}

// A request the handler refuses must not read as a success. Before the
// response status was consulted, a server returning 404 to everything would
// have published an unbroken line of ok.
func TestRefusedRequestsAreNotCountedAsOK(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := cl.GetDocument(ctx, cl.PeerID(), "metrics/does-not-exist")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Status != 404 {
		t.Fatalf("status = %d, want 404", resp.Status)
	}

	samples := scrape(t, server.Metrics)

	if got := requestCount(samples, "sda", "GET", metrics.OutcomeClientError); got != 1 {
		t.Errorf("sda/GET/client_error = %v, want 1%s", got, describe(samples))
	}
	if got := requestCount(samples, "sda", "GET", metrics.OutcomeOK); got != 0 {
		t.Errorf("a 404 was counted as ok (%v)%s", got, describe(samples))
	}
}

// Sumi's two numbers. A rate-limited request and a shed request must land in
// different series: one says raise the client's budget, the other says add
// capacity. An operator who cannot tell them apart changes the wrong thing.
func TestRateLimitedAndOverloadedAreSeparateSeries(t *testing.T) {
	t.Run("rate limited", func(t *testing.T) {
		server := newTestServer(t, func(cfg *core.ServerConfig) {
			cfg.RateLimits.Window = time.Minute
			cfg.RateLimits.Protocols[core.RateLimitSDA] = core.ProtocolLimits{
				Read:  core.Limit{Rate: 1, Burst: 1},
				Write: core.Limit{Rate: 1, Burst: 1},
			}
		})
		cl := newTestClient(t, server)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		ownerID := cl.PeerID()
		if _, err := cl.PutDocument(ctx, ownerID, "metrics/limited", []byte("one")); err != nil {
			t.Fatalf("first write should be allowed: %v", err)
		}
		if _, err := cl.PutDocument(ctx, ownerID, "metrics/limited-2", []byte("two")); err == nil {
			t.Fatal("second write should have been rate limited")
		}

		samples := scrape(t, server.Metrics)

		if got := requestCount(samples, "sda", "", metrics.OutcomeRateLimited); got != 1 {
			t.Errorf("rate_limited = %v, want 1%s", got, describe(samples))
		}
		if got := requestCount(samples, "sda", "", metrics.OutcomeOverloaded); got != 0 {
			t.Errorf("a rate-limited request was counted as overloaded (%v)%s", got, describe(samples))
		}
	})

	t.Run("overloaded", func(t *testing.T) {
		server := newTestServer(t, func(cfg *core.ServerConfig) {
			cfg.Admission.MaxInFlight = 1
			cfg.Admission.MaxInFlightPerPeer = 0
			cfg.Admission.AcquireTimeout = 1 * time.Millisecond
		})
		cl := newTestClient(t, server)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		ownerID := cl.PeerID()

		// Batches are the longest-running request we have, so the caller
		// holding the single slot keeps it long enough for the others to
		// exhaust the 1ms acquire timeout.
		const callers = 16
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				docs := make([]client.BatchDocumentPut, 0, 100)
				for j := 0; j < 100; j++ {
					docs = append(docs, client.BatchDocumentPut{
						Path:    fmt.Sprintf("metrics/shed-%02d-%03d", n, j),
						Content: []byte(strings.Repeat("x", 4096)),
					})
				}
				_, _ = cl.PutDocuments(ctx, ownerID, docs)
			}(i)
		}
		wg.Wait()

		samples := scrape(t, server.Metrics)

		shed := requestCount(samples, "sda", "", metrics.OutcomeOverloaded)
		if shed == 0 {
			t.Fatalf("nothing was recorded as overloaded%s", describe(samples))
		}
		if got := requestCount(samples, "sda", "", metrics.OutcomeRateLimited); got != 0 {
			t.Errorf("a shed request was counted as rate limited (%v)%s", got, describe(samples))
		}

		// The admission counter must agree with the request series; they are
		// two independent views of the same event.
		var counted float64
		for _, s := range samples {
			if s.name == "ricochet_admission_shed_total" {
				counted = s.value
			}
		}
		if counted != shed {
			t.Errorf("admission_shed_total = %v but %v requests were labelled overloaded", counted, shed)
		}
	})
}

// Cardinality, checked against a live server rather than a synthetic context:
// nothing a client sends may become a label value.
func TestScrapeCarriesNoPeerIdentity(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ownerID := cl.PeerID()
	if _, err := cl.PutDocument(ctx, ownerID, "metrics/cardinality", []byte("x")); err != nil {
		t.Fatalf("put: %v", err)
	}

	peerID := fmt.Sprint(cl.PeerID())
	for _, s := range scrape(t, server.Metrics) {
		for name, value := range s.labels {
			if value == peerID || strings.HasPrefix(value, "12D3Koo") {
				t.Fatalf("%s{%s=%q} carries peer identity", s.name, name, value)
			}
		}
	}
}

// The connection pool is the resource admission control protects, so its
// saturation has to be visible next to the request series.
func TestPoolAndAdmissionSeriesArePublished(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := cl.PutDocument(ctx, cl.PeerID(), "metrics/pool", []byte("x"),
		client.WithContentType("text/plain")); err != nil {
		t.Fatalf("put: %v", err)
	}

	names := map[string]bool{}
	for _, s := range scrape(t, server.Metrics) {
		names[s.name] = true
	}

	for _, want := range []string{
		"ricochet_requests_total",
		"ricochet_request_duration_seconds",
		"ricochet_requests_in_flight",
		"ricochet_admission_shed_total",
		"ricochet_db_pool_conns_max",
		"ricochet_db_pool_empty_acquire_total",
		"ricochet_buffer_pool_hits_total",
		"go_goroutines",
	} {
		if !names[want] {
			t.Errorf("missing metric family %s", want)
		}
	}
}
