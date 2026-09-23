package metrics_test

import (
	"context"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	forge "github.com/stephanfeb/go-p2p-forge"
	"github.com/stephanfeb/go-p2p-forge/codec"

	"github.com/stephanfeb/go-ricochet/internal/admission"
	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/metrics"
)

func testPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}
	return id
}

// gatherNames lists every metric family the registry publishes.
func gatherNames(t *testing.T, m *metrics.Metrics) []string {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}

// gaugeValue returns the single unlabelled value of a metric family.
func gaugeValue(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if g := metric.GetGauge(); g != nil {
				return g.GetValue()
			}
			if c := metric.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	t.Fatalf("no series named %s; have %v", name, gatherNames(t, m))
	return 0
}

func hasFamily(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// Admission is where an operator looks to answer "is the server keeping up".
// The numbers must track the controller rather than being kept in parallel.
func TestAdmissionCollectorTracksTheController(t *testing.T) {
	cfg := core.AdmissionControl{Enabled: true, MaxInFlight: 1, MaxInFlightPerPeer: 1}
	ctl := admission.New(cfg, 0)
	if ctl == nil {
		t.Fatal("admission control did not build")
	}

	m := metrics.New()
	if err := m.Register(metrics.NewAdmissionCollector(ctl)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := gaugeValue(t, m, "ricochet_requests_in_flight"); got != 0 {
		t.Errorf("in_flight = %v on an idle server, want 0", got)
	}

	// Hold the only slot, then shed a second request against it.
	release, err := ctl.Acquire(context.Background(), testPeer(t))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if got := gaugeValue(t, m, "ricochet_requests_in_flight"); got != 1 {
		t.Errorf("in_flight = %v while holding a slot, want 1", got)
	}
	if got := gaugeValue(t, m, "ricochet_admission_max_in_flight"); got != 1 {
		t.Errorf("max_in_flight = %v, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // an already-cancelled context still charges the per-peer bound
	_, _ = ctl.Acquire(ctx, testPeer(t))

	release()

	if got := gaugeValue(t, m, "ricochet_requests_in_flight"); got != 0 {
		t.Errorf("in_flight = %v after release, want 0", got)
	}
	if got := gaugeValue(t, m, "ricochet_admission_admitted_total"); got < 1 {
		t.Errorf("admitted_total = %v, want at least 1", got)
	}
}

// A disabled controller must publish nothing. Publishing zeroes instead would
// let an operator read "switched off" as "idle" — the same class of mistake as
// a capacity endpoint that reports storage is always free.
func TestAdmissionCollectorPublishesNothingWhenDisabled(t *testing.T) {
	m := metrics.New()
	if err := m.Register(metrics.NewAdmissionCollector(nil)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if names := gatherNames(t, m); hasFamily(names, "ricochet_requests_in_flight") {
		t.Errorf("disabled admission control still published in-flight series: %v", names)
	}
}

func TestBufferPoolCollector(t *testing.T) {
	pool := codec.NewBufferPool()
	m := metrics.New()
	if err := m.Register(metrics.NewBufferPoolCollector(pool)); err != nil {
		t.Fatalf("register: %v", err)
	}

	names := gatherNames(t, m)
	for _, want := range []string{"ricochet_buffer_pool_hits_total", "ricochet_buffer_pool_misses_total"} {
		if !hasFamily(names, want) {
			t.Errorf("missing %s; got %v", want, names)
		}
	}

	pool.Hits.Store(7)
	if got := gaugeValue(t, m, "ricochet_buffer_pool_hits_total"); got != 7 {
		t.Errorf("hits = %v, want 7 — the collector is not reading the live counter", got)
	}
}

func TestPoolCollectorWithNilPoolPublishesNothing(t *testing.T) {
	m := metrics.New()
	if err := m.Register(metrics.NewPoolCollector(nil)); err != nil {
		t.Fatalf("register: %v", err)
	}
	if names := gatherNames(t, m); hasFamily(names, "ricochet_db_pool_conns_total") {
		t.Error("a nil pool still published connection series")
	}
}

// The whole exposition must stay free of peer identity. This walks every
// series the registry publishes, not just the request families, because a
// collector added later is exactly where this would slip in.
func TestNoLabelCarriesAPeerID(t *testing.T) {
	id := testPeer(t).String()

	m := metrics.New()
	if err := m.Register(metrics.NewAdmissionCollector(admission.New(
		core.AdmissionControl{Enabled: true, MaxInFlight: 4}, 0))); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := m.Register(metrics.NewBufferPoolCollector(codec.NewBufferPool())); err != nil {
		t.Fatalf("register: %v", err)
	}

	run(m, "sda", "", func(sc *forge.StreamContext) { sc.SetOperation("GET") })

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	for _, f := range families {
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				v := l.GetValue()
				if v == id {
					t.Fatalf("%s labels %s with a peer ID", f.GetName(), l.GetName())
				}
				// Peer IDs are base58 multihashes; these are their prefixes.
				if strings.HasPrefix(v, "12D3Koo") || strings.HasPrefix(v, "Qm") {
					t.Fatalf("%s{%s=%q} looks like a peer ID", f.GetName(), l.GetName(), v)
				}
				if len(v) > 40 {
					t.Errorf("%s{%s=%q} is suspiciously long for a bounded label",
						f.GetName(), l.GetName(), v)
				}
			}
		}
	}
}
