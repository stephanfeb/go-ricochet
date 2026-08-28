package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/twostack/go-p2p-forge/codec"

	"github.com/twostack/go-ricochet/internal/admission"
)

// The collectors below read live counters rather than keeping their own. That
// is deliberate: the admission controller, the connection pool and the buffer
// pool already maintain these numbers for their own use, so mirroring them
// into separate metric objects would add bookkeeping on the hot path and
// invite the two copies to disagree. Prometheus's Func collectors sample on
// scrape instead, which costs nothing between scrapes.

// AdmissionCollector publishes admission control's view of load.
//
// These are the numbers that say whether the server is keeping up.
// `in_flight` against `max_in_flight` is how close to the bound it is running,
// and `shed_total` climbing is the signal to add capacity — a database
// connection, a replica, an instance — rather than to loosen a limit.
type AdmissionCollector struct {
	ctl *admission.Controller

	inFlight    *prometheus.Desc
	maxInFlight *prometheus.Desc
	admitted    *prometheus.Desc
	shed        *prometheus.Desc
	waited      *prometheus.Desc
	peers       *prometheus.Desc
}

// NewAdmissionCollector builds a collector over the admission controller. A
// nil controller means admission control is disabled; the collector then
// publishes nothing rather than publishing zeroes, so an operator cannot
// mistake "switched off" for "idle".
func NewAdmissionCollector(ctl *admission.Controller) *AdmissionCollector {
	return &AdmissionCollector{
		ctl: ctl,
		inFlight: prometheus.NewDesc(
			"ricochet_requests_in_flight",
			"Requests currently holding an admission slot.", nil, nil),
		maxInFlight: prometheus.NewDesc(
			"ricochet_admission_max_in_flight",
			"Server-wide concurrent request bound.", nil, nil),
		admitted: prometheus.NewDesc(
			"ricochet_admission_admitted_total",
			"Requests admitted since start.", nil, nil),
		shed: prometheus.NewDesc(
			"ricochet_admission_shed_total",
			"Requests shed because the server was at capacity.", nil, nil),
		waited: prometheus.NewDesc(
			"ricochet_admission_waited_total",
			"Requests that waited for a slot rather than being admitted immediately.", nil, nil),
		peers: prometheus.NewDesc(
			"ricochet_admission_tracked_peers",
			"Peers currently holding or awaiting a slot.", nil, nil),
	}
}

func (c *AdmissionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inFlight
	ch <- c.maxInFlight
	ch <- c.admitted
	ch <- c.shed
	ch <- c.waited
	ch <- c.peers
}

func (c *AdmissionCollector) Collect(ch chan<- prometheus.Metric) {
	if c.ctl == nil {
		return
	}

	stats := c.ctl.Stats()
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}

	gauge(c.inFlight, float64(c.ctl.InFlight()))
	gauge(c.peers, float64(c.ctl.TrackedPeers()))
	gauge(c.maxInFlight, statNumber(stats["maxInFlight"]))
	counter(c.admitted, statNumber(stats["admittedTotal"]))
	counter(c.shed, float64(c.ctl.Shed()))
	counter(c.waited, statNumber(stats["waitedTotal"]))
}

// statNumber converts one of Stats()'s untyped numbers to a float. Stats is a
// map[string]any built for JSON, so the concrete types vary.
func statNumber(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

// PoolCollector publishes PostgreSQL connection pool saturation.
//
// `empty_acquire_total` is the one to watch: it counts requests that found no
// free connection and had to wait, which is the database becoming the
// bottleneck. It is also the input an adaptive admission bound would use to
// decide whether it can widen.
type PoolCollector struct {
	pool *pgxpool.Pool

	total       *prometheus.Desc
	idle        *prometheus.Desc
	acquired    *prometheus.Desc
	max         *prometheus.Desc
	emptyAcq    *prometheus.Desc
	canceledAcq *prometheus.Desc
	acqDuration *prometheus.Desc
}

// NewPoolCollector builds a collector over a pgx connection pool.
func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	return &PoolCollector{
		pool: pool,
		total: prometheus.NewDesc(
			"ricochet_db_pool_conns_total",
			"Connections currently in the pool, idle and in use.", nil, nil),
		idle: prometheus.NewDesc(
			"ricochet_db_pool_conns_idle",
			"Connections sitting idle.", nil, nil),
		acquired: prometheus.NewDesc(
			"ricochet_db_pool_conns_acquired",
			"Connections currently checked out.", nil, nil),
		max: prometheus.NewDesc(
			"ricochet_db_pool_conns_max",
			"Configured pool size.", nil, nil),
		emptyAcq: prometheus.NewDesc(
			"ricochet_db_pool_empty_acquire_total",
			"Acquires that found no free connection and had to wait.", nil, nil),
		canceledAcq: prometheus.NewDesc(
			"ricochet_db_pool_canceled_acquire_total",
			"Acquires abandoned before a connection became free.", nil, nil),
		acqDuration: prometheus.NewDesc(
			"ricochet_db_pool_acquire_seconds_total",
			"Cumulative time spent waiting to acquire a connection.", nil, nil),
	}
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.total
	ch <- c.idle
	ch <- c.acquired
	ch <- c.max
	ch <- c.emptyAcq
	ch <- c.canceledAcq
	ch <- c.acqDuration
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}

	stat := c.pool.Stat()
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}

	gauge(c.total, float64(stat.TotalConns()))
	gauge(c.idle, float64(stat.IdleConns()))
	gauge(c.acquired, float64(stat.AcquiredConns()))
	gauge(c.max, float64(stat.MaxConns()))
	counter(c.emptyAcq, float64(stat.EmptyAcquireCount()))
	counter(c.canceledAcq, float64(stat.CanceledAcquireCount()))
	counter(c.acqDuration, stat.AcquireDuration().Seconds())
}

// BufferPoolCollector publishes forge's frame buffer pool hit rate. A falling
// hit rate means frames are outgrowing their tier and every request is
// allocating, which shows up as GC pressure rather than as slow queries.
type BufferPoolCollector struct {
	pool *codec.BufferPool

	hits   *prometheus.Desc
	misses *prometheus.Desc
}

// NewBufferPoolCollector builds a collector over forge's buffer pool.
func NewBufferPoolCollector(pool *codec.BufferPool) *BufferPoolCollector {
	return &BufferPoolCollector{
		pool: pool,
		hits: prometheus.NewDesc(
			"ricochet_buffer_pool_hits_total",
			"Frame buffers served from the pool.", nil, nil),
		misses: prometheus.NewDesc(
			"ricochet_buffer_pool_misses_total",
			"Frame buffers that had to be allocated.", nil, nil),
	}
}

func (c *BufferPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
}

func (c *BufferPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(c.pool.Hits.Load()))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(c.pool.Misses.Load()))
}
