package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// StatsSource supplies the most recent aggregate sample. Implemented by
// capacity.Sampler; an interface here so metrics does not depend on the
// sampler's lifecycle.
type StatsSource interface {
	Latest() *storage.ServerStats
	MaxStorageBytes() int64
}

// CapacityCollector publishes how full the server is.
//
// These are the numbers sumi asked for and could not get: mailbox depth, and
// how close mailboxes are to their own caps. A mailbox at its cap starts
// evicting, so `near_capacity` predicts data loss rather than merely reporting
// it afterwards.
//
// Every series carries the age of the sample it came from, via
// `ricochet_stats_age_seconds`. The figures are sampled on a timer because
// they scan; without the age published alongside, a sampler that had quietly
// stopped would look exactly like a server whose numbers had stopped changing.
type CapacityCollector struct {
	src StatsSource
	now func() time.Time

	mailboxes    *prometheus.Desc
	messages     *prometheus.Desc
	messageBytes *prometheus.Desc
	dbBytes      *prometheus.Desc
	maxBytes     *prometheus.Desc
	nearCapacity *prometheus.Desc
	depth        *prometheus.Desc
	age          *prometheus.Desc
}

// NewCapacityCollector builds a collector over the aggregate sampler.
func NewCapacityCollector(src StatsSource) *CapacityCollector {
	return &CapacityCollector{
		src: src,
		now: time.Now,
		mailboxes: prometheus.NewDesc(
			"ricochet_mailboxes",
			"Mailboxes across all owners.", nil, nil),
		messages: prometheus.NewDesc(
			"ricochet_messages_stored",
			"Messages currently stored.", nil, nil),
		messageBytes: prometheus.NewDesc(
			"ricochet_message_bytes",
			"Summed message payload length. What users stored, not what it costs on disk.", nil, nil),
		dbBytes: prometheus.NewDesc(
			"ricochet_database_bytes",
			"Database size on disk, including indexes and unreclaimed space.", nil, nil),
		maxBytes: prometheus.NewDesc(
			"ricochet_storage_max_bytes",
			"Configured storage budget.", nil, nil),
		nearCapacity: prometheus.NewDesc(
			"ricochet_mailboxes_near_capacity",
			"Mailboxes at or above their own message cap ratio; these start evicting.", nil, nil),
		depth: prometheus.NewDesc(
			"ricochet_mailbox_depth",
			"Mailboxes by message-count range.", []string{"bucket"}, nil),
		age: prometheus.NewDesc(
			"ricochet_stats_age_seconds",
			"Age of the aggregate sample these figures came from.", nil, nil),
	}
}

func (c *CapacityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.mailboxes
	ch <- c.messages
	ch <- c.messageBytes
	ch <- c.dbBytes
	ch <- c.maxBytes
	ch <- c.nearCapacity
	ch <- c.depth
	ch <- c.age
}

func (c *CapacityCollector) Collect(ch chan<- prometheus.Metric) {
	if c.src == nil {
		return
	}

	// Before the first sample completes there is nothing to say. Publishing
	// zeroes would read as an empty server, which is the fabrication this
	// whole collector exists to remove.
	stats := c.src.Latest()
	if stats == nil {
		return
	}

	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	gauge(c.mailboxes, float64(stats.Mailboxes))
	gauge(c.messages, float64(stats.Messages))
	gauge(c.messageBytes, float64(stats.MessageBytes))
	gauge(c.dbBytes, float64(stats.DatabaseBytes))
	gauge(c.maxBytes, float64(c.src.MaxStorageBytes()))
	gauge(c.nearCapacity, float64(stats.MailboxesNearCapacity))
	gauge(c.age, c.now().Sub(stats.SampledAt).Seconds())

	for _, b := range stats.Depth {
		gauge(c.depth, float64(b.Mailboxes), b.Label)
	}
}
