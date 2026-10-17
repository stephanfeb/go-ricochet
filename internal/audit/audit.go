// Package audit keeps the connection log: which IP each peer connected
// from, over what transport, and when it connected and disconnected.
//
// An abuse report or a child-safety referral (NCMEC) needs to tie a peer
// ID to an IP address at a time, and nothing else on the server remembers
// that once a connection closes. The Recorder watches the host's network
// and writes a row per connection to storage (peer_connections); rows older
// than audit.connection_log_days are purged hourly. Moderators read the log
// through the moderation protocol's connections operation; nobody else can.
//
// Recording must never slow a connection down: libp2p calls a notifiee
// while it holds its own locks. The callbacks only queue an event on a
// buffered channel, which one goroutine writes to storage. When the queue
// is full the event is dropped, and a warning says how many went.
package audit

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	multiaddr "github.com/multiformats/go-multiaddr"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

const (
	// queueSize is how many connection events may wait for storage.
	queueSize = 4096
	// purgeInterval is how often rows past the retention are removed.
	purgeInterval = time.Hour
	// writeTimeout bounds each storage call, so a stuck database cannot
	// hold the queue forever.
	writeTimeout = 5 * time.Second
	// stopTimeout bounds how long Stop spends writing what is queued.
	stopTimeout = 10 * time.Second
	// warnEvery is the most often a dropped-events warning is logged.
	warnEvery = time.Minute
)

// TransportCircuit is the transport recorded for a relayed connection.
const TransportCircuit = "p2p-circuit"

// transportNames are the multiaddr protocols that name a transport. The
// last one in an address is the most specific (quic-v1 over udp,
// webtransport over quic-v1).
var transportNames = map[string]bool{
	"tcp": true, "udp": true, "udx": true, "utp": true,
	"quic": true, "quic-v1": true, "webtransport": true,
	"webrtc": true, "webrtc-direct": true, "ws": true, "wss": true,
}

// Describe returns the IP a remote multiaddr names and the transport it
// uses. For a relayed address (one with /p2p-circuit) it is the relay's IP,
// and the transport is "p2p-circuit". For a DNS address the IP is the host
// name. Either is empty when the address does not say.
func Describe(addr multiaddr.Multiaddr) (ip, transport string) {
	tls := false
	for _, c := range addr {
		name := c.Protocol().Name
		switch {
		case name == TransportCircuit:
			return ip, TransportCircuit
		case ip == "" && (c.Code() == multiaddr.P_IP4 || c.Code() == multiaddr.P_IP6 ||
			c.Code() == multiaddr.P_DNS || c.Code() == multiaddr.P_DNS4 ||
			c.Code() == multiaddr.P_DNS6 || c.Code() == multiaddr.P_DNSADDR):
			ip = c.Value()
		case name == "tls":
			tls = true
		case transportNames[name]:
			transport = name
		}
	}
	if transport == "ws" && tls {
		transport = "wss"
	}
	return ip, transport
}

// direction names a connection's direction as the log records it.
func direction(d network.Direction) string {
	switch d {
	case network.DirInbound:
		return "inbound"
	case network.DirOutbound:
		return "outbound"
	default:
		return "unknown"
	}
}

// event is a connection opening (row set) or closing.
type event struct {
	connID string
	row    *storage.PeerConnection
	at     time.Time
}

// Recorder writes the connection log. A nil Recorder (the log is off)
// records nothing, and its methods do nothing.
type Recorder struct {
	store  storage.Storage
	days   int
	logger *slog.Logger
	now    func() time.Time

	net    network.Network
	events chan event
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once

	// open maps a connection's ID to its row, for the disconnect. Only the
	// writer goroutine touches it.
	open map[string]int64

	dropped  atomic.Uint64
	lastWarn atomic.Int64

	// failed counts writes storage refused (a database without the table
	// fails every one), warned about at most once a minute. Only the
	// writer goroutine touches them.
	failed       int
	lastFailWarn time.Time
}

// New returns a recorder keeping days of log in store, or nil when days is
// zero or less: the log is off.
func New(store storage.Storage, days int, logger *slog.Logger) *Recorder {
	if days <= 0 {
		return nil
	}
	return &Recorder{
		store:  store,
		days:   days,
		logger: logger.With("component", "audit"),
		now:    time.Now,
		events: make(chan event, queueSize),
		stop:   make(chan struct{}),
		open:   map[string]int64{},
	}
}

// Start watches n: every connection from now on is recorded, and so are
// the ones already open. Rows past the retention are purged now and then
// hourly.
func (r *Recorder) Start(n network.Network) {
	if r == nil {
		return
	}
	r.net = n
	n.Notify(r.notifiee())
	for _, c := range n.Conns() {
		r.connected(c)
	}
	r.wg.Add(2)
	go r.write()
	go r.purge()
	r.logger.Info("connection log on", "retention_days", r.days)
}

// Stop stops watching, writes what is queued (up to a bound), and marks
// the connections still open as closed now, since the server is going
// away. It waits for its goroutines; safe to call more than once.
func (r *Recorder) Stop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.net != nil {
			r.net.StopNotify(r.notifiee())
		}
		close(r.stop)
		r.wg.Wait()
	})
}

// notifiee is the recorder as libp2p sees it. It is the same pointer each
// time, so StopNotify finds what Notify registered.
func (r *Recorder) notifiee() network.Notifiee {
	return (*notifiee)(r)
}

// notifiee adapts the Recorder to network.Notifiee without exporting the
// callbacks on Recorder itself.
type notifiee Recorder

func (n *notifiee) Listen(network.Network, multiaddr.Multiaddr)      {}
func (n *notifiee) ListenClose(network.Network, multiaddr.Multiaddr) {}
func (n *notifiee) Connected(_ network.Network, c network.Conn) {
	(*Recorder)(n).connected(c)
}
func (n *notifiee) Disconnected(_ network.Network, c network.Conn) {
	(*Recorder)(n).disconnected(c)
}

func (r *Recorder) connected(c network.Conn) {
	remote := c.RemoteMultiaddr()
	ip, transport := Describe(remote)
	stat := c.Stat()
	at := stat.Opened
	if at.IsZero() {
		at = r.now()
	}
	addr := ""
	if remote != nil {
		addr = remote.String()
	}
	r.enqueue(event{connID: c.ID(), at: at.UTC(), row: &storage.PeerConnection{
		PeerID:      c.RemotePeer().String(),
		IP:          ip,
		RemoteAddr:  addr,
		Transport:   transport,
		Direction:   direction(stat.Direction),
		ConnectedAt: at.UTC(),
	}})
}

func (r *Recorder) disconnected(c network.Conn) {
	r.enqueue(event{connID: c.ID(), at: r.now().UTC()})
}

// enqueue never blocks: a full queue drops the event and counts it.
func (r *Recorder) enqueue(e event) {
	select {
	case r.events <- e:
	default:
		n := r.dropped.Add(1)
		now := r.now().UnixNano()
		last := r.lastWarn.Load()
		if now-last >= int64(warnEvery) && r.lastWarn.CompareAndSwap(last, now) {
			r.logger.Warn("connection log queue full; events dropped", "dropped_total", n)
		}
	}
}

// write is the one goroutine that writes the log.
func (r *Recorder) write() {
	defer r.wg.Done()
	for {
		select {
		case e := <-r.events:
			r.apply(e)
		case <-r.stop:
			r.finish()
			return
		}
	}
}

// finish writes what is still queued, then closes the rows of the
// connections still open, within stopTimeout.
func (r *Recorder) finish() {
	deadline := time.Now().Add(stopTimeout)
drain:
	for time.Now().Before(deadline) {
		select {
		case e := <-r.events:
			r.apply(e)
		default:
			break drain
		}
	}
	at := r.now().UTC()
	for id, row := range r.open {
		if time.Now().After(deadline) {
			r.logger.Warn("connection log: stopped before closing every open row", "left", len(r.open))
			return
		}
		r.markDisconnected(row, at)
		delete(r.open, id)
	}
}

func (r *Recorder) apply(e event) {
	if e.row == nil {
		row, ok := r.open[e.connID]
		if !ok {
			return // its connect was dropped or failed to write
		}
		delete(r.open, e.connID)
		r.markDisconnected(row, e.at)
		return
	}
	if _, seen := r.open[e.connID]; seen {
		return // recorded at Start and notified as well
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	id, err := r.store.InsertPeerConnection(ctx, e.row)
	if err != nil {
		r.writeFailed("connection not logged", err)
		return
	}
	r.open[e.connID] = id
}

func (r *Recorder) markDisconnected(row int64, at time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := r.store.MarkPeerDisconnected(ctx, row, at); err != nil {
		r.writeFailed("disconnect not logged", err)
	}
}

// writeFailed reports a failed write, at most once a minute.
func (r *Recorder) writeFailed(msg string, err error) {
	r.failed++
	if now := r.now(); now.Sub(r.lastFailWarn) >= warnEvery {
		r.lastFailWarn = now
		r.logger.Warn(msg+" (run schema.sql if peer_connections is missing)", "error", err, "failed_total", r.failed)
	}
}

// purge removes rows past the retention at start and then hourly.
func (r *Recorder) purge() {
	defer r.wg.Done()
	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()
	for {
		r.purgeOnce()
		select {
		case <-ticker.C:
		case <-r.stop:
			return
		}
	}
}

func (r *Recorder) purgeOnce() {
	cutoff := r.now().Add(-time.Duration(r.days) * 24 * time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	n, err := r.store.PurgePeerConnections(ctx, cutoff)
	if err != nil {
		r.logger.Warn("connection log not purged", "error", err)
		return
	}
	if n > 0 {
		r.logger.Info("connection log purged", "rows", n, "before", cutoff.UTC().Format(time.RFC3339))
	}
}
