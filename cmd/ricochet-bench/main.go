package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/multiformats/go-multiaddr"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	client "github.com/stephanfeb/go-ricochet/pkg/client"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// benchConfig holds CLI-parsed configuration.
type benchConfig struct {
	ServerAddr   multiaddr.Multiaddr
	ServerPeerID peer.ID
	TotalReqs    int
	Concurrency  int
	Protocol     string
	PayloadSize  int
	Duration     time.Duration
	WarmupCount  int
	Verbose      bool

	// BatchSize is how many documents or messages one batched request carries.
	BatchSize int

	// DocCount is the vault size a sync scenario covers.
	DocCount int
}

// requestResult captures the outcome of a single benchmarked operation.
type requestResult struct {
	Latency time.Duration
	Err     error
}

// benchResults holds aggregated benchmark output.
type benchResults struct {
	Protocol string

	// Unit names what one request accomplishes, and UnitsPerRequest says how
	// many. Requests per second stops being the interesting number once one
	// request carries a hundred documents -- quoting it would report batching
	// as a slowdown.
	Unit            string
	UnitsPerRequest int

	Completed int
	Failed    int
	TotalTime time.Duration
	Latencies []time.Duration
	Errors    map[string]int
}

// benchFunc is the signature for a single benchmark operation.
type benchFunc func(ctx context.Context, c *client.Client, workerID, reqID int) error

// workerState holds per-worker resources created during warmup.
type workerState struct {
	client      *client.Client
	host        host.Host
	recipientID peer.ID // for MSA benchmarks
}

var validProtocols = map[string]string{
	"msa":   "MSA (Message Submission)",
	"maa":   "MAA (Message Retrieval)",
	"sda":   "SDA (Document Store)",
	"sfa":   "SFA (Feed Store)",
	"sca":   "SCA (Collection Store)",
	"mma":   "MMA (Mailbox Management)",
	"mixed": "Mixed (All Protocols)",

	"sda-batch": "SDA BATCH_PUT (batched document writes)",
	"msa-batch": "MSA batch submit (batched message submission)",

	"sync-cold": "Sync: first upload of a vault (all creates)",
	"sync-warm": "Sync: re-upload after edits (all replaces)",
	"sync-noop": "Sync: nothing changed (list and compare)",
}

// scenarioUnit names the work one request of each scenario carries. Absent
// means one request is one unit and the request count is the whole story.
var scenarioUnit = map[string]string{
	"sda-batch": "documents",
	"msa-batch": "messages",
	"sync-cold": "documents",
	"sync-warm": "documents",
	"sync-noop": "documents",
}

// isSyncScenario reports whether one request of this scenario is a whole sync
// pass rather than a single operation.
func isSyncScenario(protocol string) bool {
	switch protocol {
	case "sync-cold", "sync-warm", "sync-noop":
		return true
	}
	return false
}

// unitsPerRequest reports how many items of work one request of this scenario
// accomplishes.
func unitsPerRequest(cfg *benchConfig) int {
	switch cfg.Protocol {
	case "sda-batch", "msa-batch":
		return cfg.BatchSize
	case "sync-cold", "sync-warm", "sync-noop":
		return cfg.DocCount
	default:
		return 1
	}
}

func main() {
	n := flag.Int("n", 1000, "Total number of requests")
	c := flag.Int("c", 10, "Number of concurrent workers")
	proto := flag.String("protocol", "msa", "Protocol to benchmark: msa, maa, sda, sfa, sca, mma, mixed")
	payloadSize := flag.Int("payload-size", 1024, "Payload size in bytes")
	duration := flag.Duration("duration", 0, "Run for duration instead of fixed count (e.g. 30s, 1m)")
	warmup := flag.Int("warmup", 10, "Warmup requests before measuring")
	batchSize := flag.Int("batch-size", 100, "Documents or messages per request, for the -batch scenarios")
	docCount := flag.Int("docs", 500, "Vault size for the sync scenarios")
	verbose := flag.Bool("v", false, "Verbose output (show libp2p/UDX diagnostic logs)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: ricochet-bench [flags] <server-multiaddr> <server-peer-id>\n\n")
		fmt.Fprintf(os.Stderr, "Stress test tool for Ricochet servers (like Apache Bench for libp2p).\n\n")
		fmt.Fprintf(os.Stderr, "Example:\n")
		fmt.Fprintf(os.Stderr, "  ricochet-bench -n 5000 -c 20 -protocol sda /ip4/127.0.0.1/udp/55223/udx 12D3KooW...\n\n")
		fmt.Fprintf(os.Stderr, "Per-operation:  msa, maa, sda, sfa, sca, mma, mixed\n")
		fmt.Fprintf(os.Stderr, "Batched:        sda-batch, msa-batch  (see -batch-size)\n")
		fmt.Fprintf(os.Stderr, "Sync shapes:    sync-cold, sync-warm, sync-noop  (see -docs)\n\n")
		fmt.Fprintf(os.Stderr, "  sync-cold  first upload of a vault, every document new\n")
		fmt.Fprintf(os.Stderr, "  sync-warm  re-upload after edits, every document replaced\n")
		fmt.Fprintf(os.Stderr, "  sync-noop  nothing changed: list and compare ETags, write nothing\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// Suppress UDX/libp2p diagnostic logging unless verbose mode is on.
	if !*verbose {
		log.SetOutput(io.Discard)
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}

	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}

	if _, ok := validProtocols[*proto]; !ok {
		fmt.Fprintf(os.Stderr, "Error: unknown scenario %q. Run with -h for the list.\n", *proto)
		os.Exit(1)
	}

	serverMA, err := multiaddr.NewMultiaddr(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid server multiaddr: %v\n", err)
		os.Exit(1)
	}

	serverPeerID, err := peer.Decode(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid server peer ID: %v\n", err)
		os.Exit(1)
	}

	// A sync pass is a whole vault, not one request, so the per-operation
	// defaults are wrong for it by three orders of magnitude: -n 1000 against
	// -docs 500 would write half a million documents before reporting
	// anything. Adjust only what the caller did not ask for.
	if isSyncScenario(*proto) {
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if !set["n"] {
			*n = 10
		}
		if !set["warmup"] {
			*warmup = 1
		}
		if !set["c"] {
			*c = 1
		}
	}

	if *c > *n && *duration == 0 {
		*c = *n
	}

	cfg := &benchConfig{
		ServerAddr:   serverMA,
		ServerPeerID: serverPeerID,
		TotalReqs:    *n,
		Concurrency:  *c,
		Protocol:     *proto,
		PayloadSize:  *payloadSize,
		Duration:     *duration,
		WarmupCount:  *warmup,
		Verbose:      *verbose,
		BatchSize:    *batchSize,
		DocCount:     *docCount,
	}

	if cfg.BatchSize < 1 {
		fmt.Fprintln(os.Stderr, "Error: -batch-size must be at least 1")
		os.Exit(1)
	}
	if cfg.DocCount < 1 {
		fmt.Fprintln(os.Stderr, "Error: -docs must be at least 1")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Second Ctrl+C force-exits immediately.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		<-sigCh // first one is consumed by NotifyContext
		<-sigCh // second one = force exit
		fmt.Fprintf(os.Stderr, "\nForce exit.\n")
		os.Exit(1)
	}()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *benchConfig) error {
	fmt.Println("========================================")
	fmt.Printf("  Ricochet Bench - %s\n", validProtocols[cfg.Protocol])
	fmt.Println("========================================")
	fmt.Printf("Server: %s/p2p/%s\n\n", cfg.ServerAddr, shortPeerID(cfg.ServerPeerID))

	// Create workers (each with own libp2p host and client).
	fmt.Printf("[1/4] Creating %d workers...", cfg.Concurrency)
	workers := make([]*workerState, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		ws, err := createWorker(cfg)
		if err != nil {
			// Clean up already-created workers.
			closeWorkers(workers[:i])
			return fmt.Errorf("create worker %d: %w", i, err)
		}
		workers[i] = ws
	}
	fmt.Println(" done")

	// Verify connectivity with the first worker.
	fmt.Printf("[2/4] Connecting to server...")
	testCtx, testCancel := context.WithTimeout(ctx, 15*time.Second)
	err := verifyConnection(testCtx, workers[0])
	testCancel()
	if err != nil {
		closeWorkers(workers)
		return fmt.Errorf("server unreachable: %w", err)
	}
	fmt.Println(" connected")

	// Build the bench function and run warmup.
	payload := make([]byte, cfg.PayloadSize)
	for i := range payload {
		payload[i] = byte(rand.IntN(256))
	}

	benchFn, err := setupBench(ctx, cfg, workers, payload)
	if err != nil {
		closeWorkers(workers)
		return fmt.Errorf("setup benchmark: %w", err)
	}

	// Warmup phase.
	if cfg.WarmupCount > 0 {
		fmt.Printf("[3/4] Warming up (%d requests)...", cfg.WarmupCount)
		warmupFailed := 0
		var lastWarmupErr error
		for i := 0; i < cfg.WarmupCount; i++ {
			wIdx := i % cfg.Concurrency
			if err := benchFn(ctx, workers[wIdx].client, wIdx, -(i + 1)); err != nil {
				warmupFailed++
				lastWarmupErr = err
			}
		}
		if warmupFailed == cfg.WarmupCount {
			// Every warmup request failed, so whatever follows would be a
			// table of zeros dressed up as a measurement. This tool exists to
			// produce numbers somebody will commit and regress against; a
			// baseline taken from a run that never worked is worse than no
			// baseline. Fail here, with the reason.
			closeWorkers(workers)
			return fmt.Errorf("all %d warmup requests failed; last error: %v",
				cfg.WarmupCount, lastWarmupErr)
		}
		if warmupFailed > 0 {
			fmt.Printf(" done (%d/%d failed)\n", warmupFailed, cfg.WarmupCount)
		} else {
			fmt.Println(" done")
		}
	} else {
		fmt.Println("[3/4] Skipping warmup")
	}

	// Run benchmark.
	if cfg.Duration > 0 {
		fmt.Printf("[4/4] Benchmarking for %s with %d workers...\n", cfg.Duration, cfg.Concurrency)
	} else {
		fmt.Printf("[4/4] Benchmarking %d requests with %d workers...\n", cfg.TotalReqs, cfg.Concurrency)
	}

	results := runBench(ctx, cfg, workers, benchFn)

	// Print results BEFORE closing workers to avoid UDX noise mixing in.
	fmt.Println()
	printResults(cfg, results)

	failed := results.Completed == 0 && results.Failed > 0

	// Close workers with a timeout — host.Close() can hang due to UDX
	// readLoop blocking on ReadFrom(). If it doesn't finish quickly,
	// just exit since we already have our results.
	closeDone := make(chan struct{})
	go func() {
		closeWorkers(workers)
		close(closeDone)
	}()
	select {
	case <-closeDone:
		// Clean shutdown.
	case <-time.After(3 * time.Second):
		// UDX close is hanging — force exit since results are already printed.
	}

	if failed {
		return fmt.Errorf("every request failed; the numbers above measure nothing")
	}
	return nil
}

// closeWorkers shuts down all worker hosts, ignoring errors.
func closeWorkers(workers []*workerState) {
	for _, ws := range workers {
		if ws != nil {
			ws.host.Close()
		}
	}
}

func createWorker(cfg *benchConfig) (*workerState, error) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.DisableRelay(),
	)
	if err != nil {
		return nil, fmt.Errorf("create host: %w", err)
	}

	h.Peerstore().AddAddrs(cfg.ServerPeerID, []multiaddr.Multiaddr{cfg.ServerAddr}, time.Hour)

	cl := client.New(h, client.Config{
		PreferredServers: []client.ServerPreference{
			{PeerID: cfg.ServerPeerID, Priority: 1, Weight: 1},
		},
		ConnectionTimeout: 15 * time.Second,
		MessageTimeout:    30 * time.Second,
	})

	// Generate a random recipient peer ID for MSA benchmarks.
	recipientPriv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("generate recipient key: %w", err)
	}
	recipientID, err := peer.IDFromPrivateKey(recipientPriv)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("recipient peer id: %w", err)
	}

	return &workerState{
		client:      cl,
		host:        h,
		recipientID: recipientID,
	}, nil
}

// verifyConnection tries to send a small test message to confirm the server is reachable.
func verifyConnection(ctx context.Context, ws *workerState) error {
	// Send a tiny message to a random peer to verify the MSA path works.
	// This is more reliable than QueryCapacity which may not be registered.
	testPayload := []byte("bench-ping")
	result, err := ws.client.SendMessage(ctx, ws.recipientID, testPayload)
	if err != nil {
		return fmt.Errorf("test send failed: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("test send rejected: %s", result.ErrorMessage)
	}
	return nil
}

// setupBench creates per-protocol resources and returns the benchmark function.
func setupBench(ctx context.Context, cfg *benchConfig, workers []*workerState, payload []byte) (benchFunc, error) {
	switch cfg.Protocol {
	case "msa":
		return benchMSA(workers, payload), nil

	case "maa":
		// Seed messages into each worker's own mailbox so there's data to retrieve.
		fmt.Printf("     Seeding messages for MAA benchmark...")
		for i, ws := range workers {
			for j := 0; j < 100; j++ {
				_, err := ws.client.SendMessage(ctx, ws.host.ID(), payload)
				if err != nil {
					return nil, fmt.Errorf("seed message for worker %d: %w", i, err)
				}
			}
		}
		fmt.Println(" done")
		return benchMAA(), nil

	case "sda":
		return benchSDA(payload), nil

	case "sfa":
		// Create a feed per worker.
		fmt.Printf("     Creating feeds for SFA benchmark...")
		for i, ws := range workers {
			feedPath := fmt.Sprintf("bench/feed-w%d", i)
			if err := ws.client.CreateFeed(ctx, feedPath, "Bench Feed", "stress test"); err != nil {
				return nil, fmt.Errorf("create feed for worker %d: %w", i, err)
			}
		}
		fmt.Println(" done")
		return benchSFA(payload), nil

	case "sca":
		// Create a collection per worker.
		fmt.Printf("     Creating collections for SCA benchmark...")
		for i, ws := range workers {
			collPath := fmt.Sprintf("bench/coll-w%d", i)
			if err := ws.client.CreateCollection(ctx, collPath, "Bench Collection"); err != nil {
				return nil, fmt.Errorf("create collection for worker %d: %w", i, err)
			}
		}
		fmt.Println(" done")
		return benchSCA(payload), nil

	case "mma":
		return benchMMA(), nil

	case "sda-batch":
		return benchSDABatch(cfg, payload), nil

	case "msa-batch":
		return benchMSABatch(workers, cfg, payload), nil

	case "sync-cold":
		// Nothing to seed: every pass writes documents that do not exist yet.
		return benchSyncCold(cfg, payload), nil

	case "sync-warm":
		if err := setupSync(ctx, cfg, workers, payload); err != nil {
			return nil, err
		}
		return benchSyncWarm(cfg, payload), nil

	case "sync-noop":
		if err := setupSync(ctx, cfg, workers, payload); err != nil {
			return nil, err
		}
		return benchSyncNoop(cfg), nil

	case "mixed":
		// Setup for all protocols.
		fmt.Printf("     Setting up mixed benchmark resources...")
		for i, ws := range workers {
			for j := 0; j < 50; j++ {
				if _, err := ws.client.SendMessage(ctx, ws.host.ID(), payload); err != nil {
					return nil, fmt.Errorf("seed message for worker %d: %w", i, err)
				}
			}
			feedPath := fmt.Sprintf("bench/feed-w%d", i)
			if err := ws.client.CreateFeed(ctx, feedPath, "Bench Feed", "stress test"); err != nil {
				return nil, fmt.Errorf("create feed for worker %d: %w", i, err)
			}
			collPath := fmt.Sprintf("bench/coll-w%d", i)
			if err := ws.client.CreateCollection(ctx, collPath, "Bench Collection"); err != nil {
				return nil, fmt.Errorf("create collection for worker %d: %w", i, err)
			}
		}
		fmt.Println(" done")
		return benchMixed(workers, payload), nil

	default:
		return nil, fmt.Errorf("unknown protocol: %s", cfg.Protocol)
	}
}

// --- Protocol benchmark functions ---

func benchMSA(workers []*workerState, payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		recipient := workers[workerID].recipientID
		result, err := c.SendMessage(ctx, recipient, payload)
		if err != nil {
			return err
		}
		if !result.Success {
			return fmt.Errorf("send failed: %s", result.ErrorMessage)
		}
		return nil
	}
}

func benchMAA() benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		_, err := c.RetrieveMessages(ctx, client.WithMaxMessages(10))
		return err
	}
}

func benchSDA(payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		path := fmt.Sprintf("bench/w%d/doc-%d", workerID, reqID)
		ownerID := c.PeerID()
		if _, err := c.PutDocument(ctx, ownerID, path, payload); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		if _, err := c.GetDocument(ctx, ownerID, path); err != nil {
			return fmt.Errorf("get: %w", err)
		}
		return nil
	}
}

func benchSFA(payload []byte) benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		feedPath := fmt.Sprintf("bench/feed-w%d", workerID)
		seq, err := c.AppendFeedEntry(ctx, feedPath, payload, "bench")
		if err != nil {
			return fmt.Errorf("append: %w", err)
		}
		_, err = c.GetFeedEntry(ctx, c.PeerID(), feedPath, seq)
		if err != nil {
			return fmt.Errorf("get entry: %w", err)
		}
		return nil
	}
}

func benchSCA(payload []byte) benchFunc {
	// Hex-encode rather than casting the random bytes to a string. Raw binary
	// in a JSON string field marshals any NUL byte as \u0000, which Postgres
	// rejects for jsonb -- so the benchmark measured an error path instead of
	// the collection write it was meant to time.
	jsonPayload, _ := json.Marshal(map[string]string{"data": hex.EncodeToString(payload)})

	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		collPath := fmt.Sprintf("bench/coll-w%d", workerID)
		key := fmt.Sprintf("item-%d", reqID)
		if _, err := c.PutCollectionItem(ctx, collPath, key, jsonPayload); err != nil {
			return fmt.Errorf("put item: %w", err)
		}
		_, err := c.QueryCollection(ctx, c.PeerID(), collPath, map[string]any{})
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		return nil
	}
}

// benchMMA times the mailbox admin path: one request creates a mailbox and
// deletes it again. Both halves are MMA writes that also invalidate the
// server's mailbox cache, and the pair keeps the owner's mailbox count flat,
// which matters because an owner may hold only max_mailboxes folders.
func benchMMA() benchFunc {
	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		folder := fmt.Sprintf("bench/mbox-w%d-%d", workerID, reqID)
		if err := c.CreateMailbox(ctx, folder, wire.MailboxPrivate); err != nil {
			return fmt.Errorf("create mailbox: %w", err)
		}
		if err := c.DeleteMailbox(ctx, folder); err != nil {
			return fmt.Errorf("delete mailbox: %w", err)
		}
		return nil
	}
}

// perOperationScenarios names every scenario in which one request is one
// operation. "mixed" draws from all of them, and the test for that reads
// this list, so a per-operation scenario cannot be added without joining the
// mix or being deliberately left out here.
var perOperationScenarios = []string{"msa", "maa", "sda", "sfa", "sca", "mma"}

// mixedFuncs builds the per-operation functions "mixed" draws from.
func mixedFuncs(workers []*workerState, payload []byte) map[string]benchFunc {
	return map[string]benchFunc{
		"msa": benchMSA(workers, payload),
		"maa": benchMAA(),
		"sda": benchSDA(payload),
		"sfa": benchSFA(payload),
		"sca": benchSCA(payload),
		"mma": benchMMA(),
	}
}

// benchMixed draws each request uniformly from the per-operation scenarios.
// The setup for "mixed" creates a collection per worker, and for a long time
// this drew from four: the collections were created and never touched, and a
// "mixed" run said nothing about the collection store.
func benchMixed(workers []*workerState, payload []byte) benchFunc {
	byName := mixedFuncs(workers, payload)
	fns := make([]benchFunc, 0, len(perOperationScenarios))
	for _, name := range perOperationScenarios {
		fns = append(fns, byName[name])
	}

	return func(ctx context.Context, c *client.Client, workerID, reqID int) error {
		return fns[rand.IntN(len(fns))](ctx, c, workerID, reqID)
	}
}

// --- Benchmark runner ---

func runBench(ctx context.Context, cfg *benchConfig, workers []*workerState, fn benchFunc) *benchResults {
	var wg sync.WaitGroup
	workerResults := make([][]requestResult, cfg.Concurrency)

	var counter atomic.Int64
	counter.Store(int64(cfg.TotalReqs))

	// Progress tracking.
	var completed atomic.Int64
	var failed atomic.Int64

	var benchCtx context.Context
	var benchCancel context.CancelFunc
	if cfg.Duration > 0 {
		benchCtx, benchCancel = context.WithTimeout(ctx, cfg.Duration)
	} else {
		benchCtx, benchCancel = context.WithCancel(ctx)
	}
	defer benchCancel()

	// Progress reporter goroutine.
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-benchCtx.Done():
				return
			case <-ticker.C:
				c := completed.Load()
				f := failed.Load()
				total := c + f
				if cfg.Duration == 0 {
					fmt.Fprintf(os.Stderr, "\r     Progress: %d/%d requests (%d ok, %d failed)",
						total, cfg.TotalReqs, c, f)
				} else {
					fmt.Fprintf(os.Stderr, "\r     Progress: %d requests (%d ok, %d failed)",
						total, c, f)
				}
			}
		}
	}()

	start := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var results []requestResult

			for {
				select {
				case <-benchCtx.Done():
					workerResults[workerID] = results
					return
				default:
				}

				if cfg.Duration == 0 {
					remaining := counter.Add(-1)
					if remaining < 0 {
						workerResults[workerID] = results
						return
					}
				}

				reqCtx, reqCancel := context.WithTimeout(benchCtx, 10*time.Second)
				reqStart := time.Now()
				err := fn(reqCtx, workers[workerID].client, workerID, len(results))
				elapsed := time.Since(reqStart)
				reqCancel()

				results = append(results, requestResult{Latency: elapsed, Err: err})

				if err != nil {
					failed.Add(1)
				} else {
					completed.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(start)
	benchCancel() // stop progress reporter
	<-progressDone
	fmt.Fprintf(os.Stderr, "\r%80s\r", "") // clear progress line

	// Aggregate results.
	res := &benchResults{
		Protocol:        cfg.Protocol,
		TotalTime:       totalTime,
		Errors:          make(map[string]int),
		Unit:            scenarioUnit[cfg.Protocol],
		UnitsPerRequest: unitsPerRequest(cfg),
	}

	for _, wr := range workerResults {
		for _, r := range wr {
			if r.Err != nil {
				res.Failed++
				errMsg := r.Err.Error()
				if len(errMsg) > 100 {
					errMsg = errMsg[:100] + "..."
				}
				res.Errors[errMsg]++
			} else {
				res.Completed++
				res.Latencies = append(res.Latencies, r.Latency)
			}
		}
	}

	sort.Slice(res.Latencies, func(i, j int) bool {
		return res.Latencies[i] < res.Latencies[j]
	})

	return res
}

// --- Output ---

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fus", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func shortPeerID(id peer.ID) string {
	s := id.String()
	if len(s) > 16 {
		return s[:8] + "..." + s[len(s)-8:]
	}
	return s
}

func printResults(cfg *benchConfig, res *benchResults) {
	total := res.Completed + res.Failed

	fmt.Println("========================================")
	fmt.Println("  RESULTS")
	fmt.Println("========================================")
	fmt.Printf("Concurrency Level:      %d\n", cfg.Concurrency)
	fmt.Printf("Total Requests:         %d\n", total)
	fmt.Printf("Payload Size:           %d bytes\n", cfg.PayloadSize)
	fmt.Println()
	fmt.Printf("  Completed:            %d\n", res.Completed)
	fmt.Printf("  Failed:               %d\n", res.Failed)
	fmt.Printf("  Total time:           %.3fs\n", res.TotalTime.Seconds())

	if res.TotalTime > 0 && res.Completed > 0 {
		rps := float64(res.Completed) / res.TotalTime.Seconds()
		fmt.Printf("  Requests/sec:         %.2f\n", rps)

		// When one request carries many documents, requests per second
		// understates throughput by exactly the batch factor. Report what the
		// client actually cares about alongside it.
		if res.UnitsPerRequest > 1 && res.Unit != "" {
			units := res.Completed * res.UnitsPerRequest
			fmt.Printf("  %-21s %.2f\n", res.Unit+"/sec:",
				float64(units)/res.TotalTime.Seconds())
			fmt.Printf("  %-21s %d (%d per request)\n", "total "+res.Unit+":",
				units, res.UnitsPerRequest)
		}
	}

	if len(res.Latencies) > 0 {
		fmt.Println()
		fmt.Println("Latency Distribution:")
		fmt.Printf("  min:    %-12s\n", formatDuration(res.Latencies[0]))
		fmt.Printf("  p50:    %-12s\n", formatDuration(percentile(res.Latencies, 0.50)))
		fmt.Printf("  p75:    %-12s\n", formatDuration(percentile(res.Latencies, 0.75)))
		fmt.Printf("  p90:    %-12s\n", formatDuration(percentile(res.Latencies, 0.90)))
		fmt.Printf("  p95:    %-12s\n", formatDuration(percentile(res.Latencies, 0.95)))
		fmt.Printf("  p99:    %-12s\n", formatDuration(percentile(res.Latencies, 0.99)))
		fmt.Printf("  max:    %-12s\n", formatDuration(res.Latencies[len(res.Latencies)-1]))
	}

	if len(res.Errors) > 0 {
		fmt.Println()
		fmt.Println("Errors:")
		type errEntry struct {
			Msg   string
			Count int
		}
		entries := make([]errEntry, 0, len(res.Errors))
		for msg, count := range res.Errors {
			entries = append(entries, errEntry{msg, count})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Count > entries[j].Count
		})
		limit := 5
		if len(entries) < limit {
			limit = len(entries)
		}
		for _, e := range entries[:limit] {
			fmt.Printf("  [%d] %s\n", e.Count, e.Msg)
		}
		if len(entries) > 5 {
			remaining := 0
			for _, e := range entries[5:] {
				remaining += e.Count
			}
			fmt.Printf("  ... and %d more errors (%d types)\n", remaining, len(entries)-5)
		}
	}

	if res.Failed > 0 && total > 0 {
		rate := float64(res.Failed) / float64(total) * 100
		fmt.Printf("\nError rate: %.1f%%\n", rate)
	}

	fmt.Println("========================================")
}
