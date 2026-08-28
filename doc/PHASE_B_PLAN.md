# Phase B — See What Is Happening

Implementation plan for `SCALABILITY.md` §5 Phase B. Written 2026-08-28, after
Stage 1 and the admission-control work (`8ceff9a`).

Nothing after this phase should be tuned by guessing. The sumi team spent days
reverse-engineering a rate limit from client-side `429`s; that must not be the
debugging experience again.

## 1. What this unblocks

Three things need it, which is why it is worth doing as one body of work rather
than in pieces:

1. **Sumi's asks #3 and #4** — operator mailbox observability, and making
   `enable_metrics` mean something. Both are untouched. They said even one of
   them "would have turned our multi-day investigation into a single query."
2. **The last guessed constant.** Admission control removed the artificial
   throughput ceiling, but the bound itself is still static. Making it adaptive
   — widen while commit latency is flat, narrow when it climbs — needs the
   measurements this phase produces.
3. **Fleet readiness.** `grep net/http` across the tree returns nothing, so no
   load balancer can participate: no health check, no readiness, no draining.
   This is blocker 6 in `SCALABILITY.md` §4.

## 2. Ground truth

Verified against the code, not against the older docs:

**Already built and unwired.** forge defines `MetricsCollector` (five callbacks)
in `metrics.go` and `MetricsMiddleware` in `middleware/metrics.go`. Neither is
referenced anywhere in go-ricochet.

**The dependency is already there.** `prometheus/client_golang v1.23.2` is an
indirect dependency via libp2p. Promoting it to direct adds nothing new to the
supply chain.

**Pool saturation is free.** `PostgresStorage.Pool()` is exported, so
`pgxpool.Stat()` yields `EmptyAcquireCount`, `AcquireDuration`, `IdleConns` and
`TotalConns` with no instrumentation to write. That is the signal an adaptive
admission bound needs.

**Storage has no aggregate queries.** Every method on the `Storage` interface is
owner-scoped or single-mailbox: `GetMessageCount(mailboxID)`,
`ListMailboxes(ownerID)`. There is no cross-owner listing, no total-bytes query,
no top-N. B2 and B3 need new storage methods; this is the largest piece of work
in the phase and it is easy to underestimate.

**The operation is parsed and discarded.** `middleware/router.go` reads
`opValue` to select a route and never records it on the `StreamContext`. A
metrics middleware wrapping the pipeline therefore cannot tell a `GET` from a
`BATCH_PUT`. Fixed by a one-line forge change (§3).

**`handleQueryCapacity` reports fabricated data.** `mma/handler.go:605` returns
`AvailableStorageBytes: config.MaxStorageBytes` with a literal
`// TODO: compute actual usage`, and leaves `UsedStorageBytes`, `MessageCount`,
`ActiveMailboxes` and `HealthScore` at zero. It answers "storage is 100% free"
regardless of what is on disk. This is the same class of defect as the
unreachable rate limit: a knob that reads as working and is not.
**Fixed in B2.**

**Two more dead config knobs.** `TrustedPeers` and `EnableAuthentication`
(`config.go:61-62`) are parsed, set by the production preset, and never read.
Not in scope here, but worth knowing they are not an available primitive.

## 3. Decisions settled

**Operator access is over HTTP**, on the same listener B0 builds, bound to
localhost by default. No new authentication model, no new protocol surface, and
it works with the tooling operators already have. Rejected: an MMA operator
credential via `TrustedPeers`, which would put operator-scoped operations on a
publicly reachable protocol; and a database-reading CLI, which cannot see live
in-process state such as admission shed counters.

**Never label a metric by peer ID.** forge's collector hands `peerID string` to
every callback, and the temptation is to use it. With thousands of mobile
clients that is unbounded cardinality and it will take down the scraper before
it takes down us. Peer ID belongs in logs and in top-N views computed on
demand. Metric labels are `protocol` × `operation` × `outcome`, all bounded.

**Bind to localhost by default.** `/metrics` leaks peer IDs, storage figures and
internal topology. Exposing it beyond the host is an explicit opt-in.

**Two small forge changes**, both in a repo we own:
- `OperationRouter` records the routed operation on the `StreamContext`, so
  middleware downstream of `next()` can read it. Useful for structured logging
  independently of metrics.
- `MetricsCollector` gains an operation-aware completion callback. Additive,
  so `NoopMetrics` and any existing implementation keep compiling.

## 4. The work

### B0 — HTTP surface — **done**

Landed in `internal/opsapi` with config under `ops`. Two things the plan did
not anticipate:

- **Saturation is not unreadiness.** The plan said `/readyz` "reports admission
  saturation", which reads as though a full instance should report unready. It
  must not: a busy instance is working, and taking it out of rotation moves its
  load onto whichever instances are already busiest. Saturation and pool
  figures are carried in the readiness *body*, and only a dependency failure or
  a deliberate drain changes the verdict.
- **Draining needs a delay to mean anything.** `Stop` marked the instance
  unready and then tore it down in the same instant, so a load balancer never
  observed the 503 — it would have learned of the shutdown through failed
  requests. `ops.drain_delay` holds the listener open after the withdrawal.
  It defaults to zero, since it is only useful when something is probing.

`/metrics` is mounted in B0 already, gated on `features.enable_metrics`, over
the Prometheus default registry — so it serves Go runtime and process metrics
today. B1 adds the ricochet families to it.

Original scope, for reference:

New package `internal/opsapi`: a `*http.Server` with its own listener,
lifecycle wired into `Server.Start`/`Server.Stop` alongside the other services.

- `/healthz` — liveness. Process is up. Never touches the database.
- `/readyz` — readiness. Pings Postgres and reports admission saturation.
  Returns 503 when not ready, which is what lets a load balancer drain an
  instance rather than blackhole requests into it.
- `/debug/pprof` — behind its own config flag, off by default.
- Config: `ops.{enabled, bind, port, enable_pprof}`. `enable_metrics` finally
  gates something.

**Done when:** `curl localhost:<port>/healthz` works, killing Postgres flips
`/readyz` to 503 without killing the process, and shutdown is clean under
`Server.Stop`. **All three verified** — the first and third against the real
binary, the second in `TestOpsSurfaceReportsDatabaseLoss` against a real pool
(pgx reports `closed pool`). Both readiness tests were confirmed to fail when
the 503 path is removed.

### B1 — Prometheus collector — **done**

Landed as `internal/metrics`, installed in all seven pipelines. Four
departures from the plan, all forced by what the code turned out to be:

- **forge's `MetricsCollector` was not the hook this plan took it for.** §2
  recorded that go-ricochet references neither it nor `MetricsMiddleware`. The
  sharper fact, found while implementing: **nothing inside forge calls the
  collector either.** `WithMetrics` stores it and `Metrics()` returns it;
  `StreamStarted`, `BufferPoolStats` and `ActiveStreams` have no callers
  anywhere. Implementing the interface would have produced zero series and the
  promised "free" buffer-pool numbers do not exist. So the middleware is
  ricochet-native, and the second planned forge change — an operation-aware
  completion callback — was dropped as pointless. Only the `OperationRouter`
  change was needed (forge `ce65b76`).
- **`codec.BufferPool.Hits`/`.Misses` are exported atomics**, so the buffer
  pool is read directly. The server now holds the pool as a field rather than
  a local in `registerProtocolHandlers`.
- **Two more outcomes than planned: `client_error` and `server_error`.** The
  four in the plan all derive from `sc.Err`, but a handler that answers 404 or
  409 sets no pipeline error — so with the planned set, a server refusing
  every request would have published an unbroken line of `ok`. That is the
  same defect as `handleQueryCapacity`: a number that reads as healthy and is
  not. The middleware reads a `StatusCode()` off the response where one
  exists (`sda`, `sfa`, `sca` carry an HTTP-style status).
- **A private registry, not Prometheus's global default.** libp2p registers a
  large number of collectors into the default registry simply by being
  imported. A deliberate registry keeps the exposition a known list and lets
  the cardinality test enumerate it. B0's `/metrics` was switched over.

Original scope, for reference:

Metric families, all labelled `protocol` × `operation` × `outcome`
(`ok` / `rate_limited` / `overloaded` / `error`):

- `ricochet_requests_total` (counter)
- `ricochet_request_duration_seconds` (histogram)
- `ricochet_requests_in_flight` (gauge, from the admission controller)
- `ricochet_admission_shed_total` (counter) — the signal that capacity is short
- `ricochet_db_pool_*` (gauges, from `pgxpool.Stat()`)
- `ricochet_buffer_pool_{hits,misses}_total` — forge already reports these

**Done when:** a scrape shows non-zero series for every protocol after a bench
run, and cardinality is provably bounded — a test asserting no label value is a
peer ID. **Both verified.** `TestMetricsRecordRealTraffic` drives traffic
through the real pipelines and checks the operation labels;
`TestScrapeCarriesNoPeerIdentity` and `TestNoLabelCarriesAPeerID` walk every
published series, not just the request families, since a collector added later
is where peer identity would slip in. Removing the middleware, the status
classification, and the rate-limited/overloaded distinction each made the
matching test fail.

The bound holds structurally, not just by convention: forge records the
operation **only after a route matches**, so the label set is the routing
table rather than whatever a caller sends. An unroutable request is labelled
`unrouted`.

### B2 — The two numbers sumi could not see — **done**

Landed as `internal/capacity` plus one `Storage.ServerStats` method. Four
things differ from the plan:

- **One method, not three.** `CountMailboxesNearCapacity`, `TotalStorageBytes`
  and `MailboxDepthHistogram` all need the same per-mailbox message count, so
  three methods would have scanned `stored_messages` three times.
  `ServerStats(ctx, nearCapacityRatio)` returns all of it from a single pass.
- **Usage is `pg_database_size`, not a sum of payloads.** Indexes and
  unreclaimed space are what actually fill a volume; a server reporting 40%
  used while its disk is full has answered the wrong question. The payload sum
  is published separately as `ricochet_message_bytes` — what users stored, as
  distinct from what it costs. `pg_database_size` is also O(1), so the single
  scan left is the mailbox pass.
- **A sampler with an explicit "not yet" state.** `capacity.Sampler` caches the
  pass and refreshes it on the maintenance tick, with the first sample taken in
  the background at startup. Before one lands, `Capacity()` returns
  `ErrNotSampled` and the metrics collector publishes *nothing* — zeroes would
  read as an empty server, which is the exact fabrication being removed. Every
  answer carries `SampledAt`, and the exposition carries
  `ricochet_stats_age_seconds`: a sampler that has quietly stopped otherwise
  looks identical to a server whose numbers have stopped changing.
- **`core.ServerCapacity` gained `SampledAt`**, and `HealthScore` now has a
  stated definition — the fraction of the storage budget still free. It was
  hardcoded to zero precisely because the name invites reading it as an overall
  verdict.

**A real bug surfaced while testing this.** The near-capacity filter was
`n >= $1 * max_messages`. Postgres infers `$1` from the integer column it
multiplies, so the 0.9 ratio was truncated to 0 and *every* mailbox counted as
near capacity. It needs `$1::double precision`. The first version of the
regression test used before/after deltas and passed with the bug reintroduced;
it now measures a below-threshold mailbox on its own, and fails as it should.

Original scope, for reference:

**Throttle state.** ~~Falls out of B1, but needs a test.~~ **Done in B1.**
`TestRateLimitedAndOverloadedAreSeparateSeries` drives a real rate-limit
rejection and a real admission shed through the pipelines and asserts they land
in different series — and that `ricochet_admission_shed_total` agrees with the
count of `overloaded` requests, since the two are independent views of the same
event.

**Mailbox depth.** Needs new storage methods:
- `CountMailboxesNearCapacity(ctx, threshold float64) (int, error)`
- `TotalStorageBytes(ctx) (int64, error)`
- `MailboxDepthHistogram(ctx) ([]DepthBucket, error)`

Sampled by the existing maintenance loop rather than per request, since these
are aggregate scans. Exposed as gauges.

**Fix `handleQueryCapacity`** to report real usage from those methods. This is
a bug fix, not a feature: it currently lies.

**Done when:** filling a mailbox to its cap moves the near-capacity gauge, and
`queryCapacity` returns numbers that match the database. **Both verified.**
`TestFillingAMailboxMovesNearCapacity` checks the gauge moves for a mailbox at
its cap and does not for one with room;
`TestQueryCapacityReportsRealUsage` goes over the wire through the MMA handler
— asserting against the sampler directly left the handler free to go on
fabricating, which an earlier draft of the test did. Reintroducing either the
fabricated handler or the truncated ratio fails the matching test. A live
scrape against the real binary matched the database exactly: 31 mailboxes, 624
messages, depth buckets summing to 31.

### B3 — Operator view — **done**

All four endpoints landed as planned, in a new `internal/opsview` package
mounted on the B0 listener. Five things differ from the plan:

- **`opsapi` gained `Options.Routes`, not an exported `Handle`.** The mux is
  built inside `New` and never escapes, so extra routes are passed in rather
  than registered afterwards: there is no ordering to get wrong, everything the
  surface serves is decided before the listener exists, and each supplied route
  is wrapped so it rejects anything but a read. A pattern that collides with a
  built-in is refused and logged — losing `/healthz` to a typo would look like
  a healthy server to every probe that could still reach it. The index now
  lists what is actually mounted instead of a hand-maintained guess.
- **The handlers live outside `opsapi`.** That package stays a transport: it
  knows about listeners, readiness and draining, and nothing about mailboxes.
  Same separation that keeps the Postgres ping in `internal/server`.
- **Two storage methods, not `ListAllMailboxes(ctx, limit, offset)`.** A list
  of `MailboxRecord` would have answered the wrong question — a record carries
  the *cap* and says nothing about what is stored, which is the whole point.
  `ListMailboxUsage(ctx, MailboxUsageQuery)` returns counts, bytes, fill ratio
  and last-message time; `ListOwnerUsage(ctx, limit, offset)` totals per owner.
  The second is separate because grouping by owner from a mailbox list would
  mean reading every mailbox to add up three numbers.
- **The page cap is enforced in storage, not in the handler.** `ClampPageSize`
  (default 20, max 500) is applied inside the query. A cross-owner listing has
  no natural bound and one unbounded scan is enough to matter, so the bound
  cannot be something a caller is trusted to pass.
- **`/ops/storage` reports sampled and live figures side by side.** The
  per-owner totals are computed live; the server-wide block comes from the B2
  sampler, because it includes a database size that is expensive to compute.
  Before the first sample the block is `null` with a note rather than zeroes,
  and it always carries `sampledAt` and `ageSeconds`. On a live server this is
  visible and correct: the totals move as data arrives while the sampled block
  states its own age until the next maintenance tick.

`/ops/mailboxes/top` also accepts `sort=count` alongside the default
`sort=fill`. "Fullest" and "biggest" are different questions — an uncapped
mailbox holding a million messages is in no danger — and the ordering costs one
`ORDER BY` branch.

**A note on what the tests caught.** Reverting each change confirmed all eight
mutations fail the suite, but two of the tests had to be strengthened first:
the fullest-first ordering check was vacuous on a page of uncapped mailboxes
(all fill ratios zero, so any order is non-increasing), and the double-count
test asserted only on message totals, which a single-stage join gets right —
it is the *mailbox* count that inflates. Both now fail on revert.

**Done when:** the question "is this mailbox full?" is one `curl` away for a
mailbox the caller does not own. **Verified.**
`TestOperatorCanSeeAnotherPeersFullMailbox` creates a mailbox owned by a peer
identity the caller has never met and reads back `"full": true` over HTTP with
no handshake, no key and no client library.

### B4 — Close the client loop — **done**

All four typed errors landed, along with the wire changes they needed. Five
things differ from the plan, and one of them was a bug the plan did not
anticipate.

- **The server had to be taught to say it first.** The plan reads as a client
  change, but MSA, MMA and MAA carried no status at all — a failure was
  `success: false` plus English. Typing the client against that would have
  meant parsing prose. So `internal/protocol/wire` now owns the
  error → status mapping in one place, and the flat protocols gained `status`
  and `retryAfterMs` fields while the document protocols carry the hint in
  their existing headers map. Every addition is additive; a client ignoring
  the new fields behaves exactly as before.
- **`Retry-After` in milliseconds, not seconds.** HTTP's unit is seconds, and
  the hints that matter here — a token bucket refilling, a fleet being
  desynchronised — are well under one. Rounding a 40ms wait up to a second
  would pause a client twenty-five times longer than the server needs, which
  is the failure this endpoint exists to prevent.
- **The 429 hint is exact; the 503 hint deliberately is not.** The token bucket
  knows precisely when the next token arrives, so a 429 reports it (go-p2p-forge
  `df6668c` adds `AllowNWithRetry`, computed under the same lock as the decision
  so it cannot describe a bucket that has since refilled). A 503 is different:
  admission control already paces by blocking, so a shed request has waited the
  full acquire timeout and there is no hot loop to prevent. Its hint is a quarter
  of that timeout, clamped to [50ms, 1s], and exists to desynchronise a fleet
  rather than to pace it. A larger value would re-create the per-minute ceiling
  that concurrency bounds exist to remove.
- **507 carries no hint at all, and `IsRetryable` is false for it.** A full
  mailbox is not backpressure — only the recipient clears it — so a client that
  reads it as "retry later" retries forever. Same for 409, which needs a
  re-read rather than a wait.
- **A fifth type fell out: `ProtocolError`.** Statuses with no dedicated
  handling (400, 500) still need to reach the caller carrying their number, or
  the caller is back to reading prose for those.

**Two bugs surfaced while doing this.**

`sfa/handler.go` and `sca/handler.go` compared `sc.Err == forge.ErrRateLimited`
by equality rather than with `errors.Is`. The moment the limiter started
returning a wrapped error, both would have reported 500 for every throttled
request. Routing all three document protocols through `wire.Classify` removed
the equality checks along with the duplication.

A rejected MAA request produced an error envelope the compound frame decoder
cannot parse, so a throttled retrieve surfaced as "retrieve response truncated
at metadata" — an error that reads like a codec bug and sends the reader to the
framing rather than to the limiter. The client now checks for the envelope
first.

**A third bug found and, on Stephan's call, fixed here.**
`internal/mda/delivery.go:58` created delivery mailboxes with the hardcoded
literals 1000 and 30, ignoring `max_messages_per_mailbox` and
`retention_policy` entirely — the same class of problem as sumi's Finding A, a
configured knob that never reaches the code that would honour it. It was
initially left alone because changing it alters the effective cap for existing
deployments; with the project still in development that concern does not apply.

The literals became `mda.MailboxDefaults`, derived by `DefaultsFromConfig` and
passed to `NewMailboxServer` as a parameter rather than a setter, so a caller
cannot forget them — forgetting is exactly what the literals amounted to, and
it failed silently.

Two hazards fell out of the conversion. A cap of zero is not "unlimited": the
check is `count >= max`, so zero makes every mailbox full on its first message.
And retention is enforced as "delete anything older than N days", so a
sub-day `retention_policy` truncating to zero days would delete every message
on the next sweep. Both now fall back rather than pass through, and a partial
day rounds up. The default configuration still yields 1000 and 30, so a
deployment that never set the knobs sees no change at all.

The wiring in `server.go` has its own test. It pins a single line, deliberately:
that line was wrong for the life of the codebase and nothing failed or logged,
because the only symptom of this bug class is a setting quietly not applying.

**Done when:** a client can branch on error type, and the integration tests
stop matching on strings. **Both verified.** `isRateLimited` and `isOverloaded`
in the integration suite are now `errors.Is` checks;
`TestRateLimitedErrorCarriesAWorkingRetryHint` waits exactly the hint it was
given and succeeds on the retry, and `TestOverloadedErrorIsDistinctFromRateLimited`
drives a real admission shed and asserts it does *not* match the throttling
sentinel.

### B5 — Baselines

`cmd/ricochet-bench` already supports per-protocol, concurrency, duration,
warmup and percentiles. Missing:

- Batch scenarios (`BATCH_PUT`, batch submit) — the dominant workload now.
- Cold sync, warm re-sync, no-op re-sync — the three shapes Stage 2 will be
  judged against.
- Recorded baseline numbers committed to `doc/`, with the hardware stated.

**Done when:** there is a committed number to regress against, and Stage 2 has
a before-figure.

## 5. Out of scope

- **The adaptive admission control loop.** This phase produces the inputs; the
  controller that consumes them is Stage 3. Building both at once would mean
  tuning a control loop against metrics that have never been read in anger.
- **Activating `TrustedPeers` / `EnableAuthentication`.** They are dead, and an
  authentication model deserves its own design pass rather than being smuggled
  in behind an ops endpoint.
- **`NullResourceManager` (`SCALABILITY.md` #1) and the connection manager
  (#4, N6).** Phase C. Worth noting the exposure has grown: admission control
  protects the database, and nothing protects CPU, memory or sockets.

## 6. Risks

**Aggregate queries on a large table.** `MailboxDepthHistogram` and per-owner
storage totals are scans. They must be sampled on the maintenance loop, never
per request, and they need indexes checked against a realistically sized
database — not a test database with a hundred rows.

**Cardinality.** Stated above, restated here because it is the one mistake in
this phase that degrades production rather than just being wrong.

**Scope creep from the storage interface.** B2 and B3 add the first
non-owner-scoped queries in the codebase. That is a meaningful widening of the
interface and it wants review, not just implementation.

**Removes:** guessing as the debugging method. **Next binding:** the static
admission bound, which Stage 3 makes adaptive using what this phase measures.
