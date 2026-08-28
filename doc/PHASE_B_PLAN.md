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

### B1 — Prometheus collector

New package `internal/metrics` implementing `forge.MetricsCollector`, plus a
ricochet-side middleware installed in all seven pipelines.

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
peer ID.

### B2 — The two numbers sumi could not see

**Throttle state.** `outcome` already separates `rate_limited` (429) from
`overloaded` (503), so this falls out of B1 — but it needs a test proving the
two are distinguishable, because conflating them would tell an operator to
raise a limit when they should be adding capacity.

**Mailbox depth.** Needs new storage methods:
- `CountMailboxesNearCapacity(ctx, threshold float64) (int, error)`
- `TotalStorageBytes(ctx) (int64, error)`
- `MailboxDepthHistogram(ctx) ([]DepthBucket, error)`

Sampled by the existing maintenance loop rather than per request, since these
are aggregate scans. Exposed as gauges.

**Fix `handleQueryCapacity`** to report real usage from those methods. This is
a bug fix, not a feature: it currently lies.

**Done when:** filling a mailbox to its cap moves the near-capacity gauge, and
`queryCapacity` returns numbers that match the database.

### B3 — Operator view

JSON endpoints on the B0 listener, localhost-bound:

- `/ops/mailboxes/top?n=20` — fullest mailboxes across all owners, with
  `messageCount`, `maxMessages`, `owner`, `folderPath`.
- `/ops/mailboxes?owner=<peerID>` — one owner's mailboxes, without needing to
  be that owner.
- `/ops/storage` — per-owner storage totals, descending.
- `/ops/limits` — effective rate limits and admission settings as loaded, so
  an operator can confirm what the server actually parsed. This is the direct
  answer to sumi's Finding A: they changed a knob and could not tell whether
  it had taken effect.

Needs a cross-owner `ListAllMailboxes(ctx, limit, offset)` on the storage
interface — the first query in the codebase that is not owner-scoped, so it
needs a bounded result set by construction.

**Done when:** the question "is this mailbox full?" is one `curl` away for a
mailbox the caller does not own.

### B4 — Close the client loop

`pkg/client` currently has no typed errors at all: every status collapses into
`fmt.Errorf` text, so a caller cannot tell 429 from 503 from 500. The A4
integration tests had to match on a substring, which is the same guesswork
sumi was doing.

- Typed errors: `RateLimitedError`, `OverloadedError`, `ConflictError`,
  `MailboxFullError`, each carrying the status and any `Retry-After`.
- `Retry-After` on 429 and 503 responses. The server knows when capacity will
  free up; not telling the client is why sumi ended up pacing at
  "≤18 docs per pass, one pass per ~65s".
- Update `../overnode_v2/RICOCHET_SERVER_CHANGES.md`: 503 is a new status no
  client knows about yet, and the note does not currently mention 429 or 503.

**Done when:** a client can branch on error type, and the integration tests
stop matching on strings.

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
