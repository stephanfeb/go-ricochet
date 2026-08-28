# Scalability Re-Assessment

**Status:** current as of `2e6bfee` (post-forge-migration)
**Supersedes:** the pre-forge assessment of the same name (see `git log doc/SCALABILITY.md`)
**Related:** [`SUMI_DOCUMENT_SYNC_REQUIREMENTS.md`](./SUMI_DOCUMENT_SYNC_REQUIREMENTS.md), [`FUTURE_ENHANCEMENTS.md`](./FUTURE_ENHANCEMENTS.md)

The previous assessment was written against the hand-rolled `internal/p2p/` stack, which
`b8a7a97` deleted. Every file path in it was dead. This re-assessment re-verifies each finding
against the current tree, records what the forge migration silently fixed, and adds the
document-volume findings neither the old assessment nor the sumi field report caught.

The target workload driving this: **large document volumes and large connection counts, with a
path to horizontal scale.**

---

## 1. Scorecard: the twelve original findings, re-verified

| # | Finding | Status | Where it lives now |
|---|---------|--------|--------------------|
| 1 | `NullResourceManager` disables all libp2p limits | **Open** — and no longer in this repo | `../go-p2p-forge/host/host.go:89` |
| 2 | DB pool size of 10 | **Fixed** (`42948dc`) — 25 default / 50 production | `internal/core/config.go` |
| 3 | `GetNextSequence` race | **Fixed** (`c8b0c4a`) — `UPDATE ... RETURNING` in the delivery transaction | `internal/storage/postgres/postgres.go` |
| 4 | `MaxConcurrentConnections` never enforced | **Open** — parsed, never read | `internal/core/config.go` only |
| 5 | Rate-limit memory leak | **Fixed** — MTA closed by A4 | forge `middleware/tokenbucket.go` / `internal/ratelimit/limiters.go` |
| 6 | Mailbox cache has no eviction | **Open** | `internal/mda/delivery.go:26` |
| 7 | Global mutex on MTA rate limiter | **Fixed** — MTA now uses the sharded limiter | `internal/mta/router.go` |
| 8 | Unbounded notification goroutines | **Open** | `internal/mda/notifier.go:50,52` |
| 9 | `WorkerThreads` defined but unused | **Open** | `internal/core/config.go` only |
| 10 | No horizontal scaling path | **Open** — but closer than the old doc implies (§4) | architectural |
| 11 | JSON + base64 over libp2p is expensive | **Open** | all handlers |
| 12 | No backpressure mechanism | **Fixed** — bounded in-flight work, shed on saturation | `internal/admission/controller.go` |

### What the forge migration fixed for free

Worth calling out, because it changes the priority of two of sumi's asks. `go-p2p-forge`'s
`middleware.SingleBucket` is **64-way sharded with a background eviction loop**
(`middleware/ratelimit.go`). So for MSA, MAA, MMA, SDA, SFA and SCA, findings #5 and #7 are
already solved — per-peer state is evicted on a timer and lock contention is spread across shards.

`internal/mta/router.go` did **not** migrate. It still carries its own
`map[string][]time.Time` behind a single `sync.Mutex` with no eviction. #5 and #7 are now
*MTA-only* problems, and the fix is to delete that code and route MTA through the forge limiter.

Also relevant: forge already defines a `MetricsCollector` interface (`metrics.go`) and a
`MetricsMiddleware` (`middleware/metrics.go`). **go-ricochet implements neither and installs
neither.** Sumi's Finding C ("`enable_metrics` is a no-op") is correct, but the job is now much
smaller than they estimated — the hook exists, only the collector and the exporter are missing.

---

## 2. New findings: the document-volume path

None of these appear in the previous assessment or in the sumi report. For a
document-heavy workload they matter more than most of §1.

### N1 — `LIST` reads every document body in order to call `len()` on it ⚠️ worst offender

`internal/storage/postgres/postgres.go:600` selects `content` for every document owned by a peer.
`internal/protocol/sda/handler.go:491` uses that content for exactly one thing:

```go
Size: len(d.Content),
```

Nothing else in the response needs the body. So a `LIST` of a vault transfers every document
body out of Postgres, across the network, and into the Go heap, purely to compute a length —
then discards it. There is **no `LIMIT` and no pagination**, and the handler runs under a 30s
timeout that a large vault will simply exceed.

Read amplification is the full size of the vault, per `LIST` call.

**Fix:** `SELECT octet_length(content) AS size` and drop `content` from the projection; add
keyset pagination. This is a one-query change that removes essentially all of the cost.

### N2 — `PUT` reads the full document body twice before writing

`PutDocument` calls `GetDocument` at `postgres.go:504` (to check `If-Match`) and again at
`:514` (to compute the version number). Both `SELECT content`. A conditional 50KB `PUT`
therefore reads 100KB before writing 50KB — and with history enabled writes another full copy
into `document_versions`.

**Fix:** one statement. Fold `If-Match` into the upsert's `WHERE content_hash = $n` and derive
the version with `version_number = documents.version_number + 1`.

### N3 — the ETag optimistic-concurrency guarantee does not hold

The old assessment listed "Optimistic concurrency — ETags/content hashes prevent lost updates"
under *What You're Doing Well*. That is not what the code does. The `If-Match` check at
`postgres.go:504` and the upsert at `:533` are **separate statements with no enclosing
transaction**, so two concurrent `PUT`s presenting the same `If-Match` both pass the check and
both write. The second silently wins. This is a lost-update bug, not merely a performance issue,
and it gets substantially more likely under the concurrency levels this work is aiming for.

The same fix as N2 closes it: make the compare-and-swap a single conditional statement.

### N4 — document bodies live inline in Postgres, and the block store is dead schema

`documents.content BYTEA` with a 10MB cap (`storage.go:12`), and `document_versions.content
BYTEA` stores a **complete copy per version** — no dedup, despite `idx_documents_hash` being
commented "Content deduplication lookup". At volume this puts every document byte through WAL,
replication, backups and TOAST.

Meanwhile `schema.sql:96` defines a `block_store` table — content-addressed, with `cid`,
`tombstoned` and a GIN metadata index. It has **zero references anywhere in the Go tree.** The
content-addressed store described in `FUTURE_ENHANCEMENTS.md` §7.7 is already designed and
already in the schema; nothing was ever wired to it.

### N5 — expiry cleanup is one unbounded `DELETE`

`postgres.go:853` runs `DELETE FROM stored_messages WHERE expires_at < NOW()` with no `LIMIT`.
On a large table this is a single long transaction: a big lock footprint, a WAL spike, and
bloat the autovacuum then has to chase. Batch it with a bounded loop.

### N6 — there is no connection manager at all

Beyond `NullResourceManager` (#1), forge sets no `libp2p.ConnectionManager` either. There is no
low/high watermark, no trimming, no grace period. Combined with #4 — `MaxConcurrentConnections`
being config-only — the server has **no admission control of any kind** on the connection path.
For a "large connection counts" target this is the first thing that has to change.

---

## 3. Sumi's five asks, re-scored

Their code anchors all still resolve — the line numbers in their appendix are still exact.
Finding A in particular was fully confirmed: `sda/handler.go:94` was
`NewDualBucket(time.Minute, 100, 20)`, and the handler limits were hardcoded literals
with no config path. **A4 has since closed this**; the row below records where it landed.

| Their # | Ask | Re-scored |
|---------|-----|-----------|
| 1 | Batch/bulk write (SDA + mailbox deposit) | **Still the top item.** `590aa0a` shipped `BATCH_GET` for feeds — that is the template; extend the same shape to SDA `BATCH_PUT` and MSA batch deposit. |
| 2 | Configurable per-protocol limits + burst | **Done, then superseded.** A4 made every limiter config-driven (`rate_limiting.protocols.*`, forge `TokenBucket` with rate, burst and `AllowN`; an unrecognised protocol name is a startup error). They are now **off by default** — throughput is governed by concurrency-bounded admission control instead, so there is no per-minute ceiling to configure. The knobs remain for per-tenant capping. |
| 3 | Operator mailbox observability | **Partially advanced.** `42948dc` added `GetMailboxInfo` to `pkg/client` — but it is still owner-scoped, which is exactly the limitation they flagged. The operator view does not exist. |
| 4 | Wire `enable_metrics` | **Much cheaper now.** forge's `MetricsCollector` + `MetricsMiddleware` are the hook; this is a Prometheus implementation plus an HTTP listener, not a design problem. |
| 5 | Per-owner, shard-friendly rate-limit state | **Half done.** Keyed by peer ID and sharded, and since A4 the MTA router no longer has its own global-mutex map. What is missing is *shared* state across instances. |

Their pacing workaround (≤18 docs/pass, one pass per ~65s) means a 500-doc vault currently takes
~30 minutes. That number is a direct consequence of SDA's 20 writes/min: 500 writes ÷ 20/min ≈ 25
min floor. With a batch write carrying 50 documents per request, the same vault is 10 requests —
under a minute against the *unchanged* limit. This is why their #1 is correctly ranked first: it
is the only item that changes the workload's shape rather than raising a number.

---

## 4. Horizontal scalability: closer than the old assessment claimed

The previous doc said "there is no horizontal scaling path" and left it there. Re-reading the
current architecture, that is too pessimistic. The important structural facts:

**What already works in your favour**

- Every protocol is **stateless request/response** over a libp2p stream. No session affinity is
  required by the protocol itself.
- **All durable state is in PostgreSQL.** Documents, feeds, collections, mailboxes and messages
  are all owner-scoped rows. Any instance pointed at the same database can already serve any
  request for any owner, today, with no code change.
- `pkg/client` already models **multiple servers with priorities** (`PreferredServers []ServerPreference`,
  `selectServer` at `client.go:105`). The client-side shape for failover exists.
- The GossipSub `Registry` (`internal/registry/registry.go`) already announces server instances
  on `/sf-network/services/announce`. A multi-instance fleet already has a discovery mechanism.

So the data plane is *already* horizontally scalable. What blocks it is a short, specific list:

**What actually blocks it**

1. **Per-instance identity.** Each instance loads one keypair (`server.go:428`) and is addressed
   by that peer ID. N instances means N identities, and a client's `PreferredServers` must
   enumerate them. Workable, but there is no story for adding capacity without reconfiguring
   clients — this is the main design decision to make.
2. **In-process caches go incoherent.** `mailboxCache` (`delivery.go:26`) is the sharp one: instance
   A caches a mailbox, instance B deletes it, A keeps serving stale state. Presence
   (`presence/cache.go:56`) and the registry map have the same shape but tolerate staleness better.
3. **Notifications are instance-local.** `notifyDirect` (`notifier.go:57`) opens a stream from
   *this* host to the owner. If the owner is connected to a different instance, the notification
   silently fails. Shared mailboxes go via GossipSub and already fan out correctly — private
   mailboxes do not.
4. ~~**Rate limits are per-instance.** A fleet of 5 gives every client 5× the intended budget.~~
   **No longer a blocker.** Throughput is governed by `internal/admission`, which bounds
   concurrent work per instance — and per instance is the *correct* scope, since each one is
   protecting its own database connections. Adding an instance adds capacity instead of
   multiplying a budget. Per-peer rate limits are off by default; a deployment that turns them
   on for per-tenant capping would still want shared state, but that is billing, not capacity.
5. ~~**`GetNextSequence` (#3) escalates from a race to a guarantee-breaker.**~~ **Fixed**
   (`c8b0c4a`). Sequence assignment is now a single `UPDATE ... RETURNING` inside the delivery
   transaction, so it is correct across instances as well as within one.
6. ~~**No health check, no draining, no HTTP surface at all.**~~ **Fixed** (B0).
   `internal/opsapi` serves `/healthz`, `/readyz` and, when `features.enable_metrics` is on,
   `/metrics`, on a loopback listener configured under `ops`. `/readyz` pings PostgreSQL and
   returns 503 when it is unreachable or when the instance is draining, which is the signal a
   load balancer acts on. Liveness deliberately touches nothing: a probe that fails on a
   database outage gets the process restarted for a fault a restart cannot fix. Saturation is
   reported in the readiness body but is not unreadiness — withdrawing a busy instance moves
   its load onto whichever instances are already busiest.

---

## 5. Proposed program of work

Ordered so that each phase makes the next one measurable.

### Phase A — make one node fast (unblocks sumi, prerequisite for everything else)

| | Item | Notes |
|---|------|-------|
| A1 | ✅ `octet_length` + pagination for `LIST` (N1) | Largest win for the smallest change |
| A2 | ✅ Single-statement conditional `PUT` (N2 + N3) | Kills two reads *and* the lost-update bug |
| A3 | ✅ `BATCH_PUT` for SDA, batch deposit for MSA | Sumi #1; `BATCH_GET` in `590aa0a` is the template |
| A4 | ✅ Config-drive the limiters; token bucket with burst | Sumi #2. There were seven sites, not six. Superseded by C3: the limits are now off by default |
| A5 | ✅ Transactional `GetNextSequence` (#3) | Correctness, and a prerequisite for Phase D |
| A6 | ✅ Batch the expiry `DELETE` (N5) | |

### Phase B — see what is happening

Nothing after this phase should be tuned by guessing. Sumi spent days reverse-engineering a
rate limit from client-side `429`s; that must not be the debugging experience again.

**Planned in detail: [`PHASE_B_PLAN.md`](./PHASE_B_PLAN.md).** Two items were added to the
table below after checking the code: an operator HTTP view (sumi's #3, decided against a
protocol-level operator credential) and the client error surface, which still collapses every
status into a string.

| | Item | Notes |
|---|------|-------|
| B1 | Prometheus `MetricsCollector` + install `MetricsMiddleware` | The forge hook already exists |
| B2 | HTTP listener: `/metrics`, `/healthz`, `/debug/pprof` | Also unblocks D6 |
| B3 | Per-protocol throttle counters, mailbox-depth gauge, operator HTTP view | The two numbers sumi could not see. Needs the first non-owner-scoped storage queries in the codebase |
| B4 | Typed client errors + `Retry-After` on 429/503 | `pkg/client` has no error types at all; sumi paced by guesswork because we never said when to retry |
| B5 | Baseline runs with `cmd/ricochet-bench` | Already supports per-protocol, concurrency, percentiles; needs batch and re-sync scenarios |

### Phase C — bound and shed load (the "large connection counts" half)

| | Item | Notes |
|---|------|-------|
| C1 | Real `rcmgr` limits in `go-p2p-forge`, made configurable (#1) | Change lands in the forge repo |
| C2 | Connection manager with watermarks; wire `MaxConcurrentConnections` (#4, N6) | |
| C3 | ✅ Admission control on DB saturation (#12) | Done ahead of B. Concurrency-bounded, global + per-peer, derived from the pool size. Adaptive sizing still needs B's measurements. Give `WorkerThreads` (#9) a meaning or delete it |
| C4 | Bound notifier goroutines (#8); LRU the mailbox cache (#6) | |

### Phase D — horizontal

| | Item | Notes |
|---|------|-------|
| D1 | Decide the identity model | Shared key vs. per-instance + client-side failover. **Design decision, blocks the rest.** |
| D2 | Make instance-local caches coherent or bounded-stale | `mailboxCache` first |
| D3 | Cross-instance notification for private mailboxes | Extend the GossipSub path already used for shared |
| D4 | Shared rate-limit state (Redis) | Sumi #5 |
| D5 | Read replicas for read-heavy paths (MAA, SDA `GET`) | `FUTURE_ENHANCEMENTS.md` §7.10 |
| D6 | Connection draining + LB health checks | Depends on B2 |
| D7 | Offload bodies to the block store / object storage (N4) | Wire the `block_store` table that already exists |

---

## 6. Notes on `FUTURE_ENHANCEMENTS.md`

Sections worth re-scoping in light of the above:

- **§5.2 Observability** — still the correct plan; now much cheaper (forge hook).
- **§7.7 Content-addressed storage** — the schema is already written (`block_store`) and unused.
  Re-scope from "design and build" to "wire up".
- **§7.9 Admin API** — this is what sumi needs for observability, and B2's HTTP listener is
  the natural place to host it.
- **§7.10 Horizontal Scaling** — the four bullets there (read replicas, draining, LB health
  checks, sticky sessions) are all still right, but the section understates how much already
  works. §4 above is the more accurate starting point. Note that "sticky sessions by peer ID"
  is *not* required given stateless handlers and shared Postgres — it would only be a
  workaround for D2 rather than a goal.
- The priority matrix predates the forge migration and the client library landing; it needs a
  pass.

---

## 7. What is still genuinely good

Re-confirmed against the current tree, since the old list had one wrong entry (N3):

- **Clean MTA/MDA separation** — still the right seam for pulling delivery out of process.
- **Owner-scoped data everywhere** — the single most valuable property for §4. Documents,
  feeds, collections and mailboxes all key on owner peer ID.
- **Cursor-based pagination** for directory browsing — scales properly, and is the model
  `LIST` should copy in A1.
- **Transactional consistency for feeds and collections** — these use `UPDATE ... RETURNING`
  and `SELECT ... FOR UPDATE` correctly. Documents and message sequences are the outliers.
- **Read/write split in the handler limiters** — the right shape; it just needs burst and config.
- **Presence batching** — 2s window, max batch 50, prevents GossipSub flooding.
