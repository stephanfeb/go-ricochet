# Scale Roadmap: unbounded document throughput

**Status:** proposed refactor, not yet started
**Baseline:** `2e6bfee`
**Related:** [`SCALABILITY.md`](./SCALABILITY.md) (findings), [`SUMI_DOCUMENT_SYNC_REQUIREMENTS.md`](./SUMI_DOCUMENT_SYNC_REQUIREMENTS.md) (field report), [`FUTURE_ENHANCEMENTS.md`](./FUTURE_ENHANCEMENTS.md)

`SCALABILITY.md` catalogues what is broken. This document is the plan: what we are
building, in what order, and what stops being the bottleneck at each step.

---

## 1. The goal, stated precisely

The sumi team asked for a batch write so that a 500-document vault would sync in a
handful of requests instead of 500. That is the right instinct aimed one notch too
low — it raises the ceiling rather than removing it. We are aiming at the ceiling
itself.

**Target: document throughput scales linearly with fleet size, with no constant in
the system that caps it.**

Three properties, concretely:

1. **Request count decouples from document count.** Syncing N documents costs
   `O(1)` requests, not `O(N)`. Adding documents does not add round trips.
2. **Re-sync cost is proportional to change, not to vault size.** A 5,000-document
   vault with 3 edited files transfers 3 documents' worth of bytes. This is the
   dominant real workload and today it costs the same as a cold sync.
3. **Adding a node adds throughput.** No per-node constant — no `20/min`, no
   single-writer chokepoint on the hot path — is the binding limit.

**What "unbounded" honestly means.** It does not mean no limits; it means no
*arbitrary* limits. Protection stays, but caps become derived from measured
capacity rather than picked as constants, and total capacity grows with the fleet.
§9 states what remains genuinely bounded — some things do, and pretending otherwise
would make this document useless.

---

## 2. The ceiling stack

Throughput today is capped by five distinct mechanisms, stacked. Removing the top
one just exposes the next, which is why the ordering in §7 matters.

| Layer | Ceiling | Where |
|-------|---------|-------|
| 1. Rate limit | **20 writes/min/peer** — a hardcoded constant | `sda/handler.go:94` |
| 2. Stream model | **one request per stream**, then `Close()` — every document pays a full stream setup | `../go-p2p-forge/pipeline.go:39` |
| 3. Write amplification | `PUT` reads the body **twice**, writes it once, plus a full copy per version | `postgres.go:504,514` |
| 4. Data plane | document bodies are `BYTEA` **inline in Postgres** — every byte through WAL, replication, backups | `schema.sql:113` |
| 5. Deployment | **one process, one primary** | architectural |

A sixth ceiling turned up while measuring A3 and is not in the table because it
was a defect rather than a design limit: the UDX transport stalled once ~256KB
had crossed a connection, in either direction. It capped everything above it and
is now fixed (`go-udx` `d9b1dc7`).

Layer 1 is what sumi hit and what their batch-write ask addresses. Layer 2 is the
one nobody has named yet, and it is the reason a batch write alone will not get us
to the target: `Pipeline.HandleStream` runs the middleware chain exactly once and
then closes the stream. Over a relayed mobile connection, per-document stream setup
is the dominant latency term, and no per-request batch size removes it — it only
amortises it in fixed-size chunks.

---

## 3. The core refactor: content-addressed sync

This is the linchpin. Everything else in the roadmap is either a prerequisite for
it or a consequence of it.

### 3.1 The insight

Today, `PUT` is unconditional: the client sends the full body, always, and the
server writes it, always. But in the dominant workload — a device re-syncing a
vault it mostly already has — **most documents are unchanged**, and the server
already holds byte-identical content. We are paying full transfer and full write
cost to store data we already have.

The schema already anticipated this. `schema.sql:96` defines `block_store` with a
`cid TEXT` unique key, `payload_bytes`, `tombstoned` and a GIN metadata index. It is
**entirely unreferenced by the Go tree** — designed, migrated, never wired. So is
`documents.version_vector`, which is read into the model at `postgres.go:483` and
never written by anything.

### 3.2 Split blobs from metadata

Documents stop carrying content and start pointing at it:

```
documents(owner_peer_id, path, content_cid, content_type, size, version, ...)
block_store(cid, payload_bytes, ...)          -- immutable, owner-agnostic
```

Consequences that fall out for free:

- **Version history stops duplicating bodies.** `document_versions` holds a CID per
  version instead of a full `BYTEA` copy. History becomes nearly free, which means
  it can be on by default instead of an opt-in that costs 2× storage per write.
- **Re-writing identical content becomes a metadata-only update.** No blob write at
  all. The `idx_documents_hash` index that is commented "Content deduplication
  lookup" finally does that job.
- **Blobs are immutable**, so they are cacheable, replicable, and offloadable to
  object storage without any consistency protocol (§5).
- **Dedup crosses owners.** Two users syncing the same shared document store one copy.

### 3.3 A new agent: SBA (Store Block Agent)

Blobs are a different concern from documents — content-addressed, not owner-scoped,
different size and rate profile — so they get their own protocol alongside the
existing six, rather than more operations bolted onto SDA.

`/ricochet/store/block/1.0.0`, three operations:

| Op | Purpose |
|----|---------|
| `HAS` | Given a set of CIDs, return which are missing. Cheap, read-only, batched. |
| `PUT` | Upload blobs by CID. Idempotent by construction — a re-upload is a no-op. |
| `GET` | Fetch blobs by CID. Cacheable forever; the CID *is* the ETag. |

Because `PUT` is keyed by content hash it is inherently idempotent and retry-safe,
which removes a whole class of partial-failure handling from the client.

### 3.4 Manifest negotiation on SDA

SDA gains two owner-scoped operations that use SBA underneath:

```
SYNC_OFFER   { basePath, entries: [{ path, cid, size }] }  ->  { want: [cid...] }
SYNC_COMMIT  { basePath, entries: [{ path, cid, ... }] }   ->  { applied, conflicts }
```

The full exchange, for any vault size:

1. **Offer.** Client sends the manifest — every document's path and content hash.
   One request. A 5,000-entry manifest is a few hundred KB.
2. **Want.** Server replies with only the CIDs it does not already hold.
3. **Upload.** Client `PUT`s only those blobs, pipelined on one stream (§4).
4. **Commit.** Server atomically upserts the document rows to point at the CIDs.

This is the git / IPFS / Docker-registry pattern, and it is the right one here for
the same reason it is right there: **the common case transfers nothing.**

Cost comparison for sumi's stated workload:

| Scenario | Today | After |
|----------|-------|-------|
| Cold sync, 500 docs | 500 SDA writes + 500 mailbox messages | 1 offer + 500 pipelined blobs + 1 commit |
| Re-sync, 3 of 500 changed | 500 writes + 500 messages (identical cost) | 1 offer + **3 blobs** + 1 commit |
| Re-sync, 0 changed | 500 writes + 500 messages | 1 offer, **zero bytes of content** |

### 3.5 This also fixes the mailbox problem

Sumi's Finding B — mailboxes filling at 1,000 and failing deposits silently — is a
direct consequence of their "one document = one pointer message" design. At 500
documents per sync, two syncs fill a mailbox.

With manifest sync there is one logical sync event, so there is **one notification
per sync instead of one per document**. A 500× reduction in mailbox pressure, as a
side effect of a change made for throughput reasons. Finding B stops being a
capacity problem and becomes what it should have been: a rare condition that
observability (`SCALABILITY.md` phase B) surfaces when it does happen.

### 3.6 The hard part: block garbage collection

Content-addressed storage moves the difficulty from writing to deleting. A blob may
be referenced by many documents, many versions, and many owners; it can only be
removed when nothing references it.

The `tombstoned` column already in `block_store` suggests mark-and-sweep was the
original intent, and that is the right call over refcounting — refcounts get
corrupted by crashes and require a transaction on every reference change, which
puts a write back on the hot path we are trying to clear.

Sweep runs in the existing maintenance loop: mark blocks unreferenced by any
`documents.content_cid` or `document_versions.content_cid`, respect a grace period
so an in-flight `SYNC_OFFER`/`SYNC_COMMIT` cannot have its blobs swept between the
two calls, then delete in bounded batches (the same batching `SCALABILITY.md` N5
wants for message expiry).

**This must be designed before the first blob is written**, not after. A CAS store
without a working sweep is a disk leak with extra steps.

---

## 4. Pipelined streams

Ceiling layer 2. `Pipeline.HandleStream` builds one `StreamContext`, runs the chain
once, and closes the stream. Every document therefore pays a full yamux stream
open/close, and on a relayed mobile connection that round trip dominates everything
else we are optimising.

The change lands in `go-p2p-forge`: a streaming pipeline mode that loops the
middleware chain until the client half-closes, reusing the connection and the
buffer pool across iterations. SBA `PUT` and `GET` are the first consumers; the
existing six protocols keep the one-shot behaviour and are unaffected.

**Security note, and it is not optional.** Rate limiting today runs as middleware
*inside* the chain, which under a looping pipeline means it runs per message — good.
But any limiter that gets hoisted to per-stream during this work would silently
delete rate limiting entirely, since one stream could then carry unbounded requests.
The limiter must stay per-message, and it must become **cost-based** (§6) rather
than count-based, because "one request" stops being a meaningful unit of work once
a request can carry a 5,000-entry manifest.

---

## 5. Storage layering

Once bodies are content-addressed and immutable, the blob store becomes a pluggable
backend behind an interface:

```go
type BlockStore interface {
    Has(ctx context.Context, cids []string) ([]string, error)  // returns missing
    Put(ctx context.Context, cid string, r io.Reader) error
    Get(ctx context.Context, cid string) (io.ReadCloser, error)
}
```

Two implementations, in order:

1. **Postgres** — the `block_store` table that already exists. Ships first, no new
   infrastructure, immediately correct. Still puts blob bytes through WAL, so it is
   a staging post, not the destination.
2. **Object storage** (S3-compatible) — the destination. Postgres keeps metadata
   only: rows of a few hundred bytes, which is what relational storage is good at.
   Blob durability, replication and horizontal scale become the object store's
   problem, and it is already good at them.

The interface is the point. Landing it in stage 2 means stage 4 is a config change
rather than a migration.

### Migration path

Non-breaking, in this order:

1. Add `documents.content_cid TEXT NULL` alongside the existing `content BYTEA`.
2. Write both — new writes populate the block store *and* the inline column.
3. Read `content_cid` when present, fall back to `content`.
4. Backfill existing rows in bounded batches from the maintenance loop.
5. Stop writing `content`; keep reading it.
6. Drop `content` once the backfill is verified complete.

Steps 2–5 are all live-safe and individually revertible. The wire protocol never
sees the difference: `GET` returns bytes either way.

---

## 6. Admission control, not rate limits

The hardcoded limiters were the wrong mechanism, not just the wrong numbers.
Making `20/min` configurable (`SCALABILITY.md` A4) was a necessary interim step,
but a configured constant is still a constant, and it still needs an operator to
guess a number that our own measurements should be producing.

**This has now landed, and it replaced rate limiting rather than tuning it.**
Per-peer rate limits are off by default. Throughput is governed by
`internal/admission`, which bounds concurrent work instead of work per unit time.

The reason that removes the ceiling rather than raising it: throughput is
concurrency divided by latency. Fix the concurrency and throughput becomes a
function of how fast the work completes — it rises with the hardware and falls on
its own when the database slows, with no number in the config that has to be
revised. Measured on a laptop Postgres: **4,964 documents/sec** from one client
writing in batches, **17,685 documents/sec** across eight. The limit A4 shipped
was 20 per minute.

It is also self-weighting, which quietly solves the cost problem below. A batch
write holds its slot for as long as it takes, so a request that does more work
occupies more capacity, without anyone maintaining a cost model.

The remaining layers:

**Cost-based budgeting.** Still relevant where a *cap* is wanted for non-capacity
reasons — per-tenant billing, or containing a client known to loop. A request's
cost derived from work performed rather than from being one request. The limiters
have `AllowN` for this; nothing uses it yet.

**Capacity-derived bound.** ✅ Landed. The global bound is derived from the
database pool size rather than guessed, so it tracks the resource it protects. This
is `SCALABILITY.md` #12 (backpressure) and it is what "unbounded" actually rests
on: throughput is limited by what the hardware can do, and by nothing else. What
remains is making the bound *adaptive* — widening it while commit latency stays
flat and narrowing it when latency climbs — which needs Stage 3's measurements.

**Per-owner fairness.** ✅ Landed, as a per-peer concurrency bound rather than
weighted fair queueing. The reason to keep limits low was fear that one heavy
client starves the rest; bounding how many slots any single peer may hold removes
that fear directly, and is what makes it safe to ship with no per-peer rate limit
at all. Sumi asked for budgeting by owner peer ID rather than by connection; this
grants it, and their multi-device case works because the bound is keyed by peer ID.

**Shedding, not blocking.** ✅ Landed. A request waits for a slot — that is how a
fast client is paced to the database — but only up to `acquire_timeout`, after
which it is shed with a 503. An unbounded queue converts a throughput problem into
a memory problem and then into an outage, the failure mode `SCALABILITY.md` #12
describes. The shed counter is the signal to add capacity, and it is a measurement
a guessed rate limit could never produce.

---

## 7. The staged plan

Each stage names the ceiling it removes and the ceiling that becomes binding next.
That last column is the point of the table: it is how we know the stage worked, and
it is what stage `n+1` is aimed at.

### Stage 1 — Fix the write path ✅ complete
Correctness and waste. Prerequisite for everything.

- Single-statement conditional `PUT` — folds `If-Match` into the upsert, removes
  both redundant body reads, and closes the lost-update bug (`SCALABILITY.md` N2, N3).
- `octet_length` + pagination for `LIST` — stops reading every body to compute a
  length (N1).
- Transactional `GetNextSequence` (#3), batched expiry `DELETE` (N5).
- Config-drive the per-protocol limiters with burst — the interim step before §6.

**Removes:** ceiling layers 1 (partially) and 3. **Next binding:** per-document
request count — ceiling layer 2.

**Landed** in `e0be9b6` (conditional `PUT`, `LIST`), `c8b0c4a` (sequence
assignment, expiry sweep) and `98216e6` (`BATCH_PUT`, batch submit). Measured
result for the workload that prompted this: a 500-document vault, previously
1,000 requests with a ~25 minute floor from the 20/min write limiter, now syncs
in **10 requests and about 300ms** (`TestVaultSyncBatched`).

A3 also surfaced a transport ceiling that had nothing to do with batching — every
connection stalled after ~256KB — which is now fixed in `go-udx` `d9b1dc7`. See
[`TRANSPORT_WINDOW_BUG.md`](./TRANSPORT_WINDOW_BUG.md). It mattered more than the
stage it was found in: Stage 2 moves blobs in bulk and could not have worked at
all underneath it. Measured after the fix, one connection now sustains 2.8MB of
reads in 132ms where it previously died at 229KB.

**A4** completed the stage. Every limiter is now built from configuration
(`rate_limiting.protocols.*` in the YAML) rather than from a literal, and each
bucket carries a burst allowance separate from its sustained rate — a token
bucket, where the old sliding window could only express "n per window". The MTA
router's own unevicted `map[string][]time.Time` behind a global mutex is gone
with it, closing the last instance of `SCALABILITY.md` #5 and #7.

Note there were **seven** hardcoded sites by the time A4 ran, not six: batch
submission added one in `98216e6`.

A4 left two gaps, and the admission-control work that followed closed both by
changing the mechanism rather than the numbers (§6):

- **Cost was per request.** A `BATCH_PUT` of 100 documents cost one unit, the
  same as a single `PUT`. Concurrency bounds are self-weighting — a batch holds
  its slot for as long as it takes — so this no longer needs a cost model.
- **Limits were per instance**, so a fleet of five gave every client five times
  its budget. A concurrency bound is *correctly* per instance: each one is
  protecting its own database connections, and adding an instance adds real
  capacity. This removes blocker 4 from `SCALABILITY.md` §4 rather than
  deferring it to shared state.

### Stage 2 — Content-addressed sync
The core refactor. This is where the shape of the workload changes.

- `BlockStore` interface + Postgres implementation over the existing table.
- SBA protocol: `HAS` / `PUT` / `GET`.
- SDA `SYNC_OFFER` / `SYNC_COMMIT`.
- `content_cid` migration steps 1–4.
- Block GC design and implementation (§3.6) — **lands with this stage, not after.**
- Pipelined streams in forge (§4).

**Removes:** ceiling layers 1 and 2 entirely; re-sync cost becomes proportional to
change. **Next binding:** Postgres as the blob data plane — WAL and replication
throughput on `block_store`.

**The figures to beat** (`doc/BASELINES.md`, taken at `ca32f10`): a 500-document
vault syncs cold in 117ms over 5 requests, warm in 121ms over 5, and no-op in
4.4ms over 1. The interesting one is warm: it currently costs what cold costs,
because a replace does the same work as a create. Making re-sync proportional to
change is precisely the gap between the warm and no-op rows.

### Stage 3 — Measure, then bound by capacity
No numbers in this roadmap should survive contact with measurement. This stage is
where they get replaced.

**Most of it landed early, as Phase B** (`doc/PHASE_B_PLAN.md`), because Stage 2
needed a before-figure and none existed:

- ~~Prometheus `MetricsCollector` + `MetricsMiddleware`, `/metrics`,
  `/healthz`, `/debug/pprof`.~~ **Done** (B0, B1). Note the forge hook the plan
  counted on is called by nothing inside forge, so `internal/metrics` publishes
  its own series rather than implementing it.
- ~~Throughput, commit latency, pool saturation.~~ **Done** (B1, B2) — request
  counts and latency by protocol × operation × outcome, admission, pool and
  buffer-pool series, plus storage aggregates and mailbox depth. Blob hit rate
  and dedup ratio wait for Stage 2, which is what creates blobs.
- ~~`cmd/ricochet-bench` scenarios for cold sync, warm re-sync, and no-op
  re-sync.~~ **Done** (B5). Numbers in `doc/BASELINES.md`.

Still outstanding:

- Adaptive admission bounds driven by those measurements — widen while commit
  latency is flat, narrow when it climbs. The bound and the shedding exist
  (§6); what is missing is the feedback loop that sets the bound.
- Resource manager and connection manager limits (`SCALABILITY.md` C1, C2) — the
  connection-count half of the target.

**Removes:** the last constant an operator has to guess. **Next binding:**
single-node hardware — which, after §6, is already what binds.

### Stage 4 — Fleet
- Object-storage `BlockStore` implementation. Blob throughput stops being ours.
- Postgres holds metadata only; read replicas for `GET` and `LIST`.
- ~~Shared rate-limit state so a fleet of N does not grant N× the budget.~~
  Dropped: with concurrency bounds as the governor and per-peer rate limits off
  by default, there is no per-instance budget to multiply. Shared state is only
  needed if a deployment turns rate limits on for per-tenant capping, and that
  is a billing concern rather than a capacity one.
- `mailboxCache` coherence; cross-instance notification for private mailboxes.
- Client-side fleet discovery via the GossipSub `Registry` — server-side
  `GetAvailableServers()` already exists at `registry.go:107`, but `pkg/client` has
  no registry integration at all and relies on a static `PreferredServers` list.
- Connection draining and load-balancer health checks.

**Removes:** the single-node ceiling. **Next binding:** metadata write rate on the
Postgres primary.

### Stage 5 — Shard, if measurement says so
Deliberately last, and deliberately conditional. Metadata rows are small and
owner-scoped, so a single primary should sustain this workload well past the point
where the earlier stages have done their job. **Do not build this until stage 3's
metrics show the primary is actually the constraint.**

- Shard `documents`, `feeds`, `collections`, `mailboxes` by owner peer ID.
- Blobs need no sharding scheme — they are content-addressed, which is already a
  distribution key.

**Removes:** the last shared constant. **Next binding:** client bandwidth, which is
where we want it.

---

## 8. Identity: the decision that gates stage 4

Each instance loads one keypair (`server.go:428`) and is addressed by that peer ID.
A fleet therefore has N identities, and clients must know them. Three options:

| Option | Verdict |
|--------|---------|
| **Per-instance identity + client-side discovery** | **Recommended.** The GossipSub `Registry` already announces instances on `/sf-network/services/announce`, and `pkg/client` already models a prioritised server list. The missing piece is small: teach the client to populate that list from the registry instead of from static config. Reuses two mechanisms that already exist. |
| **Shared keypair across instances** | Rejected. libp2p permits it, but the DHT then holds conflicting provider records for one peer ID at many addresses, and connection deduplication misbehaves. We spent real time on DHT/relay stability already (see the routing-table filter work); do not reopen it. |
| **Front-door proxy tier** | Rejected for now. Adds a hop and a component to operate, and solves a problem the registry already solves. |

Nothing else in stage 4 can be designed until this is settled, which is why it is
called out separately.

---

## 9. What stays bounded

Stating these plainly, because a roadmap that claims everything scales is not a
roadmap:

- **Per-document ordering.** Writes to a single `(owner, path)` serialise. This is
  correct and we are not removing it — but it is per-document, so it never limits
  aggregate throughput across a vault.
- **A single document's size.** The 10MB cap stays until there is a reason to
  chunk. Chunking a document into multiple blocks is a natural extension of CAS,
  but it is not needed for the target workload.
- **Manifest size.** A 5,000-entry offer is a few hundred KB; a 5,000,000-entry one
  is not. Manifests need chunking by path prefix past some threshold — set that
  threshold from stage 3's measurements, not from a guess here.
- **Client upload bandwidth.** After stage 4 this becomes the binding constraint
  for cold syncs, and that is the correct place for it to sit.

---

## 10. Open decisions

1. **CID format.** IPFS-compatible multihash, or a plain `sha256:` prefix?
   Interoperability against simplicity. Sumi has no IPFS dependency, but
   `FUTURE_ENHANCEMENTS.md` §6's CRDT work might.
2. **Does `SYNC_COMMIT` need to be atomic across the whole manifest**, or is
   per-document atomicity sufficient? Full-manifest atomicity is a long transaction
   proportional to vault size — exactly the shape we are trying to eliminate.
   Per-document is cheaper and probably adequate, but it makes a partial commit
   observable to a concurrent reader.
3. **Conflict semantics for `SYNC_COMMIT`.** Per-document `If-Match` in the
   manifest, or last-writer-wins with conflicts reported back? This intersects with
   the dead `version_vector` column and the CRDT plan in §6.
4. **Blob GC grace period.** Long enough that a slow client's offer→commit cannot be
   swept mid-flight; short enough that deleted content actually goes away.
5. **Does the mailbox pointer message survive at all?** With manifest sync, the
   notification could be a presence/notify event rather than a stored message,
   which removes mailbox depth from the document-sync path entirely.

---

## 11. Relationship to the sumi report

Their five asks, against this plan:

| Their ask | Where it lands |
|-----------|----------------|
| 1. Batch/bulk write | **Exceeded.** Stage 1 config work covers the interim; stage 2 removes the per-document request entirely rather than batching it. |
| 2. Configurable limits + burst | Stage 1 (interim), replaced by capacity-derived admission control in stage 3. |
| 3. Operator mailbox observability | Stage 3. Also largely obviated for their workload by §3.5. |
| 4. Wire `enable_metrics` | Stage 3, and cheap — the forge hook exists. |
| 5. Per-owner, shard-friendly limits | Stage 3 (fair queueing) and stage 4 (shared state). |

Their pacing workaround — ≤18 documents per pass, one pass per ~65 seconds — should
be removed entirely once stage 2 ships. Worth telling them explicitly: after
stage 2, client-side pacing becomes counterproductive, because the server wants the
whole manifest at once in order to tell them how little they actually need to send.
