# Document Sync: Requirements & Learnings (from the sumi integration)

**Author:** sumi team (a document-centric notes app syncing over go-ricochet)
**Audience:** go-ricochet maintainers
**Status:** field report + prioritized requests, grounded in production debugging
**Related:** [`MAILBOX_LIFECYCLE_AND_ACLS.md`](./MAILBOX_LIFECYCLE_AND_ACLS.md) (sibling field guide: mailbox lifecycle, retention, ACL semantics — the "why bidirectional sync silently stalled" learnings), [`SCALABILITY.md`](./SCALABILITY.md), [`FUTURE_ENHANCEMENTS.md`](./FUTURE_ENHANCEMENTS.md)

This is a consumer's-eye view. We hit real walls syncing document vaults through go-ricochet,
traced each to its source, and want to feed that back so the team can prioritise. Where a need
maps to something you've **already identified** in `SCALABILITY.md` / `FUTURE_ENHANCEMENTS.md`, we
say so — the goal is to ground your existing roadmap in concrete workload data, not to re-derive it.

---

## 1. Who we are and what our workload looks like

sumi is a note-taking app. A user's **vault** is a folder of `.org` documents synced across their
own devices (and, when shared, to a peer's device). We use go-ricochet as:

- **the document store (SDA)** — each `.org` doc is a `PUT` at `<vaultId>/<docPath>` (ciphertext); the
  peer `GET`s it. This is our bulk data path.
- **the mailbox (MSA→MTA→MDA / MAA)** — after each doc `PUT`, we deliver a small "pointer" message
  announcing the new version, so the peer knows to fetch it.

So **one document = one write to SDA + one message to the mailbox.**

Our traffic has two very distinct shapes:

| Phase | Shape | Example |
|-------|-------|---------|
| **Initial sync** | a **burst** — the entire vault at once | 54 docs → 54 SDA writes + 54 mailbox messages, back-to-back |
| **Steady state** | a **trickle** — only edited docs | 1–3 writes when a note changes |

The initial-sync burst is where everything hurt. Real vaults are hundreds to thousands of docs.

---

## 2. What we hit (with real numbers)

We spent a long debugging session on "shared vault shows 0 files on the peer." The root causes were
all server-side limits interacting badly with a burst workload. Three findings:

### Finding A — the doc-store write cap is 20/min, hardcoded, and unreachable from config

`internal/protocol/sda/handler.go:94`:

```go
limiter := middleware.NewDualBucket(time.Minute, 100, 20)   // 100 reads/min, 20 WRITES/min
```

A 54-doc initial sync issues 54 writes. At **20 writes/min**, the first ~20 succeed and the rest get
`429`. The math was exact in our logs: 54 − 20 = **34 throttled**. A vault can't sync in one pass.

The trap: our operator had raised `rate_limiting.max_requests_per_window` to 1000 in the YAML and
expected relief — but that knob **only feeds the MTA router** (`internal/server/server.go` →
`mta.NewRouter(...)`). The per-protocol handler limits — **SDA, SFA, SCA at `(100, 20)`; MSA & MAA at
`100`; MMA at `50`** — are all **hardcoded literals** with no config path:

| Protocol | Site | Limit (per minute) | Config-reachable? |
|----------|------|--------------------|-------------------|
| SDA (documents) | `sda/handler.go:94` | 100 read / **20 write** | ❌ hardcoded |
| SFA (feeds) | `sfa/handler.go:99` | 100 read / 20 write | ❌ hardcoded |
| SCA (collections) | `sca/handler.go:82` | 100 read / 20 write | ❌ hardcoded |
| MSA (submit) | `msa/handler.go:24` | 100 | ❌ hardcoded |
| MAA (access) | `maa/handler.go:27` | 100 | ❌ hardcoded |
| MMA (admin) | `mma/handler.go:118` | 50 | ❌ hardcoded |
| MTA (routing) | `mta/router.go` via `server.go` | `rate_limiting.*` | ✅ **the only one** |

**So the one limit an operator can tune is the one that wasn't binding us.** This cost us days.

### Finding B — mailbox fills at 1000, and the client can't see it coming

`storage.max_messages_per_mailbox` (default **1000**) is enforced at `internal/mda/mailboxes/private.go:45`
and `shared.go:55` → `MailboxFullError`. When a peer stops draining a mailbox (e.g. the user removed a
shared vault on that device) while the sender keeps depositing pointers, the mailbox climbs to 1000 and
**every subsequent deposit fails** — silently, from the sender's perspective (it just sees delivery fail).

We had no way to observe this. `getMailboxInfo` (`mma/handler.go:556`) *does* return a live
`messageCount`, but it's **owner-scoped** (`verifyOwner` + `FindMailbox(callerID, …)`) — a peer can only
query *its own* mailboxes, and there's **no operator/global view** and **no CLI**. We ended up adding a
`GetMailboxInfo` wrapper to `pkg/client` ourselves just to read the number.

> **Update (later session):** the deeper cause of mailboxes climbing to the cap was
> on us — a client must **delete** messages it has consumed (`retrieveMessages`
> doesn't), or the inbox fills and rejects all delivery *both ways*. That, plus the
> shared-vs-private ACL semantics that gate who may deliver, are written up as a
> client-integration field guide in
> [`MAILBOX_LIFECYCLE_AND_ACLS.md`](./MAILBOX_LIFECYCLE_AND_ACLS.md). With those
> understood, bidirectional device sync works.

### Finding C — `enable_metrics: true` appears to be a no-op

`config.go` defines and defaults `EnableMetrics = true`, but we found **no `/metrics` endpoint, no
Prometheus registry, no exporter** wired anywhere in the tree. As integrators we had zero server-side
visibility into request rates, throttle counts, mailbox depths, or storage — we were reverse-engineering
limits from client-side `429`s. (This is your `FUTURE_ENHANCEMENTS.md` §5.2, still open.)

### What we did on our side (so you know where the client stands)

To stop making it worse, sumi now **paces** its writes: ≤18 docs per pass, one pass per ~65s, and it
stops the instant it sees a `429` rather than hammering. That made sync *reliable and polite* — but it
also means a 54-doc vault takes ~2 min and a 500-doc vault ~30 min. **The client is now well-behaved;
the throughput ceiling is entirely server-side.** We'd rather remove the ceiling than pace under it.

---

## 3. What we need (prioritised, mapped to your existing roadmap)

### Need 1 — a write path that isn't O(docs) requests  ⭐ highest leverage

The core mismatch: document sync is **bursty and batchable**, but every doc costs one rate-limited
request. The single change that would help us most is a **batch/bulk write** — one request carrying N
documents (and, symmetrically, a batch mailbox-deposit). This:

- decouples *doc count* from *request count*, so a 500-doc vault is a handful of requests, not 500;
- **keeps** your abuse protection (still rate-limited *per request* — a request just carries more);
- amortises the JSON-over-libp2p per-message overhead you flag in `SCALABILITY.md` #11.

This is, in our opinion, the highest-value item on this page. It sidesteps the rate-limit problem for
our dominant workload without weakening the server. (Adjacent to `FUTURE_ENHANCEMENTS.md` §7.7
content-addressed storage and the CRDT/replication thinking in §6.)

### Need 2 — make the per-protocol limits configurable, with burst allowance

Short of batching (or alongside it): lift the six hardcoded limiters into config, and give the write
buckets a **burst** so an initial-sync burst is allowed while sustained abuse still isn't. A token
bucket with `rate` + `burst` (e.g. 20/min sustained but 200 burst) fits document sync far better than a
flat sliding window. Bonus: it fixes the **rate-limit memory leak** you already flagged (`SCALABILITY.md`
#5) and the **global MTA mutex** (#7), since a per-owner token bucket is constant-memory and shardable.

Please also budget by **owner peer ID**, not by connection — our multi-device sync means one identity
legitimately drives a whole vault's worth of writes.

### Need 3 — operator observability for mailboxes and limits

We need to answer "is this mailbox full?" and "which mailboxes are hot?" **without owning them.**
Concretely, any of:

- an **operator-scoped** admin op (MMA extended with an operator credential that bypasses `verifyOwner`)
  exposing per-mailbox `messageCount` / "top-N fullest" / per-owner storage — this is your
  `FUTURE_ENHANCEMENTS.md` §7.9 Admin API;
- **wire `enable_metrics`** to a real `/metrics` endpoint (per-protocol request + throttle counters,
  mailbox depth histogram, storage gauge) — your §5.2 Observability;
- a **server-side CLI** (`ricochet admin mailbox-info <owner> <folder>`, `… top-mailboxes`,
  `… capacity`) reading storage directly. `cmd/ricochet` has no admin subcommands today.

Even one of these would have turned our multi-day investigation into a single query.

### Need 4 — a horizontal-scaling story for bursty, owner-scoped writes

You already note (`SCALABILITY.md` #10, `FUTURE_ENHANCEMENTS.md` §7.10) that document/feed/collection
data is **owner-scoped and naturally shardable** — that's exactly right for us: a vault belongs to one
owner. Our ask is that the scaling design treat **write throughput as elastic**: shard mailboxes/docs
by owner peer ID, and if you go multi-instance, move rate-limit state to a shared store (Redis) so caps
stay coherent across nodes rather than each instance enforcing its own local 20/min. The goal state for
us is "throughput scales with the fleet," so the per-node write cap stops being the ceiling on how fast
a user's vault can sync.

---

## 4. Priority, from our seat

| # | Ask | Maps to | Why it matters to us |
|---|-----|---------|----------------------|
| 1 | **Batch/bulk write** op (SDA + mailbox deposit) | new; adjacent §7.7 | Removes the per-doc request tax entirely — biggest single win |
| 2 | **Configurable** per-protocol limits + **burst** tokens | `SCALABILITY.md` #5/#7 | Unblocks operators today; right shape for bursty sync |
| 3 | **Operator mailbox observability** (op-scoped admin / metrics / CLI) | §7.9, §5.2 | We're flying blind on mailbox depth + throttle state |
| 4 | **Wire `enable_metrics`** to a real endpoint | §5.2 | The flag reads as on but exports nothing |
| 5 | **Per-owner, shard-friendly** rate-limit state | `SCALABILITY.md` #10, §7.10 | So throughput scales horizontally instead of capping per node |

If only one thing ships: **#1 (batch write).** It makes the rate limit a non-issue for our workload
without weakening the server.

---

## 5. Appendix — exact code anchors

Everything above, pinned to source (as of this writing):

- **Doc-store write cap (the wall):** `internal/protocol/sda/handler.go:94` — `NewDualBucket(time.Minute, 100, 20)`.
- **Same cap on feeds/collections:** `sfa/handler.go:99`, `sca/handler.go:82`.
- **Mailbox agents:** `msa/handler.go:24` (100), `maa/handler.go:27` (100), `mma/handler.go:118` (50).
- **Only config-driven limiter (MTA):** `internal/mta/router.go` (`checkRateLimit`, `RateLimitError` at
  `:158`), wired from `internal/server/server.go` via `rate_limiting.{window_minutes,max_requests_per_window}`.
- **`429` mapping:** doc handlers emit `StatusTooManyRequests` **only** on `ErrRateLimited`
  (`sda/handler.go:145`, `sfa:139`, `sca:134`) — every other failure is a 500, so a `429` is
  unambiguously "rate limited."
- **Mailbox cap enforcement:** `internal/mda/mailboxes/private.go:45`, `shared.go:55` → `MailboxFullError`;
  configured by `storage.max_messages_per_mailbox` (default 1000).
- **Owner-scoped mailbox insight (no operator view):** `internal/protocol/mma/handler.go:556`
  (`handleGetMailboxInfo`, returns `messageCount`) guarded by `verifyOwner` (`:201`) + `FindMailbox(callerID,…)`.
- **Metrics flag with no exporter found:** `internal/core/config.go:48` / `:157` (`EnableMetrics`).
- **Client admin surface we lean on:** `pkg/client/client.go` — `ListMailboxes`, `QueryCapacity`,
  `CreateMailbox`, `DeleteMailbox`, and a `GetMailboxInfo` wrapper we added for `messageCount`.

---

*Written from the sumi side after tracing a real "0 files synced" failure to these limits. Happy to
pair on any of it — especially the batch-write API, which we'd adopt immediately.*
