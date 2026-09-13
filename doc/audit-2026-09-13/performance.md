# Appendix B: Performance and scalability audit (full findings)

Part of the [release audit of 2026-09-13](README.md). Audited at commit `65cfa9e` (forge `df6668c`). `forge/` = `../go-p2p-forge`.

## Tooling

- `go vet ./...` — clean.
- `go test -bench=. -run='^$' ./...` — **no `func Benchmark*` exists anywhere in the repo**. The only perf tooling is `cmd/ricochet-bench` (end-to-end).

---

## Findings, ranked

### C1 — Nothing bounds connections, streams, or per-stream memory; the 10k/50k connection presets are decorative — **Critical**, CONFIRMED

- `forge/host/host.go:89` — `libp2p.ResourceManager(&network.NullResourceManager{})`. No rcmgr limits on conns, streams, memory, or FDs.
- `forge/host/host.go:85-100` — no `libp2p.ConnectionManager(...)`; no low/high watermark, no trimming.
- `internal/core/config.go:38,427,475,485` — `MaxConcurrentConnections` is parsed and preset (10,000 / 50,000) but read nowhere outside `config.go`. Same for `WorkerThreads` (`:41,430,488`).
- libp2p yamux default (`go-libp2p@v0.47.0/p2p/muxer/yamux/transport.go:32`) sets `MaxIncomingStreams = math.MaxUint32`, so a single connection may open unlimited streams.
- `forge/pipeline.go:39-61` — one goroutine per inbound stream, `Ctx: context.Background()`, **no read/write deadline is ever set on a server stream**.
- `forge/codec_middleware.go:11-23` → `forge/codec/frame.go:66-86` — the frame length prefix is trusted up to 10 MB and `pool.Get(length)` allocates (`codec/pool.go:102-105`, tier -1 = plain `make`) **before** any byte of body arrives and **before** admission (`admission.Middleware` sits after `FrameDecodeMiddleware` in every pipeline, e.g. `internal/protocol/sda/handler.go:164-166`).

Why it matters: a peer can open N streams, send a 4-byte header claiming 10 MB on each, and never send the body; each stream holds a goroutine and a 10 MB heap allocation forever. Admission control never sees it. The "Production = 10,000 connections" and "High-Capacity = 50,000" rows in README are not backed by any enforcing code path.

Fix: (1) real `rcmgr` limits scaled from `MaxConcurrentConnections` and a `connmgr.NewConnManager(low, high, grace)` in forge host; (2) `SetReadDeadline` in `FrameDecodeMiddleware` (or `HandleStream`) and `SetWriteDeadline` in the response writers; (3) cap yamux `MaxIncomingStreams`; (4) move admission (or a cheap per-peer stream semaphore) *before* the frame read, or read the frame into the pool only after admission.

### C2 — MAA retrieve deletes non-persistent messages before the response is written; default retrieve is unbounded; single 10 MB frame cap → silent data loss at scale — **Critical**, CONFIRMED

- `internal/mda/mailboxes/private.go:66-88` — `RetrieveMessages` reads rows, then immediately `DeleteMessages` for every non-persistent one, returns the slice.
- `internal/protocol/maa/handler.go:235-257` — only *after* that does the handler encode and set `sc.Response`; `maaResponseWriter` (`:94-99`) writes via `codec.WriteFrame`, which fails with `frame too large` above `MaxFrameSize` (`forge/codec/frame.go:47-49`) and merely logs.
- `internal/storage/postgres/postgres.go:280-283` — `LIMIT` is applied only if `maxMessages != nil`; `core.RetrieveRequest.MaxMessages` is `*int` with no default, so a plain retrieve returns the whole mailbox (up to `max_messages`=1000 rows × up to 10 MB payloads, each base64-inflated by 33% in `Message.ToJSON`).
- `maa/handler.go:246` passes `hasMore=false` unconditionally — no pagination signal exists.

Fix: enforce a server-side default/max page size; delete (or mark delivered) only after the frame write succeeds — or better, switch to explicit `markDelivered`/ack semantics; stream frames per message (forge already has `FrameIterator`) so the 10 MB cap applies per message not per mailbox.

### H1 — Message submission costs 5 DB round-trips, a `COUNT(*)`, and a trigger double-update of the mailbox row (public mailboxes: plus two DELETEs with a sort) — **High**, CONFIRMED

Path per single `MSA submit` (steady state, mailbox cached):
1. `internal/mda/mailboxes/private.go:41` (`shared.go:51`) — `GetMessageCount` → `SELECT COUNT(*) FROM stored_messages WHERE mailbox_id=$1` (`postgres.go:315-319`): an index-range scan of up to `max_messages` entries **per submission**.
2. `postgres.go:208` `Begin` (RT), `:215-221` `UPDATE mailboxes ... RETURNING` (RT, takes row lock), `:229-240` `INSERT` (RT), `:245` `Commit` (RT).
3. `schema.sql` trigger `trg_message_access` → `update_mailbox_access()` runs `UPDATE mailboxes SET last_access_at = NOW()` **inside the same transaction that already updated that row** → a second tuple version of the `mailboxes` row per message; on a 100-message batch that is 200 dead tuples on one hot row, plus lock-hold time extended by the trigger.
4. Public mailboxes: `public.go:60` calls `EnforceRetentionPolicy` on **every** store → `postgres.go:1006-1011` time DELETE + `:1018-1027` count DELETE that does `ORDER BY sequence_number DESC OFFSET $2` over the whole mailbox per message.

Plus first-touch: `GetOrCreateMailbox` (`postgres.go:96-126`) is `FindMailbox` then `INSERT` (2 more RTs, and see H6).

Fix: drop the trigger (the `UPDATE ... RETURNING` can set `last_access_at = NOW()` in the same statement); enforce the cap in SQL or keep a `message_count` column maintained by the same statement instead of a separate `COUNT(*)`; move public-mailbox retention to the maintenance loop; use `pgx.Batch` or a single CTE to make submit one round trip.

### H2 — Info-level logging on every request in the hot path — **High**, CONFIRMED

One MSA submit emits four Info lines: `internal/protocol/msa/handler.go:258`, `internal/mta/router.go:60`, `internal/mda/delivery.go:175`, `:207`. One MAA retrieve emits three: `delivery.go:222`, `:241`, `maa/handler.go:257`. Also `postgres.go:911` logs the **full SQL text and args** at Info for every directory browse, and `sda/handler.go:906,935` log every browse twice. At the measured 5.4k submits/s that is ~22k synchronous `slog` writes/s through one handler mutex to stdout.

Fix: demote all per-request lines to Debug.

### H3 — Push notifications: unbounded goroutines, a dial attempt per message, and a GossipSub topic leak — **High**, CONFIRMED

- `internal/mda/notifier.go:50-52` — `go n.notifyDirect(...)` / `go n.notifyPubSub(...)` per delivered message, no bound (SCALABILITY.md #8, still open).
- `notifier.go:59-67` → `internal/presence/monitor.go:34-68`: `CheckPresence` **never reads the cache** before acting; if the peer is not currently connected it calls `host.Connect` (a real dial, 5 s timeout) — one dial per message to an offline-but-recently-seen recipient. A 100-message batch to an offline peer = 100 concurrent dials. (Note: in practice `n.presence` is nil because of the wiring-order bug in Appendix C M2, so today the direct path skips this and always attempts the stream.)
- `notifier.go:99` → `forge/node/node.go:155-191`: `JoinTopic` also **subscribes** (`:181`) and nothing ever reads or leaves mailbox topics; each shared/public mailbox that receives a message adds a `Topic` + `Subscription` to `sync.Map` for the life of the process, and the unread subscription's buffer fills and drops.

Fix: bounded worker pool / channel with drop-on-full; read the presence cache (TTL 30 s) before dialing and only dial when unknown; publish via `pubsub.Topic` without subscribing (or `Join` once and `Publish`, leave on idle); coalesce notifications per (owner, folder) within a window.

### H4 — Presence heartbeat serializes every connected peer ID; and every connected peer is falsely marked offline after 120 s — **High**, CONFIRMED

`internal/presence/service.go:283-296` builds `OnlinePeerIDs` from the full `connectedPeers` map every `HeartbeatInterval` (60 s). At 10,000 connections that is a ~540 KB JSON message per minute fanned out to every subscriber; go-libp2p-pubsub `DefaultMaxMessageSize = 1<<20` means at ~18k peers `Publish` fails and the warning at `:305` is the only symptom.

Related correctness bug: `service.go:330-341` `checkTimeouts` evicts any peer whose `connectedPeers[pid]` timestamp is older than 120 s, but that timestamp is only ever written at connect (`:103`, `:152`) — so every still-connected peer is marked Offline and removed after two minutes, and an `Offline` event is broadcast for it.

Fix: heartbeat carries only counts/sequence; presence is queried per peer or delivered as deltas; refresh `connectedPeers[pid]` on activity or drop the timeout path when the notifiee is authoritative.

### H5 — Handlers run with `context.Background()` and admission slots are held by clients that have gone away — **High**, CONFIRMED

- `forge/pipeline.go:46` — `sc.Ctx = context.Background()`; never cancelled on stream reset/close.
- `internal/admission/middleware.go:18` — `Acquire(sc.Ctx, ...)`, so a disconnected client still waits the full 5 s `AcquireTimeout` (`config.go:135`) and then, if admitted, runs the handler against nobody.
- MSA (`msa/handler.go:149,265`), MAA (`:235,265,284,318,342`), MMA, SFA (`sfa/handler.go:219,269,305,...`), SCA (`sca/handler.go:237,...`) all call storage with a bare `context.Background()` — **no timeout**. Only SDA uses 30 s/60 s timeouts (`sda/handler.go:298,366,601`). No `statement_timeout` is set on the pool either (`postgres.go:53-54`).

Why: a slow or wedged Postgres holds every admission slot (100 by default) indefinitely; the server then sheds everything with 503 while doing nothing.

Fix: derive `sc.Ctx` from the stream (cancel on close) and apply a per-request timeout in one shared middleware; set `statement_timeout` / `pool_max_conn_lifetime` in the DSN.

### H6 — Check-then-insert races on the write path — **High**, CONFIRMED

- `postgres.go:96-126` `GetOrCreateMailbox`: `FindMailbox` then plain `INSERT`. Two first-ever deliveries to the same new recipient race on `uq_mailbox_owner_folder`; the loser returns a unique-violation error to the sender (a failed submit, not a retry). Needs `INSERT ... ON CONFLICT DO NOTHING RETURNING` + re-select, as `CreateFeed` (`feeds.go:31-39`) already does.
- `postgres.go:622-649` `PatchDocument`: `GetDocument` outside any transaction, compares `ifMatch`, then calls `PutDocument(..., ifMatch=nil)` — the CAS that A2 fixed for PUT does not apply to PATCH; concurrent patches lose updates.

### M1 — HISTORY, HEAD and version GET load full document bodies they don't need — **Medium**, CONFIRMED

- `sda/handler.go:767-795` + `postgres.go:714-749`: `GetDocumentHistory` first calls `GetDocument` (full body) then selects `content` for **every version**, no `LIMIT` unless `maxVersions` is set, purely so the handler can do `Size: len(v.Content)` (`:793`).
- `sda/handler.go:441-486` HEAD: `GetDocument` pulls the body to report `Content-Length: len(doc.Content)` (`:482`).
- `postgres.go:751-756` `GetDocumentAtVersion` loads the current body just to get `doc.ID`.

Fix: `octet_length(content)` projections, an id-only lookup helper, and a default/max limit on history.

### M2 — SCA QUERY does two full filter evaluations, unindexed JSON sorts and OFFSET paging — **Medium**, CONFIRMED (explains the 5× baseline gap)

`internal/storage/postgres/collections.go:296-317`: `SELECT COUNT(*) ... WHERE <filter>` then `SELECT ... WHERE <filter> ORDER BY content->>'field' LIMIT/OFFSET`. With `sortField` the sort is over an expression no index covers (the GIN index only helps `@>`), and `OFFSET` pagination is O(offset). `ListCollectionKeys` (`:236-242`) also uses OFFSET. The bench's `QueryCollection(...{})` returns 50 items + a `COUNT(*)` over a collection that grows by one row per request — hence `sca` at 1,076 req/s vs ~5.5k for the rest. Fix: drop the count (or `COUNT(*) OVER()` on the page), keyset paginate on `(sort expr, key)`, add expression indexes for known sort fields.

### M3 — Directory browse ignores the FTS index and logs SQL per call — **Medium**, CONFIRMED

`postgres.go:853-860`: `ILIKE '%q%'` on `display_name`/`bio` — sequential scan; `idx_directory_fts` (GIN `to_tsvector`) in `schema.sql` is never used. Plus the Info log at `:911`. Use `@@ plainto_tsquery(...)` or a `pg_trgm` GIN index if substring semantics are required.

### M4 — Full-table aggregate scans on a 5-minute timer and per operator request — **Medium**, SUSPECTED impact / CONFIRMED path

`stats.go:56-76` joins `mailboxes` to all of `stored_messages` with `SUM(octet_length(payload))` every `CleanupInterval` (`server.go:553-567`, default 5 min; dev preset 1 min). `operator.go:38-63,99-118` do the same scan per `/ops/*` hit, bounded only by the 10 s query timeout. On a 50 GB production table this is a sustained background load. Consider a maintained `message_count`/`bytes` column (which would also remove the `COUNT(*)` in H1) or `pg_stat`-based estimates.

### M5 — `mailboxCache` is unbounded and stale — **Medium**, CONFIRMED

`internal/mda/delivery.go:96,129-171`: `map[string]mailboxes.Mailbox` with no eviction. The cached `MailboxRecord` is never refreshed: `mma/handler.go:524-567` `updateConfig` writes `max_messages` to the DB but `private.go:45` keeps comparing against the cached value, so a raised or lowered cap is ignored until restart. `PerformMaintenance` (`delivery.go:302-319`) enforces retention only for *cached* public mailboxes, so nothing is retained for mailboxes untouched since boot.

### M6 — Every SDA request is JSON-parsed 4 times and copied twice — **Medium**, CONFIRMED

For one `PUT` of a 6 MB body (8 MB frame): `FrameDecodeMiddleware` copy into pool → `isWriteClassifier` full `json.Unmarshal` (`sda/handler.go:173-180`) → `commonValidation` full `json.Unmarshal` (`:239`) → `OperationRouter` full unmarshal into `map[string]json.RawMessage` (`forge/middleware/router.go:32-33`) → `JSONDeserialize[DocRequest]` full unmarshal (`:149-157`) → `base64.DecodeString` (`:348`). SFA's classifier does `strings.Contains` over the whole frame (`sfa/handler.go:133-138`). MMA re-marshals the entire request (`mma/handler.go:193-211`) and MAA does the same for `defaultOperationType` (`maa/handler.go:120-137`). Fix: peek the operation with a streaming decoder once and stash it in `sc`; route on the decoded struct; long term, move bodies out of JSON.

### M7 — pgx pool: `ConnectTimeout` ignored, no statement timeout, no min conns — **Medium**, CONFIRMED

`postgres.go:53-54` builds the DSN with only `pool_max_conns`; `core.PostgresConfig.ConnectTimeout` (`config.go:392,423`) is never applied. No `statement_timeout`, `pool_min_conns`, `pool_max_conn_lifetime`.

### M8 — Graceful drain is a no-op for ricochet — **Medium**, CONFIRMED

`internal/server/server.go:453-461` registers handlers on the host directly and never calls `Pipeline.WithActiveStreams`. `forge/forge.go:287-301` `Stop` removes handlers from `s.handlers` (empty for ricochet) and waits on an untouched WaitGroup, so in-flight requests are cut when `node.Close()` runs. Only the ops `DrainDelay` sleep (`server.go:157-163`) provides any drain.

### M9 — `GetMultiFeedEntries` is N+1 on feed lookup — **Medium**, CONFIRMED

`feeds.go:272-291`: a sequential `GetFeed` round trip per batch entry (up to 50) before the parallel entry fetch. One `WHERE (owner_peer_id, path) IN (...)` resolves all.

### L1 — Registry map grows from unauthenticated announcements — **Low**, CONFIRMED
`internal/registry/registry.go:227-229` stores any `ServerID` any peer publishes; never pruned.

### L2 — Unused/inert config the README advertises — **Low**, CONFIRMED
`WorkerThreads`, `ConnectionTimeout`, `MessageTimeout`, `MaxConcurrentConnections` (server side) are parsed only. README preset table says "Rate Limit 100/min" and "Workers 4/8"; code defaults rate limits to *off* (`config.go:320-337`) and has no worker pool.

### L3 — `LogDHTStatus` prints every routing-table peer ID every 5 min — **Low**, CONFIRMED
`forge/node/node.go:213-228`.

---

## Claims vs. code

| Claim | Where | Reality |
|---|---|---|
| Production = 10,000 conns, High-Capacity = 50,000 | README:258, `config.go:475,485` | No enforcing code (C1). |
| "Rate Limit 100/min" all presets | README:260 | Defaults are unlimited (`config.go:320-337`); forge `MaxRequestsPerWindow:100` (`forge/config.go:44`) is never used by ricochet. |
| "Workers 4 / 8" | README:261 | `WorkerThreads` unused. |
| Sample output 81 req/s | README:412-428 | Stale; BASELINES measures 5,428. |
| "Each worker creates its own libp2p host and client connection" | README:435 | **True**: `cmd/ricochet-bench/main.go:373-420` calls `libp2p.New` per worker. |
| BASELINES `maa retrieve` 7,886 req/s | doc/BASELINES.md | Mostly empty retrieves: 100 seeded non-persistent messages per worker (`main.go:446-453`), `WithMaxMessages(10)` (`:550`), private mailbox deletes on read → after 10 requests per worker the remaining 40 of 50 measure an empty mailbox. |
| BASELINES `msa` figure | | Sends to a random recipient never drained; at 1,000 per worker recipient submits start failing with 507 — a `-duration` run will hit it. |
| `sync-noop` 115,702 documents/s | doc/BASELINES.md | Derived: 500 metadata rows in one 4.4 ms LIST; no document bytes move. Fine as a ratio, misleading as "documents/s". |
| Admission control "governs throughput" | doc/SCALE_ROADMAP.md §6 | Bench uses `-c 10` workers, each sequential, so at most 10 in flight against a bound of 100: the baselines never exercise admission, shedding, or the pool ceiling. |
| "17,685 documents/sec across eight" | SCALE_ROADMAP §6 | Not reproducible from any committed scenario. |
| SCALABILITY.md #5/#7 "MTA limiter fixed" | | Confirmed: `mta/router.go` now uses the forge token bucket with idle eviction. |
| N1/N2/N3/N5 fixed | | Confirmed for LIST, PUT and expiry. Not fixed for HISTORY/HEAD (M1) or PATCH (H6). |

---

## Checked and found SOUND

- `StoreMessage` sequence assignment: `UPDATE ... RETURNING` inside the tx (`postgres.go:207-251`).
- `PutDocument`: single transaction, `SELECT ... FOR UPDATE` without `content`, If-Match under the lock, server-side `INSERT ... SELECT` archive copy (`postgres.go:533-620,1115-1149`).
- `ListDocuments`: keyset pagination, bounded 1000/5000, served entirely by `uq_document_owner_path` (`postgres.go:670-712`).
- Expiry sweep: batched 5000 × 200 with ctx checks (`postgres.go:966-1002`), served by `idx_messages_expires`.
- Indexes cover: mailbox lookup `(owner, folder)`; message-by-id ops via `uq_message_id`; retrieve ordering via `idx_messages_priority`; ACL `(mailbox_id, peer_id)`; cursors `(mailbox_id, reader)`; feed entries; collection items `(collection_id, key)` + GIN; directory cursor matches the row-comparison query exactly.
- Feeds/collections writes are transactional with `UPDATE ... RETURNING` / `FOR UPDATE`; `CreateFeed` is a proper upsert.
- `GetMultiFeedEntries` bounds fan-out with a 10-slot semaphore.
- Admission controller: sharded per-peer state freed when refs hit zero, timer stopped, idempotent release, ctx-cancel not counted as shed (`internal/admission/controller.go:160-260`).
- Forge token buckets: 64-way sharded, lazy refill, idle eviction goroutine.
- Metrics: bounded labels (operation set only after route match), peer ID never a label, test enforces it (`internal/metrics/collectors_test.go:171-207`).
- Buffer pool tiers 4 KB/64 KB/1 MB with `sync.Pool`, released in `HandleStream` defer.
- `ServerStats` uses `octet_length` and is sampled off the request path; `/ops/*` pages are clamped to 500.
- Rate limiters and presence/registry loops all stop on ctx / `Close`; tickers are deferred-stopped.
- Client sets stream deadlines from `MessageTimeout` (`pkg/client/client.go:146-150`).
- UDX transport: window bug is fixed; per-stream window auto-tunes to 4 MB max.

---

## Top 5 changes before public release

1. **Bound the network edge** (C1): rcmgr limits + ConnManager in `forge/host/host.go` scaled from `MaxConcurrentConnections`, stream read/write deadlines in the pipeline, yamux `MaxIncomingStreams`, and admission before the 10 MB frame allocation.
2. **Fix retrieve semantics** (C2): default + max page size, `hasMore`, delete-after-write (or ack-based deletion), per-message frames.
3. **Collapse the submit path** (H1 + H6): drop `trg_message_access`, fold `last_access_at` and the capacity check into the `UPDATE ... RETURNING`, make `GetOrCreateMailbox` an upsert, move public-mailbox retention to maintenance.
4. **Request lifecycle**: derive `sc.Ctx` from the stream and add one timeout middleware for all six protocols; set `statement_timeout`/`connect_timeout` on the pool; wire `WithActiveStreams` so drain actually drains (H5, M7, M8).
5. **Silence and bound the side channels**: per-request Info logs to Debug (H2); bounded notifier with cache-first presence and no per-mailbox pubsub subscription (H3); heartbeat without the peer-ID list and fix the 120 s false-offline eviction (H4).
