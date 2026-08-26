# Go-Ricochet Scalability Assessment

## Critical Issues

### 1. `NullResourceManager` disables all libp2p limits

`internal/p2p/host.go` uses `network.NullResourceManager{}`, which means there are **zero limits** on connections, streams, memory, and file descriptors. A single misbehaving client can exhaust the server. This is the kind of issue that brought down early Mastodon instances — unbounded resource consumption from federated peers.

**Fix:** Use `rcmgr.NewResourceManager()` with explicit limits scaled to your hardware. At minimum, set per-peer connection/stream limits and a global memory cap.

### 2. Database pool size of 10 with 50,000 max connections

`internal/core/config.go` — every config preset (default, production, high-capacity) sets `pool_max_conns=10`. Every protocol handler blocks on a DB query, so with even modest concurrency you'll have goroutines piling up waiting for a connection. The `HighCapacityConfig` advertises 50,000 max connections but only 10 DB connections to serve them — that's a 5000:1 ratio.

**Fix:** Scale pool size to at least `max(CPUs * 2, 50)` for production. Consider separate pools for read-heavy (MAA, SDA, SFA, SCA) and write-heavy (MSA) paths.

### 3. Message sequence generation race condition

`internal/storage/postgres/messages.go` — `GetNextSequence()` uses `SELECT COALESCE(MAX(sequence_number), 0) + 1` which is **not safe under concurrent inserts** to the same mailbox. Two goroutines can read the same max and produce duplicate sequence numbers. The feed and collection code correctly uses transactional `UPDATE ... RETURNING` and `SELECT ... FOR UPDATE`, but messages don't.

**Fix:** Use a `SERIAL`/`BIGSERIAL` column, or adopt the same transactional pattern already used for feeds.

### 4. `MaxConcurrentConnections` is never enforced

`internal/core/config.go` defines `MaxConcurrentConnections: 50000` but **nothing in the codebase checks it**. Combined with the `NullResourceManager`, there is literally no admission control.

**Fix:** Either enforce it in the libp2p resource manager configuration, or add a connection gater that tracks and limits active connections.

---

## Moderate Issues

### 5. Rate limit memory leak

Every protocol handler (`msa`, `maa`, `mma`, `sda`, `sfa`, `sca`) maintains an in-memory `map[peer.ID][]time.Time` for rate limiting. Entries for peers that disconnect are **never evicted**. Over time, with many transient mobile clients, this map grows without bound.

**Fix:** Add a periodic sweep (e.g., every 5 minutes) that evicts entries older than the rate limit window, or switch to a token-bucket algorithm that uses constant memory per peer.

### 6. Mailbox cache has no eviction policy

`internal/mda/delivery.go` — the `mailboxCache` map grows as mailboxes are accessed and entries are only removed on explicit deletion. With 100,000 mailboxes configured as the max, this could consume significant memory.

**Fix:** Use an LRU cache with a bounded size. The `hashicorp/golang-lru` package or similar would work well here.

### 7. Global mutex on MTA rate limiter

`internal/mta/router.go` — a single `sync.Mutex` protects the `rateLimitHistory` map for **all peers**. Every message submission must acquire this lock. Under high throughput, this serializes all submissions.

**Fix:** Use a `sync.Map` or shard the map (e.g., by hashing the peer ID) to reduce contention.

### 8. Unbounded notification goroutines

`internal/mda/notifier.go` — push notifications spawn `go n.notifyDirect(...)` or `go n.notifyPubSub(...)` with no bound. A burst of messages to a popular shared mailbox could spawn thousands of goroutines simultaneously.

**Fix:** Use a semaphore (`golang.org/x/sync/semaphore`) or a worker pool with a buffered channel to cap concurrent notification goroutines.

### 9. `WorkerThreads` config is defined but never used

`internal/core/config.go` defines `WorkerThreads: 4` (8 for high-capacity) but nothing references it. There's no worker pool — all work runs directly in libp2p stream handler goroutines with no concurrency control.

**Fix:** Either implement worker pools for DB-bound work (which provides backpressure when the DB is saturated), or remove the config field to avoid confusion.

---

## Structural / Mastodon-Relevant Concerns

### 10. No horizontal scaling path

This is where Mastodon struggled most. The architecture is single-process, single-PostgreSQL. There is no:

- **Sharding** of mailboxes across DB instances
- **Message queue** between components (everything is synchronous in-process)
- **Distributed locking** for multi-instance deployment
- **Read replicas** for read-heavy paths

The `EnableForwarding` flag exists but forwarding isn't implemented. The GossipSub registry announces server capabilities to peers, but there's no mechanism for servers to forward mail to each other.

**Recommendation:** This doesn't need to be solved now, but design with it in mind:

- Extract the MTA-to-MDA path behind an interface that could be backed by NATS or Redis Streams
- Shard mailboxes by owner peer ID so you can partition across DB instances later
- The document/feed/collection stores are already owner-scoped, which is a good foundation for sharding

### 11. JSON over libp2p streams is expensive at scale

Every message is JSON-encoded with base64 for binary content. For a messaging server, this adds ~33% overhead on binary payloads and CPU cost for JSON marshaling/unmarshaling on every message.

**Recommendation:** Consider Protocol Buffers or CBOR for the wire format. This is a breaking protocol change, so worth doing before you have a large client base.

### 12. No backpressure mechanism

If PostgreSQL slows down (e.g., during vacuum, replication lag, or high load), there's nothing to push back against incoming streams. Goroutines accumulate, memory grows, and eventually the process becomes unresponsive. Mastodon hit this pattern with Sidekiq queue depth spiraling during traffic spikes.

**Recommendation:** Implement admission control — track in-flight DB operations and reject new requests with a "server busy" status when a threshold is reached.

---

## What You're Doing Well

- **Clean MTA/MDA separation** — the mail metaphor gives you clear boundaries for future extraction into separate services
- **Owner-scoped data** — documents, feeds, and collections are all keyed by owner peer ID, which is naturally shardable
- **Optimistic concurrency** — ETags/content hashes for documents and collections prevent lost updates without pessimistic locking
- **Cursor-based pagination** — directory browsing uses cursor-based pagination rather than OFFSET, which scales properly
- **Presence batching** — the 2-second batching window with max batch size of 50 prevents GossipSub flooding
- **Per-handler rate limiting** with read/write separation for document-oriented protocols
- **Transactional consistency** for feeds and collections

---

## Priority Order for Fixes

If addressing these for a production mobile app launch:

| Priority | Issue | Why |
|----------|-------|-----|
| 1 | Resource manager (#1) | Prevents resource exhaustion attacks |
| 2 | DB pool size (#2) | Immediate bottleneck under any real load |
| 3 | Sequence race (#3) | Data corruption bug |
| 4 | Connection limit enforcement (#4) | Pairs with #1 |
| 5 | Rate limit memory leak (#5) | Slow-burn operational issue |
| 6 | Backpressure (#12) | Prevents cascading failure under load |
| 7 | Global MTA mutex (#7) | Throughput bottleneck |
| 8 | Bounded notifications (#8) | Prevents goroutine explosion |

Items 9-11 are architectural and can be addressed when scaling beyond a single instance.
