# Future Enhancements: go-ricochet

**Status:** Living document
**Date:** February 2026
**Current state:** Phases 1-3 complete (foundation, server core, full server)

---

## Current Baseline

The Go port is feature-complete for single-server operation. 26 source files (~5,800 lines) implement:

- 4 protocol handlers: MSA, MAA, MMA, SDA
- MTA router with rate limiting and message validation
- MDA mailbox server with private, shared, and public mailbox types
- PostgreSQL storage backend (pgx)
- P2P networking: UDX transport, Kademlia DHT, GossipSub
- Service registry and presence monitoring
- CLI with development/production presets

What follows is **not implemented yet** and represents the roadmap from this point forward.

---

## Phase 4 — Client Library & Test Suite

### 4.1 Client API (`pkg/client/`)

Port the Dart `SFClient` (~800 lines) to idiomatic Go. The Dart version uses `Stream<T>` and `Future<T>`; the Go version should use channels and `context.Context`.

**Core client:**
```
pkg/client/
├── client.go           # SFClient — high-level messaging API
├── mailbox_manager.go  # Remote mailbox administration
├── server_selector.go  # Server discovery and failover
└── options.go          # Functional options for client configuration
```

**Key methods to implement:**

| Method | Purpose |
|--------|---------|
| `SendMessage(ctx, msg)` | Submit message via MSA, auto-route through S&F |
| `RetrieveMessages(ctx, opts)` | Pull pending messages via MAA |
| `AutoRetrieve(ctx, interval) <-chan *Message` | Channel-based polling with backpressure |
| `MarkDelivered(ctx, ids)` | Set \Seen flag on retrieved messages |
| `UpdateFlags(ctx, msgID, add, remove)` | IMAP-style flag manipulation |
| `Expunge(ctx, folder)` | Delete \Deleted-flagged messages |
| `GetDocument(ctx, owner, key)` | Document retrieval with ETag support |
| `PutDocument(ctx, owner, key, content)` | Create/overwrite documents |
| `PatchDocument(ctx, owner, key, patch)` | JSON Merge Patch (RFC 7396) |
| `ListDocuments(ctx, owner, prefix)` | Document enumeration |

**Mailbox management:**

| Method | Purpose |
|--------|---------|
| `CreateMailbox(ctx, addr, opts)` | Create private/shared/public mailboxes |
| `DeleteMailbox(ctx, addr)` | Remove a mailbox |
| `GrantAccess(ctx, addr, peerID, mode)` | ACL grant |
| `RevokeAccess(ctx, addr, peerID)` | ACL revoke |
| `ListACL(ctx, addr)` | View access control entries |
| `ListMailboxes(ctx)` | Enumerate own mailboxes |
| `QueryCapacity(ctx)` | Check server storage capacity |

**Server selection:**
- Preferred server list with MX-style priority/weight
- Automatic failover when preferred server is unreachable
- Capacity-aware selection (query `ServerCapacity` before routing)
- Connection protection to preferred servers via `host.ConnManager().Protect()`

**Design considerations:**
- Use `context.Context` for cancellation and timeouts on all operations
- Return `<-chan *Message` for streaming retrieval (auto-retrieve loop)
- `sync.Pool` for frame buffer reuse in high-throughput scenarios
- Expose `ClientOption` functional options for configuration

### 4.2 Unit Tests

| Package | Test File | Coverage Target |
|---------|-----------|----------------|
| `pkg/wire` | `message_test.go` | Message creation, expiry, JSON round-trip, hop count, flags |
| `internal/core` | `mailbox_test.go` | Address parsing, validation, FullPath formatting |
| `internal/core` | `config_test.go` | Preset creation, validation, edge cases |
| `internal/protocol/frame` | `frame_test.go` | Encode/decode round-trip for all 12+ message types |
| `internal/mta` | `router_test.go` | Rate limiting (sliding window), message validation, sender mismatch |
| `internal/mda` | `delivery_test.go` | Mailbox cache, deliver + retrieve cycle |
| `internal/mda/mailboxes` | `private_test.go` | Owner-only access, non-persistent deletion after read |
| `internal/mda/mailboxes` | `shared_test.go` | ACL enforcement, independent reader cursors |
| `internal/mda/mailboxes` | `public_test.go` | Open read, ACL write, retention enforcement |
| `internal/storage/postgres` | `postgres_test.go` | Full CRUD against live PostgreSQL |
| `internal/presence` | `cache_test.go` | TTL expiry, concurrent access |
| `internal/registry` | `registry_test.go` | Announcement encoding, stale server filtering |

### 4.3 Integration Tests

Real libp2p network over UDX transport with two nodes:

| Test | Scenario |
|------|----------|
| `client_server_test.go` | Client sends message, server stores, client retrieves |
| `store_forward_test.go` | Recipient offline → message stored → recipient connects → message delivered |
| `document_test.go` | PUT → GET → PATCH → HEAD → DELETE with ETag versioning |
| `mailbox_test.go` | Create private/shared/public mailboxes, test ACL enforcement |
| `admin_test.go` | MMA operations: create, delete, grant, revoke, list |
| `rate_limit_test.go` | Verify rate limiting rejects excess requests |
| `presence_test.go` | Presence cache updates when peer connects/disconnects |

### 4.4 Benchmarks

```
test/benchmark/
├── message_throughput_test.go   # Messages/sec at 1KB, 10KB, 100KB payloads
├── storage_throughput_test.go   # PostgreSQL insert/query rates
├── frame_encoding_test.go       # Encode/decode latency and allocations
└── concurrent_streams_test.go   # Max concurrent protocol streams
```

Compare against Dart baseline:

| Metric | Measurement |
|--------|-------------|
| Message throughput | Messages/second (1KB, 10KB, 100KB payloads) |
| Latency (p50, p99) | Round-trip time for submit+retrieve |
| Memory usage | RSS under sustained load |
| Connection density | Max concurrent streams per server |
| Storage throughput | PostgreSQL insert/query rates |
| Allocation rate | `go test -benchmem` allocations per operation |

---

## Phase 5 — Production Readiness

### 5.1 Embedded Storage Backend

Add a `bbolt` backend for development and single-node deployments where PostgreSQL is overkill.

```
internal/storage/bbolt/
└── bbolt.go    # Implements storage.Storage interface
```

**Key design:**
- Single-file database, no external dependencies
- Bucket-per-table layout: `mailboxes`, `messages`, `acl`, `documents`, `versions`
- Secondary indexes via composite key schemes (e.g., `owner:folder` prefix for mailbox lookups)
- ACID transactions for multi-key operations
- Suitable for dev/testing and low-traffic single-server deployments

**Alternative:** `badger` (LSM-based) if write-heavy benchmarks favor it over bbolt's B+tree.

### 5.2 Observability

**pprof endpoint:**
- Start `net/http/pprof` on configurable debug port (default `:6060`)
- CPU, memory, goroutine, block, mutex profiles
- Available in development mode by default, opt-in for production

**Structured metrics:**
- `slog` already provides structured logging
- Add OpenTelemetry SDK for metrics and traces:
  - Protocol handler latency histograms (per operation type)
  - Message throughput counters
  - Storage operation latency
  - Active connections / streams gauge
  - Rate limit rejection counter
  - Mailbox count and message count gauges

**Health check endpoint:**
- HTTP `/healthz` on debug port
- Reports: storage connectivity, P2P host status, uptime, peer count

### 5.3 Graceful Shutdown Hardening

The current CLI has basic signal handling. Enhance with:
- Drain in-flight protocol streams before closing (configurable timeout)
- Flush pending GossipSub announcements
- Close PostgreSQL pool with connection drain
- Log shutdown progress at each stage

### 5.4 Configuration Enhancements

- YAML config file loading with environment variable expansion (`${VAR:-default}`)
- Config precedence: preset > CLI flags > config file > defaults
- Config validation with actionable error messages
- `--dry-run` flag to validate config without starting

### 5.5 Build & Release

| Artifact | Tool |
|----------|------|
| Multi-platform binaries | `goreleaser` (`.goreleaser.yaml`) |
| Linux package | `goreleaser` nFPM (generates .deb and .rpm) |
| Docker image | Multi-stage `Dockerfile` with scratch base |
| Systemd unit | `deploy/ricochet.service` |
| Makefile | `build`, `test`, `lint`, `bench`, `release` targets |

**Cross-compilation matrix:**
- `linux/amd64`, `linux/arm64`
- `darwin/amd64`, `darwin/arm64`
- `windows/amd64`

### 5.6 Security Hardening

- TLS for PostgreSQL connections (`sslmode=verify-full`)
- Rate limit tuning per protocol (MSA vs MAA vs SDA have different profiles)
- Maximum payload size enforcement at the frame layer (currently 10MB)
- Peer blocklist support (deny connections from specific peer IDs)
- Audit logging for admin operations (MMA)

---

## Phase 6 — CRDT Replication

### Context

The Dart version implements four CRDT types for multi-server eventual consistency:

| CRDT Type | Strategy | Purpose |
|-----------|----------|---------|
| `MessageCollectionCRDT` | Append-only log, dedup by CID | Message references (immutable metadata) |
| `MailboxMetadataCRDT` | Last-Write-Wins per field | Mailbox settings (maxMessages, retention) |
| `MailboxACLCRDT` | OR-Set with tombstoning | Access control (revoke wins on conflict) |
| `ReaderCursorsCRDT` | LWW-Map per reader | Per-peer read positions |

### Options

**Option A — Port Merkle CRDTs (3-4 weeks)**
- Port `dart_libp2p_merkle_crdt` logic to Go
- Content-addressed storage (DAG) with gossip-based head announcements
- Lazy message content fetching via block stores
- Full control over merge semantics

**Option B — Adopt `go-ds-crdt` (2-3 weeks)**
- Textile/IPFS ecosystem CRDT library
- Different API surface, may require adaptation layer
- Proven in production (Textile threads)

**Option C — Raft-based replication (2 weeks)**
- Simpler model: strong consistency with leader election
- Uses `hashicorp/raft` or `etcd/raft`
- Different consistency trade-off (CP vs AP)
- Better for small clusters (3-5 nodes)

**Recommendation:** Start with Option A for consistency with the Dart version's semantics. Evaluate Option C if deployment patterns favor small, fixed clusters over dynamic mesh.

### Replication Architecture

```
                    ┌─────────────┐
                    │  GossipSub  │
                    │  Announce   │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
         ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
         │ Server A │  │ Server B │  │ Server C │
         │          │  │          │  │          │
         │ CRDT     │  │ CRDT     │  │ CRDT     │
         │ Heads    │  │ Heads    │  │ Heads    │
         └────┬─────┘  └────┬─────┘  └────┬─────┘
              │             │             │
              └──────DAG Fetch────────────┘
                (lazy content retrieval)
```

- Servers announce CRDT heads via GossipSub topics
- Peers fetch missing DAG nodes via P2P streams
- Message content stored separately, fetched lazily by CID
- Convergence guaranteed by CRDT merge semantics

---

## Phase 7 — Advanced Features

### 7.1 Push Notifications

Real-time message delivery when recipients are connected:

- Use libp2p streams for direct push (no polling required)
- Protocol: `/sf-network/notify/1.0.0`
- Fallback to polling via MAA when push fails
- Notification types: new message, flag change, mailbox update

### 7.2 Message Encryption (End-to-End)

- Envelope encryption: message payload encrypted with recipient's public key
- Server stores encrypted blobs, cannot read content
- Key exchange via X25519 (derive from Ed25519 identity keys)
- Group messaging: encrypt per-recipient or use shared group key
- The `SFMessageFlags.Encrypted` flag (bit 0) already exists in the type system

### 7.3 Message Compression

- Compress payloads above a configurable threshold (e.g., 1KB)
- LZ4 for speed or zstd for ratio
- The `SFMessageFlags.Compressed` flag (bit 2) already exists in the type system
- Transparent: compress on submit, decompress on retrieve

### 7.4 Protocol Versioning

Clean break from Dart means protocol versions can be bumped:

| Current | Potential v2 |
|---------|-------------|
| `/sf-network/submit/1.0.0` | `/sf-network/submit/2.0.0` |
| JSON frames | Protobuf or CBOR frames |
| Length-prefixed | Varint-prefixed (smaller overhead) |

**v2 frame format considerations:**
- Protobuf: schema evolution, smaller wire size, kaggen-aligned
- CBOR: schema-less, Dart-compatible if interop ever needed
- Either way, keep v1 handler registered for backward compatibility during migration

### 7.5 Multi-Transport Support

Currently UDX-only. Future transports to support:

| Transport | Use Case |
|-----------|----------|
| QUIC | Fallback when UDP is blocked |
| WebSocket | Browser clients |
| TCP | Legacy environments |

go-libp2p's transport interface makes this straightforward:
```go
libp2p.Transport(quic.NewTransport)
libp2p.Transport(websocket.New)
```

### 7.6 Relay Support

For peers behind restrictive NATs:

- libp2p Circuit Relay v2 (resource-limited relaying)
- AutoRelay for automatic relay discovery
- Hole punching via DCUtR (Direct Connection Upgrade through Relay)

### 7.7 Content-Addressed Storage

Store large payloads by content hash (CID) rather than inline:

- Messages reference CIDs instead of carrying full payloads
- Block store for content-addressed data (reuse for CRDT Phase 6)
- Deduplication across messages
- Efficient for forwarded/replicated messages (store once, reference many)

### 7.8 Webhooks / External Integrations

- Configurable webhook endpoints for message events
- HTTP callback on: message received, mailbox created, document updated
- Auth: HMAC-SHA256 signed payloads
- Retry with exponential backoff

### 7.9 Admin API

HTTP REST API for server administration:

- `GET /api/v1/stats` — Server statistics (uptime, message count, peer count)
- `GET /api/v1/mailboxes` — List all mailboxes with stats
- `GET /api/v1/peers` — Connected peers and presence
- `POST /api/v1/maintenance` — Trigger manual maintenance
- `DELETE /api/v1/peers/:id/block` — Block a peer
- Authenticated via API key or mTLS

### 7.10 Horizontal Scaling

Beyond CRDT replication, support for high-availability deployments:

- PostgreSQL read replicas for MAA (read-heavy) workloads
- Connection draining for zero-downtime upgrades
- Load balancer awareness (health check endpoints)
- Sticky sessions by peer ID (route same peer to same server)

---

## Open Questions

1. **CBOR vs Protobuf frames:** The Dart version uses JSON frames. Kaggen uses Protobuf. Standardize on one format for v2? JSON is debuggable; Protobuf is compact and schema-validated.

2. **bbolt vs badger:** bbolt (B+tree, single writer) vs badger (LSM, concurrent writes). Ricochet's message store pattern is write-heavy — benchmark both before choosing.

3. **Client as separate module:** Should `pkg/client/` have its own `go.mod` for lighter imports by consumer applications? This avoids pulling in server-side dependencies.

4. **Graceful degradation for CRDT:** Should replication be opt-in (single-server default) or opt-out? This affects config design now — worth deciding before Phase 5 finalizes config.

5. **Push notification protocol:** Implement as a new libp2p protocol or reuse GossipSub topics for real-time delivery? GossipSub is simpler but less targeted.

---

## Priority Matrix

| Enhancement | Impact | Effort | Priority |
|-------------|--------|--------|----------|
| Client library | High | 1-2 weeks | P0 — Required for any consumer |
| Unit + integration tests | High | 2-3 weeks | P0 — Required for confidence |
| Benchmarks | Medium | 3-4 days | P1 — Validates Go port motivation |
| bbolt backend | Medium | 1 week | P1 — Dev/test without PostgreSQL |
| pprof + health check | Medium | 2-3 days | P1 — Operational basics |
| OpenTelemetry metrics | Medium | 1 week | P2 — Production visibility |
| goreleaser + systemd | Medium | 2-3 days | P2 — Deployment automation |
| CRDT replication | High | 3-4 weeks | P2 — Multi-server is future work |
| Push notifications | High | 1-2 weeks | P2 — Real-time delivery |
| E2E encryption | High | 2 weeks | P3 — Security enhancement |
| Message compression | Low | 2-3 days | P3 — Performance optimization |
| Protocol v2 (Protobuf) | Medium | 2 weeks | P3 — Wire efficiency |
| Multi-transport | Low | 1 week | P4 — Broader connectivity |
| Admin HTTP API | Medium | 1 week | P4 — Operational tooling |
| Webhooks | Low | 3-4 days | P4 — External integrations |
