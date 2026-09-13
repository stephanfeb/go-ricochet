# go-ricochet

A production-ready store-and-forward messaging server for P2P networks, built on [libp2p](https://libp2p.io/). Provides reliable message delivery when recipients are offline, following an email MX server architecture.

go-ricochet is the Go implementation of the [Ricochet](https://github.com/user/ricochet) protocol, designed for decentralized messaging, document storage, and mailbox management over peer-to-peer networks.

## Features

- **Store-and-Forward Messaging** -- Messages are stored on the server until the recipient comes online to retrieve them
- **Mailbox Management** -- Private, shared, and public mailboxes with ACL-based access control
- **Document Store** -- Key-value document storage with ETag-based conditional operations, versioning, and merge-patch support
- **End-to-End Encryption** -- NaCl box encryption (X25519 + XSalsa20-Poly1305) derived from Ed25519 identity keys
- **LZ4 Compression** -- Transparent payload compression with configurable threshold
- **Push Notifications** -- Hybrid delivery: direct P2P streams for private mailboxes, GossipSub for shared/public
- **Relay Support** -- Circuit Relay v2, AutoRelay with static relays, and DCUtR hole punching for NAT traversal
- **IMAP-Style Flags** -- Seen, flagged, deleted, draft flags with expunge support
- **Presence Detection** -- Real-time peer online/offline status with TTL-based caching
- **Service Discovery** -- GossipSub-based server announcements and health monitoring
- **PostgreSQL Storage** -- Production-grade storage with BYTEA binary encoding and connection pooling

## Architecture

The system follows an email-inspired agent separation:

```
                          +-----------------------+
  Client                  |      Server           |
  ------                  |      ------           |
                          |                       |
  SendMessage() --------> |  MSA (Submit)         |
                          |    |                  |
                          |    v                  |
                          |  MTA (Route)          |
                          |    |                  |
                          |    v                  |
                          |  MDA (Deliver)        |
                          |    |                  |
  RetrieveMessages() <--- |  MAA (Access)         |
                          |                       |
  CreateMailbox() ------> |  MMA (Admin)          |
                          |                       |
  PutDocument() --------> |  SDA (Documents)      |
                          +-----------------------+
```

| Agent | Protocol ID | Purpose |
|-------|------------|---------|
| **MSA** (Mail Submission) | `/sf-network/submit/1.0.0` | Message submission (write path) |
| **MAA** (Mail Access) | `/sf-network/access/1.0.0` | Message retrieval, flags, expunge |
| **MMA** (Mailbox Management) | `/sf-network/admin/1.0.0` | Mailbox lifecycle, ACLs, capacity |
| **SDA** (Store-Document) | `/ricochet/store/doc/1.0.0` | Document CRUD operations |
| **MTA** (Mail Transfer) | Internal | Message routing and validation |
| **MDA** (Mail Delivery) | Internal | Local storage and push notifications |

## Quick Start

### Prerequisites

- Go 1.24.6+
- PostgreSQL 14+

### Database Setup

```bash
createdb ricochet
psql ricochet < schema.sql
```

### Build and Run

```bash
# Build
go build -o ricochet ./cmd/ricochet

# Run in development mode (TLS to PostgreSQL off for a local database)
export RICOCHET_PG_PASSWORD=secret
./ricochet --development \
  --pg-host localhost \
  --pg-database ricochet \
  --pg-username ricochet \
  --pg-sslmode disable

# Run in production mode (TLS to PostgreSQL required by default)
export RICOCHET_PG_PASSWORD=secret
./ricochet --production \
  --pg-host db.example.com \
  --pg-database ricochet \
  --pg-username ricochet
```

The PostgreSQL password comes from the `RICOCHET_PG_PASSWORD` environment
variable or the config file. A `--pg-password` flag also exists, but the
command line of a running process is readable by every local user, so keep it
to throwaway local runs.

### CLI Options

```
--port              Listen port (default: 55223)
--development       Development mode (1GB, relaxed limits, debug logging)
--production        Production mode (50GB, 10K connections, auth enabled)
--data-dir          Data directory path
--identity-file     Path to Ed25519 identity key file
--pg-host           PostgreSQL host
--pg-port           PostgreSQL port (default: 5432)
--pg-database       PostgreSQL database name
--pg-username       PostgreSQL username
--pg-password       PostgreSQL password (prefer RICOCHET_PG_PASSWORD)
--pg-sslmode        PostgreSQL SSL mode: require (default), disable
```

## Client Library

The `pkg/client` package provides a full-featured client for interacting with Ricochet servers. The message model it speaks (messages, flags, priorities, mailbox types, acknowledgements, notifications, status codes) is in `pkg/wire`, so both packages are importable from any module. The snippets below compile as written; CI checks them.

### Creating a Client

```go
import (
    "github.com/libp2p/go-libp2p"
    client "github.com/twostack/go-ricochet/pkg/client"
    "github.com/twostack/go-ricochet/pkg/wire"
)

// Create a libp2p host
h, _ := libp2p.New()

// Create the Ricochet client
cl := client.New(h, client.Config{
    PreferredServers: []client.ServerPreference{
        {PeerID: serverPeerID, Priority: 1},
    },
    ConnectionTimeout: 10 * time.Second,
    MessageTimeout:    30 * time.Second,
})
```

### Sending Messages

```go
// Simple message
result, err := cl.SendMessage(ctx, recipientID, []byte("hello"))

// With options
result, err = cl.SendMessage(ctx, recipientID, payload,
    client.WithFolderPath("orders"),
    client.WithPriority(wire.PriorityUrgent),
    client.WithExpiry(24 * time.Hour),
    client.WithCompression(),       // LZ4, default 1KB threshold
    client.WithEncryption(),        // NaCl box E2E encryption
)
```

### Retrieving Messages

```go
// Retrieve from inbox
messages, err := cl.RetrieveMessages(ctx)

// With filters
messages, err = cl.RetrieveMessages(ctx,
    client.WithRetrieveFolderPath("orders"),
    client.WithFromSequence(100),
    client.WithMaxMessages(50),
    client.WithMinPriority(wire.PriorityHigh),
)
```

### Message Flags

```go
// Acknowledge: removes non-persistent messages, marks persistent ones seen.
// Retrieval never removes anything on its own.
ack, err := cl.MarkDelivered(ctx, []string{msg.MessageID})

// Page through a large mailbox (default page 100, never more than one frame)
page, err := cl.RetrievePage(ctx, client.WithMaxMessages(50))
for page.HasMore {
    last := page.Messages[len(page.Messages)-1]
    page, err = cl.RetrievePage(ctx, client.WithMaxMessages(50),
        client.WithFromSequence(last.SequenceNumber+1))
}

// Flag a message
flagAck, err := cl.UpdateFlags(ctx, msg.MessageID, uint32(wire.MsgFlagFlagged), 0)

// Mark for deletion
flagAck, err = cl.UpdateFlags(ctx, msg.MessageID, uint32(wire.MsgFlagDeleted), 0)

// Permanently remove deleted messages
expungeAck, err := cl.Expunge(ctx)

// Immediate delete (bypasses flag/expunge)
deleteAck, err := cl.DeleteMessages(ctx, []string{msg.MessageID})
```

### Mailbox Management

```go
// Create mailboxes
cl.CreateMailbox(ctx, "invoices", wire.MailboxPrivate)
cl.CreateMailbox(ctx, "team-updates", wire.MailboxShared)
cl.CreateMailbox(ctx, "announcements", wire.MailboxPublic,
    client.WithMailboxMaxMessages(500),
    client.WithRetentionDays(7),
)

// Access control
cl.GrantAccess(ctx, "team-updates", colleaguePeerID, wire.AccessReadWrite)
cl.RevokeAccess(ctx, "team-updates", colleaguePeerID)
entries, _ := cl.ListACL(ctx, "team-updates")

// List and delete
mailboxes, _ := cl.ListMailboxes(ctx)
cl.DeleteMailbox(ctx, "old-folder")

// Server capacity
capacity, _ := cl.QueryCapacity(ctx)
fmt.Printf("Usage: %.1f%%\n", capacity.UsagePercent())
```

### Document Store

```go
ownerID := cl.PeerID()

// Create/replace a document
putResp, err := cl.PutDocument(ctx, ownerID, "config/settings",
    []byte(`{"theme":"dark"}`),
    client.WithContentType("application/json"),
)

// Conditional put (optimistic locking)
_, err = cl.PutDocument(ctx, ownerID, "config/settings",
    []byte(`{"theme":"light"}`),
    client.WithIfMatch(putResp.ETag),
)

// Merge-patch update
_, err = cl.PatchDocument(ctx, ownerID, "config/settings",
    map[string]any{"fontSize": 14},
)

// Conditional get (returns 304 if unchanged)
getResp, err := cl.GetDocument(ctx, ownerID, "config/settings",
    client.WithIfNoneMatch(putResp.ETag),
)

// Metadata only
meta, err := cl.HeadDocument(ctx, ownerID, "config/settings")

// List all documents
docs, err := cl.ListDocuments(ctx, ownerID)

// Delete
deleted, err := cl.DeleteDocument(ctx, ownerID, "config/settings")
```

### Push Notifications

```go
cl.RegisterNotificationHandler(func(n *wire.Notification) {
    fmt.Printf("New message in %s (type: %s)\n",
        n.MailboxPath, n.MailboxType)
})
```

## Configuration Presets

| Setting | Default | Development | Production | High-Capacity |
|---------|---------|-------------|------------|---------------|
| Storage | 10 GB | 1 GB | 50 GB | 100 GB |
| Connections (hard cap) | 10,000 | 100 | 10,000 | 50,000 |
| Messages/Mailbox | 1,000 | 1,000 | 1,000 | 1,000 |
| Folders/Owner | 100 | 100 | 100 | 100 |
| Retention | 30 days | 30 days | 30 days | 30 days |
| Authentication | off | off | off | off |
| Forwarding | off | off | off | on |
| Rate limits | off | off | off | off |
| Push workers | 4 | 4 | 4 | 8 |

Every row is enforced. Authentication is an allow-list, so no preset can turn
it on: set `features.enable_authentication: true` and list the peers in
`security.trusted_peers`, and only those peers may connect. Forwarding lets a
trusted peer submit mail on another sender's behalf; with nobody trusted it is
on but unusable. Rate limits are off by design — the server bounds
concurrency (`admission_control`) and connections rather than request rate —
and can be turned on per protocol under `rate_limits`. Select a preset with
`--development`, `--production` or `--high-capacity`.

## Security

### End-to-End Encryption

Messages can be encrypted client-side using NaCl box:

- **Key Agreement**: X25519 (derived from Ed25519 identity keys)
- **Cipher**: XSalsa20-Poly1305 (authenticated encryption)
- **Wire Format**: `[24-byte nonce][ciphertext + Poly1305 tag]`

The server never sees plaintext when encryption is enabled. Keys are derived from the libp2p peer identity, so no additional key exchange is needed.

### Trusted Peers

`security.trusted_peers` is one list with two uses. With
`enable_authentication` on it is the set of peers allowed to connect at all;
an untrusted peer costs one Noise handshake and is refused before any stream
opens. With `enable_forwarding` on it is the set of peers allowed to submit a
message whose sender is not themselves: the message is marked forwarded, its
hop count advanced, and it is refused past the hop limit.

### Transport Security

All P2P connections use the [Noise protocol framework](https://noiseprotocol.org/) for transport encryption and mutual authentication.

### Identity

Server and client identities are Ed25519 keypairs. Identity can be provided via:
1. `RICOCHET_SEED_HEX` environment variable (highest priority)
2. `--identity-file` CLI flag
3. Auto-generated and persisted to `{data-dir}/identity.key`

## Networking

### Transport Stack

```
Application (Ricochet protocols)
    |
Yamux (stream multiplexing)
    |
Noise (transport encryption)
    |
UDX (unreliable datagram transport)
    |
UDP
```

### NAT Traversal

When relay support is enabled in the configuration:

```go
cfg.EnableRelay = true          // Circuit Relay v2
cfg.EnableAutoRelay = true      // Auto-discover relay peers
cfg.EnableHolePunching = true   // DCUtR hole punching
cfg.BootstrapPeers = []string{  // Static relay candidates
    "/ip4/relay.example.com/udp/55223/udx/p2p/12D3KooW...",
}
```

### Service Discovery

Servers announce themselves via GossipSub on the `/sf-network/services/announce` topic. Announcements include capabilities, storage capacity, region, and uptime score.

## Project Structure

```
cmd/ricochet/           Server CLI entry point
cmd/ricochet-bench/     Stress test / load testing tool
internal/
  core/                 Message types, config, mailbox addressing
  protocol/
    frame/              Length-prefix frame encoding
    msa/                Mail Submission Agent (write path)
    maa/                Mail Access Agent (read path)
    mma/                Mailbox Management Agent (admin)
    sda/                Store-Document Agent (documents)
    notify/             Push notification protocol
  storage/              Storage interface and models
    postgres/           PostgreSQL backend
  mda/                  Mail Delivery Agent + push notifier
  mta/                  Mail Transfer Agent (routing)
  p2p/                  Host, node, identity management
  presence/             Peer presence detection + cache
  registry/             Service discovery via GossipSub
  server/               Server orchestration
pkg/client/             Public client library
  client.go             All client operations
  options.go            Functional options
  compression.go        LZ4 compression
  encryption.go         NaCl box encryption
  notifications.go      Push notification handler
test/integration/       Integration tests (requires PostgreSQL)
schema.sql              PostgreSQL database schema
```

## Stress Testing

`ricochet-bench` is an Apache Bench-style load testing tool for Ricochet servers. It measures throughput, latency percentiles, and error rates across all protocol handlers.

### Build

```bash
go build -o ricochet-bench ./cmd/ricochet-bench
```

### Usage

```
ricochet-bench [flags] <server-multiaddr> <server-peer-id>
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-n` | 1000 | Total number of requests |
| `-c` | 10 | Number of concurrent workers |
| `-protocol` | `msa` | Protocol to benchmark (see below) |
| `-payload-size` | 1024 | Payload size in bytes |
| `-duration` | — | Run for a duration instead of fixed count (e.g. `30s`, `1m`) |
| `-warmup` | 10 | Warmup requests before measuring |

**Protocols:**

| Protocol | Operation | Description |
|----------|-----------|-------------|
| `msa` | `SendMessage` | Message submission throughput |
| `maa` | `RetrieveMessages` | Message retrieval (pre-seeds mailbox data) |
| `sda` | `PutDocument` + `GetDocument` | Document store round-trip |
| `sfa` | `AppendFeedEntry` + `GetFeedEntry` | Feed append and read cycle |
| `sca` | `PutCollectionItem` + `QueryCollection` | Collection write and query cycle |
| `mixed` | Random mix of all protocols | Combined workload |

### Examples

```bash
# Basic message submission benchmark
ricochet-bench -n 1000 -c 10 -protocol msa \
  /ip4/127.0.0.1/udp/55223/udx 12D3KooW...

# Document store with larger payloads
ricochet-bench -n 5000 -c 20 -protocol sda -payload-size 4096 \
  /ip4/127.0.0.1/udp/55223/udx 12D3KooW...

# Duration-based mixed workload
ricochet-bench -duration 60s -c 50 -protocol mixed \
  /ip4/127.0.0.1/udp/55223/udx 12D3KooW...
```

### Sample Output

```
Ricochet Bench - Protocol: MSA (Message Submission)
Server: /ip4/127.0.0.1/udp/55223/udx/p2p/12D3KooW...

Concurrency Level:      10
Total Requests:         1000
Payload Size:           1024 bytes

Results:
  Completed:            985
  Failed:               15
  Total time:           12.345s
  Requests/sec:         81.00

Latency Distribution:
  min:    2.1ms
  p50:    8.3ms
  p75:    12.1ms
  p90:    18.7ms
  p95:    25.4ms
  p99:    45.2ms
  max:    120.5ms
```

### Architecture Notes

Each concurrent worker creates its own libp2p host and client connection, avoiding yamux stream multiplexing contention. For protocols that require pre-existing data (MAA, SFA, SCA), the tool automatically creates the necessary resources during the warmup phase.

## Testing

```bash
# Run unit tests
go test ./internal/core/... ./internal/protocol/... ./internal/mta/... ./pkg/...

# Run integration tests (requires PostgreSQL)
RICOCHET_TEST_POSTGRES_DSN="postgresql://user:pass@localhost:5432/ricochet_test?sslmode=disable" \
  go test ./test/integration/...

# Run all tests
go test ./...

# With verbose output
go test -v ./pkg/client/...
```

### Test Coverage

- **Unit tests**: Core types, frame encoding, MTA routing, compression, encryption (46 tests)
- **Integration tests**: Full client-server workflows, document CRUD, flag operations, mailbox management (21 tests, require PostgreSQL)

## Protocol Wire Format

All protocol messages use a length-prefixed JSON frame:

```
+-------------------+--------------------+
| Length (4 bytes)   | JSON Payload       |
| big-endian uint32  | (variable length)  |
+-------------------+--------------------+
```

### Message Processing Pipeline

```
Send:    raw payload -> compress (LZ4) -> encrypt (NaCl box) -> encode frame -> send
Receive: receive -> decode frame -> decrypt (NaCl box) -> decompress (LZ4) -> raw payload
```

## Database Schema

The PostgreSQL schema (`schema.sql`) includes:

| Table | Purpose |
|-------|---------|
| `mailboxes` | Mailbox records with owner, type, retention policy |
| `stored_messages` | Messages with BYTEA payload, priority, flags, expiry |
| `mailbox_acls` | Per-peer access control entries |
| `reader_cursors` | Per-reader position tracking (public mailboxes) |
| `documents` | Document storage with ETag versioning |
| `document_versions` | Document version history |
| `block_store` | CRDT block storage |

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.
