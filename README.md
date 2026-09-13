# go-ricochet

A store-and-forward messaging server for P2P networks, built on [libp2p](https://libp2p.io/). Provides reliable message delivery when recipients are offline, following an email MX server architecture.

go-ricochet is the Go implementation of the Ricochet protocol (module `github.com/twostack/go-ricochet`), designed for decentralized messaging, document storage, and mailbox management over peer-to-peer networks.

## Features

- **Store-and-Forward Messaging** -- Messages are stored on the server until the recipient comes online to retrieve them
- **Mailbox Management** -- Private, shared, and public mailboxes with ACL-based access control
- **Document Store** -- Key-value document storage with ETag-based conditional operations, versioning, and merge-patch support. Documents, feeds, collections and the directory are readable by any peer; see [What Any Peer Can Read](#what-any-peer-can-read)
- **End-to-End Encryption** -- NaCl box encryption (X25519 + XSalsa20-Poly1305) derived from Ed25519 identity keys
- **LZ4 Compression** -- Transparent payload compression with configurable threshold
- **Push Notifications** -- Hybrid delivery: direct P2P streams for private mailboxes, GossipSub for shared/public
- **Relay Support** -- Circuit Relay v2 service, on by default. AutoRelay through static relays and DCUtR hole punching are off by default and take their relay candidates from `bootstrap_peers`; see [NAT Traversal](#nat-traversal)
- **IMAP-Style Flags** -- Seen, flagged, deleted, draft flags with expunge support
- **Presence Detection** -- Real-time peer online/offline status with TTL-based caching
- **Service Discovery** -- GossipSub-based server announcements; a server that stops announcing drops out of the list after two hours
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
| **MSA** batch | `/sf-network/submit/batch/1.0.0` | Many submissions in one request |
| **SDA** (Store-Document) | `/ricochet/store/doc/1.0.0` | Document CRUD operations |
| **SFA** (Store-Feed) | `/ricochet/store/feed/1.0.0` | Append-only feeds |
| **SCA** (Store-Collection) | `/ricochet/store/collection/1.0.0` | Keyed collections with queries |
| Notify | `/ricochet/mailbox-notify/1.0.0` | Push notifications, server to client |
| **MTA** (Mail Transfer) | Internal | Message routing and validation |
| **MDA** (Mail Delivery) | Internal | Local storage and push notifications |

## Quick Start

### Prerequisites

- Go 1.25+
- PostgreSQL 14+

### Database Setup

The schema grants everything to a `ricochet` role. Create it with a password
first; the schema creates it without one if it is missing, so the file runs on
a fresh cluster either way.

```bash
psql postgres -c "CREATE ROLE ricochet LOGIN PASSWORD 'secret'"
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
--development       Development preset (1 GB storage, 100 connections, TLS to PostgreSQL off)
--production        Production preset (50 GB storage, 10,000 connections, pool of 50)
--high-capacity     High-capacity preset (100 GB storage, 50,000 connections, forwarding on, 8 push workers)
--config            YAML config file (default: /etc/ricochet/config.yaml when it exists)
--data-dir          Data directory path
--identity-file     Path to Ed25519 identity key file
--external-addrs    Comma-separated external multiaddrs to advertise
--debug-dht         Verbose DHT logging
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
defer cl.Close() // removes the client's handlers; the host stays yours to close
```

Servers are tried in priority order and a server that cannot be dialled is
passed over for the next, so listing more than one keeps the client working
while one is down. A server that answers is never second-guessed: a refusal
comes back from the first server that gave one. Naming a server explicitly
(`WithDocServer` and friends) makes exactly one attempt.

Every failure the server reports arrives as a typed error: `errors.Is(err,
client.ErrForbidden)`, `client.ErrNotFound`, `client.ErrConflict` and so on,
with the status and message on `*client.ProtocolError`. The delete calls
return `false, nil` only for something that was already absent; a refusal or
a server fault is an error, never "deleted". `GetDocument` is the one call
that reports its status in the result, so a conditional get can see a 304.

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

A config file (`--config`, or `/etc/ricochet/config.yaml` if present) is
applied on top of the preset; `config.example.yaml` documents every key. A
key the file omits keeps the preset's value, including feature flags. A file
that cannot be read or parsed, or that contains a key the server does not
know, stops the server at startup rather than running on the preset.

## Security

### What Any Peer Can Read

Only mailboxes have access control. Every other store is public-read by
design: any peer that knows an owner's peer ID can read everything that
owner has put there. Writes are the owner's alone, with one exception.

| Store | Readable by | Writable by |
|-------|-------------|-------------|
| Mailboxes | Owner; shared mailboxes by the peers on their ACL; public mailboxes by anyone | Per mailbox type and ACL; see [doc/MAILBOX_LIFECYCLE_AND_ACLS.md](doc/MAILBOX_LIFECYCLE_AND_ACLS.md) |
| Documents (`GET`, `HEAD`, `LIST`, `HISTORY`) | Any peer, including every stored version | Owner only (`PUT`, `PATCH`, `DELETE`, `BATCH_PUT`) |
| Feeds (`GET`, `LIST`, `BATCH_GET`) | Any peer | Owner only, except that any peer may `APPEND` to a feed its owner created as collaborative |
| Collections (`GET`, `LIST`, `QUERY`) | Any peer | Owner only (`CREATE`, `PUT`, `DELETE`) |
| Directory | Any peer can browse and search every listing | Each peer joins, updates and leaves only its own listing |

The server stores document, feed and collection bytes exactly as sent and
never encrypts them. A client that needs a document kept from other peers
must encrypt it before storing it, the way the messaging layer does with
NaCl box, and must accept that the document's path, size, content type and
version history stay visible. There is no private document, feed or
collection: putting something in these stores publishes it to the network.

### End-to-End Encryption

Messages can be encrypted client-side using NaCl box:

- **Key Agreement**: X25519 (derived from Ed25519 identity keys)
- **Cipher**: XSalsa20-Poly1305 (authenticated encryption)
- **Wire Format**: `"RCE2" [24-byte nonce] [box(header || payload)]`, where the
  header binds the ciphertext to the recipient peer ID, the folder path and
  the message ID. The client still reads the earlier unbound format,
  `[24-byte nonce][ciphertext + Poly1305 tag]`, for messages sealed before
  the binding existed.

The server never sees the payload in plaintext when encryption is enabled; the
envelope around it stays visible, as listed below. Keys are derived
from the libp2p peer identity, so no additional key exchange is needed.

What this protects, and what it does not:

- **Confidentiality and integrity of the payload** against the server and
  the network. Only the recipient's identity key opens the box.
- **Placement.** Because the binding is sealed inside the box, a ciphertext
  copied from one message cannot be presented as another: not in a different
  folder, not under a different message ID, not to a different recipient.
  A verbatim replay of the same message into the same folder, for instance
  after the recipient deleted it, is not detected; a client that needs that
  keeps a record of message IDs it has seen.
- **No forward secrecy.** The keys are the long-term identity keys, so a
  later compromise of either party's key decrypts everything they exchanged.
  This is out of scope for this layer by design: an application that needs
  forward secrecy runs a ratcheting protocol such as Signal over these
  messages, as the OverNode client does, and uses this layer, if at all, as
  a second envelope.
- **Deniable, not signed.** NaCl box authenticates the sender to the
  recipient only; a recipient cannot prove to a third party who wrote a
  message.
- **The envelope is not encrypted.** Sender, recipient, folder path,
  timestamps, priority and flags are visible to the server, and the
  `encrypted` flag itself is not authenticated: stripping it makes the
  recipient fail to decrypt, which is a denial of service and nothing more.
- **One key, several uses.** The same Ed25519 identity signs the Noise
  handshake and GossipSub messages and, converted to X25519, does the key
  agreement here.

### Trusted Peers

`security.trusted_peers` is one list with two uses. With
`enable_authentication` on it is the set of peers allowed to connect at all;
an untrusted peer costs one Noise handshake and is refused before any stream
opens. With `enable_forwarding` on it is the set of peers allowed to submit a
message whose sender is not themselves: the message is marked forwarded, its
hop count advanced, and it is refused past the hop limit.

### Identity Is Free, So Per-Peer Limits Are Fairness Controls

A peer identity is an Ed25519 key, which costs nothing to mint, so every
limit keyed by peer ID -- the per-peer rate limiters, the per-peer admission
slots, mailbox quotas -- bounds what one *well-behaved* client can consume,
not what a determined one can. The abuse controls are the ones a client
cannot rotate out of: `max_concurrent_connections` caps the server as a
whole, and admission control bounds work in flight against the database
regardless of how many identities ask for it.

The one limit tied to a source address is `max_connections_per_ip`, and it
is off by default. Phones behind a carrier NAT and desks behind an office
gateway share an address, and any cap turns the clients past it away.
go-libp2p applies its own per-address defaults, eight connections and
0.2 new connections a second, to every host; this server replaces them with
no per-address cap and no per-address rate limit, because it bounds
concurrency, never rate. Set `max_connections_per_ip` only where one address
is known to be one client.

### Transport Security

All P2P connections use the [Noise protocol framework](https://noiseprotocol.org/) for transport encryption and mutual authentication.

### Identity

Server and client identities are Ed25519 keypairs. Identity can be provided via:
1. `RICOCHET_SEED_HEX` environment variable (highest priority)
2. `--identity-file` CLI flag
3. Auto-generated and persisted to `{data-dir}/peer_identity.key`

### Dependency Advisories

CI runs `govulncheck` on every push. Two advisories it reports have no
upstream fix, and neither reaches code this server runs:

- **GO-2026-4479** (`pion/dtls/v2`, AES-GCM nonce reuse) is linked through
  go-libp2p's WebRTC transport. The host is built with `libp2p.NoTransports`
  and only the UDX transport, so no DTLS handshake ever happens.
- **GO-2024-3218** (`go-libp2p-kad-dht`, content censorship via Sybil peers)
  concerns IPFS content routing: provider records for a CID. Ricochet uses
  the DHT for peer routing only and never publishes or looks up provider
  records, so there is nothing to censor.

## Networking

### Transport Stack

```
Application (Ricochet protocols)
    |
Yamux (stream multiplexing)
    |
Noise (transport encryption)
    |
UDX (reliable UDP transport: ordered streams, congestion and flow control)
    |
UDP
```

### NAT Traversal

The relay service is on by default, so a public server relays for the peers
behind NATs. A server that is itself behind a NAT reaches the network through
other relays; that is off by default and configured in the YAML file, with the
relay candidates taken from the bootstrap peers:

```yaml
features:
  enable_relay: true           # Circuit Relay v2 (default)
  enable_auto_relay: true      # reserve slots on the static relays below
  enable_hole_punching: true   # DCUtR: upgrade relayed connections to direct
server:
  bootstrap_peers:
    - /ip4/relay.example.com/udp/55223/udx/p2p/12D3KooW...
```

### Service Discovery

Servers announce themselves via GossipSub on the `/sf-network/services/announce` topic. Announcements include capabilities, storage capacity and region. They also carry an uptime score, which is a constant 1.0: nothing measures uptime yet, and clients that sort on it see every server as equal.

## Project Structure

```
cmd/ricochet/           Server CLI entry point
cmd/ricochet-bench/     Load testing tool
internal/
  admission/            Admission control (bounded work in flight) and the shutdown drain
  capacity/             Storage capacity sampling
  core/                 Message types, config, mailbox addressing
  metrics/              Prometheus collectors
  mda/                  Mail Delivery Agent + push notifier
    mailboxes/          Mailbox types, ACL and retention rules
  mta/                  Mail Transfer Agent (routing, forwarding)
  opsapi/               Operator HTTP surface (health, readiness, metrics, pprof)
  opsview/              Stored-data views under /ops
  presence/             Peer presence detection + cache
  protocol/
    frame/              Length-prefix frame encoding
    wire/               Shared pipeline middleware, status codes, authorization
    msa/                Mail Submission Agent (write path, single and batch)
    maa/                Mail Access Agent (read path)
    mma/                Mailbox Management Agent (admin)
    sda/                Store-Document Agent (documents)
    sfa/                Store-Feed Agent (feeds)
    sca/                Store-Collection Agent (collections)
    notify/             Push notification protocol
    protocoltest/       In-process pipeline harness for handler tests
  ratelimit/            Optional per-protocol rate limiters
  registry/             Service discovery via GossipSub
  server/               Server orchestration and lifecycle
  storage/              Storage interface and models
    postgres/           PostgreSQL backend
    storagetest/        In-memory Storage for unit tests
  trust/                Trusted-peer set and connection gate
pkg/client/             Public client library
  client.go             Messages, mailboxes, documents
  collections.go        Collection operations
  feeds.go              Feed operations
  options.go            Functional options
  errors.go             Typed errors
  compression.go        LZ4 compression
  encryption.go         NaCl box encryption
  notifications.go      Push notification handler
pkg/wire/               Wire model shared by client and server
test/integration/       Integration tests (requires PostgreSQL)
deploy/                 Supervisord and Debian deployment files
build/                  Package build output
doc/                    Design notes, audits and the backlog
schema.sql              PostgreSQL database schema
```

## Stress Testing

`ricochet-bench` is an Apache Bench-style load testing tool for Ricochet servers. It measures throughput, latency percentiles, and error rates for every protocol the server serves.

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
| `-batch-size` | 100 | Documents or messages per request, for the batch scenarios |
| `-docs` | 500 | Vault size for the sync scenarios |
| `-v` | off | Verbose output (libp2p and UDX diagnostic logs) |

**Protocols:**

| Protocol | Operation | Description |
|----------|-----------|-------------|
| `msa` | `SendMessage` | Message submission throughput |
| `maa` | `RetrieveMessages` | Message retrieval (pre-seeds mailbox data) |
| `sda` | `PutDocument` + `GetDocument` | Document store round-trip |
| `sfa` | `AppendFeedEntry` + `GetFeedEntry` | Feed append and read cycle |
| `sca` | `PutCollectionItem` + `QueryCollection` | Collection write and query cycle |
| `mma` | `CreateMailbox` + `DeleteMailbox` | Mailbox admin round-trip |
| `mixed` | Random mix of `msa`, `maa`, `sda`, `sfa`, `sca`, `mma` | Combined workload |
| `sda-batch` | `PutDocuments` | Batched document writes, `-batch-size` per request |
| `msa-batch` | `SendMessages` | Batched message submission, `-batch-size` per request |
| `sync-cold` | `PutDocument` × `-docs` | First upload of a vault, every document new |
| `sync-warm` | `PutDocument` × `-docs` | Re-upload after edits, every document replaced |
| `sync-noop` | `ListDocuments` + compare | Nothing changed: list and compare ETags, write nothing |

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

A run against a development server on the same laptop, PostgreSQL local:

```
========================================
  Ricochet Bench - MSA (Message Submission)
========================================
Server: /ip4/127.0.0.1/udp/55999/udx/p2p/12D3KooW...oY6B3C4R

[1/4] Creating 10 workers... done
[2/4] Connecting to server... connected
[3/4] Warming up (10 requests)... done
[4/4] Benchmarking 1000 requests with 10 workers...
========================================
  RESULTS
========================================
Concurrency Level:      10
Total Requests:         1000
Payload Size:           1024 bytes

  Completed:            1000
  Failed:               0
  Total time:           1.297s
  Requests/sec:         770.86

Latency Distribution:
  min:    628.0us
  p50:    2.0ms
  p75:    6.8ms
  p90:    25.8ms
  p95:    76.6ms
  p99:    188.7ms
  max:    397.4ms
========================================
```

Ten workers on one machine do not saturate the server; `doc/BASELINES.md`
records the throughput it reaches with more.

### Architecture Notes

Each concurrent worker creates its own libp2p host and client connection, avoiding yamux stream multiplexing contention. For protocols that require pre-existing data (MAA, SFA, SCA), the tool creates the necessary resources before the warmup and the measured run.

## Testing

```bash
# Run the unit tests (no database needed; packages that want one skip without it)
go test $(go list ./... | grep -v test/integration)

# Run integration tests (requires PostgreSQL)
RICOCHET_TEST_POSTGRES_DSN="postgresql://user:pass@localhost:5432/ricochet_test?sslmode=disable" \
  go test ./test/integration/...

# Run all tests
go test ./...

# With verbose output
go test -v ./pkg/client/...
```

### Test Coverage

- **Unit tests**: the six protocol handlers driven in-process against an in-memory store, mailbox and ACL rules, routing, the client against loopback servers, server lifecycle, config parsing, frame codec, compression and encryption. The server, storage and delivery packages also carry tests that run only when `RICOCHET_TEST_POSTGRES_DSN` is set.
- **Integration tests**: full client-server workflows over UDX against PostgreSQL: messaging, flags and expunge, mailbox management and ACLs, documents, feeds, collections, the directory, notifications, shutdown and authorization.

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
| `directory_listings` | Peer directory entries for search |
| `feeds` | Append-only feeds |
| `feed_entries` | Feed entries with sequence numbers |
| `collections` | Keyed collections |
| `collection_items` | Collection items with JSONB content and versions |
| `block_store` | Reserved for content-addressed body offload (see the scale roadmap); unused today |

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.
