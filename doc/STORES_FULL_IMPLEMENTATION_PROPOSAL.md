# Ricochet Stores: Full Implementation Proposal

**Status:** Active (Phase 1+2 Complete)
**Author:** Architecture Team
**Date:** February 2026
**Version:** 2.0 — Go + PostgreSQL
**Prerequisites:** [DOCUMENT_STORE_MVP_PROPOSAL.md](DOCUMENT_STORE_MVP_PROPOSAL.md)
**Related:** [RICOCHET_STORES_PROPOSAL.md](../RICOCHET_STORES_PROPOSAL.md)

---

## Executive Summary

This proposal defines the complete implementation roadmap for the Ricochet Stores architecture, targeting **Go + PostgreSQL** as the canonical stack.

**Phase 1+2 (Document Store) are fully implemented.** The Document Store supports GET, PUT, PATCH, DELETE, LIST, HEAD, HISTORY, and DIRECTORY operations with version history, optimistic locking, and rate limiting. The remaining phases are:

3. **Feed Store** — Append-only broadcast streams for social posts, activity logs, RSS
4. **Collection Store** — Mutable key-value records with JSONB queries
5. **Blob Store** — Content-addressed immutable files (filesystem + PostgreSQL metadata)
6. **Cross-cutting Features** — Subscriptions via PG LISTEN/NOTIFY + GossipSub, unified discovery

**Design decisions:**

- **Storage interface**: Compose sub-interfaces (`MailboxStore`, `DocumentStore`, `FeedStore`, `CollectionStore`, `BlobStore`)
- **CRDTs**: Deferred to a future proposal — focus on single-server correctness
- **Blob storage**: Filesystem for content + PostgreSQL for metadata/CID index
- **Protocol handlers**: Separate libp2p protocol IDs per store type (e.g., SDA, SFA, SCA, SBA)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Phase 2: Document Store — COMPLETE](#2-phase-2-document-store--complete)
3. [Phase 3: Feed Store](#3-phase-3-feed-store)
4. [Phase 4: Collection Store](#4-phase-4-collection-store)
5. [Phase 5: Blob Store](#5-phase-5-blob-store)
6. [Cross-cutting Features](#6-cross-cutting-features)
7. [Storage Interface Refactor](#7-storage-interface-refactor)
8. [Migration & Compatibility](#8-migration--compatibility)
9. [Implementation Timeline](#9-implementation-timeline)

---

## 1. Architecture Overview

### 1.1 Target Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Ricochet Server (Go + PostgreSQL)                      │
├──────────────────────────────────────────────────────────────────────────────┤
│  Protocol Layer (libp2p stream handlers)                                     │
│  ├── MSA /ricochet/msa/1.0.0           Mail Store Agent (send)              │
│  ├── MAA /ricochet/maa/1.0.0           Mail Access Agent (retrieve)         │
│  ├── MMA /ricochet/mma/1.0.0           Mail Management Agent (admin)        │
│  │                                                                           │
│  └── Store Protocols                                                         │
│       ├── SDA /ricochet/store/doc/1.0.0        Document Store  ✅ COMPLETE  │
│       ├── SFA /ricochet/store/feed/1.0.0       Feed Store                   │
│       ├── SCA /ricochet/store/collection/1.0.0 Collection Store             │
│       └── SBA /ricochet/store/blob/1.0.0       Blob Store                   │
├──────────────────────────────────────────────────────────────────────────────┤
│  Go Packages                                                                 │
│  ├── internal/protocol/sda/   Document handler   ✅                         │
│  ├── internal/protocol/sfa/   Feed handler                                   │
│  ├── internal/protocol/sca/   Collection handler                             │
│  ├── internal/protocol/sba/   Blob handler                                   │
│  ├── internal/storage/        Storage interface + models                      │
│  ├── internal/storage/postgres/ PostgreSQL implementation                    │
│  └── pkg/client/              Public client API                              │
├──────────────────────────────────────────────────────────────────────────────┤
│  PostgreSQL Storage                                                          │
│  ├── mailboxes, stored_messages, mailbox_acls   (existing)                   │
│  ├── documents, document_versions               ✅ COMPLETE                 │
│  ├── directory_listings                          ✅ COMPLETE                 │
│  ├── feeds, feed_entries                         (Phase 3)                   │
│  ├── collections, collection_items               (Phase 4)                   │
│  └── blobs, blob_pins                            (Phase 5)                   │
├──────────────────────────────────────────────────────────────────────────────┤
│  Filesystem (Phase 5 only)                                                   │
│  └── blob content: {data_dir}/blobs/{prefix}/{cid}                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Unified Addressing

All stores follow a consistent addressing pattern:

```
/{ownerId}/{storeType}/{storePath}[/{key}]

Store Types:
  doc        → Document Store
  feed       → Feed Store
  collection → Collection Store
  blob       → Blob Store

Examples:
/12D3KooWAlice.../doc/profile              → Alice's profile document
/12D3KooWAlice.../feed/posts               → Alice's posts feed
/12D3KooWAlice.../feed/posts/42            → Entry #42 in Alice's posts
/12D3KooWAlice.../collection/products      → Alice's product catalog
/12D3KooWAlice.../collection/products/sku-1 → Specific product record
/12D3KooWAlice.../blob/bafybeig...         → Blob pinned by Alice
/blob/bafybeig...                          → Global blob by CID
```

### 1.3 Store Type Comparison

| Aspect | Document | Feed | Collection | Blob |
|--------|----------|------|------------|------|
| **Data Pattern** | Single mutable object | Append-only stream | Mutable key-value set | Immutable file |
| **Addressing** | By path | By path + sequence | By path + key | By CID |
| **Mutability** | Replace/patch | Append only | Per-record upsert | Immutable |
| **Versioning** | ETag (content hash) | Sequence numbers | Per-record version | CID is version |
| **Query** | GET by path | Range by sequence | Filter via JSONB | GET by CID |
| **Use Cases** | Profiles, settings | Posts, logs, RSS | Catalogs, contacts | Media, files |
| **PostgreSQL** | `documents` table | `feeds` + `feed_entries` | `collections` + `collection_items` | `blobs` + `blob_pins` (metadata only) |

### 1.4 Protocol Handler Pattern

All store protocols follow the established SDA pattern:

1. Client opens a libp2p stream with the protocol ID
2. Client sends a length-prefixed JSON frame (`frame.WriteFrame`)
3. Server reads the frame, parses the request, dispatches by operation
4. Server sends a length-prefixed JSON response frame
5. Stream is closed

```go
// Every handler follows this structure (see internal/protocol/sda/handler.go)
func (h *Handler) HandleStream(s network.Stream) {
    callerID := s.Conn().RemotePeer()
    defer s.CloseWrite()

    data, err := frame.ReadFrame(s)
    // ... parse request, validate, dispatch by operation ...
    h.writeResponse(s, resp)
}
```

---

## 2. Phase 2: Document Store — COMPLETE

**Status:** ✅ Fully implemented
**Protocol ID:** `/ricochet/store/doc/1.0.0`

### 2.1 Implemented Operations

| Operation | Description | Handler Method |
|-----------|-------------|----------------|
| `GET` | Retrieve document (supports `If-None-Match`) | `handleGet` |
| `PUT` | Create or replace document (supports `If-Match`) | `handlePut` |
| `PATCH` | JSON Merge Patch (RFC 7396) | `handlePatch` |
| `HEAD` | Metadata without body | `handleHead` |
| `DELETE` | Remove document | `handleDelete` |
| `LIST` | Enumerate all documents for an owner | `handleList` |
| `HISTORY` | Version history listing or retrieve specific version | `handleHistory` |
| `DIRECTORY` | Join/leave/browse/get the public directory | `handleDirectory` |

### 2.2 Key Files

| File | Purpose |
|------|---------|
| `internal/protocol/sda/handler.go` | Protocol handler — stream handling, operation dispatch, rate limiting |
| `internal/storage/storage.go` | `Storage` interface — `GetDocument`, `PutDocument`, `PatchDocument`, etc. |
| `internal/storage/models.go` | `DocumentRecord`, `DocumentVersionRecord`, `DocumentPutResult`, `DirectoryEntry` |
| `internal/storage/postgres/postgres.go` | PostgreSQL implementation — pgx queries, merge-patch, version history |
| `schema.sql` | `documents`, `document_versions`, `directory_listings` tables |
| `pkg/client/client.go` | Public client API — `GetDocument`, `PutDocument`, `PatchDocument`, etc. |

### 2.3 PostgreSQL Schema (existing)

```sql
CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    owner_peer_id TEXT NOT NULL,
    path TEXT NOT NULL,
    content BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    content_hash TEXT NOT NULL,       -- sha256:{hex} ETag
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    updated_by_peer_id TEXT NOT NULL,
    version_number INT NOT NULL DEFAULT 1,
    history_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    max_history_versions INT,
    version_vector TEXT,              -- reserved for future CRDT use
    CONSTRAINT uq_document_owner_path UNIQUE(owner_peer_id, path)
);

CREATE TABLE IF NOT EXISTS document_versions (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    version_number INT NOT NULL,
    content BYTEA NOT NULL,
    content_hash TEXT NOT NULL,
    content_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    created_by_peer_id TEXT NOT NULL,
    CONSTRAINT uq_document_version UNIQUE(document_id, version_number)
);
```

---

## 3. Phase 3: Feed Store

**Builds on:** Document Store (complete)
**Protocol ID:** `/ricochet/store/feed/1.0.0`
**Handler package:** `internal/protocol/sfa/` (Store Feed Agent)

### 3.1 Overview

Feed Stores are **append-only broadcast streams** for one-to-many publication:

```
Owner (Publisher)                    Subscribers
┌─────────────┐                    ┌─────────────┐
│    Alice    │                    │     Bob     │
└──────┬──────┘                    └──────┬──────┘
       │                                  │
       │ APPEND                           │ GET (from=cursor)
       ▼                                  │
┌─────────────────────────────────────────┴─────────────────┐
│                        Feed Store                         │
│  /{Alice}/feed/posts                                      │
│  ├── Metadata: {title, description, icon, created}        │
│  └── Entries:                                             │
│      ├── seq=1: {type: "post", text: "Hello!", ts: ...}  │
│      ├── seq=2: {type: "post", text: "Check out...", ...}│
│      └── seq=N: ...                                       │
└───────────────────────────────────────────────────────────┘
```

### 3.2 Data Model

Add to `internal/storage/models.go`:

```go
// FeedRecord represents a stored feed.
type FeedRecord struct {
    ID               int64     `json:"id"`
    OwnerPeerID      string    `json:"ownerPeerId"`
    Path             string    `json:"path"`             // e.g. "posts", "activity"
    Title            string    `json:"title,omitempty"`
    Description      string    `json:"description,omitempty"`
    EntryContentType string    `json:"entryContentType"` // default: application/json
    CreatedAt        time.Time `json:"createdAt"`
    LastEntryAt      time.Time `json:"lastEntryAt"`
    CurrentSequence  int       `json:"currentSequence"`
    MaxEntries       *int      `json:"maxEntries,omitempty"`       // retention cap
    MaxAgeDays       *int      `json:"maxAgeDays,omitempty"`       // retention TTL
}

// FeedEntryRecord represents a single entry in a feed.
type FeedEntryRecord struct {
    ID              int64     `json:"id"`
    FeedID          int64     `json:"feedId"`
    SequenceNumber  int       `json:"sequenceNumber"`
    Content         []byte    `json:"content"`
    ContentHash     string    `json:"contentHash"`
    CreatedAt       time.Time `json:"createdAt"`
    CreatedByPeerID string    `json:"createdByPeerId"`
    EntryType       string    `json:"entryType,omitempty"` // e.g. "post", "reply"
}
```

### 3.3 PostgreSQL Schema

Add to `schema.sql`:

```sql
-- =============================================================================
-- FEEDS
-- =============================================================================
CREATE TABLE IF NOT EXISTS feeds (
    id BIGSERIAL PRIMARY KEY,
    owner_peer_id TEXT NOT NULL,
    path TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    entry_content_type TEXT NOT NULL DEFAULT 'application/json',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_entry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    current_sequence INT NOT NULL DEFAULT 0,
    max_entries INT,
    max_age_days INT,

    CONSTRAINT uq_feed_owner_path UNIQUE(owner_peer_id, path)
);

CREATE INDEX IF NOT EXISTS idx_feeds_owner ON feeds(owner_peer_id);

-- =============================================================================
-- FEED ENTRIES
-- =============================================================================
CREATE TABLE IF NOT EXISTS feed_entries (
    id BIGSERIAL PRIMARY KEY,
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    sequence_number INT NOT NULL,
    content BYTEA NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by_peer_id TEXT NOT NULL,
    entry_type TEXT NOT NULL DEFAULT '',

    CONSTRAINT uq_feed_entry_seq UNIQUE(feed_id, sequence_number)
);

-- Range queries: entries by feed, ordered by sequence
CREATE INDEX IF NOT EXISTS idx_feed_entries_feed_seq
    ON feed_entries(feed_id, sequence_number);

-- Entry type filtering
CREATE INDEX IF NOT EXISTS idx_feed_entries_type
    ON feed_entries(feed_id, entry_type)
    WHERE entry_type != '';

-- Retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_feed_entries_created
    ON feed_entries(feed_id, created_at);
```

### 3.4 FeedStore Sub-Interface

Add to `internal/storage/storage.go`:

```go
// FeedStore defines feed operations.
type FeedStore interface {
    // Feed lifecycle
    CreateFeed(ctx context.Context, ownerID peer.ID, path string, title, description string) (*FeedRecord, error)
    GetFeed(ctx context.Context, ownerID peer.ID, path string) (*FeedRecord, error)
    DeleteFeed(ctx context.Context, ownerID peer.ID, path string) (bool, error)
    ListFeeds(ctx context.Context, ownerID peer.ID) ([]*FeedRecord, error)

    // Entry operations
    AppendFeedEntry(ctx context.Context, feedID int64, content []byte, contentType string, createdBy peer.ID, entryType string) (*FeedEntryRecord, error)
    GetFeedEntry(ctx context.Context, feedID int64, sequenceNumber int) (*FeedEntryRecord, error)
    GetFeedEntries(ctx context.Context, feedID int64, fromSeq, toSeq *int, entryType string, limit int) ([]*FeedEntryRecord, bool, error)

    // Retention
    EnforceFeedRetention(ctx context.Context, feed *FeedRecord) (int, error)
}
```

### 3.5 Protocol Handler

`internal/protocol/sfa/handler.go`:

```go
package sfa

import (
    "context"
    "encoding/base64"
    "encoding/json"
    "log/slog"

    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/core/protocol"

    "github.com/twostack/go-ricochet/internal/protocol/frame"
    "github.com/twostack/go-ricochet/internal/storage"
)

const ProtocolID = protocol.ID("/ricochet/store/feed/1.0.0")

const (
    OpCREATE = "CREATE"
    OpGET    = "GET"
    OpAPPEND = "APPEND"
    OpDELETE = "DELETE"
    OpLIST   = "LIST"
)

// FeedRequest is the JSON request format for feed operations.
type FeedRequest struct {
    Operation   string            `json:"operation"`
    OwnerPeerID string            `json:"ownerPeerId"`
    Path        string            `json:"path,omitempty"`
    Headers     map[string]string `json:"headers,omitempty"`
    Body        string            `json:"body,omitempty"` // base64 encoded

    // GET range parameters
    FromSequence *int   `json:"fromSequence,omitempty"`
    ToSequence   *int   `json:"toSequence,omitempty"`
    Limit        *int   `json:"limit,omitempty"`
    EntryType    string `json:"entryType,omitempty"`

    // GET specific entry
    SequenceNumber *int `json:"sequenceNumber,omitempty"`

    // CREATE parameters
    Title       string `json:"title,omitempty"`
    Description string `json:"description,omitempty"`
}

// FeedResponse is the JSON response format for feed operations.
type FeedResponse struct {
    Status  int            `json:"status"`
    Headers map[string]any `json:"headers,omitempty"`
    Body    string         `json:"body,omitempty"` // base64 encoded
}

type Handler struct {
    store  storage.Storage
    logger *slog.Logger
}

func NewHandler(store storage.Storage, logger *slog.Logger) *Handler {
    return &Handler{store: store, logger: logger}
}

func (h *Handler) HandleStream(s network.Stream) {
    callerID := s.Conn().RemotePeer()
    defer s.CloseWrite()

    data, err := frame.ReadFrame(s)
    if err != nil {
        h.writeResponse(s, &FeedResponse{Status: 400})
        return
    }

    var req FeedRequest
    if err := json.Unmarshal(data, &req); err != nil {
        h.writeResponse(s, &FeedResponse{Status: 400})
        return
    }

    ownerID, err := peer.Decode(req.OwnerPeerID)
    if err != nil {
        h.writeResponse(s, &FeedResponse{Status: 400,
            Headers: map[string]any{"Error": "invalid ownerPeerId"}})
        return
    }

    // Enforce owner-only for write operations
    isWrite := req.Operation == OpCREATE || req.Operation == OpAPPEND || req.Operation == OpDELETE
    if isWrite && callerID != ownerID {
        h.writeResponse(s, &FeedResponse{Status: 403,
            Headers: map[string]any{"Error": "write operations require owner access"}})
        return
    }

    ctx := context.Background()

    switch req.Operation {
    case OpCREATE:
        h.handleCreate(ctx, s, &req, ownerID)
    case OpGET:
        h.handleGet(ctx, s, &req, ownerID)
    case OpAPPEND:
        h.handleAppend(ctx, s, &req, ownerID, callerID)
    case OpDELETE:
        h.handleDelete(ctx, s, &req, ownerID)
    case OpLIST:
        h.handleList(ctx, s, ownerID)
    default:
        h.writeResponse(s, &FeedResponse{Status: 400,
            Headers: map[string]any{"Error": "unknown operation: " + req.Operation}})
    }
}
```

### 3.6 Protocol Specification

#### CREATE — Create Feed

```json
// Request
{
  "operation": "CREATE",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "title": "Alice's Posts",
  "description": "Thoughts and updates"
}

// Response
{
  "status": 201,
  "headers": {
    "Content-Type": "application/json"
  }
}
```

#### APPEND — Add Entry

```json
// Request
{
  "operation": "APPEND",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "headers": {
    "Content-Type": "application/json"
  },
  "entryType": "post",
  "body": "eyJ0ZXh0IjoiSGVsbG8gd29ybGQhIn0="
}

// Response
{
  "status": 201,
  "headers": {
    "X-Sequence": 42,
    "ETag": "sha256:entry123..."
  }
}
```

#### GET — Read Feed Metadata or Entries

```json
// Get feed metadata
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts"
}

// Get specific entry by sequence number
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "sequenceNumber": 42
}

// Get range of entries
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "fromSequence": 30,
  "toSequence": 40,
  "limit": 10,
  "entryType": "post"
}

// Range response
{
  "status": 200,
  "headers": {
    "X-Has-More": true,
    "X-Next-Sequence": 40,
    "Content-Type": "application/json"
  },
  "body": "<base64-encoded JSON array of entries>"
}
```

### 3.7 Client API

Add to `pkg/client/client.go`:

```go
// CreateFeed creates a new feed on the server.
func (c *Client) CreateFeed(ctx context.Context, path, title, description string, opts ...FeedOption) error {
    req := &sfa.FeedRequest{
        Operation:   sfa.OpCREATE,
        OwnerPeerID: c.host.ID().String(),
        Path:        path,
        Title:       title,
        Description: description,
    }
    resp, err := c.doFeed(ctx, req, opts)
    if err != nil {
        return err
    }
    if resp.Status >= 400 {
        return fmt.Errorf("create feed: status %d", resp.Status)
    }
    return nil
}

// AppendFeedEntry appends an entry to a feed.
func (c *Client) AppendFeedEntry(ctx context.Context, path string, content []byte, entryType string, opts ...FeedOption) (int, error) {
    req := &sfa.FeedRequest{
        Operation:   sfa.OpAPPEND,
        OwnerPeerID: c.host.ID().String(),
        Path:        path,
        EntryType:   entryType,
        Body:        base64.StdEncoding.EncodeToString(content),
    }
    resp, err := c.doFeed(ctx, req, opts)
    if err != nil {
        return 0, err
    }
    seq := int(headerToInt64(resp.Headers["X-Sequence"]))
    return seq, nil
}

// GetFeedEntries retrieves a range of feed entries.
func (c *Client) GetFeedEntries(ctx context.Context, ownerPeerID peer.ID, path string, fromSeq *int, limit *int, opts ...FeedOption) ([]*FeedEntry, bool, error) {
    req := &sfa.FeedRequest{
        Operation:    sfa.OpGET,
        OwnerPeerID:  ownerPeerID.String(),
        Path:         path,
        FromSequence: fromSeq,
        Limit:        limit,
    }
    resp, err := c.doFeed(ctx, req, opts)
    if err != nil {
        return nil, false, err
    }
    // ... decode response body into []*FeedEntry ...
}
```

### 3.8 Use Cases

| Use Case | Feed Path | Entry Structure |
|----------|-----------|-----------------|
| Microblog posts | `/feed/posts` | `{type, text, media[], replyTo?, ts}` |
| Activity log | `/feed/activity` | `{action, target, metadata, ts}` |
| RSS/Atom feed | `/feed/articles` | `{guid, title, link, description, pubDate}` |
| Order history | `/feed/orders` | `{orderId, status, items[], total, ts}` |
| Notifications | `/feed/notifications` | `{type, title, body, action?, ts}` |

---

## 4. Phase 4: Collection Store

**Builds on:** Document Store (complete)
**Protocol ID:** `/ricochet/store/collection/1.0.0`
**Handler package:** `internal/protocol/sca/` (Store Collection Agent)

### 4.1 Overview

Collection Stores hold **mutable key-value records** with query capabilities powered by PostgreSQL JSONB:

```
┌─────────────────────────────────────────────────────────────┐
│                     Collection Store                         │
│  /{Alice}/collection/products                                │
│                                                              │
│  Metadata:                                                   │
│    name: "Bob's Hardware Products"                           │
│    recordCount: 156                                          │
│                                                              │
│  Records (JSONB content):                                    │
│    "sku-001" → {"name": "Drill", "price": 49.99, ...}      │
│    "sku-002" → {"name": "Hammer", "price": 19.99, ...}     │
│    "sku-003" → {"name": "Paint", "price": 29.99, ...}      │
│    ...                                                       │
└─────────────────────────────────────────────────────────────┘

Operations:
  CREATE /collection/products               → Create collection
  GET    /collection/products               → Metadata
  GET    /collection/products/sku-001       → Single record
  PUT    /collection/products/sku-001       → Upsert record
  DELETE /collection/products/sku-001       → Remove record
  QUERY  /collection/products               → Filter via JSONB operators
  LIST   /collection/products               → List all keys
```

### 4.2 Data Model

Add to `internal/storage/models.go`:

```go
// CollectionRecord represents a stored collection.
type CollectionRecord struct {
    ID               int64     `json:"id"`
    OwnerPeerID      string    `json:"ownerPeerId"`
    Path             string    `json:"path"`
    Name             string    `json:"name,omitempty"`
    CreatedAt        time.Time `json:"createdAt"`
    LastModifiedAt   time.Time `json:"lastModifiedAt"`
    RecordCount      int       `json:"recordCount"`
}

// CollectionItemRecord represents a single record in a collection.
type CollectionItemRecord struct {
    ID              int64     `json:"id"`
    CollectionID    int64     `json:"collectionId"`
    Key             string    `json:"key"`
    Content         []byte    `json:"content"`          // stored as JSONB in PG
    ContentHash     string    `json:"contentHash"`
    Version         int       `json:"version"`
    CreatedAt       time.Time `json:"createdAt"`
    UpdatedAt       time.Time `json:"updatedAt"`
    UpdatedByPeerID string    `json:"updatedByPeerId"`
}

// CollectionQueryResult wraps a paged query response.
type CollectionQueryResult struct {
    Items      []*CollectionItemRecord `json:"items"`
    TotalCount int                     `json:"totalCount"`
    HasMore    bool                    `json:"hasMore"`
}
```

### 4.3 PostgreSQL Schema

Add to `schema.sql`:

```sql
-- =============================================================================
-- COLLECTIONS
-- =============================================================================
CREATE TABLE IF NOT EXISTS collections (
    id BIGSERIAL PRIMARY KEY,
    owner_peer_id TEXT NOT NULL,
    path TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    record_count INT NOT NULL DEFAULT 0,

    CONSTRAINT uq_collection_owner_path UNIQUE(owner_peer_id, path)
);

CREATE INDEX IF NOT EXISTS idx_collections_owner ON collections(owner_peer_id);

-- =============================================================================
-- COLLECTION ITEMS
-- =============================================================================
CREATE TABLE IF NOT EXISTS collection_items (
    id BIGSERIAL PRIMARY KEY,
    collection_id BIGINT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    content JSONB NOT NULL,               -- JSONB enables native query operators
    content_hash TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by_peer_id TEXT NOT NULL,

    CONSTRAINT uq_collection_item_key UNIQUE(collection_id, key)
);

-- GIN index for JSONB containment and key-exists queries
CREATE INDEX IF NOT EXISTS idx_collection_items_content
    ON collection_items USING GIN(content);

-- Key listing and lookup
CREATE INDEX IF NOT EXISTS idx_collection_items_key
    ON collection_items(collection_id, key);

-- Version tracking
CREATE INDEX IF NOT EXISTS idx_collection_items_updated
    ON collection_items(collection_id, updated_at DESC);
```

### 4.4 Query Support via PostgreSQL JSONB

**No custom query parser needed.** PostgreSQL JSONB operators provide powerful querying natively. The QUERY operation accepts a JSON filter object that maps directly to JSONB SQL operators:

```go
// buildJSONBFilter translates a filter map into SQL WHERE clauses.
// This leverages PostgreSQL's native JSONB operators rather than
// implementing a custom query parser.
func buildJSONBFilter(filter map[string]any) (string, []any) {
    // Equality:  {"category": "tools"}
    //   → content @> '{"category": "tools"}'::jsonb
    //
    // Comparison: {"price": {"$lt": 50}}
    //   → (content->>'price')::numeric < 50
    //
    // Containment: {"tags": {"$contains": "sale"}}
    //   → content->'tags' @> '"sale"'::jsonb
    //
    // Text search: {"name": {"$ilike": "%drill%"}}
    //   → content->>'name' ILIKE '%drill%'
}
```

**Example queries:**

```json
// Equality (uses @> containment)
{"category": "tools"}

// Comparison operators (cast via ->>)
{"price": {"$lt": 50}}
{"price": {"$gte": 10, "$lte": 100}}

// Text matching
{"name": {"$ilike": "%drill%"}}

// Array containment
{"tags": {"$contains": "sale"}}

// Compound
{"$and": [{"category": "tools"}, {"price": {"$lt": 50}}]}
```

### 4.5 CollectionStore Sub-Interface

Add to `internal/storage/storage.go`:

```go
// CollectionStore defines collection operations.
type CollectionStore interface {
    // Collection lifecycle
    CreateCollection(ctx context.Context, ownerID peer.ID, path, name string) (*CollectionRecord, error)
    GetCollection(ctx context.Context, ownerID peer.ID, path string) (*CollectionRecord, error)
    DeleteCollection(ctx context.Context, ownerID peer.ID, path string) (bool, error)
    ListCollections(ctx context.Context, ownerID peer.ID) ([]*CollectionRecord, error)

    // Item operations
    GetCollectionItem(ctx context.Context, collectionID int64, key string) (*CollectionItemRecord, error)
    PutCollectionItem(ctx context.Context, collectionID int64, key string, content []byte, updatedBy peer.ID, ifMatch *string) (*CollectionItemRecord, bool, error)
    DeleteCollectionItem(ctx context.Context, collectionID int64, key string) (bool, error)
    ListCollectionKeys(ctx context.Context, collectionID int64, limit, offset int) ([]string, int, error)

    // Query
    QueryCollection(ctx context.Context, collectionID int64, filter map[string]any, sortField string, sortAsc bool, limit, offset int) (*CollectionQueryResult, error)
}
```

### 4.6 Protocol Handler

`internal/protocol/sca/handler.go` — follows the same pattern as SDA:

```go
package sca

const ProtocolID = protocol.ID("/ricochet/store/collection/1.0.0")

const (
    OpCREATE = "CREATE"
    OpGET    = "GET"
    OpPUT    = "PUT"
    OpDELETE = "DELETE"
    OpLIST   = "LIST"
    OpQUERY  = "QUERY"
)

type CollectionRequest struct {
    Operation   string            `json:"operation"`
    OwnerPeerID string            `json:"ownerPeerId"`
    Path        string            `json:"path,omitempty"`
    Key         string            `json:"key,omitempty"`
    Headers     map[string]string `json:"headers,omitempty"`
    Body        string            `json:"body,omitempty"` // base64 encoded

    // CREATE parameters
    Name string `json:"name,omitempty"`

    // QUERY parameters
    Filter    map[string]any `json:"filter,omitempty"`
    SortField string         `json:"sortField,omitempty"`
    SortAsc   *bool          `json:"sortAsc,omitempty"`
    Limit     *int           `json:"limit,omitempty"`
    Offset    *int           `json:"offset,omitempty"`
}

type CollectionResponse struct {
    Status  int            `json:"status"`
    Headers map[string]any `json:"headers,omitempty"`
    Body    string         `json:"body,omitempty"`
}
```

### 4.7 Client API

```go
// CreateCollection creates a new collection on the server.
func (c *Client) CreateCollection(ctx context.Context, path, name string, opts ...CollectionOption) error

// PutCollectionItem upserts a record in a collection.
func (c *Client) PutCollectionItem(ctx context.Context, ownerPeerID peer.ID, path, key string, content []byte, opts ...CollectionOption) (*CollectionItemResult, error)

// GetCollectionItem retrieves a single record by key.
func (c *Client) GetCollectionItem(ctx context.Context, ownerPeerID peer.ID, path, key string, opts ...CollectionOption) (*CollectionItemResult, error)

// QueryCollection queries records using JSONB filters.
func (c *Client) QueryCollection(ctx context.Context, ownerPeerID peer.ID, path string, filter map[string]any, opts ...CollectionOption) (*CollectionQueryResponse, error)
```

### 4.8 Use Cases

| Use Case | Collection Path | Record Structure |
|----------|-----------------|------------------|
| Product catalog | `/collection/products` | `{"sku": "...", "name": "...", "price": 49.99, "category": "tools"}` |
| Contact list | `/collection/contacts` | `{"name": "...", "email": "...", "phone": "...", "tags": ["friend"]}` |
| Inventory | `/collection/inventory` | `{"sku": "...", "location": "...", "quantity": 42}` |
| Following list | `/collection/following` | `{"peerId": "...", "since": "...", "nickname": "..."}` |
| Bookmarks | `/collection/bookmarks` | `{"url": "...", "title": "...", "tags": ["tech"], "added": "..."}` |

---

## 5. Phase 5: Blob Store

**Builds on:** Document Store (complete)
**Protocol ID:** `/ricochet/store/blob/1.0.0`
**Handler package:** `internal/protocol/sba/` (Store Blob Agent)

### 5.1 Overview

Blob Stores provide **content-addressed immutable file storage** with a split architecture:

- **Filesystem**: Blob content stored in a content-addressed directory structure
- **PostgreSQL**: Metadata, CID index, and pin tracking

```
┌─────────────────────────────────────────────────────────────┐
│                       Blob Store                            │
│                                                             │
│  Upload:   Client → PUT /{owner}/blob → CID returned       │
│  Download: Client → GET /blob/{cid}   → Content returned   │
│                                                             │
│  Filesystem ({data_dir}/blobs/):                            │
│    ba/fybei.../  → raw file content                        │
│                                                             │
│  PostgreSQL:                                                │
│    blobs table   → CID, size, content_type, timestamps     │
│    blob_pins     → which owners have pinned which CIDs     │
│                                                             │
│  Properties:                                                │
│    - Content-addressed (CID = hash of content)             │
│    - Immutable (CID never changes)                         │
│    - Deduplication (same content = same CID)               │
│    - Pinning (owners explicitly pin blobs they care about) │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Data Model

Add to `internal/storage/models.go`:

```go
// BlobRecord represents blob metadata in PostgreSQL.
// The actual blob content lives on the filesystem.
type BlobRecord struct {
    ID              int64     `json:"id"`
    CID             string    `json:"cid"`             // CIDv1 content identifier
    Size            int64     `json:"size"`            // bytes
    ContentType     string    `json:"contentType"`
    CreatedAt       time.Time `json:"createdAt"`
    LastAccessAt    time.Time `json:"lastAccessAt"`
    AccessCount     int       `json:"accessCount"`
}

// BlobPinRecord tracks which owners have pinned a blob.
type BlobPinRecord struct {
    ID          int64      `json:"id"`
    CID         string     `json:"cid"`
    OwnerPeerID string     `json:"ownerPeerId"`
    PinnedAt    time.Time  `json:"pinnedAt"`
    ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
}
```

### 5.3 PostgreSQL Schema

Add to `schema.sql`:

```sql
-- =============================================================================
-- BLOBS (metadata only — content is on the filesystem)
-- =============================================================================
CREATE TABLE IF NOT EXISTS blobs (
    id BIGSERIAL PRIMARY KEY,
    cid TEXT NOT NULL,
    size BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_access_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    access_count INT NOT NULL DEFAULT 0,

    CONSTRAINT uq_blob_cid UNIQUE(cid)
);

-- =============================================================================
-- BLOB PINS
-- =============================================================================
CREATE TABLE IF NOT EXISTS blob_pins (
    id BIGSERIAL PRIMARY KEY,
    cid TEXT NOT NULL REFERENCES blobs(cid) ON DELETE CASCADE,
    owner_peer_id TEXT NOT NULL,
    pinned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,

    CONSTRAINT uq_blob_pin UNIQUE(cid, owner_peer_id)
);

CREATE INDEX IF NOT EXISTS idx_blob_pins_owner
    ON blob_pins(owner_peer_id);

-- Expired pin cleanup
CREATE INDEX IF NOT EXISTS idx_blob_pins_expires
    ON blob_pins(expires_at)
    WHERE expires_at IS NOT NULL;
```

### 5.4 Filesystem Layout

Blob content is stored in a content-addressed directory structure:

```
{data_dir}/blobs/
  ba/fybeiA.../   → first two chars of CID as prefix directory
    bafybeiABC... → raw file content
  ba/fybeiD.../
    bafybeiDEF...
```

```go
// BlobFS handles filesystem operations for blob content.
type BlobFS struct {
    rootDir string // e.g. "/var/lib/ricochet/blobs"
}

// blobPath returns the filesystem path for a given CID.
func (fs *BlobFS) blobPath(cid string) string {
    if len(cid) < 6 {
        return filepath.Join(fs.rootDir, cid)
    }
    // Two-level prefix directory: first 2 chars / next 4 chars
    return filepath.Join(fs.rootDir, cid[:2], cid[2:6], cid)
}

// Write stores blob content to the filesystem.
func (fs *BlobFS) Write(cid string, content []byte) error {
    path := fs.blobPath(cid)
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
        return fmt.Errorf("create blob directory: %w", err)
    }
    return os.WriteFile(path, content, 0o644)
}

// Read retrieves blob content from the filesystem.
func (fs *BlobFS) Read(cid string) ([]byte, error) {
    return os.ReadFile(fs.blobPath(cid))
}

// Exists checks if a blob exists on the filesystem.
func (fs *BlobFS) Exists(cid string) bool {
    _, err := os.Stat(fs.blobPath(cid))
    return err == nil
}

// Delete removes blob content from the filesystem.
func (fs *BlobFS) Delete(cid string) error {
    return os.Remove(fs.blobPath(cid))
}
```

### 5.5 BlobStore Sub-Interface

Add to `internal/storage/storage.go`:

```go
// BlobStore defines blob operations.
type BlobStore interface {
    // Blob lifecycle
    PutBlob(ctx context.Context, content []byte, contentType string, ownerID peer.ID) (*BlobRecord, error)
    GetBlob(ctx context.Context, cid string) (*BlobRecord, []byte, error)          // metadata + content
    GetBlobMetadata(ctx context.Context, cid string) (*BlobRecord, error)           // metadata only (HEAD)
    DeleteBlob(ctx context.Context, cid string) error                               // removes content if no pins remain

    // Pin management
    PinBlob(ctx context.Context, cid string, ownerID peer.ID, expiresAt *time.Time) error
    UnpinBlob(ctx context.Context, cid string, ownerID peer.ID) error
    ListPins(ctx context.Context, ownerID peer.ID) ([]*BlobPinRecord, error)

    // Garbage collection
    CollectGarbage(ctx context.Context, unpinnedRetention time.Duration) (int, error)
}
```

### 5.6 Protocol Specification

#### PUT — Upload Blob

```json
// Request
{
  "operation": "PUT",
  "ownerPeerId": "12D3KooWAlice...",
  "headers": {
    "Content-Type": "image/png"
  },
  "body": "<base64-encoded content>"
}

// Response
{
  "status": 201,
  "headers": {
    "X-CID": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
    "Content-Length": 102400
  }
}
```

#### GET — Download Blob

```json
// Request (by CID)
{
  "operation": "GET",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
}

// Response
{
  "status": 200,
  "headers": {
    "Content-Type": "image/png",
    "Content-Length": 102400,
    "X-CID": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
  },
  "body": "<base64-encoded content>"
}
```

#### HEAD — Check Existence

```json
// Request
{
  "operation": "HEAD",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
}

// Response (exists)
{
  "status": 200,
  "headers": {
    "Content-Type": "image/png",
    "Content-Length": 102400
  }
}
```

#### PIN / UNPIN

```json
// Pin
{
  "operation": "PIN",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
  "headers": {
    "X-Expires": "2027-12-31T23:59:59Z"
  }
}

// Unpin
{
  "operation": "UNPIN",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
}
```

### 5.7 Garbage Collection

Unpinned blobs are garbage collected on a configurable schedule:

```go
// CollectGarbage removes blobs that have no active pins and whose
// last access exceeds the unpinnedRetention duration.
func (s *PostgresStorage) CollectGarbage(ctx context.Context, unpinnedRetention time.Duration) (int, error) {
    cutoff := time.Now().Add(-unpinnedRetention)

    // Find blobs with no pins and last access before cutoff
    rows, err := s.pool.Query(ctx, `
        SELECT b.cid FROM blobs b
        LEFT JOIN blob_pins bp ON b.cid = bp.cid
        WHERE bp.id IS NULL AND b.last_access_at < $1`,
        cutoff,
    )
    if err != nil {
        return 0, err
    }
    defer rows.Close()

    var cids []string
    for rows.Next() {
        var cid string
        if err := rows.Scan(&cid); err != nil {
            return 0, err
        }
        cids = append(cids, cid)
    }

    // Delete from filesystem and database
    deleted := 0
    for _, cid := range cids {
        if err := s.blobFS.Delete(cid); err != nil {
            s.logger.Warn("failed to delete blob file", "cid", cid, "error", err)
            continue
        }
        if _, err := s.pool.Exec(ctx, `DELETE FROM blobs WHERE cid = $1`, cid); err != nil {
            s.logger.Warn("failed to delete blob record", "cid", cid, "error", err)
            continue
        }
        deleted++
    }

    return deleted, nil
}
```

---

## 6. Cross-cutting Features

### 6.1 Subscriptions via PG LISTEN/NOTIFY + GossipSub

Real-time change notifications use a two-tier approach:

1. **PostgreSQL LISTEN/NOTIFY** for intra-server notification (store operations trigger NOTIFY)
2. **GossipSub** for cross-server propagation

```go
// NotifyChange is called by storage methods after successful writes.
func (s *PostgresStorage) NotifyChange(ctx context.Context, channel, payload string) error {
    _, err := s.pool.Exec(ctx, fmt.Sprintf("NOTIFY %s, '%s'",
        pgx.Identifier{channel}.Sanitize(), payload))
    return err
}

// Example: After a document PUT
func (s *PostgresStorage) PutDocument(ctx context.Context, ...) (*DocumentPutResult, error) {
    // ... existing put logic ...

    // Notify subscribers
    s.NotifyChange(ctx, "store_changes", fmt.Sprintf(
        `{"type":"doc","owner":"%s","path":"%s","etag":"%s"}`,
        ownerID.String(), path, result.ContentHash,
    ))

    return result, nil
}
```

```sql
-- PostgreSQL trigger for automatic NOTIFY on changes
CREATE OR REPLACE FUNCTION notify_store_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('store_changes', json_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'id', NEW.id
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### 6.2 Unified Discovery

Well-known endpoint for store discovery (served via SDA DIRECTORY or a dedicated handler):

```json
// GET /{peerId}/.well-known/stores
{
  "stores": {
    "documents": [
      {"path": "profile", "contentType": "application/json"},
      {"path": "settings", "contentType": "application/json"}
    ],
    "feeds": [
      {"path": "posts", "title": "My Posts", "currentSequence": 156}
    ],
    "collections": [
      {"path": "products", "name": "Product Catalog", "recordCount": 89}
    ],
    "pinnedBlobs": 42
  }
}
```

### 6.3 Access Control

The existing owner-only write enforcement (check `callerID != ownerID`) continues as the baseline. A unified ACL system may be added in a future phase:

```go
// Current pattern (used in all handlers):
if isWrite && callerID != ownerID {
    h.writeResponse(s, &Response{Status: 403,
        Headers: map[string]any{"Error": "write operations require owner access"}})
    return
}
```

---

## 7. Storage Interface Refactor

### 7.1 Sub-Interface Composition

The current monolithic `Storage` interface will be refactored into composed sub-interfaces:

```go
// Storage composes all store sub-interfaces.
type Storage interface {
    Lifecycle
    MailboxStore
    DocumentStore
    FeedStore
    CollectionStore
    BlobStore
}

// Lifecycle manages storage lifecycle.
type Lifecycle interface {
    Initialize(ctx context.Context) error
    Close() error
}

// MailboxStore defines mailbox and message operations.
type MailboxStore interface {
    GetOrCreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (*MailboxRecord, error)
    FindMailbox(ctx context.Context, ownerID peer.ID, folderPath string) (*MailboxRecord, error)
    // ... existing mailbox methods ...
}

// DocumentStore defines document operations.
type DocumentStore interface {
    GetDocument(ctx context.Context, ownerID peer.ID, path string) (*DocumentRecord, error)
    PutDocument(ctx context.Context, ownerID peer.ID, path string, content []byte, contentType string, updatedBy peer.ID, ifMatch *string) (*DocumentPutResult, error)
    PatchDocument(ctx context.Context, ownerID peer.ID, path string, patch map[string]any, updatedBy peer.ID, ifMatch *string) (*DocumentPutResult, error)
    DeleteDocument(ctx context.Context, ownerID peer.ID, path string) (bool, error)
    ListDocuments(ctx context.Context, ownerID peer.ID) ([]*DocumentRecord, error)
    GetDocumentHistory(ctx context.Context, ownerID peer.ID, path string, maxVersions *int) ([]*DocumentVersionRecord, error)
    GetDocumentAtVersion(ctx context.Context, ownerID peer.ID, path string, versionNumber int) (*DocumentVersionRecord, error)
    UpsertDirectoryEntry(ctx context.Context, entry *DirectoryEntry) error
    RemoveDirectoryEntry(ctx context.Context, ownerPeerID string) error
    GetDirectoryEntry(ctx context.Context, ownerPeerID string) (*DirectoryEntry, error)
    BrowseDirectory(ctx context.Context, query string, cursor string, limit int) (*DirectoryPage, error)
}

// FeedStore defines feed operations (see Section 3.4).
// CollectionStore defines collection operations (see Section 4.5).
// BlobStore defines blob operations (see Section 5.5).
```

### 7.2 Handler Dependency

Each handler only depends on the sub-interface it needs:

```go
// SDA handler depends on DocumentStore
type SDAHandler struct {
    store storage.DocumentStore
    // ...
}

// SFA handler depends on FeedStore
type SFAHandler struct {
    store storage.FeedStore
    // ...
}
```

The `PostgresStorage` struct implements all sub-interfaces (compile-time checked):

```go
var _ storage.Storage = (*PostgresStorage)(nil)
```

---

## 8. Migration & Compatibility

### 8.1 Schema Migrations

New tables are additive — no changes to existing tables. Migration approach:

1. All schema changes use `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
2. Schema additions are applied by running the updated `schema.sql`
3. No data migration needed for existing mailbox/document data

### 8.2 Backward Compatibility

- Existing mailbox protocols (MSA, MAA, MMA) remain unchanged
- Document Store protocol `/ricochet/store/doc/1.0.0` remains valid
- New store protocols are registered as additional libp2p stream handlers
- Clients that only use documents continue to work without changes

### 8.3 Server Registration

New protocols are registered alongside existing ones:

```go
// In server initialization:
h.SetStreamHandler(sda.ProtocolID, sdaHandler.HandleStream)  // existing
h.SetStreamHandler(sfa.ProtocolID, sfaHandler.HandleStream)  // new
h.SetStreamHandler(sca.ProtocolID, scaHandler.HandleStream)  // new
h.SetStreamHandler(sba.ProtocolID, sbaHandler.HandleStream)  // new
```

---

## 9. Implementation Timeline

### 9.1 Phase Summary

| Phase | Focus | Status | Dependencies |
|-------|-------|--------|--------------|
| **1+2** | Document Store | ✅ COMPLETE | — |
| **3** | Feed Store | Planned | Phase 2 |
| **4** | Collection Store | Planned | Phase 2 |
| **5** | Blob Store | Planned | Phase 2 |
| **6** | Cross-cutting (Subscriptions, Discovery) | Planned | Phases 3–5 |

### 9.2 Deliverables per Phase

#### Phase 3: Feed Store

| Deliverable | Files |
|-------------|-------|
| Models | `internal/storage/models.go` — `FeedRecord`, `FeedEntryRecord` |
| Schema | `schema.sql` — `feeds`, `feed_entries` tables |
| Sub-interface | `internal/storage/storage.go` — `FeedStore` interface |
| PostgreSQL impl | `internal/storage/postgres/feeds.go` |
| Protocol handler | `internal/protocol/sfa/handler.go` |
| Client API | `pkg/client/feeds.go` |
| Tests | `internal/protocol/sfa/handler_test.go`, `internal/storage/postgres/feeds_test.go` |

#### Phase 4: Collection Store

| Deliverable | Files |
|-------------|-------|
| Models | `internal/storage/models.go` — `CollectionRecord`, `CollectionItemRecord` |
| Schema | `schema.sql` — `collections`, `collection_items` tables with GIN indexes |
| Sub-interface | `internal/storage/storage.go` — `CollectionStore` interface |
| PostgreSQL impl | `internal/storage/postgres/collections.go` |
| JSONB query builder | `internal/storage/postgres/jsonb_filter.go` |
| Protocol handler | `internal/protocol/sca/handler.go` |
| Client API | `pkg/client/collections.go` |
| Tests | `internal/protocol/sca/handler_test.go`, `internal/storage/postgres/collections_test.go` |

#### Phase 5: Blob Store

| Deliverable | Files |
|-------------|-------|
| Models | `internal/storage/models.go` — `BlobRecord`, `BlobPinRecord` |
| Schema | `schema.sql` — `blobs`, `blob_pins` tables |
| Filesystem layer | `internal/storage/blobfs/blobfs.go` |
| Sub-interface | `internal/storage/storage.go` — `BlobStore` interface |
| PostgreSQL impl | `internal/storage/postgres/blobs.go` |
| Protocol handler | `internal/protocol/sba/handler.go` |
| Garbage collection | `internal/storage/postgres/blobs.go` — `CollectGarbage` |
| Client API | `pkg/client/blobs.go` |
| Tests | `internal/protocol/sba/handler_test.go`, `internal/storage/postgres/blobs_test.go` |

#### Phase 6: Cross-cutting

| Deliverable | Files |
|-------------|-------|
| PG LISTEN/NOTIFY integration | `internal/storage/postgres/notify.go` |
| GossipSub bridge | `internal/pubsub/store_events.go` |
| Unified discovery endpoint | `internal/protocol/sda/handler.go` — extend DIRECTORY |
| Storage interface refactor | `internal/storage/storage.go` — sub-interface composition |

---

## Appendix A: API Quick Reference

### Document Store (✅ Complete)

```
Protocol: /ricochet/store/doc/1.0.0
Handler:  internal/protocol/sda/handler.go

GET       /{owner}/doc/{path}           Read document
PUT       /{owner}/doc/{path}           Replace document
PATCH     /{owner}/doc/{path}           Merge update (RFC 7396)
DELETE    /{owner}/doc/{path}           Remove document
HEAD      /{owner}/doc/{path}           Check version
LIST      /{owner}/doc/                 List documents
HISTORY   /{owner}/doc/{path}           Version history
DIRECTORY /{owner}/doc/                 Directory join/leave/browse/get
```

### Feed Store

```
Protocol: /ricochet/store/feed/1.0.0
Handler:  internal/protocol/sfa/handler.go

CREATE    /{owner}/feed/{path}          Create feed
GET       /{owner}/feed/{path}          Feed metadata
GET       /{owner}/feed/{path}?seq=N    Single entry
GET       /{owner}/feed/{path}?from=N   Entry range
APPEND    /{owner}/feed/{path}          Add entry
DELETE    /{owner}/feed/{path}          Delete feed
LIST      /{owner}/feed/               List feeds
```

### Collection Store

```
Protocol: /ricochet/store/collection/1.0.0
Handler:  internal/protocol/sca/handler.go

CREATE    /{owner}/collection/{path}              Create collection
GET       /{owner}/collection/{path}              Metadata
GET       /{owner}/collection/{path}/{key}        Single record
PUT       /{owner}/collection/{path}/{key}        Upsert record
DELETE    /{owner}/collection/{path}/{key}        Remove record
QUERY     /{owner}/collection/{path}              Query via JSONB filter
LIST      /{owner}/collection/{path}              List all keys
```

### Blob Store

```
Protocol: /ricochet/store/blob/1.0.0
Handler:  internal/protocol/sba/handler.go

PUT       /{owner}/blob                 Upload, returns CID
GET       /blob/{cid}                   Download by CID
HEAD      /blob/{cid}                   Check existence
PIN       /{owner}/blob/{cid}           Pin blob
UNPIN     /{owner}/blob/{cid}           Remove pin
```

---

*End of Proposal*
