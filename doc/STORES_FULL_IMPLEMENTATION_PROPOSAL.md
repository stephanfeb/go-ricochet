# Ricochet Stores: Full Implementation Proposal

**Status:** Proposal  
**Author:** Architecture Team  
**Date:** January 2026  
**Version:** 1.0  
**Prerequisites:** [DOCUMENT_STORE_MVP_PROPOSAL.md](DOCUMENT_STORE_MVP_PROPOSAL.md)  
**Related:** [RICOCHET_STORES_PROPOSAL.md](../RICOCHET_STORES_PROPOSAL.md)

---

## Executive Summary

This proposal defines the complete implementation roadmap from the Document Store MVP to the full Ricochet Stores architecture. Building on the MVP foundation, it introduces:

1. **Document Store Completion** - PATCH, DELETE, LIST, history tracking, CRDT replication
2. **Feed Store** - Append-only broadcast streams for social posts, activity logs, RSS
3. **Collection Store** - Mutable key-value records with queries for catalogs, inventories
4. **Blob Store** - Content-addressed immutable files for media storage
5. **Cross-cutting Features** - Subscriptions, unified discovery, indexing services

**Timeline:** 4 phases over approximately 8-12 weeks of development effort.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Phase 2: Document Store Completion](#2-phase-2-document-store-completion)
3. [Phase 3: Feed Store](#3-phase-3-feed-store)
4. [Phase 4: Collection Store](#4-phase-4-collection-store)
5. [Phase 5: Blob Store](#5-phase-5-blob-store)
6. [Cross-cutting Features](#6-cross-cutting-features)
7. [Replication Strategy](#7-replication-strategy)
8. [Protocol Unification](#8-protocol-unification)
9. [Migration & Compatibility](#9-migration--compatibility)
10. [Implementation Timeline](#10-implementation-timeline)

---

## 1. Architecture Overview

### 1.1 Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Ricochet Server (Full)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  Protocol Layer                                                             │
│  ├── MSA/MAA/MMA (existing mailbox protocols)                              │
│  │                                                                          │
│  └── Store Protocols                                                        │
│       ├── /ricochet/store/doc/1.0.0        Document Store (MVP ✓)          │
│       ├── /ricochet/store/feed/1.0.0       Feed Store                      │
│       ├── /ricochet/store/collection/1.0.0 Collection Store                │
│       ├── /ricochet/store/blob/1.0.0       Blob Store                      │
│       └── /ricochet/store/subscribe/1.0.0  Unified Subscriptions           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Store Delivery Agent (SDA)                                                 │
│  ├── DocumentStore    (mutable single objects)                             │
│  ├── FeedStore        (append-only streams)                                │
│  ├── CollectionStore  (mutable key-value records)                          │
│  └── BlobStore        (immutable content-addressed)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  Replication Layer                                                          │
│  ├── DocumentCRDT     (LWW with version vectors)                           │
│  ├── FeedCRDT         (append-only merge)                                  │
│  ├── CollectionCRDT   (per-record LWW + tombstones)                        │
│  └── BlobRegistry     (CID → provider DHT mapping)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  Storage (Isar)                                                             │
│  ├── IsarMailbox, IsarStoredMessage (existing)                             │
│  ├── IsarDocument (MVP ✓)                                                  │
│  ├── IsarFeed, IsarFeedEntry                                               │
│  ├── IsarCollection, IsarRecord                                            │
│  └── IsarBlob                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Discovery & Indexing                                                       │
│  ├── DHT (content discovery)                                               │
│  ├── PubSub (real-time notifications)                                      │
│  └── Optional: Indexing services (search, aggregation)                     │
└─────────────────────────────────────────────────────────────────────────────┘
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
| **Query** | GET by path | Range by sequence | Filter/search | GET by CID |
| **Use Cases** | Profiles, settings | Posts, logs, RSS | Catalogs, contacts | Media, files |

---

## 2. Phase 2: Document Store Completion

**Builds on:** Document Store MVP (Phase 1)  
**Estimated effort:** 2-3 weeks

### 2.1 New Operations

#### PATCH - Merge Update

Apply partial updates using JSON Merge Patch (RFC 7396):

```json
// Request
{
  "operation": "PATCH",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "Content-Type": "application/merge-patch+json",
    "If-Match": "sha256:abc123..."  // Optional
  },
  "body": "eyJiaW8iOiJVcGRhdGVkIGJpbyJ9"  // {"bio": "Updated bio"}
}

// Response
{
  "status": 200,
  "headers": {
    "ETag": "sha256:new456...",
    "Last-Modified": 1704873700000
  }
}
```

**Implementation:**
```dart
Future<DocumentPatchResult> patchDocument({
  required PeerId ownerId,
  required String path,
  required Map<String, dynamic> patch,
  required PeerId updatedBy,
  String? ifMatch,
}) async {
  return await isar.writeTxn(() async {
    final doc = await getDocument(ownerId, path);
    if (doc == null) throw DocumentNotFoundException(path);
    
    if (ifMatch != null && doc.contentHash != ifMatch) {
      throw DocumentConflictException(ifMatch, doc.contentHash);
    }
    
    // Parse existing content as JSON
    final existing = jsonDecode(utf8.decode(doc.content));
    
    // Apply JSON Merge Patch
    final merged = _applyMergePatch(existing, patch);
    
    // Store updated document
    final newContent = utf8.encode(jsonEncode(merged));
    return await putDocument(
      ownerId: ownerId,
      path: path,
      content: Uint8List.fromList(newContent),
      contentType: 'application/json',
      updatedBy: updatedBy,
    );
  });
}
```

#### DELETE - Remove Document

```json
// Request
{
  "operation": "DELETE",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "If-Match": "sha256:abc123..."  // Optional
  }
}

// Response
{
  "status": 204,
  "headers": {}
}
```

#### LIST - Enumerate Documents

```json
// Request
{
  "operation": "LIST",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "",  // List all, or "settings/" for prefix
  "headers": {}
}

// Response
{
  "status": 200,
  "headers": {
    "Content-Type": "application/json"
  },
  "body": {
    "documents": [
      {"path": "profile", "contentType": "application/json", "size": 256, "etag": "sha256:..."},
      {"path": "avatar", "contentType": "image/png", "size": 102400, "etag": "sha256:..."},
      {"path": "settings/notifications", "contentType": "application/json", "size": 128, "etag": "sha256:..."}
    ]
  }
}
```

### 2.2 Version History

Optional history tracking for audit trails and rollback:

```dart
/// Document version history entry
@collection  
class IsarDocumentVersion {
  Id id = Isar.autoIncrement;
  
  /// Reference to current document
  @Index()
  late int documentId;
  
  /// Version number (1, 2, 3, ...)
  late int versionNumber;
  
  /// Content at this version
  late List<byte> content;
  
  /// Content hash (ETag)
  late String contentHash;
  
  /// When this version was created
  late int timestamp;
  
  /// Who created this version
  late String updatedByPeerId;
  
  /// Optional: Diff from previous version (for space efficiency)
  late List<byte>? diffFromPrevious;
}
```

**HISTORY Operation:**
```json
// Request
{
  "operation": "HISTORY",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "X-Max-Versions": "10"
  }
}

// Response
{
  "status": 200,
  "body": {
    "versions": [
      {"version": 5, "etag": "sha256:current...", "timestamp": 1704873700000},
      {"version": 4, "etag": "sha256:prev...", "timestamp": 1704873600000},
      {"version": 3, "etag": "sha256:older...", "timestamp": 1704873500000}
    ]
  }
}
```

**GET with version:**
```json
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "X-Version": "3"  // Get specific historical version
  }
}
```

### 2.3 Document CRDT Replication

For multi-server consistency, implement `DocumentCRDT`:

```dart
/// Document CRDT using Last-Writer-Wins with version vectors
class DocumentCRDT {
  /// Version vector: serverId → local clock
  final Map<String, int> versionVector;
  
  /// Current content
  final Uint8List content;
  
  /// Content hash
  final String contentHash;
  
  /// Last update timestamp (wall clock for LWW tiebreaker)
  final int timestamp;
  
  /// Merge with another replica
  DocumentCRDT merge(DocumentCRDT other) {
    // Compare version vectors
    final dominance = _compareVersionVectors(versionVector, other.versionVector);
    
    if (dominance == Dominance.thisWins) return this;
    if (dominance == Dominance.otherWins) return other;
    
    // Concurrent: use wall clock as tiebreaker (LWW)
    if (timestamp >= other.timestamp) return this;
    return other;
  }
  
  /// Increment version for local update
  DocumentCRDT localUpdate(String serverId, Uint8List newContent) {
    final newVector = Map<String, int>.from(versionVector);
    newVector[serverId] = (newVector[serverId] ?? 0) + 1;
    
    return DocumentCRDT(
      versionVector: newVector,
      content: newContent,
      contentHash: _computeHash(newContent),
      timestamp: DateTime.now().millisecondsSinceEpoch,
    );
  }
}
```

**Replication Protocol:**
```
Server A: PUT /doc/profile → Update local CRDT
    ↓ GossipSub: Announce CRDT head CID
Server B: Receive announcement
    ↓ Fetch CRDT delta via sync protocol
Server B: Merge into local state
    ↓ Document now consistent
```

### 2.4 Data Model Updates

```dart
// Update IsarDocument to support history and CRDT
@collection
class IsarDocument {
  Id id = Isar.autoIncrement;
  
  // ... existing fields from MVP ...
  
  // NEW: Version tracking
  late int versionNumber;
  
  // NEW: CRDT version vector (JSON-encoded)
  late String? versionVector;
  
  // NEW: Whether history is enabled for this document
  late bool historyEnabled;
  
  // NEW: Max versions to retain (null = unlimited)
  late int? maxHistoryVersions;
}
```

### 2.5 Phase 2 Deliverables

| Deliverable | Description | Effort |
|-------------|-------------|--------|
| PATCH operation | JSON Merge Patch support | 4 hours |
| DELETE operation | Document removal | 2 hours |
| LIST operation | Document enumeration | 3 hours |
| Version history | Storage and retrieval | 8 hours |
| DocumentCRDT | Replication support | 16 hours |
| Sync protocol | Server-to-server sync | 12 hours |
| Tests | Unit + integration | 8 hours |
| **Total** | | **~53 hours (1.5 weeks)** |

---

## 3. Phase 3: Feed Store

**Builds on:** Document Store (complete)  
**Estimated effort:** 2-3 weeks

### 3.1 Overview

Feed Stores are **append-only broadcast streams** for one-to-many publication:

```
Owner (Publisher)                    Subscribers
┌─────────────┐                    ┌─────────────┐
│    Alice    │                    │     Bob     │
└──────┬──────┘                    └──────┬──────┘
       │                                  │
       │ APPEND                           │ GET (from=cursor)
       ▼                                  │ SUBSCRIBE
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

```dart
/// Feed store metadata
@collection
class IsarFeed {
  Id id = Isar.autoIncrement;
  
  /// Owner's peer ID (Base58)
  @Index(composite: [CompositeIndex('path')])
  late String ownerPeerId;
  
  /// Feed path (e.g., "posts", "activity", "orders")
  late String path;
  
  /// Feed title
  late String? title;
  
  /// Feed description
  late String? description;
  
  /// Icon CID (blob reference)
  late String? iconCid;
  
  /// Content type of entries (default: application/json)
  late String entryContentType;
  
  /// When feed was created
  late int createdTimestamp;
  
  /// Last entry timestamp
  late int lastEntryTimestamp;
  
  /// Current highest sequence number
  late int currentSequence;
  
  /// Retention: max entries (null = unlimited)
  late int? maxEntries;
  
  /// Retention: max age in days (null = unlimited)
  late int? maxAgeDays;
  
  /// CRDT version vector for replication
  late String? versionVector;
}

/// Feed entry (individual item in feed)
@collection
class IsarFeedEntry {
  Id id = Isar.autoIncrement;
  
  /// Reference to parent feed
  @Index()
  late int feedId;
  
  /// Monotonically increasing sequence number
  @Index()
  late int sequenceNumber;
  
  /// Entry content (JSON/CBOR)
  late List<byte> content;
  
  /// Content hash for integrity
  late String contentHash;
  
  /// When entry was created
  late int createdTimestamp;
  
  /// Who created this entry (for feeds with multiple writers)
  late String createdByPeerId;
  
  /// Optional: Entry type for filtering (e.g., "post", "reply", "repost")
  late String? entryType;
}
```

### 3.3 Protocol Specification

**Protocol ID:** `/ricochet/store/feed/1.0.0`

#### APPEND - Add Entry

```json
// Request
{
  "operation": "APPEND",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "headers": {
    "Content-Type": "application/json",
    "X-Entry-Type": "post"  // Optional type tag
  },
  "body": "eyJ0ZXh0IjoiSGVsbG8gd29ybGQhIiwidHMiOjE3MDQ4NzM2MDAwMDB9"
}

// Response
{
  "status": 201,
  "headers": {
    "X-Sequence": "42",
    "ETag": "sha256:entry123..."
  }
}
```

#### GET - Read Feed/Entry

```json
// Get feed metadata
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "headers": {}
}

// Response
{
  "status": 200,
  "body": {
    "title": "Alice's Posts",
    "description": "Thoughts and updates",
    "entryCount": 42,
    "lastEntry": 1704873600000,
    "currentSequence": 42
  }
}

// Get specific entry
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts/42",
  "headers": {}
}

// Get range of entries
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "headers": {
    "X-From-Sequence": "30",
    "X-To-Sequence": "40",      // Optional
    "X-Limit": "10",            // Max entries to return
    "X-Entry-Type": "post"      // Optional filter
  }
}

// Response (range)
{
  "status": 200,
  "headers": {
    "X-Has-More": "true",
    "X-Next-Sequence": "40"
  },
  "body": {
    "entries": [
      {"seq": 30, "type": "post", "content": {...}, "ts": 1704873500000},
      {"seq": 31, "type": "reply", "content": {...}, "ts": 1704873510000},
      // ...
    ]
  }
}
```

#### CREATE - Create Feed

```json
{
  "operation": "CREATE",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "posts",
  "headers": {
    "Content-Type": "application/json"
  },
  "body": {
    "title": "Alice's Posts",
    "description": "My thoughts and updates",
    "maxEntries": 1000,
    "maxAgeDays": 365
  }
}
```

#### SUBSCRIBE - Real-time Updates

See [Section 6.1: Subscriptions](#61-subscriptions).

### 3.4 Feed CRDT Replication

Feeds use append-only merge semantics:

```dart
/// Feed CRDT - append-only with causal ordering
class FeedCRDT {
  /// Feed metadata CRDT (same as DocumentCRDT)
  final DocumentCRDT metadata;
  
  /// Entry set: entries identified by (serverId, localSeq)
  /// Merging takes union of all entries
  final Map<String, List<FeedEntryCRDT>> entriesByServer;
  
  /// Merge with another replica
  FeedCRDT merge(FeedCRDT other) {
    // Merge metadata
    final mergedMetadata = metadata.merge(other.metadata);
    
    // Merge entries (union, deduplicated by origin)
    final mergedEntries = <String, List<FeedEntryCRDT>>{};
    
    for (final serverId in {...entriesByServer.keys, ...other.entriesByServer.keys}) {
      final thisEntries = entriesByServer[serverId] ?? [];
      final otherEntries = other.entriesByServer[serverId] ?? [];
      mergedEntries[serverId] = _mergeEntryLists(thisEntries, otherEntries);
    }
    
    return FeedCRDT(metadata: mergedMetadata, entriesByServer: mergedEntries);
  }
  
  /// Get entries in global sequence order
  /// Uses Lamport timestamps for deterministic ordering
  List<FeedEntry> getOrderedEntries() {
    final all = entriesByServer.values.expand((e) => e).toList();
    all.sort((a, b) {
      final tsCompare = a.timestamp.compareTo(b.timestamp);
      if (tsCompare != 0) return tsCompare;
      // Tiebreaker: serverId + localSeq
      return '${a.serverId}:${a.localSeq}'.compareTo('${b.serverId}:${b.localSeq}');
    });
    return all.map((e) => e.toFeedEntry()).toList();
  }
}
```

### 3.5 Use Cases

| Use Case | Feed Path | Entry Structure |
|----------|-----------|-----------------|
| Microblog posts | `/feed/posts` | `{type, text, media[], replyTo?, ts}` |
| Activity log | `/feed/activity` | `{action, target, metadata, ts}` |
| RSS/Atom feed | `/feed/articles` | `{guid, title, link, description, pubDate}` |
| Order history | `/feed/orders` | `{orderId, status, items[], total, ts}` |
| Notifications | `/feed/notifications` | `{type, title, body, action?, ts}` |

### 3.6 Phase 3 Deliverables

| Deliverable | Description | Effort |
|-------------|-------------|--------|
| IsarFeed/IsarFeedEntry models | Data layer | 4 hours |
| Feed storage operations | CRUD for feeds/entries | 8 hours |
| FeedHandler protocol | APPEND, GET, CREATE | 12 hours |
| FeedCRDT | Replication logic | 12 hours |
| Feed sync protocol | Server-to-server | 8 hours |
| Subscription integration | Real-time updates | 6 hours |
| Tests | Unit + integration | 8 hours |
| **Total** | | **~58 hours (1.5 weeks)** |

---

## 4. Phase 4: Collection Store

**Builds on:** Document Store + Feed Store  
**Estimated effort:** 3-4 weeks

### 4.1 Overview

Collection Stores hold **mutable key-value records** with query capabilities:

```
┌─────────────────────────────────────────────────────────────┐
│                     Collection Store                         │
│  /{Alice}/collection/products                                │
│                                                              │
│  Metadata:                                                   │
│    schema: {...}  (optional JSON Schema)                     │
│    indexes: ["category", "price"]                           │
│    recordCount: 156                                          │
│                                                              │
│  Records:                                                    │
│    "sku-001" → {name: "Drill", price: 49.99, category: "tools"}
│    "sku-002" → {name: "Hammer", price: 19.99, category: "tools"}
│    "sku-003" → {name: "Paint", price: 29.99, category: "paint"}
│    ...                                                       │
└─────────────────────────────────────────────────────────────┘

Operations:
  GET    /collection/products           → Metadata + list keys
  GET    /collection/products/sku-001   → Single record
  PUT    /collection/products/sku-001   → Upsert record
  DELETE /collection/products/sku-001   → Remove record
  QUERY  /collection/products?category=tools&price<50 → Filtered list
```

### 4.2 Data Model

```dart
/// Collection store metadata
@collection
class IsarCollection {
  Id id = Isar.autoIncrement;
  
  /// Owner's peer ID (Base58)
  @Index(composite: [CompositeIndex('path')])
  late String ownerPeerId;
  
  /// Collection path (e.g., "products", "contacts", "inventory")
  late String path;
  
  /// Collection name (human-readable)
  late String? name;
  
  /// Optional JSON Schema for validation
  late String? schemaJson;
  
  /// Indexed fields for query optimization
  late List<String> indexedFields;
  
  /// When collection was created
  late int createdTimestamp;
  
  /// Last modification timestamp
  late int lastModifiedTimestamp;
  
  /// Current record count (cached)
  late int recordCount;
  
  /// CRDT version vector for replication
  late String? versionVector;
}

/// Collection record (single entry)
@collection
class IsarRecord {
  Id id = Isar.autoIncrement;
  
  /// Reference to parent collection
  @Index(composite: [CompositeIndex('recordKey')])
  late int collectionId;
  
  /// Record key (unique within collection)
  late String recordKey;
  
  /// Record content (JSON/CBOR)
  late List<byte> content;
  
  /// Content hash for versioning
  late String contentHash;
  
  /// Record version (increments on each update)
  late int version;
  
  /// When record was created
  late int createdTimestamp;
  
  /// When record was last updated
  late int updatedTimestamp;
  
  /// Who last updated this record
  late String updatedByPeerId;
  
  /// Tombstone flag for soft-delete (CRDT requirement)
  late bool deleted;
  
  /// When tombstone was created (for GC)
  late int? deletedTimestamp;
  
  // Indexed field values (denormalized for query performance)
  // These are extracted from content based on collection's indexedFields
  
  @Index()
  late String? indexValue1;  // First indexed field
  
  @Index()
  late String? indexValue2;  // Second indexed field
  
  @Index()
  late double? indexNumeric1;  // Numeric index for range queries
}
```

### 4.3 Protocol Specification

**Protocol ID:** `/ricochet/store/collection/1.0.0`

#### GET - Read Collection/Record

```json
// Get collection metadata
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "products",
  "headers": {}
}

// Response
{
  "status": 200,
  "body": {
    "name": "Bob's Hardware Products",
    "recordCount": 156,
    "indexedFields": ["category", "price"],
    "lastModified": 1704873600000
  }
}

// Get single record
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "products/sku-001",
  "headers": {
    "If-None-Match": "sha256:abc..."  // Conditional
  }
}

// Response
{
  "status": 200,
  "headers": {
    "ETag": "sha256:record123...",
    "X-Version": "5"
  },
  "body": {
    "sku": "sku-001",
    "name": "Professional Drill",
    "price": 49.99,
    "category": "tools",
    "stock": 42
  }
}
```

#### PUT - Upsert Record

```json
{
  "operation": "PUT",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "products/sku-001",
  "headers": {
    "Content-Type": "application/json",
    "If-Match": "sha256:old..."  // Optional optimistic locking
  },
  "body": {
    "sku": "sku-001",
    "name": "Professional Drill 2.0",
    "price": 54.99,
    "category": "tools",
    "stock": 38
  }
}

// Response
{
  "status": 200,  // or 201 for new record
  "headers": {
    "ETag": "sha256:new...",
    "X-Version": "6"
  }
}
```

#### DELETE - Remove Record

```json
{
  "operation": "DELETE",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "products/sku-001",
  "headers": {}
}

// Response
{
  "status": 204
}
```

#### QUERY - Search Records

```json
{
  "operation": "QUERY",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "products",
  "headers": {
    "X-Limit": "50",
    "X-Offset": "0",
    "X-Sort": "price:asc"
  },
  "body": {
    "filter": {
      "category": "tools",
      "price": {"$lt": 50},
      "stock": {"$gt": 0}
    },
    "fields": ["sku", "name", "price"]  // Projection
  }
}

// Response
{
  "status": 200,
  "headers": {
    "X-Total-Count": "23",
    "X-Has-More": "false"
  },
  "body": {
    "records": [
      {"key": "sku-001", "data": {"sku": "sku-001", "name": "Drill", "price": 49.99}},
      {"key": "sku-002", "data": {"sku": "sku-002", "name": "Hammer", "price": 19.99}},
      // ...
    ]
  }
}
```

### 4.4 Query Language

Simple but powerful query syntax:

```
// Equality
{"category": "tools"}

// Comparison operators
{"price": {"$lt": 50}}       // Less than
{"price": {"$lte": 50}}      // Less than or equal
{"price": {"$gt": 10}}       // Greater than
{"price": {"$gte": 10}}      // Greater than or equal
{"price": {"$ne": 0}}        // Not equal

// Logical operators
{"$and": [{"category": "tools"}, {"price": {"$lt": 50}}]}
{"$or": [{"category": "tools"}, {"category": "paint"}]}

// String matching
{"name": {"$contains": "drill"}}
{"name": {"$startsWith": "Pro"}}

// Array membership
{"category": {"$in": ["tools", "paint", "hardware"]}}

// Existence
{"discount": {"$exists": true}}
```

### 4.5 Collection CRDT Replication

Per-record LWW with tombstones:

```dart
/// Collection CRDT - per-record last-writer-wins
class CollectionCRDT {
  /// Collection metadata CRDT
  final DocumentCRDT metadata;
  
  /// Records: key → RecordCRDT
  final Map<String, RecordCRDT> records;
  
  /// Merge with another replica
  CollectionCRDT merge(CollectionCRDT other) {
    final mergedMetadata = metadata.merge(other.metadata);
    final mergedRecords = <String, RecordCRDT>{};
    
    // Merge each record independently
    for (final key in {...records.keys, ...other.records.keys}) {
      final thisRecord = records[key];
      final otherRecord = other.records[key];
      
      if (thisRecord == null) {
        mergedRecords[key] = otherRecord!;
      } else if (otherRecord == null) {
        mergedRecords[key] = thisRecord;
      } else {
        mergedRecords[key] = thisRecord.merge(otherRecord);
      }
    }
    
    return CollectionCRDT(metadata: mergedMetadata, records: mergedRecords);
  }
}

/// Single record CRDT
class RecordCRDT {
  final Map<String, int> versionVector;
  final Uint8List content;
  final String contentHash;
  final int timestamp;
  final bool deleted;  // Tombstone
  
  RecordCRDT merge(RecordCRDT other) {
    // Same logic as DocumentCRDT, but respects tombstones
    final dominance = _compareVersionVectors(versionVector, other.versionVector);
    
    if (dominance == Dominance.thisWins) return this;
    if (dominance == Dominance.otherWins) return other;
    
    // Concurrent: LWW with delete-wins on tie
    if (deleted && !other.deleted) return this;
    if (!deleted && other.deleted) return other;
    if (timestamp >= other.timestamp) return this;
    return other;
  }
}
```

### 4.6 Use Cases

| Use Case | Collection Path | Record Structure |
|----------|-----------------|------------------|
| Product catalog | `/collection/products` | `{sku, name, price, stock, category}` |
| Contact list | `/collection/contacts` | `{id, name, email, phone, tags[]}` |
| Inventory | `/collection/inventory` | `{sku, location, quantity, lastCount}` |
| Following list | `/collection/following` | `{peerId, since, nickname}` |
| Bookmarks | `/collection/bookmarks` | `{url, title, tags[], added}` |

### 4.7 Phase 4 Deliverables

| Deliverable | Description | Effort |
|-------------|-------------|--------|
| IsarCollection/IsarRecord models | Data layer | 6 hours |
| Collection storage operations | CRUD + indexing | 12 hours |
| CollectionHandler protocol | GET, PUT, DELETE | 10 hours |
| QUERY operation | Filter + sort + pagination | 16 hours |
| Query parser | Filter expression parsing | 8 hours |
| CollectionCRDT | Per-record replication | 12 hours |
| Collection sync protocol | Server-to-server | 8 hours |
| Schema validation | Optional JSON Schema | 6 hours |
| Tests | Unit + integration | 12 hours |
| **Total** | | **~90 hours (2.5 weeks)** |

---

## 5. Phase 5: Blob Store

**Builds on:** Document Store + Feed Store + Collection Store  
**Estimated effort:** 2 weeks

### 5.1 Overview

Blob Stores provide **content-addressed immutable file storage**:

```
┌─────────────────────────────────────────────────────────────┐
│                       Blob Store                            │
│                                                             │
│  Upload Flow:                                               │
│    Client → PUT /{owner}/blob → Server stores, returns CID  │
│                                                             │
│  Retrieval Flow:                                            │
│    Client → GET /blob/{cid} → Any server with content       │
│                                                             │
│  Addressing:                                                │
│    /{owner}/blob/{cid}    → Blob pinned by owner           │
│    /blob/{cid}            → Global blob (any provider)      │
│                                                             │
│  Features:                                                  │
│    - Content-addressed (CID = hash of content)             │
│    - Immutable (CID never changes)                         │
│    - Deduplication (same content = same CID)               │
│    - Distributed (DHT discovery)                           │
│    - Chunked (large files split into blocks)               │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Content Addressing

Using CIDv1 with SHA-256:

```dart
/// Compute CID for content
String computeCid(Uint8List content, {String codec = 'raw'}) {
  final hash = sha256.convert(content);
  
  // CIDv1: multibase + version + codec + multihash
  // Simplified: "bafybeig..." prefix (base32 encoded)
  return 'bafybeig${base32Encode(hash.bytes)}';
}

/// For large files, use Merkle DAG chunking
class BlobChunker {
  static const int defaultChunkSize = 256 * 1024;  // 256KB
  
  /// Chunk content and return root CID
  Future<ChunkedBlob> chunk(Uint8List content) async {
    if (content.length <= defaultChunkSize) {
      // Small file: single block
      return ChunkedBlob(
        rootCid: computeCid(content),
        chunks: [BlobChunk(cid: computeCid(content), data: content)],
      );
    }
    
    // Large file: Merkle DAG
    final chunks = <BlobChunk>[];
    final links = <String>[];
    
    for (var i = 0; i < content.length; i += defaultChunkSize) {
      final end = (i + defaultChunkSize).clamp(0, content.length);
      final chunkData = content.sublist(i, end);
      final chunkCid = computeCid(chunkData);
      
      chunks.add(BlobChunk(cid: chunkCid, data: chunkData));
      links.add(chunkCid);
    }
    
    // Root node contains links to chunks
    final rootData = jsonEncode({'links': links, 'size': content.length});
    final rootCid = computeCid(utf8.encode(rootData), codec: 'dag-json');
    
    return ChunkedBlob(
      rootCid: rootCid,
      chunks: chunks,
      isChunked: true,
    );
  }
}
```

### 5.3 Data Model

```dart
/// Blob metadata and content
@collection
class IsarBlob {
  Id id = Isar.autoIncrement;
  
  /// Content ID (CIDv1)
  @Index(unique: true)
  late String cid;
  
  /// Blob content (for small blobs stored inline)
  /// Null for chunked blobs (content in IsarBlobChunk)
  late List<byte>? content;
  
  /// Total size in bytes
  late int size;
  
  /// MIME content type
  late String contentType;
  
  /// Whether this blob is chunked
  late bool isChunked;
  
  /// For chunked blobs: ordered list of chunk CIDs (JSON array)
  late String? chunkCids;
  
  /// When blob was first stored locally
  late int createdTimestamp;
  
  /// Last time blob was accessed
  late int lastAccessTimestamp;
  
  /// Number of times blob has been accessed
  late int accessCount;
}

/// Blob chunk (for large files)
@collection
class IsarBlobChunk {
  Id id = Isar.autoIncrement;
  
  /// Chunk CID
  @Index(unique: true)
  late String cid;
  
  /// Chunk content
  late List<byte> content;
  
  /// Chunk size
  late int size;
}

/// Blob pin (tracks which owners have pinned a blob)
@collection
class IsarBlobPin {
  Id id = Isar.autoIncrement;
  
  /// Blob CID
  @Index(composite: [CompositeIndex('ownerPeerId')])
  late String cid;
  
  /// Owner who pinned this blob
  late String ownerPeerId;
  
  /// When pin was created
  late int pinnedTimestamp;
  
  /// Pin expiry (null = permanent)
  late int? expiryTimestamp;
}
```

### 5.4 Protocol Specification

**Protocol ID:** `/ricochet/store/blob/1.0.0`

#### PUT - Upload Blob

```json
// Request
{
  "operation": "PUT",
  "ownerPeerId": "12D3KooWAlice...",
  "headers": {
    "Content-Type": "image/png",
    "Content-Length": "102400"
  },
  "body": "base64-encoded-binary..."
}

// Response
{
  "status": 201,
  "headers": {
    "X-CID": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
    "Content-Length": "102400"
  }
}
```

#### GET - Download Blob

```json
// Request (by CID - global)
{
  "operation": "GET",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
  "headers": {}
}

// Request (by owner + CID)
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
  "headers": {}
}

// Response
{
  "status": 200,
  "headers": {
    "Content-Type": "image/png",
    "Content-Length": "102400",
    "X-CID": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
  },
  "body": "base64-encoded-binary..."
}
```

#### HEAD - Check Existence

```json
// Request
{
  "operation": "HEAD",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
  "headers": {}
}

// Response (exists)
{
  "status": 200,
  "headers": {
    "Content-Type": "image/png",
    "Content-Length": "102400"
  }
}

// Response (not found)
{
  "status": 404
}
```

#### PIN / UNPIN - Manage Pins

```json
// Pin blob
{
  "operation": "PIN",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
  "headers": {
    "X-Expiry": "2026-12-31T23:59:59Z"  // Optional
  }
}

// Unpin blob
{
  "operation": "UNPIN",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
}
```

### 5.5 DHT Discovery

Blobs are discoverable via libp2p DHT:

```dart
/// Blob discovery and retrieval
class BlobDiscovery {
  final DHT dht;
  
  /// Announce that we have a blob
  Future<void> provide(String cid) async {
    await dht.provide(cid);
  }
  
  /// Find providers for a blob
  Future<List<PeerId>> findProviders(String cid) async {
    return await dht.findProviders(cid, count: 10);
  }
  
  /// Fetch blob from any provider
  Future<Uint8List?> fetchBlob(String cid) async {
    final providers = await findProviders(cid);
    
    for (final provider in providers) {
      try {
        // Try to fetch from this provider
        final content = await _fetchFromProvider(provider, cid);
        if (content != null) {
          // Verify CID matches
          if (computeCid(content) == cid) {
            return content;
          }
        }
      } catch (e) {
        // Try next provider
        continue;
      }
    }
    
    return null;
  }
}
```

### 5.6 Garbage Collection

Unpinned blobs are garbage collected:

```dart
/// Blob garbage collection
class BlobGC {
  final Duration gcInterval = Duration(hours: 24);
  final Duration unpinnedRetention = Duration(days: 7);
  
  Future<int> collectGarbage() async {
    final cutoff = DateTime.now().subtract(unpinnedRetention).millisecondsSinceEpoch;
    int deleted = 0;
    
    await isar.writeTxn(() async {
      // Find blobs with no active pins
      final unpinnedBlobs = await isar.isarBlobs
          .filter()
          .lastAccessTimestampLessThan(cutoff)
          .findAll();
      
      for (final blob in unpinnedBlobs) {
        // Check if any pins exist
        final pinCount = await isar.isarBlobPins
            .filter()
            .cidEqualTo(blob.cid)
            .count();
        
        if (pinCount == 0) {
          // Delete blob and chunks
          if (blob.isChunked) {
            final chunkCids = jsonDecode(blob.chunkCids!) as List;
            for (final chunkCid in chunkCids) {
              await isar.isarBlobChunks.filter().cidEqualTo(chunkCid).deleteAll();
            }
          }
          await isar.isarBlobs.delete(blob.id);
          deleted++;
        }
      }
    });
    
    return deleted;
  }
}
```

### 5.7 Phase 5 Deliverables

| Deliverable | Description | Effort |
|-------------|-------------|--------|
| IsarBlob/IsarBlobChunk models | Data layer | 4 hours |
| CID computation | Content addressing | 4 hours |
| Blob chunking | Large file support | 8 hours |
| BlobHandler protocol | PUT, GET, HEAD, PIN | 12 hours |
| DHT integration | Provider announcements | 8 hours |
| Blob discovery | Find and fetch | 8 hours |
| Garbage collection | Unpinned blob cleanup | 6 hours |
| Tests | Unit + integration | 10 hours |
| **Total** | | **~60 hours (1.5 weeks)** |

---

## 6. Cross-cutting Features

### 6.1 Subscriptions

Unified subscription system for real-time updates across all store types:

**Protocol ID:** `/ricochet/store/subscribe/1.0.0`

```dart
/// Subscription request
class SubscribeRequest {
  final String storePath;  // e.g., "/12D3KooW.../doc/profile"
  final String? filter;    // For collections: query filter
  final int? fromSequence; // For feeds: start position
}

/// Subscription event
class SubscriptionEvent {
  final String type;       // "update", "delete", "append"
  final String storePath;
  final String? key;       // For collections
  final int? sequence;     // For feeds
  final String? etag;
  final Uint8List? content;
}
```

**Implementation:**
```dart
/// Subscription manager
class SubscriptionManager {
  final PubSub pubsub;
  final Map<String, Set<SubscriptionHandler>> subscriptions = {};
  
  /// Subscribe to store updates
  Future<Subscription> subscribe(String storePath, {
    String? filter,
    int? fromSequence,
  }) async {
    // Subscribe to PubSub topic
    final topic = _storePathToTopic(storePath);
    await pubsub.subscribe(topic);
    
    // Track subscription
    final handler = SubscriptionHandler(storePath, filter, fromSequence);
    subscriptions.putIfAbsent(topic, () => {}).add(handler);
    
    return Subscription(
      id: handler.id,
      storePath: storePath,
      cancel: () => _unsubscribe(topic, handler),
    );
  }
  
  /// Publish update to subscribers
  Future<void> notifyUpdate(String storePath, SubscriptionEvent event) async {
    final topic = _storePathToTopic(storePath);
    await pubsub.publish(topic, event.encode());
  }
  
  String _storePathToTopic(String path) {
    // Topic format: /ricochet/store/{ownerHash}/{storeType}/{storePath}
    return '/ricochet/store/${path.hashCode.abs()}';
  }
}
```

**Usage:**
```dart
// Subscribe to profile updates
final sub = await client.subscribe('/12D3KooWAlice.../doc/profile');
sub.events.listen((event) {
  print('Profile updated: ${event.etag}');
});

// Subscribe to new posts
final feedSub = await client.subscribe('/12D3KooWAlice.../feed/posts',
  fromSequence: lastKnownSequence,
);
feedSub.events.listen((event) {
  print('New post: ${event.sequence}');
});

// Subscribe to collection changes
final collSub = await client.subscribe('/12D3KooWBob.../collection/products',
  filter: '{"category": "tools"}',
);
collSub.events.listen((event) {
  print('Product ${event.key} ${event.type}d');
});
```

### 6.2 Unified Discovery

Well-known paths for store discovery:

```json
// GET /{peerId}/.well-known/stores
{
  "stores": {
    "documents": [
      {"path": "profile", "contentType": "application/json"},
      {"path": "avatar", "contentType": "image/png"},
      {"path": "settings", "contentType": "application/json"}
    ],
    "feeds": [
      {"path": "posts", "title": "My Posts", "entryCount": 156},
      {"path": "activity", "title": "Activity Log", "entryCount": 2341}
    ],
    "collections": [
      {"path": "products", "name": "Product Catalog", "recordCount": 89},
      {"path": "contacts", "name": "Contacts", "recordCount": 234}
    ],
    "pinnedBlobs": 42
  }
}
```

### 6.3 Access Control

Unified ACL model across all store types:

```dart
/// Store access control entry
@collection
class IsarStoreACL {
  Id id = Isar.autoIncrement;
  
  /// Store type (doc, feed, collection, blob)
  late String storeType;
  
  /// Owner peer ID
  @Index(composite: [CompositeIndex('storeType'), CompositeIndex('storePath')])
  late String ownerPeerId;
  
  /// Store path
  late String storePath;
  
  /// Grantee peer ID ("*" for public)
  late String granteePeerId;
  
  /// Access level
  @Enumerated(EnumType.ordinal)
  late StoreAccessLevel accessLevel;
  
  /// When granted
  late int grantedTimestamp;
  
  /// Optional expiry
  late int? expiryTimestamp;
}

enum StoreAccessLevel {
  none,      // No access
  read,      // GET, HEAD, LIST, QUERY
  write,     // read + PUT, PATCH, DELETE, APPEND
  admin,     // write + ACL management
}
```

---

## 7. Replication Strategy

### 7.1 Sync Protocol

**Protocol ID:** `/ricochet/store/sync/1.0.0`

```
Server A                              Server B
   │                                      │
   │──── SYNC_REQUEST ───────────────────▶│
   │     {storeType, storePath, heads}    │
   │                                      │
   │◀─── SYNC_RESPONSE ──────────────────│
   │     {missing CIDs, deltas}           │
   │                                      │
   │──── CRDT_DELTAS ────────────────────▶│
   │     {CID → CRDT data}                │
   │                                      │
   │◀─── ACK ────────────────────────────│
   │                                      │
```

### 7.2 Replication Topology

```
                    ┌─────────────┐
                    │   Server A  │
                    │  (Primary)  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Server B │ │ Server C │ │ Server D │
        │(Replica) │ │(Replica) │ │(Replica) │
        └──────────┘ └──────────┘ └──────────┘

All servers sync via GossipSub announcements.
CRDTs ensure eventual consistency regardless of sync order.
```

### 7.3 Conflict Resolution Summary

| Store Type | Strategy | Tiebreaker |
|------------|----------|------------|
| Document | Last-Writer-Wins | Wall clock timestamp |
| Feed | Append merge | Lamport timestamp |
| Collection | Per-record LWW | Wall clock + delete-wins |
| Blob | No conflicts | Content-addressed (immutable) |

---

## 8. Protocol Unification

### 8.1 Unified Request Frame

All store protocols share a common frame structure:

```json
{
  "protocol": "/ricochet/store/1.0.0",
  "storeType": "doc" | "feed" | "collection" | "blob",
  "operation": "GET" | "PUT" | "PATCH" | "DELETE" | "APPEND" | "QUERY" | ...,
  "ownerPeerId": "12D3KooW...",
  "path": "profile",
  "key": "record-key",  // For collections
  "headers": { ... },
  "body": "base64..."
}
```

### 8.2 Unified Handler

```dart
/// Unified store protocol handler
class StoreHandler {
  static const String protocolId = '/ricochet/store/1.0.0';
  
  final DocumentHandler docHandler;
  final FeedHandler feedHandler;
  final CollectionHandler collectionHandler;
  final BlobHandler blobHandler;
  
  Future<void> handleStream(P2PStream stream, PeerId callerId) async {
    final request = await _readRequest(stream);
    
    // Route to appropriate handler
    switch (request.storeType) {
      case 'doc':
        return docHandler.handle(stream, request, callerId);
      case 'feed':
        return feedHandler.handle(stream, request, callerId);
      case 'collection':
        return collectionHandler.handle(stream, request, callerId);
      case 'blob':
        return blobHandler.handle(stream, request, callerId);
      default:
        await _sendError(stream, 400, 'Unknown store type');
    }
  }
}
```

---

## 9. Migration & Compatibility

### 9.1 Backward Compatibility

- Existing mailbox protocols remain unchanged
- Document Store MVP protocol `/ricochet/store/doc/1.0.0` remains valid
- New features are additive, not breaking

### 9.2 Migration Paths

**Profile storage (MVP → Full):**
- MVP clients continue to work
- Full implementation adds PATCH, history support
- No data migration needed

**Mailbox → Feed migration:**
- Public mailboxes (collaborative) remain as-is
- Broadcast use cases (social posts) migrate to Feed Store
- Provided migration utility for one-time conversion

**Message-based catalogs → Collection:**
- Export existing message-based data
- Import into Collection Store
- Update client code to use new API

### 9.3 Version Negotiation

```dart
/// Protocol version negotiation
class StoreProtocolNegotiator {
  // Supported versions in preference order
  static const supportedVersions = [
    '/ricochet/store/1.1.0',  // Full implementation
    '/ricochet/store/1.0.0',  // MVP
  ];
  
  String negotiate(List<String> remoteVersions) {
    for (final version in supportedVersions) {
      if (remoteVersions.contains(version)) {
        return version;
      }
    }
    throw UnsupportedProtocolException();
  }
}
```

---

## 10. Implementation Timeline

### 10.1 Phase Summary

| Phase | Focus | Duration | Dependencies |
|-------|-------|----------|--------------|
| **1** | Document Store MVP | 1 week | None |
| **2** | Document Store Complete | 1.5 weeks | Phase 1 |
| **3** | Feed Store | 1.5 weeks | Phase 2 |
| **4** | Collection Store | 2.5 weeks | Phase 2 |
| **5** | Blob Store | 1.5 weeks | Phase 2 |
| **6** | Cross-cutting (Subs, Discovery) | 1.5 weeks | Phases 3-5 |
| **Total** | | **~10 weeks** | |

### 10.2 Detailed Timeline

```
Week 1:   [Phase 1] Document Store MVP
Week 2-3: [Phase 2] Document PATCH, DELETE, LIST, History
Week 3-4: [Phase 2] Document CRDT replication
Week 5-6: [Phase 3] Feed Store implementation
Week 6-8: [Phase 4] Collection Store + queries
Week 9:   [Phase 5] Blob Store + DHT
Week 10:  [Phase 6] Subscriptions + Discovery + Testing
```

### 10.3 Milestone Deliverables

| Milestone | Deliverables | Target |
|-----------|--------------|--------|
| **M1: MVP** | Document GET/PUT/HEAD | Week 1 |
| **M2: Documents** | Full Document Store + replication | Week 4 |
| **M3: Feeds** | Feed Store for social/activity | Week 6 |
| **M4: Collections** | Collection Store with queries | Week 8 |
| **M5: Blobs** | Blob Store with DHT | Week 9 |
| **M6: Complete** | Subscriptions, discovery, docs | Week 10 |

---

## Appendix A: API Quick Reference

### Document Store

```
Protocol: /ricochet/store/doc/1.0.0

GET    /{owner}/doc/{path}           Read document
PUT    /{owner}/doc/{path}           Replace document
PATCH  /{owner}/doc/{path}           Merge update
DELETE /{owner}/doc/{path}           Remove document
HEAD   /{owner}/doc/{path}           Check version
LIST   /{owner}/doc/{prefix}         List documents
HISTORY /{owner}/doc/{path}          Version history
```

### Feed Store

```
Protocol: /ricochet/store/feed/1.0.0

CREATE /{owner}/feed/{path}          Create feed
GET    /{owner}/feed/{path}          Feed metadata
GET    /{owner}/feed/{path}/{seq}    Single entry
GET    /{owner}/feed/{path}?from=N   Entry range
APPEND /{owner}/feed/{path}          Add entry
DELETE /{owner}/feed/{path}          Delete feed
```

### Collection Store

```
Protocol: /ricochet/store/collection/1.0.0

CREATE /{owner}/collection/{path}              Create collection
GET    /{owner}/collection/{path}              Metadata
GET    /{owner}/collection/{path}/{key}        Single record
PUT    /{owner}/collection/{path}/{key}        Upsert record
DELETE /{owner}/collection/{path}/{key}        Remove record
QUERY  /{owner}/collection/{path}?filter=...   Query records
LIST   /{owner}/collection/{path}              List all keys
```

### Blob Store

```
Protocol: /ricochet/store/blob/1.0.0

PUT    /{owner}/blob                 Upload, returns CID
GET    /blob/{cid}                   Download by CID
GET    /{owner}/blob/{cid}           Download pinned blob
HEAD   /blob/{cid}                   Check existence
PIN    /{owner}/blob/{cid}           Pin blob
UNPIN  /{owner}/blob/{cid}           Remove pin
```

### Subscriptions

```
Protocol: /ricochet/store/subscribe/1.0.0

SUBSCRIBE   /{owner}/{type}/{path}   Subscribe to updates
UNSUBSCRIBE {subscriptionId}         Cancel subscription
```

---

## Appendix B: Beads Issue Breakdown

### Phase 2: Document Store Completion
- `ricochet-xxx`: Implement PATCH operation
- `ricochet-xxx`: Implement DELETE operation
- `ricochet-xxx`: Implement LIST operation
- `ricochet-xxx`: Add version history support
- `ricochet-xxx`: Implement DocumentCRDT
- `ricochet-xxx`: Add document sync protocol

### Phase 3: Feed Store
- `ricochet-xxx`: Add IsarFeed/IsarFeedEntry models
- `ricochet-xxx`: Implement FeedHandler protocol
- `ricochet-xxx`: Implement FeedCRDT
- `ricochet-xxx`: Add feed sync protocol

### Phase 4: Collection Store
- `ricochet-xxx`: Add IsarCollection/IsarRecord models
- `ricochet-xxx`: Implement CollectionHandler protocol
- `ricochet-xxx`: Implement query parser and execution
- `ricochet-xxx`: Implement CollectionCRDT
- `ricochet-xxx`: Add collection sync protocol

### Phase 5: Blob Store
- `ricochet-xxx`: Add IsarBlob models
- `ricochet-xxx`: Implement CID computation and chunking
- `ricochet-xxx`: Implement BlobHandler protocol
- `ricochet-xxx`: Add DHT integration
- `ricochet-xxx`: Implement garbage collection

### Phase 6: Cross-cutting
- `ricochet-xxx`: Implement SubscriptionManager
- `ricochet-xxx`: Add unified discovery (.well-known/stores)
- `ricochet-xxx`: Implement unified ACL system
- `ricochet-xxx`: Documentation and examples

---

*End of Proposal*

