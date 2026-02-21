# Document Store MVP Proposal

**Status:** Proposal  
**Author:** Architecture Team  
**Date:** January 2026  
**Version:** 1.0  
**Related:** [RICOCHET_STORES_PROPOSAL.md](../RICOCHET_STORES_PROPOSAL.md)

---

## Executive Summary

This proposal defines the **minimum viable implementation** of a Document Store for Ricochet, focused specifically on enabling **user profile storage** for OverNode. It extracts the smallest useful subset from the comprehensive [Ricochet Stores Proposal](../RICOCHET_STORES_PROPOSAL.md) that delivers immediate value while establishing the foundation for the full Store architecture.

**Scope:** GET and PUT operations for single mutable JSON/binary documents, with ETag-based versioning and public-read/owner-write access control.

**Estimated effort:** ~300 lines of new Dart code across server and client.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Proposed Solution](#2-proposed-solution)
3. [Technical Design](#3-technical-design)
4. [Protocol Specification](#4-protocol-specification)
5. [Data Model](#5-data-model)
6. [Implementation Plan](#6-implementation-plan)
7. [Client Integration](#7-client-integration)
8. [Security Considerations](#8-security-considerations)
9. [Future Extensions](#9-future-extensions)

---

## 1. Problem Statement

### Current Situation

OverNode currently stores user profiles using Ricochet's mailbox system as a workaround:

```dart
// Current approach in ProfileSyncService
await p2pCoordinator.ricochetSendMessage(
  recipientPeerId: p2pCoordinator.peerId!,
  payload: Uint8List.fromList(jsonBytes),
  folderPath: 'profiles/profile.json',
  priority: 1,
  persistent: true,  // Keep the latest profile
);
```

### Problems with This Approach

| Issue | Impact |
|-------|--------|
| **Append-only semantics** | Each profile update adds a new message; old versions accumulate |
| **No GET semantics** | Must retrieve all messages, parse, and find the latest |
| **No versioning** | Can't determine if profile has changed without full retrieval |
| **No caching** | Every request transfers full content, even if unchanged |
| **Wrong abstraction** | Mailboxes are for communication, not content storage |
| **Complex retrieval** | Client must understand mailbox semantics to get a profile |

### Specific Pain Points

1. **Profile lookup is expensive:**
   ```dart
   // Current: Must retrieve messages and parse
   await p2pCoordinator.ricochetRetrieveMessagesFromPeer(
     targetPeerId: peerId,
     folderPath: 'profiles/profile.json',
     maxMessages: 1,
   );
   // Then wait for async delivery via inbox...
   ```

2. **No conditional fetching:**
   - Every profile view triggers a full network round-trip
   - No way to check "has this profile changed since I last fetched it?"

3. **Storage bloat:**
   - Each profile edit creates a new message
   - Old versions persist until retention policy kicks in

---

## 2. Proposed Solution

### Document Store (Minimal)

Implement a lightweight **Document Store** primitive alongside existing mailboxes:

```
┌─────────────────────────────────────────────────────────────┐
│                     Ricochet Server                         │
├─────────────────────────────────────────────────────────────┤
│  Existing:                                                  │
│  ├── MSA (Submit)    /sf-network/submit/1.0.0              │
│  ├── MAA (Access)    /sf-network/access/1.0.0              │
│  └── MMA (Admin)     /sf-network/admin/1.0.0               │
│                                                             │
│  NEW:                                                       │
│  └── SDA (Store Document Access)                           │
│       └── /ricochet/store/doc/1.0.0                        │
│           ├── GET   - Read document                         │
│           ├── PUT   - Write/replace document                │
│           └── HEAD  - Check version (ETag)                  │
├─────────────────────────────────────────────────────────────┤
│  Storage (Isar)                                             │
│  ├── IsarMailbox, IsarStoredMessage (existing)             │
│  └── IsarDocument (NEW)                                     │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Separate protocol ID**: `/ricochet/store/doc/1.0.0` (not mixed with mailbox protocols)
2. **HTTP-like semantics**: GET, PUT, HEAD with status codes and headers
3. **ETag versioning**: Content hash enables conditional requests
4. **Public-read by default**: Anyone can GET, only owner can PUT (like public mailboxes)
5. **Single-document paths**: Each path stores exactly one document

### User Profile Example

**Before (mailbox-based):**
```
POST /sf-network/submit/1.0.0
  recipient: self
  folder: profiles/profile.json
  payload: {"name": "Alice", ...}
  persistent: true
  
GET /sf-network/access/1.0.0
  folder: profiles/profile.json
  → Returns list of messages, client must find latest
```

**After (document-based):**
```
PUT /ricochet/store/doc/1.0.0
  path: /12D3KooW.../doc/profile
  content: {"name": "Alice", ...}
  → Returns ETag: "abc123..."

GET /ricochet/store/doc/1.0.0
  path: /12D3KooW.../doc/profile
  → Returns content + ETag

GET (conditional)
  path: /12D3KooW.../doc/profile
  If-None-Match: "abc123..."
  → Returns 304 Not Modified (no body transfer)
```

---

## 3. Technical Design

### 3.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Document Store MVP                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Client                           Server                    │
│  ┌─────────────┐                 ┌─────────────────────┐   │
│  │ SFClient    │                 │ DocumentHandler     │   │
│  │ (extended)  │───libp2p───────▶│ /ricochet/store/    │   │
│  │             │   stream        │ doc/1.0.0           │   │
│  └─────────────┘                 └──────────┬──────────┘   │
│                                              │              │
│                                              ▼              │
│                                  ┌─────────────────────┐   │
│                                  │ IsarStorageManager  │   │
│                                  │ (document ops)      │   │
│                                  └──────────┬──────────┘   │
│                                              │              │
│                                              ▼              │
│                                  ┌─────────────────────┐   │
│                                  │ Isar Database       │   │
│                                  │ ├── IsarMailbox     │   │
│                                  │ ├── IsarMessage     │   │
│                                  │ └── IsarDocument    │◀──NEW
│                                  └─────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| `DocumentHandler` | Protocol handler for `/ricochet/store/doc/1.0.0` stream |
| `DocumentFrame` | Request/response encoding (JSON) |
| `IsarDocument` | Isar collection for document storage |
| `IsarStorageManager` | Database operations (getDocument, putDocument) |
| `SFClient` (extended) | Client-side document API |

### 3.3 Addressing Scheme

Documents are addressed by owner PeerId and path:

```
/{ownerId}/doc/{path}

Examples:
/12D3KooWAlice.../doc/profile       → User profile
/12D3KooWAlice.../doc/avatar        → Avatar image
/12D3KooWAlice.../doc/settings      → App settings
/12D3KooWAlice.../doc/store-info    → Marketplace store metadata
```

Path constraints:
- Must be non-empty
- Can contain `/` for hierarchy (e.g., `settings/notifications`)
- No `.` or `..` components
- Max length: 256 characters

---

## 4. Protocol Specification

### 4.1 Protocol Hierarchy

```
/ricochet/store/doc/1.0.0
```

This is a standalone protocol, not a sub-protocol of the existing mailbox system. This allows:
- Independent versioning
- Clear separation of concerns
- Optional deployment (servers can support mailboxes without documents)

### 4.2 Request Frame

```
┌─────────────────────────────────────────────────────────────┐
│ Document Request Frame (JSON-encoded)                       │
├─────────────────────────────────────────────────────────────┤
│ {                                                           │
│   "operation": "GET" | "PUT" | "HEAD",                     │
│   "ownerPeerId": "12D3KooW...",                            │
│   "path": "profile",                                        │
│   "headers": {                                              │
│     "If-None-Match": "etag...",     // Conditional GET     │
│     "If-Match": "etag...",          // Conditional PUT     │
│     "Content-Type": "application/json"                     │
│   },                                                        │
│   "body": "base64-encoded-content"  // For PUT only        │
│ }                                                           │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 Response Frame

```
┌─────────────────────────────────────────────────────────────┐
│ Document Response Frame (JSON-encoded)                      │
├─────────────────────────────────────────────────────────────┤
│ {                                                           │
│   "status": 200,                    // HTTP-like status     │
│   "headers": {                                              │
│     "ETag": "sha256:abc123...",                            │
│     "Content-Type": "application/json",                    │
│     "Last-Modified": 1704873600000, // Unix ms             │
│     "Content-Length": 1234                                  │
│   },                                                        │
│   "body": "base64-encoded-content"  // For 200 responses   │
│ }                                                           │
└─────────────────────────────────────────────────────────────┘
```

### 4.4 Status Codes

| Code | Meaning | When Used |
|------|---------|-----------|
| 200 | OK | Successful GET with content |
| 201 | Created | Successful PUT (new document) |
| 204 | No Content | Successful PUT (update) |
| 304 | Not Modified | GET with matching If-None-Match |
| 400 | Bad Request | Invalid path or malformed request |
| 403 | Forbidden | PUT by non-owner |
| 404 | Not Found | GET for non-existent document |
| 409 | Conflict | PUT with non-matching If-Match |
| 413 | Payload Too Large | Document exceeds size limit |

### 4.5 Operations

#### GET - Read Document

**Request:**
```json
{
  "operation": "GET",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "If-None-Match": "sha256:abc123..."  // Optional
  }
}
```

**Response (200 OK):**
```json
{
  "status": 200,
  "headers": {
    "ETag": "sha256:abc123...",
    "Content-Type": "application/json",
    "Last-Modified": 1704873600000,
    "Content-Length": 256
  },
  "body": "eyJuYW1lIjoiQWxpY2UiLC4uLn0="
}
```

**Response (304 Not Modified):**
```json
{
  "status": 304,
  "headers": {
    "ETag": "sha256:abc123..."
  }
}
```

#### PUT - Write Document

**Request:**
```json
{
  "operation": "PUT",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile",
  "headers": {
    "Content-Type": "application/json",
    "If-Match": "sha256:old123..."  // Optional optimistic locking
  },
  "body": "eyJuYW1lIjoiQWxpY2UgVXBkYXRlZCIsLi4ufQ=="
}
```

**Response (201 Created / 204 No Content):**
```json
{
  "status": 201,
  "headers": {
    "ETag": "sha256:new456...",
    "Last-Modified": 1704873700000
  }
}
```

#### HEAD - Check Version

**Request:**
```json
{
  "operation": "HEAD",
  "ownerPeerId": "12D3KooWAlice...",
  "path": "profile"
}
```

**Response:**
```json
{
  "status": 200,
  "headers": {
    "ETag": "sha256:abc123...",
    "Content-Type": "application/json",
    "Last-Modified": 1704873600000,
    "Content-Length": 256
  }
}
```

---

## 5. Data Model

### 5.1 Isar Collection

Add to `lib/storage/isar_models.dart`:

```dart
/// Document store entry for mutable single-object storage
@collection
class IsarDocument {
  Id id = Isar.autoIncrement;
  
  /// Owner's peer ID (Base58 string)
  /// Composite index with path for efficient lookups
  @Index(composite: [CompositeIndex('path')])
  late String ownerPeerId;
  
  /// Document path (e.g., "profile", "avatar", "settings/notifications")
  late String path;
  
  /// Document content (JSON, CBOR, or binary)
  late List<byte> content;
  
  /// MIME content type
  late String contentType;
  
  /// SHA-256 hash of content for ETag
  /// Also indexed for potential deduplication
  @Index()
  late String contentHash;
  
  /// When this document was created
  late int createdTimestamp;
  
  /// When this document was last updated
  late int updatedTimestamp;
  
  /// Who last updated this document (Base58)
  late String updatedByPeerId;
  
  /// Computed full path
  @ignore
  String get fullPath => '$ownerPeerId/doc/$path';
}
```

### 5.2 Storage Operations

Add to `lib/mda/storage/isar_storage_manager.dart`:

```dart
// ============================================================================
// Document Operations
// ============================================================================

/// Maximum document size (10MB)
static const int maxDocumentSize = 10 * 1024 * 1024;

/// Get a document by owner and path
Future<IsarDocument?> getDocument(PeerId ownerId, String path) async {
  return await isar.isarDocuments
      .filter()
      .ownerPeerIdEqualTo(ownerId.toBase58())
      .and()
      .pathEqualTo(path)
      .findFirst();
}

/// Put a document (upsert semantics)
/// 
/// Returns the content hash (ETag) of the stored document.
/// Throws if content exceeds maxDocumentSize.
Future<DocumentPutResult> putDocument({
  required PeerId ownerId,
  required String path,
  required Uint8List content,
  required String contentType,
  required PeerId updatedBy,
  String? ifMatch,  // Optimistic locking
}) async {
  if (content.length > maxDocumentSize) {
    throw DocumentSizeExceededException(content.length, maxDocumentSize);
  }
  
  final contentHash = _computeContentHash(content);
  final now = DateTime.now().millisecondsSinceEpoch;
  bool created = false;
  
  await isar.writeTxn(() async {
    // Find existing document
    var doc = await isar.isarDocuments
        .filter()
        .ownerPeerIdEqualTo(ownerId.toBase58())
        .and()
        .pathEqualTo(path)
        .findFirst();
    
    // Check If-Match precondition
    if (ifMatch != null && doc != null && doc.contentHash != ifMatch) {
      throw DocumentConflictException(ifMatch, doc.contentHash);
    }
    
    if (doc == null) {
      // Create new document
      created = true;
      doc = IsarDocument()
        ..ownerPeerId = ownerId.toBase58()
        ..path = path
        ..createdTimestamp = now;
    }
    
    // Update document
    doc
      ..content = content
      ..contentType = contentType
      ..contentHash = contentHash
      ..updatedTimestamp = now
      ..updatedByPeerId = updatedBy.toBase58();
    
    await isar.isarDocuments.put(doc);
  });
  
  _logger.info('${created ? "Created" : "Updated"} document: '
      '${ownerId.toBase58().substring(0, 12)}.../doc/$path '
      '(${content.length} bytes, hash: ${contentHash.substring(0, 16)}...)');
  
  return DocumentPutResult(
    contentHash: contentHash,
    created: created,
    updatedTimestamp: now,
  );
}

/// Delete a document
Future<bool> deleteDocument(PeerId ownerId, String path) async {
  bool deleted = false;
  
  await isar.writeTxn(() async {
    final count = await isar.isarDocuments
        .filter()
        .ownerPeerIdEqualTo(ownerId.toBase58())
        .and()
        .pathEqualTo(path)
        .deleteAll();
    deleted = count > 0;
  });
  
  if (deleted) {
    _logger.info('Deleted document: ${ownerId.toBase58().substring(0, 12)}.../doc/$path');
  }
  
  return deleted;
}

/// List all documents for an owner
Future<List<IsarDocument>> listDocuments(PeerId ownerId) async {
  return await isar.isarDocuments
      .filter()
      .ownerPeerIdEqualTo(ownerId.toBase58())
      .findAll();
}

/// Compute SHA-256 content hash
String _computeContentHash(Uint8List content) {
  final digest = sha256.convert(content);
  return 'sha256:${digest.toString()}';
}
```

### 5.3 Supporting Types

```dart
/// Result of a document PUT operation
class DocumentPutResult {
  final String contentHash;
  final bool created;
  final int updatedTimestamp;
  
  DocumentPutResult({
    required this.contentHash,
    required this.created,
    required this.updatedTimestamp,
  });
}

/// Thrown when document exceeds size limit
class DocumentSizeExceededException implements Exception {
  final int actualSize;
  final int maxSize;
  
  DocumentSizeExceededException(this.actualSize, this.maxSize);
  
  @override
  String toString() => 
      'Document size $actualSize exceeds maximum $maxSize bytes';
}

/// Thrown when If-Match precondition fails
class DocumentConflictException implements Exception {
  final String expectedHash;
  final String actualHash;
  
  DocumentConflictException(this.expectedHash, this.actualHash);
  
  @override
  String toString() => 
      'Document conflict: expected $expectedHash, found $actualHash';
}
```

---

## 6. Implementation Plan

### 6.1 File Structure

```
ricochet/lib/
├── protocol/
│   └── sda/                           # NEW: Store Document Access
│       ├── document_handler.dart      # Protocol handler
│       └── document_frame.dart        # Frame encoding/decoding
├── storage/
│   └── isar_models.dart               # Add IsarDocument
├── mda/storage/
│   └── isar_storage_manager.dart      # Add document operations
├── server/
│   └── sf_server.dart                 # Register DocumentHandler
└── client/
    └── sf_client.dart                 # Add document client methods
```

### 6.2 Implementation Steps

#### Phase 1: Storage Layer (~1 hour)

1. **Add `IsarDocument` model** to `isar_models.dart`
2. **Regenerate Isar schemas**: `dart run build_runner build`
3. **Add document operations** to `IsarStorageManager`
4. **Add exception types** for document errors

#### Phase 2: Protocol Handler (~2 hours)

1. **Create `DocumentFrame`** class for request/response encoding
2. **Create `DocumentHandler`** with GET, PUT, HEAD operations
3. **Implement authorization** (owner-only writes, public reads)
4. **Add rate limiting** (reuse pattern from AccessHandler)

#### Phase 3: Server Integration (~30 min)

1. **Register `DocumentHandler`** in `SFServer`
2. **Add `IsarDocumentSchema`** to Isar database initialization
3. **Add configuration** for document size limits

#### Phase 4: Client Integration (~1 hour)

1. **Add document methods** to `SFClient`
2. **Create `DocumentResponse`** type for client use
3. **Implement conditional GET** with ETag caching

#### Phase 5: Testing (~1 hour)

1. **Unit tests** for document storage operations
2. **Integration tests** for document protocol
3. **End-to-end tests** with client/server

### 6.3 Estimated Effort

| Component | Lines of Code | Time |
|-----------|---------------|------|
| `IsarDocument` model | ~30 | 15 min |
| Storage operations | ~100 | 30 min |
| `DocumentFrame` | ~80 | 30 min |
| `DocumentHandler` | ~200 | 1 hour |
| Server integration | ~20 | 15 min |
| Client methods | ~80 | 30 min |
| Tests | ~150 | 1 hour |
| **Total** | **~660** | **~4 hours** |

---

## 7. Client Integration

### 7.1 SFClient Extensions

Add to `lib/client/sf_client.dart`:

```dart
/// Get a document from a peer's store
/// 
/// [ownerPeerId] The peer who owns the document
/// [path] Document path (e.g., "profile", "avatar")
/// [ifNoneMatch] Optional ETag for conditional GET (returns null if unchanged)
Future<DocumentResponse?> getDocument({
  required PeerId ownerPeerId,
  required String path,
  String? ifNoneMatch,
  PeerId? fromServer,
}) async {
  final serverId = fromServer ?? await _serverSelector.selectServer(config.preferredServers);
  if (serverId == null) throw Exception('No available servers');
  
  final context = Context();
  final stream = await host.newStream(
    serverId,
    [DocumentHandler.protocolId],
    context,
  ).timeout(config.connectionTimeout);
  
  return await DocumentHandler.getDocument(
    stream,
    ownerPeerId: ownerPeerId,
    path: path,
    ifNoneMatch: ifNoneMatch,
  );
}

/// Put a document to your own store
/// 
/// [path] Document path (e.g., "profile", "avatar")
/// [content] Document content bytes
/// [contentType] MIME content type
/// [ifMatch] Optional ETag for optimistic locking
Future<DocumentPutResponse> putDocument({
  required String path,
  required Uint8List content,
  String contentType = 'application/json',
  String? ifMatch,
  PeerId? toServer,
}) async {
  final serverId = toServer ?? await _serverSelector.selectServer(config.preferredServers);
  if (serverId == null) throw Exception('No available servers');
  
  final context = Context();
  final stream = await host.newStream(
    serverId,
    [DocumentHandler.protocolId],
    context,
  ).timeout(config.connectionTimeout);
  
  return await DocumentHandler.putDocument(
    stream,
    ownerPeerId: host.id,  // Can only PUT to own documents
    path: path,
    content: content,
    contentType: contentType,
    ifMatch: ifMatch,
  );
}

/// Check if a document has changed (HEAD request)
Future<DocumentMetadata?> headDocument({
  required PeerId ownerPeerId,
  required String path,
  PeerId? fromServer,
}) async {
  final serverId = fromServer ?? await _serverSelector.selectServer(config.preferredServers);
  if (serverId == null) throw Exception('No available servers');
  
  final context = Context();
  final stream = await host.newStream(
    serverId,
    [DocumentHandler.protocolId],
    context,
  ).timeout(config.connectionTimeout);
  
  return await DocumentHandler.headDocument(
    stream,
    ownerPeerId: ownerPeerId,
    path: path,
  );
}
```

### 7.2 Response Types

```dart
/// Response from GET document
class DocumentResponse {
  final int status;
  final String? etag;
  final String? contentType;
  final int? lastModified;
  final Uint8List? content;
  
  DocumentResponse({
    required this.status,
    this.etag,
    this.contentType,
    this.lastModified,
    this.content,
  });
  
  bool get isNotModified => status == 304;
  bool get isNotFound => status == 404;
  bool get isSuccess => status == 200;
}

/// Response from PUT document
class DocumentPutResponse {
  final int status;
  final String? etag;
  final int? lastModified;
  final bool created;
  
  DocumentPutResponse({
    required this.status,
    this.etag,
    this.lastModified,
    required this.created,
  });
  
  bool get isSuccess => status == 201 || status == 204;
  bool get isConflict => status == 409;
}

/// Metadata from HEAD request
class DocumentMetadata {
  final String etag;
  final String contentType;
  final int lastModified;
  final int contentLength;
  
  DocumentMetadata({
    required this.etag,
    required this.contentType,
    required this.lastModified,
    required this.contentLength,
  });
}
```

### 7.3 OverNode Integration

Update `ProfileSyncService` in OverNode:

```dart
class ProfileSyncService {
  // Cache ETags for conditional fetching
  final Map<String, String> _profileEtags = {};
  
  /// Sync profile to Ricochet using Document Store
  Future<void> syncProfileToRicochet(
    UserProfile profile, {
    Uint8List? avatarData,
    Uint8List? bannerData,
  }) async {
    _log.info('Syncing profile to Ricochet for ${profile.publicKey}');
    
    // PUT profile document
    final profileJson = jsonEncode({
      'publicKey': profile.publicKey,
      'displayName': profile.displayName,
      'bio': profile.bio,
      'location': profile.location,
      'updatedAt': DateTime.now().toIso8601String(),
    });
    
    await p2pCoordinator.ricochetPutDocument(
      path: 'profile',
      content: Uint8List.fromList(utf8.encode(profileJson)),
      contentType: 'application/json',
    );
    
    // PUT avatar if provided
    if (avatarData != null) {
      await p2pCoordinator.ricochetPutDocument(
        path: 'avatar',
        content: avatarData,
        contentType: 'image/png',
      );
    }
    
    // PUT banner if provided
    if (bannerData != null) {
      await p2pCoordinator.ricochetPutDocument(
        path: 'banner',
        content: bannerData,
        contentType: 'image/jpeg',
      );
    }
    
    _log.info('Profile sync complete');
  }
  
  /// Fetch profile using Document Store with caching
  Future<UserProfile?> fetchProfileFromRicochet(String peerId) async {
    try {
      final ownerPeerId = PeerId.fromString(peerId);
      
      // Try conditional GET if we have cached ETag
      final cachedEtag = _profileEtags[peerId];
      
      final response = await p2pCoordinator.ricochetGetDocument(
        ownerPeerId: ownerPeerId,
        path: 'profile',
        ifNoneMatch: cachedEtag,
      );
      
      if (response == null || response.isNotFound) {
        _log.info('No profile found for $peerId');
        return null;
      }
      
      if (response.isNotModified) {
        _log.info('Profile unchanged for $peerId');
        // Return cached profile (caller should maintain cache)
        return null;
      }
      
      // Cache the new ETag
      if (response.etag != null) {
        _profileEtags[peerId] = response.etag!;
      }
      
      // Parse profile
      final json = jsonDecode(utf8.decode(response.content!));
      return UserProfile.fromJson(json);
      
    } catch (e) {
      _log.warning('Failed to fetch profile: $e');
      return null;
    }
  }
  
  /// Fetch avatar image
  Future<Uint8List?> fetchAvatarFromRicochet(String peerId) async {
    final response = await p2pCoordinator.ricochetGetDocument(
      ownerPeerId: PeerId.fromString(peerId),
      path: 'avatar',
    );
    
    return response?.content;
  }
  
  /// Fetch banner image  
  Future<Uint8List?> fetchBannerFromRicochet(String peerId) async {
    final response = await p2pCoordinator.ricochetGetDocument(
      ownerPeerId: PeerId.fromString(peerId),
      path: 'banner',
    );
    
    return response?.content;
  }
}
```

---

## 8. Security Considerations

### 8.1 Authorization Model

| Operation | Who Can Perform |
|-----------|-----------------|
| GET | Anyone (public read) |
| HEAD | Anyone (public read) |
| PUT | Owner only (authenticated via libp2p Noise) |
| DELETE | Owner only |

The authorization model mirrors **public mailboxes**: anyone can read, only the owner can write.

### 8.2 Authentication

All requests are authenticated via libp2p's Noise protocol:
1. Client opens stream to server
2. Noise handshake performs mutual authentication  
3. Server extracts caller's PeerId from authenticated connection
4. For PUT operations, server verifies `callerId == ownerPeerId`

### 8.3 Rate Limiting

Apply rate limiting per-peer to prevent abuse:
- **GET/HEAD**: 100 requests/minute (higher limit for reads)
- **PUT**: 20 requests/minute (lower limit for writes)

### 8.4 Size Limits

| Limit | Value | Rationale |
|-------|-------|-----------|
| Max document size | 10 MB | Reasonable for profiles, avatars, etc. |
| Max path length | 256 chars | Prevent path abuse |
| Max documents per owner | 1000 | Prevent storage abuse |

### 8.5 Content Validation

- Path validation: No `..`, no leading `/`, alphanumeric + `/` + `-` + `_` only
- Content type validation: Must be valid MIME type
- Size validation: Enforced before storage

---

## 9. Future Extensions

This MVP establishes the foundation for the full Document Store from the Ricochet Stores Proposal. Future extensions include:

### 9.1 Near-term (Phase 2)

| Feature | Description |
|---------|-------------|
| PATCH operation | JSON Merge Patch for partial updates |
| Version history | Keep last N versions for rollback |
| DELETE operation | Remove documents |
| LIST operation | List all documents for an owner |

### 9.2 Medium-term (Phase 3)

| Feature | Description |
|---------|-------------|
| CRDT replication | Sync documents across S&F servers |
| Subscriptions | PubSub notifications on document changes |
| Schema validation | Optional JSON Schema enforcement |
| Access control | Fine-grained ACLs beyond public/owner |

### 9.3 Long-term (Full Stores)

| Feature | Description |
|---------|-------------|
| Feed Store | Append-only streams for social posts |
| Collection Store | Key-value records with queries |
| Blob Store | Content-addressed large files |

---

## Appendix A: Quick Reference

### Protocol ID
```
/ricochet/store/doc/1.0.0
```

### Operations

```
GET  /{owner}/doc/{path}              Read document
PUT  /{owner}/doc/{path}              Write document (owner only)
HEAD /{owner}/doc/{path}              Check version
```

### Headers

| Header | Direction | Purpose |
|--------|-----------|---------|
| `ETag` | Response | Content hash for caching |
| `If-None-Match` | Request (GET) | Conditional GET |
| `If-Match` | Request (PUT) | Optimistic locking |
| `Content-Type` | Both | MIME type |
| `Last-Modified` | Response | Update timestamp |
| `Content-Length` | Response | Content size |

### Status Codes

```
200 OK              - Successful GET
201 Created         - Successful PUT (new)
204 No Content      - Successful PUT (update)
304 Not Modified    - Conditional GET (unchanged)
400 Bad Request     - Invalid request
403 Forbidden       - Not owner (PUT)
404 Not Found       - Document doesn't exist
409 Conflict        - If-Match failed
413 Payload Too Large
```

---

## Appendix B: Migration Checklist

For migrating from mailbox-based profile storage to document store:

- [ ] Deploy Ricochet servers with Document Store support
- [ ] Update OverNode's `ProfileSyncService` to use document API
- [ ] Add fallback: try document API, fall back to mailbox for old servers
- [ ] After transition period, remove mailbox-based profile code
- [ ] Clean up old profile messages from mailboxes

---

*End of Proposal*

