-- Ricochet Server PostgreSQL Schema
-- Version: 1.0
-- Date: January 2026

-- =============================================================================
-- MAILBOXES
-- =============================================================================
CREATE TABLE IF NOT EXISTS mailboxes (
    id BIGSERIAL PRIMARY KEY,
    owner_peer_id TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    mailbox_type SMALLINT NOT NULL,  -- MailboxType enum ordinal
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_access_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    max_messages INTEGER NOT NULL DEFAULT 1000,
    retention_days INTEGER NOT NULL DEFAULT 30,
    retention_count INTEGER,
    -- Monotonic per-mailbox message counter. Incremented with UPDATE ... RETURNING
    -- inside the same transaction as the message insert, so concurrent deliveries
    -- to one mailbox cannot be handed the same sequence number.
    current_sequence INTEGER NOT NULL DEFAULT 0,
    -- Live count of rows in stored_messages for this mailbox. Raised by the
    -- same UPDATE that hands out a sequence number, which also checks it
    -- against max_messages under the row lock; lowered by the statement-level
    -- delete trigger on stored_messages. Delivery used to run COUNT(*) per
    -- message and check the cap outside any lock.
    message_count INTEGER NOT NULL DEFAULT 0,
    -- Summed payload length of those rows, maintained the same way, so the
    -- capacity sampler and the operator listings never sum stored_messages.
    message_bytes BIGINT NOT NULL DEFAULT 0,

    CONSTRAINT uq_mailbox_owner_folder UNIQUE(owner_peer_id, folder_path)
);

CREATE INDEX IF NOT EXISTS idx_mailboxes_owner ON mailboxes(owner_peer_id);

-- =============================================================================
-- STORED MESSAGES
-- =============================================================================
CREATE TABLE IF NOT EXISTS stored_messages (
    id BIGSERIAL PRIMARY KEY,
    mailbox_id BIGINT NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    recipient_peer_id TEXT NOT NULL,
    sender_peer_id TEXT NOT NULL,
    payload BYTEA NOT NULL,
    priority SMALLINT NOT NULL,  -- MessagePriority enum ordinal
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    hop_count INTEGER NOT NULL DEFAULT 0,
    flags_bitmap INTEGER NOT NULL DEFAULT 0,  -- IMAP-style MessageFlags (\Seen, \Deleted, ...)
    -- Protocol-level SFMessageFlags: encrypted, compressed, signed. Set by the
    -- sender and needed verbatim by the recipient to undo what the sender did.
    sf_flags INTEGER NOT NULL DEFAULT 0,
    persistent BOOLEAN NOT NULL DEFAULT FALSE,
    folder_path TEXT,
    
    CONSTRAINT uq_message_id UNIQUE(message_id)
);

-- Primary query pattern: messages by mailbox, ordered by sequence
CREATE INDEX IF NOT EXISTS idx_messages_mailbox_seq ON stored_messages(mailbox_id, sequence_number);

-- Expiration cleanup (index all non-null expires_at for efficient range queries)
CREATE INDEX IF NOT EXISTS idx_messages_expires ON stored_messages(expires_at) 
    WHERE expires_at IS NOT NULL;

-- Priority filtering
CREATE INDEX IF NOT EXISTS idx_messages_priority ON stored_messages(mailbox_id, priority DESC, sequence_number);

-- =============================================================================
-- ACCESS CONTROL LISTS
-- =============================================================================
CREATE TABLE IF NOT EXISTS mailbox_acls (
    id BIGSERIAL PRIMARY KEY,
    mailbox_id BIGINT NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL,
    access_mode SMALLINT NOT NULL,  -- AccessMode enum ordinal
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_acl_mailbox_peer UNIQUE(mailbox_id, peer_id)
);

CREATE INDEX IF NOT EXISTS idx_acls_peer ON mailbox_acls(peer_id);

-- =============================================================================
-- READER CURSORS
-- =============================================================================
CREATE TABLE IF NOT EXISTS reader_cursors (
    id BIGSERIAL PRIMARY KEY,
    mailbox_id BIGINT NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    reader_peer_id TEXT NOT NULL,
    last_sequence_read INTEGER NOT NULL DEFAULT 0,
    last_access_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_cursor_mailbox_reader UNIQUE(mailbox_id, reader_peer_id)
);

-- =============================================================================
-- BLOCK STORE (CRDTs)
-- =============================================================================
-- Reserved. No Go code reads or writes this table yet: it is the storage the
-- scale roadmap's body offload is designed around (doc/SCALE_ROADMAP.md,
-- "content-addressed storage"; doc/SCALABILITY.md §7.7), where message bodies
-- move out of stored_messages into immutable, content-addressed blocks. It
-- stays so that the offload ships as a code change against a table every
-- deployment already has. Its shape is provisional until then; nothing
-- depends on it, so it may change freely when that work lands.
CREATE TABLE IF NOT EXISTS block_store (
    id BIGSERIAL PRIMARY KEY,
    cid TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    collection TEXT,
    metadata JSONB,
    tombstoned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_block_cid UNIQUE(cid)
);

-- Collection queries
CREATE INDEX IF NOT EXISTS idx_blocks_collection ON block_store(collection) 
    WHERE collection IS NOT NULL;

-- Non-tombstoned blocks (most queries exclude tombstoned)
CREATE INDEX IF NOT EXISTS idx_blocks_active ON block_store(tombstoned) 
    WHERE tombstoned = FALSE;

-- JSONB metadata queries (GIN index for flexible querying)
CREATE INDEX IF NOT EXISTS idx_blocks_metadata ON block_store USING GIN(metadata);

-- =============================================================================
-- DOCUMENTS
-- =============================================================================
CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    owner_peer_id TEXT NOT NULL,
    path TEXT NOT NULL,
    content BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    updated_by_peer_id TEXT NOT NULL,
    version_number INT NOT NULL DEFAULT 1,
    history_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    max_history_versions INT,
    version_vector TEXT,
    -- Who may read: 0 private (owner), 1 shared (owner + document_acls),
    -- 2 public. Covers every version. Writes are the owner's regardless.
    visibility SMALLINT NOT NULL DEFAULT 0,
    
    CONSTRAINT uq_document_owner_path UNIQUE(owner_peer_id, path)
);

-- Content deduplication lookup
CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(content_hash);

-- Reader list of a shared document. Rows go with the document.
CREATE TABLE IF NOT EXISTS document_acls (
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (document_id, peer_id)
);

-- =============================================================================
-- DOCUMENT VERSIONS (Version History)
-- =============================================================================
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

-- Index for querying versions by document
CREATE INDEX IF NOT EXISTS idx_document_versions_document_id ON document_versions(document_id);

-- =============================================================================
-- DIRECTORY LISTINGS
-- =============================================================================
CREATE TABLE IF NOT EXISTS directory_listings (
    id              BIGSERIAL PRIMARY KEY,
    owner_peer_id   TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT '',
    bio             TEXT NOT NULL DEFAULT '',
    avatar_hash     TEXT NOT NULL DEFAULT '',
    listed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extras          JSONB,

    CONSTRAINT uq_directory_owner UNIQUE(owner_peer_id)
);

-- Full-text search on display_name and bio
CREATE INDEX IF NOT EXISTS idx_directory_fts
    ON directory_listings
    USING GIN(to_tsvector('english', coalesce(display_name, '') || ' ' || coalesce(bio, '')));

-- Cursor-based pagination: ordered by (updated_at, owner_peer_id)
CREATE INDEX IF NOT EXISTS idx_directory_cursor
    ON directory_listings(updated_at DESC, owner_peer_id DESC);

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
    collaborative_mode BOOLEAN NOT NULL DEFAULT FALSE,
    -- Who may read: 0 private, 1 shared (owner + feed_acls), 2 public. A feed
    -- is a publication, so the default is public; the other stores default to
    -- private.
    visibility SMALLINT NOT NULL DEFAULT 2,

    CONSTRAINT uq_feed_owner_path UNIQUE(owner_peer_id, path)
);

CREATE INDEX IF NOT EXISTS idx_feeds_owner ON feeds(owner_peer_id);

-- Reader list of a shared feed. Rows go with the feed.
CREATE TABLE IF NOT EXISTS feed_acls (
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (feed_id, peer_id)
);

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
    -- Who may read: 0 private (owner), 1 shared (owner + collection_acls),
    -- 2 public. Covers every item and every query.
    visibility SMALLINT NOT NULL DEFAULT 0,

    CONSTRAINT uq_collection_owner_path UNIQUE(owner_peer_id, path)
);

CREATE INDEX IF NOT EXISTS idx_collections_owner ON collections(owner_peer_id);

-- Reader list of a shared collection. Rows go with the collection.
CREATE TABLE IF NOT EXISTS collection_acls (
    collection_id BIGINT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    peer_id TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (collection_id, peer_id)
);

-- =============================================================================
-- COLLECTION ITEMS
-- =============================================================================
CREATE TABLE IF NOT EXISTS collection_items (
    id BIGSERIAL PRIMARY KEY,
    collection_id BIGINT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    content JSONB NOT NULL,
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

-- Version tracking / recent updates
CREATE INDEX IF NOT EXISTS idx_collection_items_updated
    ON collection_items(collection_id, updated_at DESC);

-- =============================================================================
-- MAINTENANCE FUNCTIONS AND TRIGGERS
-- =============================================================================

-- The per-row insert trigger that bumped last_access_at is gone (2026-09):
-- it updated the mailbox row a second time inside every delivery
-- transaction, and the sequence UPDATE in StoreMessage now sets
-- last_access_at itself. Dropped explicitly so upgraded databases lose it.
DROP TRIGGER IF EXISTS trg_message_access ON stored_messages;
DROP FUNCTION IF EXISTS update_mailbox_access();

-- Keeps mailboxes.message_count in step with deletes, whichever path issues
-- them: acknowledgement, expunge, expiry, retention, an operator purge, or the
-- cascade from a dropped mailbox. Statement-level with a transition table, so
-- a sweep that removes ten thousand rows costs one grouped UPDATE, not ten
-- thousand. The increment side has exactly one path, StoreMessage, and lives
-- in its sequence UPDATE.
CREATE OR REPLACE FUNCTION stored_messages_deleted()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE mailboxes m
    SET message_count = GREATEST(m.message_count - d.n, 0),
        message_bytes = GREATEST(m.message_bytes - d.bytes, 0)
    FROM (SELECT mailbox_id, COUNT(*) AS n, SUM(octet_length(payload)) AS bytes
          FROM deleted GROUP BY mailbox_id) d
    WHERE m.id = d.mailbox_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_stored_messages_deleted ON stored_messages;
CREATE TRIGGER trg_stored_messages_deleted
    AFTER DELETE ON stored_messages
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT
    EXECUTE FUNCTION stored_messages_deleted();

-- =============================================================================
-- PERMISSIONS
-- =============================================================================

-- The service role. Created here when it does not exist, so that this file
-- runs on a fresh cluster: every grant below names the role, and on a server
-- where nothing had created it the first grant failed with "role ricochet
-- does not exist" and the rest of the file was never applied. A role created
-- here has no password; set one before the server connects
-- (ALTER ROLE ricochet PASSWORD '...'). A role created beforehand, as the
-- deployment guide does, is left exactly as it was.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ricochet') THEN
        CREATE ROLE ricochet LOGIN;
    END IF;
END
$$;

-- Grant schema usage to ricochet user
GRANT USAGE ON SCHEMA public TO ricochet;

-- =============================================================================
-- IN-PLACE UPGRADES
-- =============================================================================
-- This file is re-run against existing databases, so changes to tables that
-- already exist go here rather than in the CREATE TABLE above (which is skipped
-- by IF NOT EXISTS). Everything in this section must be idempotent.

-- mailboxes.current_sequence (added 2026-08): replaces computing the next
-- message sequence as SELECT MAX(sequence_number) + 1, which two concurrent
-- deliveries could both read before either inserted, producing duplicates.
-- Nothing enforced uniqueness on (mailbox_id, sequence_number), so the failure
-- was silent -- it corrupted retrieval order and reader cursors rather than
-- raising an error.
ALTER TABLE mailboxes
    ADD COLUMN IF NOT EXISTS current_sequence INTEGER NOT NULL DEFAULT 0;

-- Backfill from existing messages. Without this, an upgraded mailbox restarts
-- numbering at 1 and collides with the sequence numbers it already handed out.
-- Idempotent: re-running only ever raises the counter to the true maximum.
UPDATE mailboxes m
SET current_sequence = GREATEST(
        m.current_sequence,
        COALESCE((SELECT MAX(sm.sequence_number) FROM stored_messages sm
                  WHERE sm.mailbox_id = m.id), 0)
    )
WHERE m.current_sequence < COALESCE(
        (SELECT MAX(sm.sequence_number) FROM stored_messages sm
         WHERE sm.mailbox_id = m.id), 0);

-- A UNIQUE constraint on (mailbox_id, sequence_number) would turn any future
-- regression here into a loud error rather than silent corruption. It is
-- deliberately not added: databases predating this change may already hold
-- duplicates, and the constraint would fail to build against them. Add it once
-- a deployment has verified it is clean.

-- mailboxes.message_count (added 2026-09): the live row count the delivery
-- path checks against max_messages under the mailbox row lock. Backfilled
-- from stored_messages; idempotent, and the same statement is what the
-- maintenance sweep runs to repair any drift.
ALTER TABLE mailboxes
    ADD COLUMN IF NOT EXISTS message_count INTEGER NOT NULL DEFAULT 0;

-- mailboxes.message_bytes (added 2026-09) rides on the same statements.
ALTER TABLE mailboxes
    ADD COLUMN IF NOT EXISTS message_bytes BIGINT NOT NULL DEFAULT 0;

UPDATE mailboxes m
SET message_count = c.n, message_bytes = c.bytes
FROM (SELECT mailbox_id, COUNT(*) AS n, SUM(octet_length(payload)) AS bytes
      FROM stored_messages GROUP BY mailbox_id) c
WHERE m.id = c.mailbox_id AND (m.message_count <> c.n OR m.message_bytes <> c.bytes);

UPDATE mailboxes m
SET message_count = 0, message_bytes = 0
WHERE (m.message_count <> 0 OR m.message_bytes <> 0)
  AND NOT EXISTS (SELECT 1 FROM stored_messages sm WHERE sm.mailbox_id = m.id);

-- stored_messages.sf_flags (added 2026-09): the protocol-level flags a sender
-- sets (encrypted, compressed) were never stored -- only the IMAP flags were --
-- and retrieval returned every message with flags = 0. A client that had
-- encrypted a message therefore got its own ciphertext back with nothing to
-- say it was ciphertext, and never decrypted it. Rows predating this column
-- keep 0, which is the honest value: whatever flags they were sent with are
-- unrecoverable.
ALTER TABLE stored_messages
    ADD COLUMN IF NOT EXISTS sf_flags INTEGER NOT NULL DEFAULT 0;

-- mailbox_stats and message_priority_stats views (dropped 2026-09): nothing
-- read them. mailbox_stats also recomputed message_count with a join over
-- every stored message, when mailboxes.message_count has been maintained by
-- trigger since the counter column arrived; an operator who ran it on a large
-- database got a slow, redundant answer. The operator surface is the ops HTTP
-- API (readiness, metrics, and the stored-data views under /ops).
DROP VIEW IF EXISTS mailbox_stats;
DROP VIEW IF EXISTS message_priority_stats;

-- documents.visibility, feeds.visibility, collections.visibility (added
-- 2026-09): until this column existed any peer could read every document,
-- feed and collection. Rows predating it take the same defaults as new ones:
-- documents and collections become owner-only, feeds stay public. An owner
-- who meant a document to be read by others sets its visibility with the
-- ACCESS operation. The reader-list tables are created above; being new,
-- IF NOT EXISTS covers them.
ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS visibility SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS visibility SMALLINT NOT NULL DEFAULT 2;
ALTER TABLE collections
    ADD COLUMN IF NOT EXISTS visibility SMALLINT NOT NULL DEFAULT 0;

-- =============================================================================
-- PERMISSIONS
-- =============================================================================

-- Grant table permissions to ricochet user
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ricochet;

-- Grant sequence permissions (required for BIGSERIAL auto-increment columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ricochet;

-- Set default privileges for future objects created in public schema
-- This ensures new tables/sequences are automatically accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ricochet;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT USAGE, SELECT ON SEQUENCES TO ricochet;

-- Note: After running this schema, the ricochet user will have full access
-- to all tables, sequences, and views. This is required for the application
-- to function properly.
