# Appendix A: Security audit (full findings)

Part of the [release audit of 2026-09-13](README.md). Audited at commit `65cfa9e` (forge `df6668c`). `R/` = this repo, `F/` = `../go-p2p-forge`. "Confirmed" means the exploit path is fully visible in code; "Suspected" means a runtime test is needed to confirm magnitude.

**Headline:** peer identity is taken from the Noise-authenticated stream everywhere (`F/pipeline.go:48` `PeerID: s.Conn().RemotePeer()`), and every SQL statement is parameterised. The serious problems are authorization gaps *after* identity is known (four operations act on objects the caller does not own), a cross-peer retrieve that creates public mailboxes in other peers' namespaces, a destructive-read path that loses mail, and a networking layer with no resource limits at all. The `enable_authentication` flag that the README advertises as "on" in production gates nothing.

---

## Critical

### C-1. Cross-peer MAA `retrieve` creates a PUBLIC mailbox in the victim's namespace (mailbox squatting / inbox hijack)
- `R/internal/protocol/maa/handler.go:205-208` — `isOwnMailbox := req.PeerID == callerID.String()`; if false, `mailboxType = core.MailboxPublic`. `req.PeerID` and `req.FolderPath` are attacker-supplied.
- `handler.go:236` → `mda.Retrieve` (`R/internal/mda/delivery.go:221`) → `getMailbox` → `R/internal/mda/delivery.go:142` `s.Storage.GetOrCreateMailbox(ctx, addr, ...)` → `R/internal/storage/postgres/postgres.go:96-126` inserts a new row with `int(addr.Type)` = 2 (public) when `(owner, folder)` does not exist.
- What it does: any peer can *create* `victim/<anyFolder>` as a **public** mailbox owned by the victim. Type is fixed at creation (`GetOrCreateMailbox` returns the existing record on later calls).
- Exploit: (a) before a new user ever connects, attacker retrieves `victim/inbox` → victim's inbox is created public. `PublicMailbox.StoreMessage` (`R/internal/mda/mailboxes/public.go:42`) then rejects every sender who is not the owner or on the ACL, so the victim can never receive mail until they discover the problem and `deleteMailbox`; anything that *is* stored there is world-readable (`public.go:67` has no reader check). (b) Attacker creates unlimited rows in anyone's namespace (`max_mailboxes` is never enforced, see H-6). The doc `R/doc/MAILBOX_LIFECYCLE_AND_ACLS.md:42` only acknowledges the *delivery* auto-create as private; the retrieve auto-create as public is undocumented.
- Confidence: Confirmed.
- Fix: retrieve must never create. Use `FindMailbox` and return 404 when absent; if you keep the "read a public mailbox" path, resolve the stored type from the record rather than inferring it from caller≠owner.

---

## High

### H-1. `deleteMessages`, `updateFlags`, `markDelivered` have no ownership check — any peer can delete or re-flag any message by ID
- `R/internal/protocol/maa/handler.go:266` `MarkMessagesDelivered(ctx, req.MessageIDs)`, `:285` `UpdateMessageFlags(ctx, req.MessageID, ...)`, `:343` `DeleteMessages(ctx, req.MessageIDs)` — none consult `sc.PeerID`.
- Storage is global by ID: `R/internal/storage/postgres/postgres.go:307-313` `DELETE FROM stored_messages WHERE message_id = ANY($1)`, `:325-336`, `:376-390` — no `mailbox_id`/owner scoping.
- `message_id` is client-chosen (`R/internal/core/message.go:20`, decoded verbatim by `MessageFromJSON`) and globally unique (`R/schema.sql:47`).
- Exploit: a sender retracts/deletes a message after delivery (they chose its ID); anyone with read access to a shared/public mailbox learns every message ID and can delete other senders' messages or set `\Deleted`/`\Seen` on them; message IDs leaked through logs (`msa/handler.go` logs `message_id` at Info) or guessed are deletable. Only expunge (`handler.go:305`) checks the caller.
- Confidence: Confirmed.
- Fix: scope every mutation to mailboxes the caller owns: `... WHERE message_id = ANY($1) AND mailbox_id IN (SELECT id FROM mailboxes WHERE owner_peer_id = $2)`; for shared mailboxes decide whether readers may flag (probably only `\Seen` via the cursor, not delete).

### H-2. Private-mailbox retrieve deletes messages before the response is written; oversized responses are dropped → remote-triggerable permanent mail loss
- `R/internal/mda/mailboxes/private.go:56-88`: `RetrieveMessages` fetches, then at `:80` `DeleteMessages(ctx, toDelete)` for every non-persistent message, then returns.
- `R/internal/protocol/maa/handler.go:236-256`: response is built after that, and written by `maaResponseWriter` at `:95` via `codec.WriteFrame`, which refuses anything over 10 MB (`F/codec/frame.go:46-49`) and merely logs the error.
- Client default sends no `maxMessages` (`R/pkg/client/options.go:89`, nil unless `WithMaxMessages`), so `RetrieveMessages` (`postgres.go:280`) runs with no `LIMIT`.
- Exploit: attacker sends the victim two ~6 MB messages (allowed: `MaxPayloadSize` 10 MB, `mta/router.go:110`). Victim's next retrieve: all queued messages (including legitimate ones) are deleted, the 12 MB compound frame fails `WriteFrame`, the victim receives nothing. Same outcome if the client disconnects mid-write. The `markDelivered` operation exists but private mailboxes ignore it.
- Confidence: Confirmed.
- Fix: never delete on read. Delete on `markDelivered`/explicit ack (or on the *next* retrieve with `fromSequence` past them), and cap the response by bytes (stop appending when the compound blob would exceed the frame limit, set `hasMore=true`).

### H-3. No libp2p resource limits, unlimited yamux streams, no stream read deadline, attacker-sized buffer allocation → trivial memory/goroutine exhaustion
- `F/host/host.go:89` `libp2p.ResourceManager(&network.NullResourceManager{})`; no `ConnManager` option → go-libp2p falls back to `NullConnMgr` (`go-libp2p@v0.47.0/defaults.go:192`). go-libp2p's yamux transport sets `MaxIncomingStreams = math.MaxUint32` (`p2p/muxer/yamux/transport.go:32`).
- `F/pipeline.go:39-61` `HandleStream` sets no read deadline; `F/codec_middleware.go:13` → `F/codec/frame.go:66-87` `ReadFramePooled` reads a 4-byte length (attacker-controlled, up to 10 MB), then `pool.Get(int(length))` — for >1 MB this is `make([]byte, size)` (`F/codec/pool.go:101-105`) — and blocks in `io.ReadFull` until the peer sends the bytes.
- Admission control runs *after* frame decode (`R/internal/protocol/*/handler.go` pipeline order), so a stream stalled in decode holds no admission slot and is uncounted.
- Exploit: one peer opens N streams, sends the length prefix `0x00A00000` and nothing else: N × 10 MB pinned allocations and N parked goroutines, forever (yamux keepalive only detects dead *connections*). Rotating peer identities is free, so no per-peer counter would help even if one existed. `max_concurrent_connections` in config is never read (see H-6).
- Confidence: Confirmed (allocation and blocking are unconditional in code); exact crash threshold Suspected.
- Fix: replace `NullResourceManager` with the default `rcmgr` with a scaling limiter and a `connmgr.BasicConnMgr`; set a `Stream.SetReadDeadline` in `HandleStream` (or a `context.WithTimeout` honoured by the decoder); read the frame body incrementally into a growing buffer rather than pre-allocating the declared size.

### H-4. Any peer can fill any other peer's storage indefinitely; server storage caps are not enforced
- Delivery auto-creates a private mailbox for whatever `folderPath` the *sender* supplies (`R/internal/mda/delivery.go:182-195`), with no `validateFolderPath` call and no length bound (up to the 10 MB frame).
- Per mailbox the cap is `MaxMessages` (default 1000) × 10 MB; folders per recipient are unbounded; `MaxMailboxes` and `MaxStorageBytes` are read by nothing but reporting (grep: only `capacity/`, `opsview/`, `metrics/`).
- `Persistent` and `ExpiryTimestamp` are sender-controlled (`R/internal/core/message.go:32`, `:58`); private mailboxes are never subject to `retention_days` — `EnforceRetentionPolicy` is invoked only for public mailboxes (`delivery.go:302-317`, `public.go:57`) — so a message with expiry in year 2200 and `persistent=true` stays until the recipient deletes it.
- `PublicMailbox.StoreMessage` (`public.go:51`) has **no** `MaxMessages` check at all (compare `private.go:45`), so a public-mailbox owner can store without limit.
- Exploit: (a) blanket: pick random peer IDs, send 10 MB messages to `victim/f1…fN`; (b) targeted: send 1000 tiny persistent, non-expiring messages to `victim/inbox` → every legitimate sender gets 507 "mailbox full" until the victim finds and deletes them. Rate limits are off by default (`R/internal/core/config.go:320-337`).
- Confidence: Confirmed.
- Fix: validate/bound `folderPath` on delivery; enforce `MaxMailboxes` per owner and a per-owner byte budget; clamp `ExpiryTimestamp` to `RetentionPolicy`; do not let *senders* set `persistent` on someone else's private mailbox (or make it recipient-controlled); apply retention to private mailboxes; add the capacity check to `PublicMailbox.StoreMessage`; refuse deliveries when the sampler says the server is over `MaxStorageBytes`.

### H-5. SFA: non-owner `APPEND` auto-creates a *collaborative* feed in any owner's namespace and appends unbounded content; feed retention never runs
- `R/internal/protocol/sfa/handler.go:216-243`: when `callerID != ownerID` and the feed is missing, `:228` `store.CreateFeed(ctx, ownerID, req.Path, "", "", true)` then the append proceeds (`:473`).
- `CreateFeed` upsert (`R/internal/storage/postgres/feeds.go:29-50`) deliberately never changes `collaborative_mode` on conflict, so once squatted the owner's own `CREATE` cannot make it private; they must `DELETE` (and the attacker can immediately re-create it collaborative).
- Entry size is bounded only by the 10 MB frame; `EnforceFeedRetention` (`feeds.go:339`) has no callers; no per-feed entry cap.
- Exploit: create `victim/<any path>` for any victim, fill it with 10 MB entries; readers of `victim`'s feed list (`LIST` is public) see attacker-created feeds under the victim's identity.
- Confidence: Confirmed.
- Fix: remove auto-create for non-owners (404 instead); require the owner to create collaborative feeds explicitly; cap entry size and entries per feed; call `EnforceFeedRetention` from maintenance.

### H-6. `enable_authentication`, `trusted_peers`, `max_mailboxes`, `max_concurrent_connections`, `max_storage_bytes` (as a limit), `enable_forwarding`, `worker_threads` are inert
- These fields exist only in `R/internal/core/config.go` (struct, presets at `:477`/`:487`, YAML loader). A repo-wide grep finds no reader outside reporting code.
- `R/README.md:261` advertises `Authentication | off | off | on | on` across presets and `R/config.example.yaml:118` exposes `enable_authentication`. Operators will believe production has an authentication gate and a connection cap. There is no peer allow-list, no per-peer connection limit, no mailbox count limit.
- Confidence: Confirmed.
- Fix: either implement them (a `TrustedPeers` gater via `libp2p.ConnectionGater` when `EnableAuthentication`, `MaxMailboxes` check in `GetOrCreateMailbox`, connmgr from `MaxConcurrentConnections`) or delete them from config/README before release. Do not ship a documented security knob that is a no-op.

---

## Medium

### M-1. `PatchDocument` evaluates `If-Match` outside the transaction (TOCTOU lost update)
- `R/internal/storage/postgres/postgres.go:622-650`: `GetDocument` (no lock) → compare at `:631` → `PutDocument(..., ifMatch=nil)` at `:649`. The comment on `PutDocument` (`:524-528`) explains this exact race for PUT; PATCH still has it.
- Exploit: two devices of the same owner PATCH concurrently; both pass the check; one merge is silently discarded (ETag CAS is the README-advertised concurrency control).
- Confidence: Confirmed.
- Fix: do the read+merge inside the same `FOR UPDATE` transaction as `PutDocument`, or pass `ifMatch` through and re-check under the lock.

### M-2. Unbounded server-side growth driven by peers: GossipSub topics never left, mailbox cache never evicted, registry map never pruned
- `R/internal/mda/notifier.go:99` joins `/ricochet/mailbox/<owner>/<folder>` per shared/public delivery; `F/node/node.go:153-191` has `JoinTopic` but no leave. Any peer can create public mailboxes with arbitrary names for itself (or, per C-1, for others) and deliver to them → one permanent topic+subscription+goroutine each.
- `R/internal/mda/delivery.go:162` `mailboxCache[key] = mb` grows one entry per mailbox ever touched; nothing evicts (and cached records go stale after `updateConfig`).
- `R/internal/registry/registry.go:228` `r.servers[info.ServerID.String()] = &info` — one entry per *claimed* server ID from any signed publisher; entries are never deleted, only filtered as stale on read.
- Confidence: Confirmed.
- Fix: leave topics after publish (or use a single notification topic with the mailbox in the payload); bound the mailbox cache (LRU) or drop it; prune stale registry entries and cap the map.

### M-3. Registry announcements and presence events are spoofable: body `server_id` is trusted, signer is not checked
- GossipSub uses `StrictSign` (`F/node/node.go:124`), so `msg.GetFrom()` is authenticated — but `R/internal/registry/registry.go:214-228` only skips `msg.ReceivedFrom == self` and then stores whatever `server_id`/`uptime_score`/`port`/`regions` the JSON claims. `R/internal/presence/tracker.go:129` processes any message on `/sf-network/presence/<server>` without checking it came from `<server>`.
- Exploit: any peer publishes `{server_id:<real server>, port:1, regions:[...], uptime_score:1e9}` and overwrites the real entry; any peer publishes fake online/offline events for a server's presence topic. Server-side impact today is only stats (nothing consumes `GetAvailableServers`), but this is the wire protocol Dart clients implement.
- Confidence: Confirmed (code); client impact Suspected.
- Fix: register a pubsub topic validator that rejects messages where `msg.GetFrom() != claimed server ID`, and in the presence tracker require `msg.GetFrom() == serverID`.

### M-4. Clients set their own mailbox caps and retention with no bounds; server-wide `max_messages_per_mailbox` is only a default
- `R/internal/protocol/mma/handler.go:264` `maxMessages = *req.MaxMessages`, `:269` `retentionDays = *req.RetentionDays`, `:547` (updateConfig) — no min/max. `max_messages_per_mailbox` in config validates `> 0` for the *default* only.
- Exploit: own mailbox `maxMessages = 2_000_000_000` → per-mailbox cap bypassed (combined with H-4, unlimited storage per owner). `retentionDays = -5` → `INTERVAL '1 day' * -5` in `EnforceRetentionPolicy` (`postgres.go:1006`) deletes everything created before now+5d. Own-data only, but it defeats the operator's capacity model.
- Confidence: Confirmed.
- Fix: clamp client values to `[1, config.MaxMessagesPerMailbox]` and `[1, config.RetentionDays]`.

### M-5. Database password on the command line; env file permissions unmanaged; production deploy defaults to `sslmode=disable`
- `R/deploy/run.sh:35` `--pg-password "$DB_PASSWORD"` → visible to every local user via `ps`/`/proc/<pid>/cmdline` for the process lifetime. `R/deploy/debian/postinst` sets 640 on `config.yaml` but never creates or chmods `/etc/ricochet/env`. `run.sh:36` passes `${DB_SSLMODE:-disable}`, overriding the `--production` preset's `require` (`config.go:422`).
- Confidence: Confirmed.
- Fix: read the password from an env var (`RICOCHET_PG_PASSWORD`) or `config.yaml` (already 640) instead of a flag; have `postinst` create `/etc/ricochet/env` 640 root:ricochet; default SSL to `require`.

### M-6. Client-controllable expensive queries and raw Postgres errors returned to clients
- Directory browse: `postgres.go:857` `likePattern := "%" + query + "%"` with no length cap and `%`/`_` unescaped, ILIKE over two columns (the GIN FTS index at `schema.sql:174` is not used by ILIKE) → full scan per request; the query is also logged at Info with the user string (`:911`).
- `QueryCollection` (`collections.go:263-344`) runs a `COUNT(*)` over the filter plus the page; `offset` is unbounded (`sca/handler.go:596`, `:656`); `$ilike` and `::numeric` casts on JSONB force sequential scans; a non-numeric field makes the cast throw and the raw Postgres error text is sent back at `sca/handler.go:667` (`"Error": err.Error()`), likewise `:194`/`:419`, and `mma/handler.go:274` (`failed to create mailbox: %v`), and MSA acks carry `err.Error()` from storage.
- MAA retrieve with no `maxMessages` selects the whole mailbox; SDA `HISTORY` without `maxVersions` returns all versions *with content*.
- Admission bounds concurrency (default 100 in flight) but not cost, so 100 concurrent scans are always admitted.
- Confidence: Confirmed (queries); cost magnitude Suspected.
- Fix: cap `directoryQuery` length (~64) and escape `%`/`_`; cap `offset`; drop the `COUNT(*)` or bound it; default `maxMessages` (e.g. 100) and `maxVersions`; return generic error strings and keep Postgres text in logs.

### M-7. All per-peer limits are per *free* identity
- Rate limiters (`F/middleware/tokenbucket.go:152`), admission per-peer slots (`R/internal/admission/controller.go:230`), reader cursors, ACL rows, relay reservations-per-peer are all keyed by peer ID. Generating a new Ed25519 identity costs nothing, so every per-peer limit is bypassable by rotation; the only non-rotatable dimensions are IP/ASN, which only the relay service uses.
- Confidence: Confirmed.
- Fix: add IP-scoped limits at the connection layer (rcmgr `LimitsPerIP`/connmgr), and treat per-peer limits as fairness controls, not abuse controls.

### M-8. Unbounded client strings persisted as keys
- `folderPath` in delivery (`delivery.go:182`) and retrieve (`maa/handler.go:210`) skips `validateFolderPath`; document `Content-Type` (`sda/handler.go:356`), collection `key`, directory `displayName`/`bio`/`extras`, feed `title`/`description`/`entryType` have no length limits (only the 10 MB frame). Multi-MB folder names become index keys in `uq_mailbox_owner_folder`.
- Confidence: Confirmed.
- Fix: apply the same 256-char/charset rule the document path already uses to every stored identifier; cap free-text fields.

---

## Low

### L-1. Client-chosen `message_id` with a global UNIQUE constraint enables delivery denial and leaks schema text
- `schema.sql:47`; insert failure surfaces as `deliver message: store message: ERROR: duplicate key value violates unique constraint "uq_message_id"` in the StoreAck. A sender who learns a message ID another sender is about to use (e.g. deterministic IDs in a client) can pre-insert it. Fix: server-assigned IDs or `(mailbox_id, message_id)` uniqueness; generic errors.

### L-2. Open (limited) relay by default, without rcmgr
- `R/internal/core/config.go:450-451` enables the relay service; `F/host/host.go:138-147` uses relay v2 defaults (128 reservations, 16 circuits, 1 reservation/peer, 8/IP, 128 KB / 2 min per circuit — `go-libp2p@v0.47.0/p2p/protocol/circuitv2/relay/resources.go:46-68`). This is the standard "limited relay" and bandwidth exposure is small, but `ForceReachabilityPublic` plus `NullResourceManager` means relay streams are not otherwise accounted. Fix: keep the limits explicit in `config.example.yaml`, and re-enable rcmgr (H-3).

### L-3. Encryption: sound primitives, but no forward secrecy, no metadata binding, cross-protocol key reuse
- `R/pkg/client/encryption.go`: Ed25519→X25519 private conversion (`:23-32`, SHA-512(seed) clamped) and public conversion (`:36-45`, `BytesMontgomery`) are the standard libsodium-compatible maps; nonce is 24 bytes from `crypto/rand` (`:100`); recipient key is extracted from the self-certifying peer ID (`:66-76`), so it is an authenticated source; `math/rand` appears only in `cmd/ricochet-bench`. Server never sees plaintext (no server-side decrypt/decompress anywhere; `pkg/client/compression.go` decompression is client-only and bounded at 16 MB).
- Gaps: static-static NaCl box → compromise of either long-term key decrypts all history; the same key signs Noise handshakes/GossipSub and does DH; the ciphertext is not bound to `messageId`/recipient/folder/timestamp/flags, so the server (or anyone who can insert into a mailbox) can replay a captured ciphertext into another folder or after deletion, and can strip/set the `encrypted` flag (DoS only). Box gives sender-authentication only to the recipient (deniable), which is fine for messaging but should be stated. Fix: document the model; consider a per-message ephemeral key (X3DH-style) and AEAD-associated-data over the envelope fields.

### L-4. Attacker-controlled strings logged at Info; per-message Info logs
- `postgres.go:911` logs the full SQL and args of every directory browse; `msa/handler.go` logs every submission at Info. Log-volume DoS and log injection of newlines. Fix: Debug level, and don't log raw user strings.

### L-5. Admission slots and DB calls have no timeouts on the mailbox protocols
- `R/internal/admission/middleware.go:18` passes `sc.Ctx`, which is `context.Background()` (`F/pipeline.go:46`), and MSA/MAA/MMA handlers use `context.Background()` for storage; a slow database keeps slots held indefinitely (SDA/SFA/SCA at least use 30–60 s timeouts). Fix: derive a per-request context with a deadline in `HandleStream`.

### L-6. Peer-ID string comparisons are fail-closed but the raw client string is persisted
- `recipient_peer_id` is stored verbatim from the client (`postgres.go:235`), while owners are normalised via `peer.Decode().String()`. A CIDv1-encoded peer ID is rejected by `PrivateMailbox.StoreMessage` (`private.go:35` string compare). Harmless today; normalise before comparing/storing.

### L-7. Cross-peer public retrieve records a reader cursor for any identity
- `public.go:77-88` upserts `reader_cursors` for every reader; combined with M-7 this is unbounded row growth. Fix: only track cursors for ACL'd readers, or expire them.

---

## Info

- **I-1 World-readable stores are by design but undocumented in the README.** Documents (`GET`/`HEAD`/`LIST`/`HISTORY`), feeds, collections (`GET`/`LIST`/`QUERY`) and directory listings are readable by any peer; `R/doc/DOCUMENT_STORE_MVP_PROPOSAL.md:119,935-940` states "public-read". The README's "Mailbox Management — Private, shared, and public mailboxes with ACL-based access control" says nothing about the stores. State it prominently before release.
- **I-2 Operator surface.** `R/internal/opsapi/server.go` binds `127.0.0.1:9090` by default, has no auth, pprof off by default, GET/HEAD only; `/ops/*` exposes peer IDs, folder names, counts and bytes but no payloads; `/metrics` has no peer labels (`R/internal/metrics/metrics.go:10-16`); DB errors are kept out of responses (`opsview.go:511-519`). Sound as long as `ops.bind` stays loopback — document that changing it exposes everything without authentication.
- **I-3 go.mod will not build for the public.** `R/go.mod:5-10` has `replace` directives to `../go-p2p-forge`, `../go-udx`, `../go-libp2p-udx-transport` (the `sed` in `docker-build.sh:49-53` is a no-op rewrite of the same paths). Publish those modules and pin tagged versions. The `go-libp2p-kad-dht => v0.37.1` pin is fine.
- **I-4 Dependencies**: see the govulncheck results in the main report (7 reachable advisories).
- **I-5 Secrets handling that is sound:** identity file written 0600 inside a 0700 dir (`F/host/identity.go:37-46`); `RICOCHET_SEED_HEX` is decoded and used directly, never logged (`R/internal/server/server.go:560-566`); the DSN is built at `postgres.go:53` and never logged (only host/port/db at `:73`); pgx redacts the password in `ParseConfigError` (`pgconn/errors.go:115-119`); `opsview` never marshals the config struct. The seed stays in the process environment for its lifetime (readable by same-uid/root) — acceptable.
- **I-6 Deploy:** runs as the unprivileged `ricochet` user under supervisord; `Dockerfile.build` is build-only (non-root `builder`); nothing bakes secrets into the package.

---

## Checked and found sound

- **Identity source:** `sc.PeerID` is `s.Conn().RemotePeer()` (`F/pipeline.go:48`), i.e. the Noise-authenticated peer; no handler derives the caller from JSON.
- **Message submission:** sender must equal the stream peer in single and batch submit (`msa/handler.go:166`, `:250`) and again in the MTA (`mta/router.go:95`); payload ≤ 10 MB (`:110`); hop count ≤ 10; expired messages refused. A cannot send as B.
- **Mailbox write ACLs:** shared/public mailboxes require owner or `writeOnly`/`readWrite` grant (`shared.go:36-49`, `public.go:34-48`); private mailboxes accept only their owner as recipient. **Read ACLs:** private read requires owner (`private.go:62`); shared read requires owner or `readOnly`/`readWrite` (`shared.go:73-84`).
- **MMA:** every operation looks up the mailbox with `callerID` (`FindMailbox(ctx, callerID, ...)`), so create/delete/grant/revoke/listACL/updateConfig/getMailboxInfo cannot touch another owner's mailbox; `verifyOwner` is fail-closed. `queryCapacity` is intentionally open and returns only aggregates.
- **SDA/SFA/SCA writes:** owner-only enforced from `sc.PeerID` before dispatch (`sda/handler.go:276-286`, `sfa/handler.go:216`, `sca/handler.go:202-206`), including `BATCH_PUT` and directory `join`/`leave`; `updated_by_peer_id` is the stream peer, not a request field.
- **Path validation:** `^[a-zA-Z0-9\-_/]+$`, ≤256, no leading `/`, no `..` (dots aren't even permitted), applied to document, feed, collection and batch paths.
- **SQL injection:** all values are parameterised; dynamic pieces are `ORDER BY` from a fixed set (`operator.go:32-36`), histogram edges compiled in (`stats.go:45-54`), and JSONB keys/sort fields validated by `^[a-zA-Z0-9_.\-]+$` before interpolation inside single quotes (`jsonb_filter.go:11,76`, `collections.go:286`) — no quote or backslash can pass.
- **Concurrency/CAS:** sequence numbers assigned via `UPDATE … RETURNING` in the insert transaction (`postgres.go:207-251`, `feeds.go:127-133`); `PutDocument` and `PutCollectionItem` compare-and-swap under `SELECT … FOR UPDATE` (`postgres.go:555-574`, `collections.go:125-164`).
- **Cascade semantics:** `ON DELETE CASCADE` from mailboxes → messages/ACLs/cursors, documents → versions, feeds → entries, collections → items; only owners can delete parents, so cascades are owner-scoped.
- **Framing and batch caps:** 10 MB frame both directions, zero-length frames rejected; batch submit ≤100 messages; `BATCH_PUT` ≤100 docs / 6 MB; `BATCH_GET` ≤50 feeds with ≤10 concurrent DB reads; list limits clamped (documents 5000, keys/query 1000, feed entries 1000, directory 100, operator views 500). JSON decoding uses `encoding/json` on bounded buffers (depth-limited by the stdlib).
- **Rate limiters** are keyed by peer ID, refill lazily, and evict idle entries (`F/middleware/tokenbucket.go:233-251`) — bounded memory. Admission control is on by default (4× pool size global, 64 per peer, 5 s acquire timeout).
- **GossipSub** uses `StrictSign`; the server signs everything it publishes.
- **Expiry sweep** is batched (5000 × 200) and cancellable.
- **Ops surface** is loopback by default, read-only, and hides DB errors.
- **No server-side decompression** exists (no LZ4 bomb surface on the server).

## Could not determine

1. Whether the Dart clients (`overnode_v2`, `dart-libp2p`) send `maxMessages` on retrieve or rely on `markDelivered` — this decides how often H-2 triggers in practice.
2. Real memory/time-to-crash for H-3 and the CPU cost of M-6 ILIKE/JSONB scans at production data sizes — needs a load test.
3. Whether the UDX transport (`../go-udx`, `../go-libp2p-udx-transport`) has its own connection/handshake flood protections; it was out of scope and, given `NullResourceManager`, is the only thing standing between the internet and the yamux layer.
4. Whether yamux's `ConnectionWriteTimeout` (10 s) ever tears down a connection whose streams are merely idle (probably not — it fires only on blocked writes), which determines whether the H-3 slow-stream variant is permanent or 10 s-bounded.
5. Whether public-mailbox reads are meant to be world-readable *by anyone* or only by peers who know the path; the code makes them the former, and C-1 lets anyone create them.
