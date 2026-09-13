# go-ricochet release audit — 2026-09-13

Audited at commit `65cfa9e` on `main` (clean tree), forge `df6668c`. Scope: security, performance and scalability, architecture and code quality, and every factual claim in `README.md`, `config.example.yaml`, `doc/`, and `deploy/`. Method: four independent full-source reviews (one per area), each finding then re-verified in source before inclusion here; plus `go build`, `go vet`, `staticcheck`, `govulncheck`, `gofmt`, `go test -race` for unit packages, and the full PostgreSQL-backed suite on a fresh database.

> **Status, 2026-09-13, after the fixes.** All eleven blockers below are fixed on `main`, one commit each and a full run of the suite on a fresh database after every one: B1 `3b4a500`, B2 `a31de16`, B3 `63410bc`, B4 `e03ee7e`, B5 `4cf5e7c` (with forge `ab6daeb`), B6 `a7e8481`, B7 `37050f5` (with forge `ea09be1`), B8 `a2e88ed` and `deea494`, B9 `04c834b`, B10 `7850bea`, B11 `bcb70e9`. The three sibling modules are tagged `v0.1.0` under Apache 2.0. The findings below the blocker line are still open. File and line references in this report are as of the audited commit and have moved since.

Full per-area findings, including everything checked and found sound, are in the appendices:

- [A. Security](security.md)
- [B. Performance and scalability](performance.md)
- [C. Architecture and code quality](architecture.md)
- [D. Documentation claims, all 104 rows (23 false, 27 partial, 52 true, 2 unverifiable)](claims.md)

## Verdict

**Not ready for public release.** The core is in better shape than the surface suggests: peer identity is taken from the authenticated stream everywhere, all SQL is parameterised, sequence assignment and document compare-and-swap are transactional, the suite is green and race-clean. But there are eleven items that would each on their own justify holding the release. Three of them let any peer damage another peer's data. One means the headline end-to-end encryption feature does not work through the server. Two make the repo unbuildable and unlicensed for an outside reader.

## Tooling results

| Check | Result |
|---|---|
| `go build ./...` | clean |
| `go vet ./...` | clean |
| `gofmt -l` | 5 files unformatted (`storage/models.go`, `presence/cache.go`, `registry/registry.go`, `postgres/collections.go`, `postgres/jsonb_filter_test.go`) |
| `staticcheck` (v0.8.1) | 5 hits: unused assignment `sfa/handler.go:228`, unused `generatePeerID` in `helpers_test.go:219`, three error strings ending in punctuation |
| `govulncheck` | **7 reachable advisories**: pgx v5.7.4 (GO-2026-5004, SQL injection via dollar-quote placeholder confusion, fixed 5.9.2), quic-go v0.59.0 (GO-2026-5676), webtransport-go v0.10.0 (GO-2026-6099), pion/dtls v3.1.1 (GO-2026-6165), pion/dtls v2 (GO-2026-4479, no fix), x/text v0.33.0 (GO-2026-5970), kad-dht (GO-2024-3218, no fix). Plus 9 in imported packages and 24 in required modules not reachable |
| `go test -race ./internal/... ./pkg/...` | all pass |
| Full suite on fresh DB | **292 pass, 0 fail** (`internal/...` + `test/integration`) |
| Second run, same DB | `TestSearchDirectory` fails (known isolation issue, not a product bug) |
| `go test -bench` | no benchmarks exist |

The pgx advisory is about the simple-protocol sanitizer; this code uses the default extended protocol with parameterised queries, so it is not exploitable here, but it will show up in every consumer's scan. Bump pgx anyway.

## Release blockers

Each of these was traced in source and independently re-verified. IDs in brackets point to the appendix entry.

### B1. Any peer can create a public mailbox in any other peer's namespace [A C-1]

`internal/protocol/maa/handler.go:205-208` infers the mailbox type from "caller is not the owner" and asks the MDA to retrieve, and `internal/mda/delivery.go:142` calls `GetOrCreateMailbox`, which inserts a new row when none exists (`postgres.go:96-126`). So a retrieve of `victim/inbox` before the victim has ever connected creates their inbox as **public**. Type is fixed at creation. Public mailboxes reject writes from anyone not on the ACL, so the victim can never receive mail there; anything that does land is world-readable. Retrieve must never create.

### B2. Any peer can delete or re-flag any message by ID [A H-1, C H1]

`handleMarkDelivered`, `handleUpdateFlags` and `handleDeleteMessages` (`maa/handler.go:262-351`) pass request message IDs straight to storage; the SQL (`postgres.go:307-313, 325-336, 376-390`) has no owner or mailbox scope. Message IDs are client-chosen and echoed in every acknowledgement, so a sender can delete a message after delivering it, and any reader of a shared or public mailbox can delete everyone else's messages. Only expunge checks the caller. `DeletedCount` also reports the request length, not rows affected.

### B3. Retrieval deletes messages before the response is written [A H-2, B C2]

`internal/mda/mailboxes/private.go:66-88` deletes every non-persistent message it just read, then returns. The MAA handler encodes and writes the response afterwards, through a writer that refuses frames over 10 MB and only logs the failure. The client sends no `maxMessages` by default, so the query has no `LIMIT`. Two 6 MB messages in a mailbox, or a client dropping mid-write, permanently loses the whole queue. `hasMore` is hard-coded false; no pagination exists. Delete on acknowledgement, cap the page by bytes, signal `hasMore`.

### B4. End-to-end encryption and compression do not survive the server [C H2]

The client sets `Message.Flags` (encrypted/compressed) correctly, but storage persists only the IMAP flags (`postgres.go:225-240` inserts `MsgFlags`) and `scanMessage` hard-codes `Flags: core.FlagNone` (`postgres.go:1094`). The client's decrypt branch (`pkg/client/client.go:440-461`) can therefore never fire on retrieval: an encrypted message comes back as ciphertext with `flags: 0`. No test covers `WithEncryption` or `WithCompression` end to end. The README's Security section describes a feature that does not work.

### B5. No resource limits at the network edge; 10 MB allocated per stream before admission [A H-3, B C1]

`../go-p2p-forge/host/host.go:89` installs `network.NullResourceManager`, no connection manager is configured, yamux allows unlimited incoming streams, and `HandleStream` sets no read deadline. The frame decoder trusts the 4-byte length prefix and allocates up to 10 MB before any body arrives, and admission control sits *after* that decode in every pipeline. One peer opening N streams and sending only a length prefix pins N × 10 MB and N goroutines indefinitely. `max_concurrent_connections` (10,000 / 50,000 in the presets) is read by nothing. Identity rotation is free, so per-peer limits do not help.

### B6. Any peer can fill any other peer's storage; server caps are not enforced [A H-4, H-5, M-4]

Delivery auto-creates a private mailbox for whatever folder path the *sender* supplies, unvalidated and unbounded in length. Folders per recipient are unbounded, `max_mailboxes` and `max_storage_bytes` are reporting-only, `persistent` and `expiryTimestamp` are sender-controlled, and private mailboxes are never subject to retention. `PublicMailbox.StoreMessage` has no message cap at all (`public.go:34-64`). The SFA handler auto-creates a *collaborative* feed in any owner's namespace on a non-owner append (`sfa/handler.go:216-243`), and the upsert never lets the owner make it private again. Clients can also set their own mailbox caps and retention to any value including negatives (`mma/handler.go:264-269, 547`).

### B7. Advertised security and capacity settings are inert [A H-6, C M7, D #25/#46/#48/#49/#50]

`enable_authentication`, `trusted_peers`, `max_mailboxes`, `max_concurrent_connections`, `worker_threads`, `enable_forwarding`, `connection_timeout`, `message_timeout`, `postgres.connect_timeout` exist in config and presets and are read by nothing outside `config.go` and the ops view. The README's preset table says production has "auth enabled", 10K connections, 100/min rate limits and 4–8 workers; none of those is true. Rate limits are off in every preset. Either implement or delete them before shipping a documented security knob that is a no-op.

### B8. The repo cannot be built or legally used from a clean clone [C H3, H5, H7]

`go.mod` has three `replace` directives to sibling directories; `go.sum` has zero entries for forge; the pinned forge pseudo-version is six commits behind what is actually compiled. The siblings are public on GitHub but untagged. There is no `LICENSE` file though the README links to one. `docker-build.sh:33-38` rsyncs the entire parent directory into the build image (in this environment that included a credentials file beside the checkout). `deploy/env.example:10` ships a real deployment IP. `deploy/run.sh` passes the DB password on argv and defaults production to `sslmode=disable`.

### B9. The public client library is unusable outside the module; README examples do not compile [C H6, D #31/#34/#36/#37/#44]

`pkg/client` returns and accepts `internal/core` and `internal/protocol/notify` types in eleven exported signatures. Five README snippets reference `core.PriorityUrgent`, `core.MsgFlagFlagged`, `core.MailboxPrivate`, `core.AccessReadWrite` and `notify.Notification`; an external module cannot compile any of them (verified). `RegisterNotificationHandler` cannot be called from outside at all. The fix is a `pkg/wire` package that both sides import.

### B10. Configuration fails unsafely [C H4, D #83/#105]

A config file with no `features:` section silently turns off relay, relay service, AutoNAT, metrics, push and presence (`config.go:782-795` assigns all eleven booleans unconditionally). Any config parse or validation error prints a warning and the server starts on the preset, discarding the whole file (`main.go:66-68`). The DSN is built by string formatting (`postgres.go:53`), so an empty password shifts the fields and `sslmode=disable` becomes the password; reproduced live.

### B11. Reachable dependency advisories

Seven advisories reachable from this code per govulncheck (table above). All but two have fixed versions available.

## Significant findings below the blocker line

**Security**
- `PatchDocument` evaluates If-Match outside the transaction; concurrent patches lose updates [A M-1, B H6].
- Registry announcements and presence events trust the JSON `server_id`, not the GossipSub signer; anyone can overwrite a server's entry or publish fake presence [A M-3].
- Raw Postgres error text is returned to clients in SCA, MMA and MSA responses [A M-6].
- Directory search builds an unbounded, unescaped `ILIKE '%q%'` that ignores the FTS index and logs the full SQL at Info [A M-6, B M3].
- Every per-peer limit is keyed by a free identity; nothing is IP-scoped [A M-7].
- Unbounded client strings persisted as index keys: folder path, content type, collection key, feed title, directory bio [A M-8].
- Documents, feeds, collections and directory are world-readable by design but the README never says so [A I-1].
- Encryption has no forward secrecy and the ciphertext is not bound to envelope fields, so a captured ciphertext can be replayed into another folder [A L-3].

**Performance**
- One message submission is 5 DB round-trips plus a `COUNT(*)` plus a trigger that updates the mailbox row a second time inside the same transaction; public mailboxes add two DELETEs with a sort per message [B H1].
- Every request emits 3–4 Info log lines on the hot path [B H2].
- Push notifier spawns an unbounded goroutine per message; joins a GossipSub topic per shared/public mailbox and never leaves; would dial once per message to an offline peer, except the presence monitor is wired *after* the notifier and is therefore always nil [B H3, C M2].
- Presence marks **every still-connected peer offline after 120 s** because the timestamp is only written at connect (`presence/service.go:103,152,330-341`). Heartbeat serialises every peer ID and hits the 1 MiB pubsub cap around 18k peers [B H4].
- All handlers except SDA call storage with `context.Background()` and no timeout; a slow Postgres holds every admission slot indefinitely [B H5].
- `GetOrCreateMailbox` is check-then-insert; concurrent first deliveries fail with a unique violation [B H6].
- Graceful shutdown drains nothing: handlers are registered in a way forge's `Stop` does not track, then storage is closed under in-flight queries [B M8, C M1].
- HISTORY and HEAD load full document bodies to report sizes; SCA QUERY runs the filter twice with OFFSET paging; 5-minute full-table aggregate scans [B M1, M2, M4].
- Mailbox cache is unbounded and stale after `updateConfig`; retention maintenance only runs for cached mailboxes [B M5].

**Architecture**
- Authorization is re-implemented in 11 places with 4 different response shapes; the README's MSA→MTA→MDA layering holds only for submit [C M3, M4].
- Four parallel status-code tables; wire structs duplicated by hand between client and server [C M5, L5].
- Zero unit tests for any of the 6 protocol handlers, the mailbox types, registry or storage interface; all authorization paths are covered only by integration tests that skip without a DSN; no CI [C M8].
- Client `Delete*` methods return `(true, nil)` on 403/500; `PatchDocument` never checks failure; no `Close()`; no failover past the first preferred server [C M9].
- No second-signal exit; `isRunning` is a racy plain bool; partial `Start` failure leaks the pool [C M10].
- Dead code and schema: `block_store` and two views, `mta.Router.HandleRetrieve`, `EnforceFeedRetention`, `HighCapacityConfig`, `ConnectionURI`, unused `yaml:` tags on `ServerConfig` [C L3, M7].

**Documentation** (Appendix D has all 104 rows)
- Quick-start DB setup fails on a fresh server: the schema GRANTs to a role the README never creates.
- Wrong Go version, placeholder project link, `internal/p2p/` listed but gone, test counts off by 2–4×, identity file name wrong.
- UDX described as "unreliable datagram transport"; it is reliable and congestion-controlled.
- "Health monitoring" does not exist; uptime score is a constant 1.0.
- `doc/SUPERVISORD_DEPLOYMENT.md` says to `dart compile` the server, references three nonexistent files, the wrong port, and a memory-limit env var nothing reads.
- The Debian package installs `config.example.yaml` verbatim, with a `YOUR_PUBLIC_IP` placeholder and a pool size of 10 against a code default of 25.
- Bench docs: `mixed` skips SCA, MMA is never benchmarked, five scenarios undocumented, sample output is 70× below the recorded baseline.

## What is sound

Worth knowing so it is not re-audited:

- Caller identity everywhere is `s.Conn().RemotePeer()` from the Noise-authenticated stream; no handler trusts a JSON peer field.
- Sender must equal stream peer for submit and batch submit, checked twice.
- Mailbox read and write ACLs for private, shared and public are correct once the mailbox exists with the right type.
- MMA operations are all scoped to the caller's own mailboxes; SDA, SFA, SCA writes are owner-only from the stream peer.
- Path validation is a strict allowlist; every SQL value is parameterised; dynamic sort/filter keys are regex-whitelisted.
- Sequence assignment is `UPDATE … RETURNING` in the insert transaction; `PutDocument` and `PutCollectionItem` compare-and-swap under `FOR UPDATE`.
- Frame and batch caps are enforced both directions; list limits are clamped.
- Admission controller and token buckets are sharded, bounded and evict idle state; metrics never use peer ID as a label and a test enforces it.
- Identity file is written 0600 in a 0700 dir; seed and DSN password are never logged.
- Ops surface is loopback-only, read-only, and hides DB errors.
- NaCl box primitives and the Ed25519→X25519 derivation are the standard libsodium-compatible maps; nonces come from `crypto/rand`.
- Indexes cover every hot query except directory search.

## Recommended order of work

1. **Data-integrity fixes with end-to-end tests** (B1, B2, B3, B4). Retrieve never creates; owner-scope the three message-ID mutations; delete on acknowledgement with a byte-capped page and `hasMore`; persist `Message.Flags` and add an encrypted round-trip test.
2. **Bound the edge** (B5). Real resource manager and connection manager in forge, stream read/write deadlines, yamux stream cap, admission before frame allocation.
3. **Enforce or delete every advertised limit** (B6, B7). Validate folder path on delivery; cap folders and bytes per owner; clamp client-set caps and retention; apply retention to private mailboxes; add the cap to public mailbox stores; remove non-owner auto-create from SFA; then delete the inert config fields or implement them.
4. **Make it publishable** (B8, B9, B11). Tag the three siblings, drop the replaces, commit `go.sum`, add a LICENSE, strip the parent-directory rsync and the live IP, bump pgx and the other advisories, create `pkg/wire` and re-export constants so the README examples compile, add a minimal CI.
5. **Config safety** (B10). Tri-state feature flags, fatal config errors, DSN built from a parsed config.
6. **Then** the request lifecycle (context and timeouts, drain), the submit path, the notifier and presence bugs, logging levels, and a README rewrite driven by Appendix D.
