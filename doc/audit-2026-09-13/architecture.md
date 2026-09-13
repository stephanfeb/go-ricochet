# Appendix C: Architecture and code quality audit (full findings)

Part of the [release audit of 2026-09-13](README.md). Audited at commit `65cfa9e` (forge `df6668c`).

## Mechanical checks

- `go vet ./...` — clean.
- `gofmt -l` — five files unformatted: `internal/storage/models.go`, `internal/presence/cache.go`, `internal/registry/registry.go`, `internal/storage/postgres/collections.go`, `internal/storage/postgres/jsonb_filter_test.go` (alignment/import order only).
- `go test -race -count=1 ./internal/... ./pkg/...` — all pass, race-clean.
- `.claude/`: nothing committed; `worktrees/` is gitignored.
- Test counts (`go test -list`): **198 unit + 95 integration = 293** `Test*` functions. README claims 46 / 21.

---

## HIGH

**H1 — MAA message-ID operations have no ownership check** (see Appendix A H-1). Also `maa:346-347` reports `DeletedCount: len(req.MessageIDs)` regardless of rows affected.

**H2 — Encryption/compression flags are silently dropped; a retrieved encrypted message is returned as ciphertext with `flags: 0`**
`core.Message.Flags` (`internal/core/message.go:27`, the `SFMessageFlags` encrypted/compressed bits) is never persisted: `postgres.go:225-240` inserts `uint32(msg.MsgFlags)` (IMAP flags) into the only flags column (`schema.sql:43 flags_bitmap`), and `scanMessage` hard-codes `Flags: core.FlagNone` (`postgres.go:1094`). The client's decrypt/decompress branch (`pkg/client/client.go:440-461`) therefore can never fire. README advertises both features (`README.md:265-275`). No integration test covers `WithEncryption`/`WithCompression` (only `pkg/client/*_test.go` unit-tests the primitives).
*Fix:* add an `sf_flags INTEGER` column, persist `msg.Flags`, and add an end-to-end test.

**H3 — Not buildable from a clean clone; committed go.mod does not describe what is actually built**
`go.mod:7-9` replaces three modules with `../go-libp2p-udx-transport`, `../go-udx`, `../go-p2p-forge`. `go.sum` has **zero** entries for `twostack/go-p2p-forge`. The required pseudo-version `v0.0.0-20260227112649-b6b924931356` (`go.mod:23`) is commit `b6b9249` (2026-02-27); the local forge HEAD `df6668c` is **6 commits ahead** and is what the binary is really compiled against. All three siblings are **public on GitHub** (`twostack/go-p2p-forge`, `stephanfeb/go-udx`, `stephanfeb/go-libp2p-udx-transport`) and in sync with `origin/main`, but **none has a tag**. `go-ricochet` itself is currently **private**. `Dockerfile.build` never COPYs the siblings; the build only works because `docker-build.sh:33-38` rsyncs the *entire* parent directory (see H7). kad-dht: forge's own `go.mod` now also requires `v0.37.1`, so the replace may be droppable.
*Fix:* tag the three siblings, `go get` them by version, delete the replaces, commit a real `go.sum`.

**H4 — A config file with no `features:` section silently turns off relay, relay service, AutoNAT, metrics, push, presence**
`internal/core/config.go:782-795` assigns all eleven `Enable*` booleans unconditionally from the zero-valued `yc.Features`. `/etc/ricochet/config.yaml` is auto-loaded if present (`cmd/ricochet/main.go:60-62`) and installed as a conffile, so any minimal operator config does this. Compounding: a config-file parse or validation error is only a `WARNING` on stderr and the server starts on the preset (`main.go:66-68`).
*Fix:* pointer-typed booleans (as already done for `admission_control.enabled`, `config.go:650-660`) and make a load failure fatal.

**H5 — No LICENSE file**
`README.md:491-493` says "See [LICENSE](LICENSE)"; no such file.

**H6 — `pkg/client` is not usable by a third party: it leaks `internal/` types in 11+ exported signatures**
Imports: `client.go:18-23` (`internal/core`, `internal/protocol/{frame,maa,mma,msa,sda}`), `options.go:7`, `errors.go:9-10`, `collections.go:11-12`, `feeds.go:11-12`, `notifications.go:9-10`, `compression.go:9`, `encryption.go:15`. Leaked types: `RetrieveMessages() []*core.Message` (`client.go:377`), `CreateMailbox(..., core.MailboxType, ...)` (`:711`), `GrantAccess(..., core.AccessMode)` (`:744`), `QueryCapacity() *core.ServerCapacity` (`:1023`), `GetDocument() *core.DocumentResponse` (`:1128`), `ListDocuments() []core.DocumentInfo` (`:1449`), `NotificationHandler func(*notify.Notification)` (`notifications.go:14`), `Encrypt/Compress*` returning `core.SFMessageFlags`. Only `MessagePriority` is re-exported (`options.go:11-17`). The README's own client examples use `core.PriorityUrgent`, `core.MsgFlagFlagged`, `core.MailboxPrivate`, `core.AccessReadWrite` and `notify.Notification`, which an external module cannot compile. `internal/protocol/frame` is imported **only** by `pkg/client` (server uses `forge/codec`).
*Fix:* move wire types to `pkg/wire` (or `pkg/ricochet`), have `internal/` import from there.

**H7 — Build/deploy scripts hard-code a developer's machine layout and copy unrelated data into the build**
`docker-compose.build.yml:15` mounts `..` (the whole `agentic/` parent) at `/workspace/IdeaProjects/agentic`; `docker-build.sh:33-38` `rsync -a` copies all of it into the image — in this environment that included a credentials file beside the checkout. The "fix replace directives" sed at `docker-build.sh:45-48` is a no-op (`../x` → `../x`) and omits `go-p2p-forge`. `Dockerfile.build:36` grants `builder ALL=(ALL) NOPASSWD: ALL`. `deploy/env.example:10` ships `EXTERNAL_IP=<a live deployment address>` — a real deployment address, tracked in git. `deploy/run.sh:35` passes `--pg-password "$DB_PASSWORD"` on argv and `:36` defaults `--pg-sslmode disable` even though it passes `--production`. `deploy/debian/control:7` `info@ricochet.io` (unverified domain).

## MEDIUM

**M1 — Graceful shutdown drains nothing; in-flight handlers are cut and then lose the DB pool**
Forge's `Server.Stop` only `RemoveStreamHandler`s protocols registered via `Handle()` (`../go-p2p-forge/forge.go:287-289`) and only waits on pipelines built with `WithActiveStreams`. Ricochet registers via `h.SetStreamHandler` after Start (`internal/server/server.go:453-491`) and never calls `WithActiveStreams`/`WithShutdownTimeout`, so the 5s default drain waits on an empty WaitGroup. `node.Close()` then closes the host, and `Stop` closes storage (`server.go:206-210`) while handler goroutines may still be mid-query. `forge.StreamContext.Ctx` is documented "cancelled on stream close" (`context.go:16`) but is `context.Background()` (`pipeline.go:46`); 40+ `context.Background()` sites in MAA/MMA/MSA/SFA/SCA with **no timeout**.

**M2 — Push notifier is wired before the presence monitor exists, so presence gating is a permanent no-op**
`server.go:415` builds `mda.NewNotifier(..., s.presenceMonitor, ...)`; `s.presenceMonitor` is assigned at `:427`. `notifier.go:56 if n.presence != nil` makes it silently skip. `EnablePresenceMonitoring` therefore has no effect on delivery. Also `presence.Monitor.StartPeriodicMonitoring` is never called anywhere (`monitor.go:72`).

**M3 — Handlers bypass MDA/MTA; the README "MSA → MTA → MDA" diagram holds only for submit and retrieve**
MAA reaches storage directly 7× via the exported field `MailboxServer.Storage` (`delivery.go:95`; `maa/handler.go:266,285,292,321,323,326,343`); MMA 8× (`mma/handler.go:361,371,415,425,457,467,536,556,582,592`); SDA/SFA/SCA pull `storage.Storage` straight from the registry. MAA discards errors with `_` at `maa:292, 323, 326`.

**M4 — Authorization is re-implemented in 11 places with 4 different response shapes**
Sender-must-match-connection: `msa/handler.go:166` and `:250` **and** a third copy in `mta/router.go:95`. Owner checks: `maa:313` (expunge only), `mma:216-224 verifyOwner`, `sda:276-286`, `sfa:211-249`, `sca:198-204`. ACL checks in `mailboxes/{private,public,shared}.go`. Responses for the same "unauthorized": MSA `StoreAck` with **no `Status`** (`msa:251-255`), SDA/SFA/SCA `403`, MMA `AdminResponse{ErrorMessage}` with no status (`mma:233`), MAA `map[string]string{"error":…}` (`maa:314`). `sfa.commonValidation` performs **writes** (auto-creates a collaborative feed, `sfa:222-236`) inside a "validation" middleware, and `sfa.isWriteClassifier` is a substring match on the raw JSON (`sfa:135-140`) — a GET whose title contains `"CREATE"` is charged against the write bucket.

**M5 — Four parallel status-code tables**
`internal/protocol/wire/status.go:30-45` (has 507, lacks 201/204/304/413), `sda/handler.go:65-78`, `sfa/handler.go:45-56`, `sca/handler.go:44-55`; plus literals in `core/message.go:283-300` and `client.go:1310`. `pkg/client` compares against all four (29 sites).

**M6 — MDA mailbox cache is unbounded, stale after config updates, and drives maintenance**
`delivery.go:96` map grows one entry per mailbox ever touched; evicted only by `DeleteMailbox` (`:278`). `mma.handleUpdateConfig` writes `MaxMessages` to storage (`mma:556`) but never invalidates the cache. `PerformMaintenance` enforces public-mailbox retention only over cached entries (`delivery.go:302-314`).

**M7 — Config: 12 dead fields, dead yaml tags, unvalidated inputs**
Nothing outside `config.go` reads: `MaxConcurrentConnections`, `ConnectionTimeout`, `MessageTimeout`, `WorkerThreads` (whole `performance:` section), `ServerRegion`, `HealthCheckInterval`, `PresenceCheckInterval`, `EnableForwarding`, `EnableAuthentication`, `TrustedPeers`, `Postgres.ConnectTimeout` (not even in the DSN), `MaxMailboxes` (display only). The `yaml:` tags on `ServerConfig` are never used — loading goes through the separate `yamlFileConfig` (`config.go:567-666`) with different keys. `HighCapacityConfig()` (`:482`) and `ConnectionURI()` (`:396`) have no callers. `Validate()` (`:493-554`) checks nothing about `PostgresConfig`, `RelayLimits`, multiaddr syntax (the example's `/ip4/YOUR_PUBLIC_IP/…` passes), `DataDirectory==""` (→ `/peer_identity.key`, `server.go:591`), or `CleanupInterval<=0` (→ `time.NewTicker` panic, `server.go:553`). `--development` and `--production` together → development wins silently (`main.go:49-51`).

**M8 — Test coverage and CI**
Zero tests in: `internal/protocol/{msa,maa,mma,sda,sfa,sca,notify,wire}`, `internal/mda/mailboxes`, `internal/registry`, `internal/storage`, `cmd/ricochet`, `cmd/ricochet-bench` — every request handler and every authorization path is exercised only through the 95 integration tests, which `t.Skip` without `RICOCHET_TEST_POSTGRES_DSN`. No CI config of any kind. Integration tests need a pre-provisioned schema (no migrations), never truncate, and `go test ./...` runs `test/integration` and `internal/storage/postgres` concurrently against the same DB while `capacity_test.go:62-74,104-135` assume no concurrent writers. Ten fixed `time.Sleep(100ms)` sync points. `test/integration/docker/` depends on seven sibling repos and a prebuilt 1.0.0 .deb.

**M9 — Client library semantics**
`DeleteDocument/DeleteFeed/DeleteCollection/DeleteCollectionItem` return `(true, nil)` for any non-404 including 403/500 (`client.go:1284`, `feeds.go:448`, `collections.go:207,343`). `PatchDocument` never checks failure (`client.go:1221`). No `Close()`; `RegisterNotificationHandler` spawns an unbounded `go handler(n)` per notification and is never unregistered (`notifications.go:18-38`). `ctx` is honoured only at stream open (`client.go:142-149`); reads block on the 30s deadline. `Config.ConnectionTimeout` is dead (`client.go:112-114`); `ServerPreference.Weight` never read; no failover past `PreferredServers[0]` (`:128-138`). `fmt.Errorf` vs typed errors: 69 vs 11 in `client.go`; no `ErrNotFound` sentinel despite four different 404 conventions.

**M10 — Process lifecycle in `main.go`/`server.go`**
No second-signal escape: after SIGINT, `Stop()` sleeps `DrainDelay` (`server.go:161`) and the signal channel is never read again (`main.go:145-153`). `Server.isRunning` is a plain bool written in `Stop` (`server.go:151`) and read from `maintenanceLoop` (`:561`) — data race. `Start` does not clean up on partial failure (storage pool left open if `initializeP2P` fails, `server.go:98-105`). Background service start failures are `Warn`-only (`server.go:497-510`) and never affect `/readyz`.

## LOW / INFO

**L1 — Go version drift.** `go.mod: go 1.25`; `README.md:59` says "Go 1.24.6+"; `Dockerfile.build:19` pins 1.25.0.

**L2 — README drift.** `README.md:337 internal/p2p/` no longer exists; layout omits `admission, capacity, metrics, opsapi, opsview, ratelimit, protocol/{sca,sfa,wire}, mda/mailboxes`; agent table omits SFA/SCA/MSA-batch; DB table lists dead `block_store` and omits five live tables; identity file named `identity.key` vs `peer_identity.key` (`server.go:591`); test counts stale.

**L3 — Dead schema and interface members.** `schema.sql:91-112` `block_store` + 3 indexes, views `mailbox_stats` and `message_priority_stats` — zero Go references; `storage/models.go:75 BlockRecord` and `:35 StoredMessageRecord` unused; `GRANT … TO ricochet` hard-codes the role. `Storage.Initialize` exists only to return an error (`postgres.go:47-49`); `UpdateMailboxAccess`, `DeleteMessage`, `EnforceFeedRetention`, `mta.Router.HandleRetrieve` are implemented but never called. `PostgresStorage.Pool()` (`:88`) leaks `*pgxpool.Pool` to `server/opsapi.go:19-21` — documented and contained.

**L4 — Docs mislabelled.** `doc/DOCUMENT_STORE_MVP_PROPOSAL.md:3` "Status: Proposal" for shipped code; `SCALE_ROADMAP.md:3` "not yet started" contradicted by `:309`; two links to nonexistent `../RICOCHET_STORES_PROPOSAL.md`; `SUMI_DOCUMENT_SYNC_REQUIREMENTS.md:178` cites `NewDualBucket` which no longer exists; `SCALABILITY.md:109` stale line numbers. TODO/FIXME count: **1**. No commented-out code.

**L5 — Hand-synced duplicate wire structs.** `client.MailboxInfo`/`ACLEntry` ↔ `mma.MailboxInfo`/`ACLEntry` (`client.go:79-92` ↔ `mma/handler.go:110-123`); `MailboxDetail` (`client.go:99`) ↔ an untyped `map[string]any` literal (`mma:599-611`); `BatchDocumentResult` ×2; `FeedEntry` defined 3× in the client and 2× in the server; batch limits mirrored as unexported server consts vs exported client consts.

**L6 — Client naming.** Six "target this server" options, three folder-path options, `AppendFeedEntry` vs `AppendToFeed` (identical bodies, `feeds.go:113,136`). 41 exported identifiers are never called by `cmd/`, `test/`, or `internal/`.

**L7 — Hard-coded tunables in `server.go`.** Yamux keepalive 15s / write timeout 10s (`:328-329`), presence cache TTL 30s (`:426,434`), presence `MaxBatchSize: 50` (`:440`).

**L8 — Unchecked assertions.** 30 `sc.Request.(*T)` and every `forge.ServiceFrom` discards `ok`; a missing registration panics into `middleware.Recovery()` → generic 500. `sca.ownerIDFrom` (`sca:220-223`) will panic if `commonValidation` is ever reordered.

**L9 — Forge coupling.** 16 of 22 ricochet packages import forge. `pkg/client` and `internal/core` do **not** — the domain layer and public client are framework-free, which is the right boundary. Forge's own comments reference ricochet by name (`middleware/ratelimit.go:139`, `codec/frame.go:4`, `service/ticker.go:13`).

**L10 — Info disclosure.** `registry.go:169-178` publishes storage cap, retention, regions, port to any GossipSub subscriber. `RICOCHET_SEED_HEX` is undocumented in README/config.example.

---

## What is well done

- **`internal/protocol/wire/status.go`** — the package doc explains *why*, and `Classify` orders rate-limit vs overload vs mailbox-full deliberately with the reasoning inline. This is the model for how the rest should be written.
- **SQL construction is safe.** Sort/filter keys are regex-whitelisted, the operator listing picks `ORDER BY` from a fixed set with a comment saying so, and the directory-cursor bug fix is documented at the site.
- **`storage.Storage` interface** is pgx-free and its operator-view section carries an explicit "these scan, none belongs on a request path" contract.
- **Ops surface**: drain-before-teardown with a load-balancer delay, `/readyz` that vouches only for Postgres, nil-receiver-safe `Metrics.Registry()` and `Controller.Stats()`, own Prometheus registry instead of the polluted global.
- **`mda.MailboxDefaults`** (`delivery.go:32-91`): a type that exists to make a previous silent bug impossible to reintroduce.
- **Config example tests** (`internal/core/config_example_test.go`) guarantee every parser key is documented and every documented key is parsed.
- **`pkg/client/errors.go`** is the right shape: sentinels + typed structs with `Is()`, `RetryAfter()`, `IsRetryable()`.
- **`-race` clean** across all unit packages; admission, ratelimit, capacity, opsview each have real tests.
- **Secrets**: no keys, tokens, or real DSNs in tracked files; `.gitignore` covers `*.key`; DSN password never logged.

## Top 5 structural changes before release

1. **Fix the two data-integrity bugs (H1, H2) and add end-to-end tests for them.**
2. **Make it buildable from a clean clone (H3, H7).** Tag the three sibling repos, drop the `replace` lines, commit `go.sum`, delete `docker-build.sh`'s parent-directory rsync, remove the live IP from `deploy/env.example`, add a LICENSE (H5), and add a minimal CI.
3. **Create a `pkg/wire` package holding every wire struct, status constant, flag/priority constant, and protocol ID** — one definition each — and have both `internal/protocol/*` and `pkg/client` import it (H6, M5, L5).
4. **Put authorization and storage access behind the MDA for all mailbox protocols (M3, M4, M6).** Unexport `MailboxServer.Storage`; add methods that take the caller ID, check ownership once, and invalidate the cache. Give the three store protocols a shared `ownerAccess` middleware.
5. **Finish the lifecycle (M1, M2, M10, H4).** Register pipelines with `WithActiveStreams`; derive a per-stream context with a configurable timeout; build the presence monitor before the notifier; make config-load failure fatal and feature flags tri-state; add a second-signal force exit and an `atomic.Bool` on `isRunning`.
