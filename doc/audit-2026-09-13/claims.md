# Appendix D: Documentation claims audit (full table)

Part of the [release audit of 2026-09-13](README.md). Every checkable claim in `README.md`, `config.example.yaml`, `doc/SUPERVISORD_DEPLOYMENT.md` + `deploy/`, `doc/SCALABILITY.md`, `doc/BASELINES.md`, and source self-claims. Client snippets were compiled from an external module to test importability; `schema.sql` was run against a fresh database; the binary was started with an invalid config file.

| # | Claim | Location | Verdict | Evidence | Fix needed |
|---|---|---|---|---|---|
| 1 | "production-ready" | README:3 | UNVERIFIABLE | Own docs list open production gaps: `NullResourceManager`, no connection manager, `MaxConcurrentConnections` unenforced, unbounded notifier goroutines (`doc/SCALABILITY.md`) | Soften, or link to SCALABILITY status |
| 2 | Link to `github.com/user/ricochet` | README:5 | FALSE | Placeholder URL; module is `github.com/twostack/go-ricochet` | Replace or remove |
| 3 | Store-and-forward messaging | README:9 | TRUE | `pkg/client/client.go:208-230` → `msa/handler.go:125` → `mta/router.go:44-56` → `mda.DeliverLocal` | — |
| 4 | Private/shared/public mailboxes with ACLs | README:10 | TRUE | `internal/core/mailbox.go:14-16,51`; `mma/handler.go:31-41`; `mailbox_acls` | — |
| 5 | Document store: ETag conditional ops, versioning, merge-patch | README:11 | TRUE | `sda/handler.go:31-41`, `:69,320,468`; `options.go:156-175`; `document_versions` | — |
| 6 | NaCl box (X25519 + XSalsa20-Poly1305) derived from Ed25519 | README:12 | TRUE (client side) | `pkg/client/encryption.go:23-47`, `:85-111`. But see Appendix C H2: the server drops the encrypted flag, so retrieval never decrypts | Fix flag persistence |
| 7 | LZ4 with configurable threshold | README:13 | TRUE (client side) | `compression.go:13,22`; `options.go:63-75`. Same flag-persistence caveat | Fix flag persistence |
| 8 | Hybrid push: direct stream for private, GossipSub for shared/public | README:14 | TRUE | `internal/mda/notifier.go:48-53` | — |
| 9 | Circuit Relay v2, AutoRelay with static relays, DCUtR hole punching | README:15 | PARTIAL | Wired via `server.go:332-335` → forge `host.go:135-158`. But `enable_auto_relay` and `enable_hole_punching` default **false**, and AutoRelay only activates when `bootstrap_peers` is non-empty | Say these are off by default and require `bootstrap_peers` |
| 10 | IMAP-style flags + expunge | README:16 | TRUE | `internal/core/types.go:156-159`; `maa/handler.go:34-37` | — |
| 11 | Presence detection with TTL cache | README:17 | TRUE | `internal/presence/cache.go:53-62`; 30s TTL in `server.go` | — |
| 12 | Service discovery: announcements **and health monitoring** | README:18 | PARTIAL | Announce loop `registry.go:150-185` is real. "Health monitoring": heartbeat topic is joined (`:77`) but **nothing ever publishes to it**; only a 2h staleness cutoff | Drop "health monitoring" |
| 13 | PostgreSQL with BYTEA and connection pooling | README:19 | TRUE | `pgxpool` at `postgres.go:53-61` | — |
| 14 | Diagram: SendMessage → MSA → MTA → MDA | README:30-37 | TRUE | See #3 | — |
| 15 | Diagram: RetrieveMessages ← MAA | README:38 | PARTIAL | MAA calls MDA directly; `mta.Router.HandleRetrieve` (`router.go:69`) is dead code | Delete `HandleRetrieve` |
| 16 | CreateMailbox → MMA, PutDocument → SDA | README:40-42 | TRUE | `client.go:711`, `:1166` | — |
| 17 | Protocol ID table | README:46-53 | PARTIAL | Four IDs exact. Omits `/sf-network/submit/batch/1.0.0`, `/ricochet/store/feed/1.0.0`, `/ricochet/store/collection/1.0.0`, `/ricochet/mailbox-notify/1.0.0` | Add rows |
| 18 | Go 1.24.6+ | README:59 | FALSE | `go.mod:3` is `go 1.25`; `Dockerfile.build` installs go1.25.0 | Say Go 1.25+ |
| 19 | PostgreSQL 14+ | README:60 | TRUE | No >14 SQL found | — |
| 20 | `createdb ricochet && psql ricochet < schema.sql` works | README:64-67 | PARTIAL | `schema.sql:334,374-392` `GRANT ... TO ricochet`; on a fresh server this is `ERROR: role "ricochet" does not exist`. README never says to create the role; `doc/SUPERVISORD_DEPLOYMENT.md:69-72` does | Add `CREATE USER ricochet` step |
| 21 | `go build -o ricochet ./cmd/ricochet` | README:73 | TRUE | Built | — |
| 22 | Dev/prod run commands | README:76-88 | TRUE | All flags exist (`main.go:28-38`) | — |
| 23 | `--port` default 55223 | README:94 | TRUE | `main.go:28`; `config.go:403` | — |
| 24 | `--development` = "1GB, relaxed limits, debug logging" | README:95 | PARTIAL | 1GB and debug true. "Relaxed limits" is not true: rate limits identical (off) in every preset; `MaxConcurrentConnections=100` is *tighter* and unenforced | Reword |
| 25 | `--production` = "50GB, 10K connections, auth enabled" | README:96 | PARTIAL | 50GB true. 10K is also the Default and unenforced. "Auth enabled" sets `EnableAuthentication=true` but **no code reads it**. The real production difference (pool 50 vs 25) is undocumented | Remove auth/connection claims; document pool size |
| 26 | CLI block is the full flag set | README:93-105 | PARTIAL | Missing: `--external-addrs`, `--config`, `--debug-dht`, and auto-load of `/etc/ricochet/config.yaml` (`main.go:41-43,59-63`) | Add |
| 27 | `--pg-sslmode` values | README:104 | TRUE | Default `require` (Default/Prod), `disable` (Dev) — README doesn't state the default | State default |
| 28 | `client.New(h, client.Config{...})`, `ServerPreference{PeerID, Priority}` | README:123-129 | TRUE | `client.go:34-45,111`; compiled externally | — |
| 29 | Import path `github.com/twostack/go-ricochet/pkg/client` | README:116 | TRUE | `go.mod:1` | — |
| 30 | `SendMessage` + `WithFolderPath/WithPriority/WithExpiry/WithCompression/WithEncryption` | README:136-145 | TRUE (signatures) | `options.go:34,41,48,63,77` | — |
| 31 | `core.PriorityUrgent`, `core.PriorityHigh` | README:141,159 | FALSE | `core` is `internal/core`; external build fails. Exported aliases exist: `client.PriorityUrgent/PriorityHigh` (`options.go:13-18`) | Use `client.Priority*` |
| 32 | `RetrieveMessages` + retrieve options | README:152-160 | TRUE | `options.go:96-123`; `client.go:377` | — |
| 33 | `MarkDelivered` marks as seen | README:167 | TRUE | `client.go:469`; `postgres.go:381-384` | — |
| 34 | `UpdateFlags(ctx, id, uint32(core.MsgFlagFlagged), 0)` | README:170-173 | FALSE | Signature right (`client.go:513`) but `core.MsgFlag*` not importable and `pkg/client` exports **no** flag constants | Export `client.MsgFlag*` aliases |
| 35 | `Expunge`, `DeleteMessages` | README:176-179 | TRUE | `client.go:559,609` | — |
| 36 | `CreateMailbox(ctx, path, core.MailboxPrivate, ...)` | README:186-191 | FALSE | `core.Mailbox*` not importable, no alias | Export `client.Mailbox*` aliases |
| 37 | `GrantAccess(..., core.AccessReadWrite)`, `RevokeAccess`, `ListACL` | README:194-196 | FALSE (as written) | `core.AccessReadWrite` not importable | Export `client.Access*` aliases |
| 38 | `ListMailboxes`, `DeleteMailbox` | README:199-200 | TRUE | `client.go:796,732` | — |
| 39 | `QueryCapacity` + `UsagePercent()` | README:203-204 | TRUE | `client.go:1023`; `message.go:206` | — |
| 40 | `PutDocument` + `WithContentType`, `WithIfMatch` | README:213-222 | TRUE | `client.go:1166`; `options.go:163,170` | — |
| 41 | `PatchDocument(ctx, owner, path, map[string]any)` | README:225-227 | TRUE | `client.go:1200` | — |
| 42 | `GetDocument` + `WithIfNoneMatch` "returns 304 if unchanged" | README:230-232 | TRUE | `sda/handler.go:320`; `IsNotModified()` `message.go:283` | Mention `IsNotModified()` |
| 43 | `HeadDocument`, `ListDocuments`, `DeleteDocument` | README:235-241 | TRUE | `client.go:1236,1449,1272` | — |
| 44 | `RegisterNotificationHandler(func(n *notify.Notification){...})` | README:247-250 | FALSE | `notify` is internal — external compile fails. No exported `Notification` alias, so the method is **unusable outside the module** | Add `type Notification = notify.Notification` |
| 45 | Presets: Storage 10/1/50/100 GB | README:257 | TRUE | `config.go:406,462,474,484` | — |
| 46 | Presets: Connections 10,000/100/10,000/50,000 | README:258 | PARTIAL | Values match but never enforced | Remove row or mark "not enforced" |
| 47 | Presets: Messages/Mailbox 1,000; Retention 30 days | README:259-260 | TRUE | `config.go:407-408` | — |
| 48 | Presets: Authentication off/off/on/on | README:261 | FALSE in effect | `EnableAuthentication` is read by nothing | Remove row |
| 49 | Presets: Rate Limit 100/min (all) | README:262 | FALSE | `DefaultRateLimits()` `config.go:320-337` sets every bucket to unlimited; `config.example.yaml:242` "off by default". Throughput is governed by admission control | Replace with admission-control row |
| 50 | Presets: Workers 4/4/4/8 | README:263 | PARTIAL | `WorkerThreads` unused | Remove row |
| 51 | High-Capacity preset column | README:256 | PARTIAL | `HighCapacityConfig()` exists but no CLI flag or YAML selects it | Note it is API-only, or add a flag |
| 52 | Wire format `[24-byte nonce][ciphertext+tag]` | README:273 | TRUE | `encryption.go:17,101-109` | — |
| 53 | "Server never sees plaintext" | README:275 | TRUE (payload only) | Metadata (sender, recipient, folder, priority, flags) is plaintext | Qualify |
| 54 | Keys derived from peer identity | README:275 | TRUE | `encryption.go:66-81` | — |
| 55 | Noise for transport encryption and mutual authentication | README:279 | TRUE | forge `host.go:87` `libp2p.Security(noise.ID, noise.New)` | — |
| 56 | Identity precedence: env > `--identity-file` > `{data-dir}/identity.key` | README:283-286 | PARTIAL | Order correct (`server.go:577-591`); filename is **`peer_identity.key`** | Fix filename |
| 57 | Stack: Yamux / Noise / UDX / UDP | README:293-301 | TRUE | `../go-libp2p-udx-transport/transport.go:37-39,142-143` hands raw UDX stream to the libp2p upgrader. (Sibling `../go-libp2p-udx-transport/README.md:7` claims the opposite — stale) | Fix sibling README |
| 58 | "UDX (unreliable datagram transport)" | README:299 | FALSE | `../go-udx/README.md:3`: "QUIC-inspired **reliable** UDP transport with ... congestion control, and flow control" | "UDX (reliable UDP transport)" |
| 59 | NAT snippet `cfg.EnableRelay/...` | README:309-314 | PARTIAL | Fields exist and are wired, but `core.ServerConfig` is internal so the snippet isn't user-writable; real surface is YAML `features.enable_*` | Show the YAML form |
| 60 | Announce topic and announcement contents incl. uptime score | README:319 | PARTIAL | Topic exact. `UptimeScore` is hard-coded `1.0` (`registry.go:175`) | Say "static uptime score" or drop |
| 61 | Project structure: `internal/p2p/` | README:339 | FALSE | Directory does not exist | Remove; mention go-p2p-forge |
| 62 | Project structure completeness | README:323-351 | PARTIAL | Unlisted: `internal/protocol/{sfa,sca,wire}`, `internal/{admission,capacity,metrics,opsapi,opsview,ratelimit}`, `internal/mda/mailboxes`, `pkg/client/{collections,feeds,errors}.go`, `deploy/`, `build/` | Regenerate tree |
| 63 | ricochet-bench measures "across all protocol handlers" | README:355 | PARTIAL | MMA never benchmarked; `mixed` is `rand.IntN(4)` over msa/maa/sda/sfa — **no sca** (`main.go:612`) | Reword; fix `mixed` |
| 64 | Bench flags | README:371-378 | TRUE but incomplete | Missing: `-batch-size`, `-docs`, `-v` | Add rows |
| 65 | Bench protocols table | README:382-389 | PARTIAL | Missing scenarios: `sda-batch`, `msa-batch`, `sync-cold/warm/noop` (`main.go:85-102`) | Add |
| 66 | Bench sample output | README:409-431 | PARTIAL | Real output format differs; 81 req/s is ~70× below the recorded baseline (5,428) | Paste a real run |
| 67 | Each worker creates its own libp2p host | README:435 | TRUE | `main.go:366-403` | — |
| 68 | Resources created "during the warmup phase" | README:435 | PARTIAL | Created in `setupBench`, which runs *before* warmup | "before measuring" |
| 69 | Unit-test command runs | README:441 | TRUE | All six handler packages report `[no test files]` | Note |
| 70 | `RICOCHET_TEST_POSTGRES_DSN` | README:444 | TRUE | `helpers_test.go:62` | — |
| 71 | "46 unit tests" | README:456 | FALSE | 90 for the README's own command; 198 non-integration repo-wide | Update |
| 72 | "21 integration tests" | README:457 | FALSE | 95 | Update |
| 73 | Wire format: 4-byte big-endian length + JSON | README:461-468 | TRUE | `frame.go:14-33` and forge `codec/frame.go:15-52` identical | — |
| 74 | Pipeline: compress → encrypt → frame | README:473-474 | TRUE | `client.go:183-203`, `:448,456` | — |
| 75 | Schema table lists all tables | README:481-489 | PARTIAL | Unlisted: `directory_listings`, `feeds`, `feed_entries`, `collections`, `collection_items`, two views | Add |
| 76 | `block_store` "CRDT block storage" | README:489 | PARTIAL | Table exists but zero Go references | Mark unused or drop |
| 77 | `reader_cursors` for public mailboxes | README:486 | TRUE | `postgres.go:474,487` | — |
| 78 | "See LICENSE" | README:493 | FALSE | No `LICENSE` file | Add a license |
| 79 | `config.example.yaml` every key parses | config.example.yaml | TRUE | All keys map to `yamlFileConfig` | — |
| 80 | Example comments' defaults | config.example.yaml | TRUE | Match `DefaultConfig()` | — |
| 81 | `database.pool_size: 10` | config.example.yaml:69 | PARTIAL | Code default is 25, production 50. Example silently ships a smaller pool, and admission derives `max_in_flight` from it (10×4=40 vs 100). Debian package installs this file as the live config | Set 25 and comment |
| 82 | `identity_file`: "If not specified, checks RICOCHET_SEED_HEX, then auto-generates" | config.example.yaml:13-15 | PARTIAL | Env var wins even when `identity_file` is set | Reword |
| 83 | "A protocol name that is not recognised is a startup error" | config.example.yaml:265; `config.go:979-983` | FALSE | `LoadConfigFromFile` returns the error, but `main.go:67` prints `WARNING` and **continues with the preset, discarding the whole file**. Verified by running with a bad key. Same for every parse error | Make `main.go` exit non-zero |
| 84 | Feature flags: omitting one turns it OFF | config.example.yaml:94-96 | TRUE | `config.go:786-796` unconditional assignment (and see Appendix C H4 for why this is a hazard) | — |
| 85 | relay_limits keys | config.example.yaml:190-194 | TRUE | `config.go:565-575` | — |
| 86 | No logging section is parsed | config.example.yaml:280-289 | TRUE | — | — |
| 87 | `/metrics` gated on `enable_metrics` | config.example.yaml:100,127 | TRUE | `server/opsapi.go:64` | — |
| 88 | Build with `dart compile exe bin/ricochet.dart` | doc/SUPERVISORD_DEPLOYMENT.md:90 | FALSE | Go repo | Replace |
| 89 | `config.postgres.example.yaml` | SUPERVISORD:106,532 | FALSE | File does not exist | Fix path |
| 90 | `doc/LINUX_POSTGRES_DEPLOYMENT.md`, `deploy/README.md` | SUPERVISORD:530-531 | FALSE | Neither exists | Remove links |
| 91 | "Firewall allows P2P ports (4001)" | SUPERVISORD:512 | FALSE | Default is UDP 55223 | Fix |
| 92 | `RICOCHET_MAX_MEMORY="2G"` limits memory | SUPERVISORD:~480 | FALSE | Only env vars read: `RICOCHET_SEED_HEX`, `RICOCHET_TEST_POSTGRES_DSN` | Remove |
| 93 | Debian package steps incl. `CREATE USER` | SUPERVISORD:52-79 | TRUE | `deploy/debian/postinst`; this is the correct DB sequence the README lacks | Copy into README |
| 94 | `run.sh` flags accepted by binary | deploy/run.sh:29-38 | TRUE | `main.go:28-41` | — |
| 95 | Package installs a working `/etc/ricochet/config.yaml` | docker-build.sh:83 | PARTIAL | Verbatim copy of `config.example.yaml`, including `external_addresses: /ip4/YOUR_PUBLIC_IP/...` placeholder and `pool_size: 10` | Ship a minimal real config |
| 96 | Health checks on `127.0.0.1:9090` | SUPERVISORD | TRUE | `DefaultOpsConfig`; `internal/opsapi/server.go` | — |
| 97 | SCALABILITY.md "current as of `2e6bfee`"; line references | doc/SCALABILITY.md | PARTIAL | Many refs stale; §1 says MTA limiter did not migrate while scorecard #7 says Fixed — internal contradiction | Refresh |
| 98 | SCALABILITY: NullResourceManager; notifier goroutines | SCALABILITY §1 | TRUE | Confirmed | — |
| 100 | BASELINES numbers | doc/BASELINES.md | UNVERIFIABLE (well-recorded) | Environment fully stated; framed as "shape not spec". Not re-run in this audit | README sample output contradicts it |
| 101 | BASELINES: `psql -d ricochet_bench -f schema.sql` | BASELINES:34-36 | PARTIAL | Same missing-role GRANT issue as #20 | Add role creation |
| 102 | `helpers_test.go:55`: "all 4 protocol handlers" | test/integration/helpers_test.go:55 | FALSE (stale comment) | Registers 7 | Update |
| 105 | Postgres connection string handles all configs | `postgres.go:53` | FALSE (bug) | `password=%s sslmode=%s`: with an **empty password**, pgx parses `password= sslmode=disable` as `Password="sslmode=disable"` and sslmode falls to `prefer`. Reproduced live: `--pg-sslmode disable` without `--pg-password` produced "server refused TLS connection" | Build config via `pgxpool.ParseConfig("")` + field assignment, or quote values |
| 106 | Sibling `../go-libp2p-udx-transport/README.md:7`: "provides CapableConn directly without needing an Upgrader" | sibling README | FALSE | `transport.go:37-39,142-143`: upgrader layers Noise + Yamux | Fix before publishing |
| 107 | Bench `-protocol` flag help | `cmd/ricochet-bench/main.go:139` | PARTIAL | Omits batch/sync scenarios | Update |

## Summary by embarrassment

### Tier 1 — a first-time reader hits these in the first ten minutes
- **#20 / #101** Quick-start DB setup fails on a fresh server: `schema.sql` GRANTs to a role the README never creates.
- **#31, #34, #36, #37, #44** Five README client snippets **do not compile** outside the module: they import `internal/core` and `internal/protocol/notify`. `RegisterNotificationHandler` is unusable by external code at all.
- **#78** No `LICENSE` file, yet the README links to one.
- **#2** Placeholder project link `github.com/user/ricochet`.
- **#18** Go version wrong.
- **#49** "Rate Limit 100/min" in every preset — rate limiting is off in all presets.
- **#88–#92** `doc/SUPERVISORD_DEPLOYMENT.md` tells people to `dart compile` a Go server, references three files that don't exist, the wrong port, and a fake `RICOCHET_MAX_MEMORY` knob.

### Tier 2 — misleading about what the software does
- **#25, #48** "auth enabled" in production: there is no authentication feature.
- **#46, #50** Connections and Workers preset rows are inert settings.
- **#58** UDX described as "unreliable datagram transport"; it is reliable and congestion-controlled.
- **#61** `internal/p2p/` does not exist.
- **#83 / #104** *Any* config-file error is downgraded to a warning and the whole file is silently ignored.
- **#105** Empty `--pg-password` silently corrupts `sslmode` (real bug, reproduced).
- **#56** Identity file is `peer_identity.key`, not `identity.key`.
- **#71, #72** Test counts off by 2×–4×.
- **#12, #60** "Health monitoring" doesn't exist; uptime score is a constant.
- **#95, #81** The Debian package installs the example YAML verbatim as live config, with a `YOUR_PUBLIC_IP` placeholder and a pool of 10.
- **#6, #7** Encryption and compression: client-side code is correct, but the server never persists the flag, so a retrieved message is returned as raw ciphertext with `flags: 0` (Appendix C H2).

### Tier 3 — incomplete or slightly off
- **#17** Protocol table omits SFA, SCA, MSA-batch, notify.
- **#62, #75** Project tree and schema table omit ~40% of what exists.
- **#63–#68** Bench docs: `mixed` skips SCA, MMA never benchmarked, five scenarios and three flags undocumented, sample output doesn't match a real run.
- **#9, #59** Relay/AutoRelay/hole-punching are off by default and the Go snippet isn't a usable API.
- **#24** "Relaxed limits" in development mode isn't true.
- **#15** `mta.Router.HandleRetrieve` is dead code.
- **#51** High-Capacity preset is unreachable from the CLI.
- **#53** "Never sees plaintext" applies to payload only.
- **#76** `block_store` is dead schema.
- **#97, #102, #107, #106** Stale docs and comments.

### Confirmed TRUE (no action)
Store-and-forward path, mailbox/ACL model, document store operations, NaCl box details and wire layout (client side), LZ4 (client side), hybrid push, flags, presence TTL, Postgres BYTEA/pool, the four listed protocol IDs, PG 14+, build/run commands, `client.New`/Config shape and every method signature, Noise + Yamux over UDX, announce topic, wire framing identical between client and forge, compress→encrypt order, `RICOCHET_TEST_POSTGRES_DSN`, `config.example.yaml` keys and stated defaults, deploy scripts' flags, ops endpoints.
