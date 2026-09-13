# Performance baselines

Numbers to regress against. They exist so a change can be shown to have helped
or hurt rather than argued about, and so Stage 2 has a before-figure.

Treat them as a shape, not a spec. They were taken on a laptop against a local
PostgreSQL over loopback, which removes network latency entirely — the point is
the *ratios* between scenarios and the direction of change between runs, not
the absolute figures.

## Conditions

| | |
|---|---|
| Server | `ca32f10` (Phase B complete) |
| Hardware | Apple M3 Pro, 12 cores |
| OS | macOS 14.6.1 |
| PostgreSQL | 14.15 (Homebrew), loopback, `pool_size: 25` |
| Transport | UDX over loopback (`/ip4/127.0.0.1/udp/55223/udx`) |
| Payload | 1024 bytes |
| Rate limiting | off (the default) |
| Admission control | on, `max_in_flight` derived from the pool (100) |
| Database | freshly created from `schema.sql` before the run |

Client and server share the machine, so both are competing for the same cores.
That understates the server on a real deployment and is fine for comparison as
long as every run does it.

## Reproducing

```bash
# Fresh database, then start a server on the default or production preset
# (--development logs at debug and costs throughput; it is not a benchmark
# condition). Check the host is idle first: a loaded machine produces stalls
# in the tail that have nothing to do with the server.
psql -d postgres -c 'CREATE DATABASE ricochet_bench'
psql -d ricochet_bench -f schema.sql
ricochet --config bench.yaml

go build -o ricochet-bench ./cmd/ricochet-bench
ADDR=/ip4/127.0.0.1/udp/55223/udx
PEER=<server peer id from the startup log>

ricochet-bench -n 500 -c 10 -protocol sda -payload-size 1024   $ADDR $PEER
ricochet-bench -n 500 -c 10 -protocol mma                      $ADDR $PEER
ricochet-bench -n 50  -c 10 -protocol sda-batch -batch-size 100 $ADDR $PEER
ricochet-bench -protocol sync-cold -docs 500                    $ADDR $PEER
```

The sync scenarios pick their own defaults (`-n 10 -c 1 -warmup 1`), because one
request there is a whole vault: the per-operation default of `-n 1000` against
`-docs 500` would write half a million documents before reporting anything.

## Per-operation

500 requests, 10 concurrent workers. One request is one operation, except SDA,
SFA and SCA which each do a write **and** a read.

| Scenario | req/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| `msa` submit | 5,428 | 1.3ms | 3.6ms | 18.8ms |
| `msa` submit, after P1 (2026-09-13) | 6,288 | 1.3ms | 2.4ms | 12.3ms |
| `maa` retrieve | 7,886 | 0.97ms | 2.6ms | 3.8ms |

> P1 (backlog) took a submission from five round-trips plus a `COUNT(*)` and a
> trigger to three: the sequence `UPDATE` now also counts the message in,
> checks the cap and records the access. Measured before and after in one
> session on the same machine, 5,000 requests so the tail is not one outlier:
> before 6,020–6,070 req/s, p50 1.6ms, p95 2.4–2.5ms, p99 2.9–3.5ms; after
> 6,750–7,070 req/s, p50 1.3ms, p95 2.1–2.3ms, p99 3.0–5.1ms. The 500-request
> row above is the shape the table uses; its p99 is the startup effect noted
> below.

> The `maa` row predates the change that made retrieval non-destructive (2026-09). It was measured while a private mailbox deleted what it returned, so after ten requests per worker most retrieves were of an empty mailbox. Re-run before comparing: each retrieve now returns ten real messages.

| `sda` put + get | 5,674 | 1.7ms | 2.5ms | 3.0ms |
| `sfa` append + get | 5,451 | 1.8ms | 2.6ms | 3.1ms |
| `sca` put item + query | 1,076 | 8.8ms | 16.8ms | 19.1ms |
| `mixed` | 5,966 | 1.7ms | 2.5ms | 2.8ms |

> The `mixed` row was measured while the mix drew from `msa`, `maa`, `sda` and
> `sfa` only: the setup created a collection per worker and the mix never
> touched it. Since 2026-09-13 `mixed` draws uniformly from all six
> per-operation scenarios, `sca` and `mma` included, so it is a heavier mix
> than this row measured. Compare against the session table below, not this.

**SCA is five times slower than the others and is the obvious next target.** Its
request is a JSONB insert followed by an unfiltered collection query, so it is
doing more work than the rest — but not five times more. Worth profiling before
assuming the shape is inherent.

The `msa` p99 of 18.8ms against a 1.3ms p50 looks like a startup effect rather
than a property of MSA: whichever scenario runs first in a session shows an
outlier of roughly that size, and MSA ran first here. In an earlier session
where SDA ran first, SDA showed a p99 of 18.0ms and MSA 6.1ms. Worth isolating
before reading anything into a single protocol's tail.

## Session of 2026-09-13: every per-operation scenario, one session

Taken to give `mma` a row and to measure `mixed` with `sca` and `mma` in it.
1,000 requests, 10 workers, server at the D5 commit, default preset (not
`--development`: that preset logs at debug, which cost this session a third
of its throughput in a first pass and is not a benchmark condition).

**These figures are not comparable to the table above.** The host was
saturated for the whole session (load average 22 on 12 cores, 0% idle, five
Docker containers and a 79-day uptime with 11 GB of compressed memory), and it
shows: p50s are close to the earlier session's, p95 and p99 are stalls of
100–400ms that the earlier session never saw, and a second `mixed` run in the
same session came out at a quarter of the first. Read the rows against each
other, not against the table above, and re-take them on an idle machine
before concluding anything about the server (backlog N8).

| Scenario | req/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| `msa` submit | 812 | 1.8ms | 37.5ms | 312.8ms |
| `maa` retrieve | 1,143 | 4.4ms | 25.8ms | 44.6ms |
| `sda` put + get | 1,390 | 4.5ms | 19.8ms | 29.5ms |
| `sfa` append + get | 1,045 | 7.9ms | 19.4ms | 29.5ms |
| `sca` put item + query | 208 | 35.5ms | 134.2ms | 345.1ms |
| `mma` create + delete mailbox | 489 | 5.3ms | 120.3ms | 244.7ms |
| `mixed` (all six) | 250 | 7.0ms | 258.1ms | 390.9ms |

What survives the noise: `sca` is still five to six times slower than the
other stores at the median, the same ratio the earlier session found, so that
finding stands. `mma` is a create and a delete, two writes that each
invalidate the mailbox cache, and lands at roughly half of `msa` at the
median, which is the expected shape for two writes against one.

## Batched

One request carries 100 items. Requests per second is not the interesting
number here — documents per second is, and the two differ by the batch factor.

| Scenario | req/s | items/s | p50 per request |
|---|---:|---:|---:|
| `sda-batch` (100 docs) | 158 | **15,848 documents** | 55.9ms |
| `msa-batch` (100 msgs) | 136 | **13,626 messages** | 69.7ms |

Against the per-operation `sda` figure, batching moves document throughput from
roughly 5,700/s (where each request also does a read) to 15,800/s — and cuts
the request count by 100×, which is what actually mattered to the client that
prompted this work.

## Sync shapes

The three shapes Stage 2 will be judged against. A "vault" is 500 documents;
one measured request is one complete sync pass.

| Shape | requests per pass | p50 per pass | documents/s |
|---|---:|---:|---:|
| **cold** — first upload, all creates | 5 | **116.8ms** | 4,242 |
| **warm** — re-upload after edits, all replaces | 5 | **120.7ms** | 4,115 |
| **no-op** — nothing changed, list and compare | 1 | **4.4ms** | 115,702 |
| cold, unbatched (`-batch-size 1`) | 500 | 347.6ms | 1,600 |

Three things this says:

**Replacing costs what creating costs.** Warm is within 3% of cold, so a client
has no reason to work hard at sending only what changed — the saving is in not
sending at all, not in sending less.

**A no-op sync is essentially free: 4.4ms and one request.** That is the shape
that runs most often — a periodic check over an unchanged vault — and it is
27× faster than a warm sync because `LIST` returns every path and ETag in a
single page. A client issuing one conditional GET per document would pay 500
requests to learn the same thing.

**Batching is worth 3× and 100 requests.** The unbatched row is the shape the
sumi team was stuck with: 500 requests and 348ms for a vault that now takes 5
requests and 117ms. Their measured cost was far worse than 348ms because they
were also pacing themselves under a rate limit that no longer exists.

## What to watch when these change

- **A `sync-noop` regression is the most serious**, because it is the most
  frequent operation and the one clients repeat on a timer. It should stay at
  one request per 1000 documents; if it grows, `LIST` pagination or the ETag
  path has regressed.
- **`documents/s` diverging from `req/s`** in the batch scenarios means the
  batch is being unpacked into per-document work somewhere.
- **A `sync-warm` that drifts above `sync-cold`** means replace has acquired a
  cost create does not have — likely a lock or an extra read.

## Known issues found while taking these

- **A collection item whose JSON contains a NUL escape returned 500** (fixed
  2026-09-13, D5). Postgres rejects `\u0000` in `jsonb` (SQLSTATE 22P05) and
  the handler passed the failure through as an internal error. It is the
  client's content, so it is now a 400 that carries the database's reason
  (`storage.ErrInvalidContent`). The benchmark hit this by putting raw random
  bytes in a JSON string field, where any NUL byte marshals to `\u0000`; it
  hex-encodes now.
- **The 2026-09-13 session is 5–10× below the earlier per-operation figures**
  with p99 stalls of hundreds of milliseconds, on a host at load 22 with no
  idle CPU. Whether any of that is the server is unknown until the rows are
  re-taken on an idle machine; backlog N8 tracks it.
- **`sca` throughput** as noted above.
