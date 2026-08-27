# Transport stalls after ~256KB per connection

**Status:** **RESOLVED** — fixed in `go-udx` `d9b1dc7`, verified from go-ricochet 2026-08-27
**Found:** 2026-08-27, while measuring `BATCH_PUT` (`98216e6`)
**Layer:** `go-udx` / `go-libp2p-udx-transport`, not go-ricochet
**Was:** capped every connection's lifetime transfer at roughly a quarter of a megabyte

## Resolution

Three independent defects were stacked behind the single symptom.

**The ceiling itself.** The receiver advertised a window size and the sender
applied it as an absolute limit against a *lifetime-cumulative* `dataSent`, so
the advertised window doubled as a lifetime transfer cap. Auto-tuning then
converged to a hard stop: each round granted about `W/4` while raising the
trigger to `(W×1.25)/4`, so after roughly seven updates the grant could never
reach the threshold again. End to end it stalled at exactly 262,144 bytes.

Fixed with QUIC `MAX_STREAM_DATA` semantics — `WINDOW_UPDATE` now carries
`dataConsumed + recvWindow` as an absolute offset, driven by `Read` rather than
`DeliverData`, triggering at half-window. Absolute offsets matter beyond
correctness: control packets use `seq=0` and are never retransmitted, so a delta
scheme would lose credit permanently on a single drop. `Write` also emits
`STREAM_DATA_BLOCKED` when stalled, which the peer answers by re-advertising.

**ACK amplification**, previously masked by the cap. Every packet with a nonzero
stream ID was ACKed, including ack-only packets, so each ACK drew an ACK in
return without end. 64KB of payload produced 183,751 ack-only datagrams for 48
data packets; now 49.

**The congestion controller had never run.** `HandleAckFrame` deleted the packet
it acknowledged, then `connection.go` called `GetPacket(seq)` on that deleted
entry, got nil, and skipped `OnPacketAcked` — always. CUBIC, cwnd, pacing and RTT
sampling were all implemented and completely unreachable. With ACKs wired
through and the send path gated, 8MB now transfers in 2.17s using 12,299
datagrams; 64MB sustains 54MB/s. Before, an 8MB transfer wedged permanently at
1.6MB delivered after 2.3 million datagrams.

### Verified from go-ricochet

Re-running the exact reproductions from this document against the fixed
transport:

| Repro | Before | After |
|-------|--------|-------|
| `GET` 32KB in a loop | stalled at read 7, 229,376 bytes | 90 reads, 2.8MB, 132ms, contents intact |
| `PUT` 205KB in a loop | stalled on the 2nd write | 15 writes, 2.9MB, 191ms |
| 500-document vault sync | could not complete | 10 requests, ~300ms |

### Consequences for go-ricochet

- `pkg/client.RecommendedBatchBytes` came off its 128KB workaround. It is now
  2MB, chosen from the vault-sync measurement rather than from this ceiling.
- **A stall now surfaces as `ErrDeadlineExceeded` rather than a 40s yamux
  keepalive timeout.** `go-udx` deadlines were previously checked only on entry
  and then waited on a condition variable with no timer, which is why the symptom
  looked like a keepalive failure. `pkg/client` already sets stream deadlines
  from the caller's context (`client.go:127`), so nothing needed changing here —
  but error text seen by clients differs.

### Known, still open in go-udx

- **Concurrent streams on one connection are broken**, and were before this work.
  `sendPacket` allocates one connection-wide sequence number, but `DeliverData`
  uses it as a per-stream ordering key, so with multiple streams everything after
  the first packet sits in `recvOOO` forever. The fix needs a per-stream offset
  in `StreamFrame` — a wire change requiring Dart coordination — and is carried
  as a skipped test.

  **This does not affect go-ricochet**, which runs a single UDX stream with yamux
  multiplexing above it. Verified: `go-libp2p-udx-transport/transport.go:122`
  opens exactly one stream per connection.
- **Connection-level flow control is unenforced.** Enforcing a 1MB cap with no
  `MAX_DATA` sender would only relocate the cliff.

---

## Original report


## Symptom

A connection stalls once about 256KB has crossed it, in either direction. The
stalled operation hangs for ~40 seconds and then fails with a yamux keepalive
timeout:

```
read frame data: stream reset: stream reset: connection closed: keepalive timeout
```

## What it is not

Not a batching bug, and not a frame-size limit. Plain single-document `PUT`s and
plain `GET`s reproduce it identically, with no batch code in the path.

## Measurements

Each run uses a fresh server and client, and each request opens its own stream
on the same connection.

| Workload | Cumulative bytes | Result |
|----------|------------------|--------|
| `PUT` 16KB × 4 | 64KB | OK |
| `PUT` 32KB × 4 | 128KB | OK |
| `PUT` 64KB × 4 | 256KB | **stalls on the 4th write** |
| `PUT` 205KB × 2 | 410KB | **stalls on the 2nd write** |
| `GET` 32KB × 8 | 256KB | **stalls on the 7th read, at 229,376 bytes** |

The ceiling tracks cumulative bytes on the connection, not the size of any one
frame and not the number of streams. Every failure lands between 224KB and
256KB.

## Probable mechanism

`go-udx` implements flow control with a growing receive window
(`flow_control.go`): `InitialMaxData` is 1MB at the connection level and
`InitialMaxStreamData` is 64KB per stream, with `GrowRecvWindow` enlarging the
window as data is consumed and a window update sent once more than 25% of the
advertised window has been received.

A libp2p connection is one UDX stream with yamux multiplexing above it, so the
binding limit is the *stream* window. It clearly grows past its 64KB initial
value — 128KB of traffic succeeds — but stops being replenished somewhere before
256KB.

That explains the keepalive timeout rather than a clean error: yamux has a
single writer goroutine. Once a UDX stream write blocks on flow control, the
keepalive ping queues behind it and never goes out, so the peer tears the
connection down.

**Suspects, in order:** window updates stop being sent; the sender does not apply
a received update to its `maxData`; or the update is sent but the growth
calculation stops advancing.

## Why it matters

- **It blocks the scale roadmap.** Stage 2's manifest sync moves blobs in bulk.
  Nothing that moves bulk data works until this is fixed.
- **It caps `BATCH_PUT`'s benefit.** Batching cuts request count — a real win
  against the rate limiter, and it works today for modest batches — but a
  500-document vault is about 1MB, so the sync still dies mid-way. The client's
  `RecommendedBatchBytes` is set to 128KB to stay clear of the ceiling.
- **It is almost certainly biting production already.** Any sustained transfer
  hits it. It is worth checking whether unexplained mobile-client disconnects
  are this.

## Reproducing

`test/integration/` with `RICOCHET_TEST_POSTGRES_DSN` set. Seed one document,
then read it in a loop and count the bytes:

```go
for i := 0; i < 40; i++ {
    doc, err := cl.GetDocument(ctx, ownerID, "c/big")  // 32KB document
    if err != nil {
        t.Fatalf("stalled at read %d after %d cumulative bytes: %v", i, total, err)
    }
    total += len(doc.Content)
}
```

Writes reproduce it too, but SDA's 20/min write limiter caps a single-`PUT` loop
at 20 requests, so reads (100/min) are the easier path.

## Next steps

All resolved. Retained for the reproductions, which are now regression tests:
`TestVaultSyncBatched` in `test/integration/batch_document_test.go` pushes ~1MB
of documents over one connection, which is where this used to fail.
