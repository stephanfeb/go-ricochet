# Transport stalls after ~256KB per connection

**Status:** open, unfixed — blocks the scale roadmap
**Found:** 2026-08-27, while measuring `BATCH_PUT` (`98216e6`)
**Layer:** `go-udx` / `go-libp2p-udx-transport`, not go-ricochet
**Severity:** caps every connection's lifetime transfer at roughly a quarter of a megabyte

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

1. Add a flow-control test in `go-udx` that pushes several megabytes through one
   stream and asserts it completes — the existing `flow_control_test.go` covers
   the accounting, not sustained transfer.
2. Instrument window updates on both sides to find whether they stop being sent
   or stop being applied.
3. Once fixed, raise `RecommendedBatchBytes` in `pkg/client` and re-run the
   vault-sync measurement.
