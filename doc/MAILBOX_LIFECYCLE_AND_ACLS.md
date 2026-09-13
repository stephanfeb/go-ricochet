# Mailbox lifecycle & ACLs: a client-integration field guide

**Author:** sumi team (a document-centric notes app syncing over go-ricochet)
**Audience:** client integrators first, go-ricochet maintainers second
**Status:** field-tested learnings + suggestions, grounded in a multi-session debugging effort
**Related:** [`SUMI_DOCUMENT_SYNC_REQUIREMENTS.md`](./SUMI_DOCUMENT_SYNC_REQUIREMENTS.md) (throughput / rate-limit / observability field report), [`FUTURE_ENHANCEMENTS.md`](./FUTURE_ENHANCEMENTS.md)

This is the sibling of the requirements field report. That one is about *throughput*
(rate limits, the 1000 cap, observability). This one is about **mailbox lifecycle
and access control** — the non-obvious semantics that, once we finally understood
them, were the difference between "sync silently stalls forever" and "bidirectional
sync just works."

We reached bidirectional device-vault sync (mobile ⇄ desktop) only after learning
each of the things below the hard way. None of them are bugs in go-ricochet — they
are *correct* behaviors that a client MUST account for, and that were not obvious
from the API surface. If you're integrating a client, read this before you ship.

Everything here is pinned to source (as of this writing) so you can verify it.

---

## The mental model that makes it all click

A mailbox on the relay is **owned** by one peer (the recipient), has a **type**
fixed at creation, and holds messages until **the client deletes them**. Three
facts, each of which bit us:

1. **Type is destiny, and it's set on first touch.** `GetOrCreateMailbox` creates
   the mailbox with the type in the address it first sees, and *an existing mailbox
   keeps the settings it was created with* — there is no "change type" operation
   (`internal/mda/delivery.go:142`, and the comment at `:139`). Whoever touches a
   folder first decides its type for life.
2. **Type decides who may write:**
   - **Private** (`MailboxPrivate`) — an *open* inbox. `StoreMessage` only checks
     that the message's recipient is the owner; it does **not** check the sender
     (`internal/mda/mailboxes/private.go:36`). **Anyone who knows your peer-id and
     the folder can deposit.**
   - **Shared / Public** (`MailboxShared` / `MailboxPublic`) — an **ACL** inbox. A
     non-owner sender must hold an explicit write grant, or delivery is rejected
     with `"no write access to shared mailbox"` (`shared.go:35-49`, `public.go:35-49`).
   - Delivery **auto-creates a missing mailbox as PRIVATE** (open). So the *default*
     for a folder nobody set up is open-to-anyone.
   - Retrieval **never creates**. An owner reading a folder that does not exist
     gets an empty result; anyone else gets a `404` (`ErrNotFound` in the client),
     and a read of an existing mailbox they may not see gets a `403`
     (`ErrForbidden`). Before 2026-09 a cross-peer read auto-created the folder as
     *public*, which let any peer squat another peer's inbox.
3. **The relay never drains a mailbox for you.** Retrieving messages does **not**
   remove them; the same page comes back until you act on it. The acknowledgement
   that consumes is `markDelivered`: it **deletes non-persistent** messages and sets
   `\Seen` on **persistent** ones. (Before 2026-09 the private mailbox deleted
   non-persistent messages *during* retrieval, before the response was written, so a
   dropped connection or an oversized page lost them.) On the private/shared write
   path there is no age-based pruning either (only `public.go` calls
   `EnforceRetentionPolicy`). Persistent messages sit until the client **explicitly
   deletes** them — or until the mailbox hits `storage.max_messages_per_mailbox`
   (default **1000**) and rejects everything.
4. **Retrieval is paged.** A retrieve returns at most `maxMessages` (default 100,
   cap 1000) and never more than fits one 10 MB frame. `hasMore` in the response
   metadata says a further page exists; fetch it with `fromSequence` set past the
   last message's `sequenceNumber`. The Go client exposes this as `RetrievePage`.

Hold those three and the gotchas below are corollaries.

---

## Gotcha 1 — Consuming a message does not delete it. You must delete it.

**Symptom we hit:** after a lot of syncing, *all* delivery failed with
`deliver message: mailbox full: 1000/1000 messages`, in **both** directions, and
sync silently stalled. Nothing was obviously broken; the mailbox was just full of
messages we thought we'd "consumed."

**Why:** we drove the inbox with `retrieveMessages` (`RetrieveMessages`, MAA) and
advanced only a *local* read cursor. `retrieveMessages` leaves the messages on the
server. They accumulated to the 1000 cap (`private.go:45`, `shared.go:55` →
`MailboxFullError`), after which every `sendMessage` bounced. Because a full inbox
rejects the *sender*, and both peers' inboxes filled, **neither** direction could
deliver — a two-sided deadlock that looks like "sync is dead."

**Fix (client side):** after you have durably consumed a message, **delete it** —
`deleteMessages(messageIds)` (MAA `0x40`, immediate) or flag `\Deleted` +
`expunge`. Retrieve returns each message's `messageId`; keep it and delete once the
payload is safely applied. Treat the mailbox as a queue you drain, not a log.

**Design note for maintainers:** the "retrieve ≠ consume" contract is IMAP-correct,
but it is a sharp edge for anyone modeling the mailbox as a delivery queue. A worked
example in the README/client docs, and/or an *optional* delete-on-retrieve or
server-side prune-below-acked-cursor, would have saved us the whole investigation.
See also [`SUMI_DOCUMENT_SYNC_REQUIREMENTS.md`](./SUMI_DOCUMENT_SYNC_REQUIREMENTS.md)
Finding B (the cap is invisible to a client that isn't its owner).

---

## Gotcha 2 — "no write access to shared mailbox" is an *authorization* failure, and it hides behind "mailbox full"

**Symptom we hit:** after we fixed Gotcha 1 and the mailboxes drained, one
direction (peer → owner) *still* failed — now with
`unauthorized: no write access to shared mailbox`. The other direction worked fine.

**Why (two parts):**

- The recipient's inbox for that folder existed as a **shared** mailbox, and the
  sending peer had **no write grant**, so `shared.go` rejected it.
- The reason we saw *"full"* before and *"unauthorized"* after is the **check
  order**: `StoreMessage` checks write-access **before** it checks capacity
  (`shared.go:35-58`). While the mailbox was full, an *authorized* sender got
  `MailboxFullError`; once drained, the *unauthorized* sender finally reached — and
  failed — the ACL check. One failure masked the other. If you're debugging, don't
  assume a changed error message means you fixed the first problem; you may have
  merely uncovered the second.

**Fix (client side):** for a mailbox a peer must write to, the **owner** must grant
that peer write access: `grantAccess(folder, type: shared, peer, AccessMode.readWrite)`.
Grant the *specific* peer — least-privilege, not open.

---

## Gotcha 3 — To have an ACL'd inbox a peer can write to, the OWNER must create it *shared* before the peer's first send

This is the subtle one, and it's a **race**.

Because delivery auto-creates a missing mailbox as **private/open** (Gotcha 2's
default), the *first* thing to touch a folder decides its type. If the peer's first
`sendMessage` lands before the owner has set the folder up, the owner's inbox is
created **private (open)** — functional, but not ACL-restricted. If the owner
creates it **shared + grants the peer** first, it's least-privilege from the start.

So the deterministic recipe for a least-privilege inbox is:

> The **owner** calls `createMailbox(folder, type: shared)` + `grantAccess(folder,
> shared, peer, readWrite)` **before** the peer can deliver to it — e.g. at the
> moment the sync relationship is established (share accepted / device paired),
> not lazily on first use.

For an **offerer/sharer** this is easy: it establishes the relationship first, so it
wins the create race deterministically. For a **joiner**, the offerer may push
eagerly the instant it shares — auto-creating the joiner's inbox private before the
joiner sets it up. Fully closing *that* window needs the offerer to defer its first
push until the joiner signals its inbox is ready (a small handshake). We accept a
private/open joiner inbox as a bounded residual (exploiting it needs the secret
folder/vault id, and all our payloads are sealed regardless).

**Design note for maintainers:** a *delivery option that creates the recipient's
mailbox shared-by-default* (or a "require existing mailbox / do not auto-create"
flag on send) would let clients make inboxes ACL'd deterministically without a
create race or a handshake.

---

## Gotcha 4 — Do NOT try to "convert" a mailbox's type by delete + recreate

Having lost the Gotcha 3 race, our first instinct was: detect the private inbox,
delete it, recreate it as shared. **Don't.** We validated this against a real relay
and it was unsafe on two counts:

- **It can drop delivered messages.** Deleting the mailbox discards anything the
  peer already delivered but you haven't consumed; the sender's own dedupe/ledger
  won't re-send it → silent data loss.
- **It's unreliable.** The delivery layer caches mailboxes (`delivery.go` mailbox
  cache), so a delete-then-recreate raced with the cache and left a "converted"
  mailbox that *still accepted a stranger* in our test. A security control that is
  only sometimes in force is worse than an honestly-open one.

**Fix:** get the type right at **creation** (Gotcha 3). If you lost the race, prefer
leaving the inbox as-is (open, but functional and sealed) over a flaky convert.

---

## Gotcha 5 — `getMailboxInfo` / `deleteMailbox` are keyed by *type*, so you must know the type to ask

The admin address includes the mailbox type
(`getMailboxInfo({folderPath, type})`, `deleteMailbox({folderPath, type})` —
`pkg`/client `mailbox_manager`). A lookup with the wrong type returns *not found*,
so you can't ask "what type is this folder?" directly — you probe (try `shared`,
then `private`) and infer from which one exists. `MailboxInfo` does carry
`messageCount`, which is handy for "is it empty?" decisions.

**Design note for maintainers:** a type-agnostic `describeFolder(path)` returning
`{type, messageCount, …}` (or including the type in a not-found response) would make
client-side reconciliation straightforward.

---

## A client-integration checklist (what we wish we'd had on day one)

1. **Drain what you consume.** After applying a retrieved message, `deleteMessages`
   it. Never let an inbox grow unbounded — it will hit 1000 and deadlock delivery
   *both ways*.
2. **Own your inbox's type.** If a peer must write to you, create the folder as a
   **shared** mailbox and `grantAccess(peer, readWrite)` **before** the peer's first
   send. Grant the specific peer, not the world.
3. **Read the error precisely.** `mailbox full` = capacity (drain it).
   `no write access to shared mailbox` = missing grant (owner must grant). They can
   mask each other; fixing one can reveal the other.
4. **Don't delete-and-recreate to change a type.** Get it right at creation.
5. **Don't assume retrieve prunes, or that age-retention will save you** on
   private/shared mailboxes — it won't; deletion is your job.
6. **Persist your send-dedupe state.** (Client-side, but it interacts badly here: an
   in-memory "what have I already sent" ledger that resets on restart re-sends the
   whole vault, which is what filled our mailboxes to the cap so fast. Persist it.)

---

## Source anchors

- **Type fixed at creation / auto-create:** `internal/mda/delivery.go:139-153`
  (`getMailbox` → `GetOrCreateMailbox`, instantiates by stored `record.Type`).
- **Private = open (no sender check):** `internal/mda/mailboxes/private.go:36-37`
  (recipient-is-owner check only).
- **Shared/Public = ACL (write-grant required), checked BEFORE capacity:**
  `internal/mda/mailboxes/shared.go:34-58`, `public.go:34-49`.
- **Capacity cap → `MailboxFullError`:** `private.go:45`, `shared.go:55`;
  `storage.max_messages_per_mailbox` (default 1000).
- **Retention only on the public write path:** `public.go:60`
  (`EnforceRetentionPolicy`); not called from private/shared `StoreMessage`.
- **Access modes:** `internal/core/mailbox.go:49-51`
  (`AccessReadOnly` / `AccessWriteOnly` / `AccessReadWrite`).
- **Delete / expunge (MAA):** message types `MsgTypeExpunge 0x3E`,
  `MsgTypeDeleteMessages 0x40` (`internal/core/types.go:26,28`); owner-only expunge
  guard at `internal/protocol/maa/handler.go:314`.
- **Admin ops keyed by type:** `getMailboxInfo` / `deleteMailbox` / `createMailbox`
  / `grantAccess` (address carries `type`); server handler
  `handleGetMailboxInfo` returns `messageCount`
  (`internal/protocol/mma/handler.go:569`, count at `:605`).

---

*Written from the sumi side after getting bidirectional device sync working. The
short version: **drain what you consume; own your inbox's type before your peer
writes to it; and read "full" vs "unauthorized" as two different problems.***
