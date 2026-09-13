package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

func newCappedMailbox(t *testing.T, store *PostgresStorage, owner peer.ID, folder string, max int) *storage.MailboxRecord {
	t.Helper()
	addr, err := core.NewMailboxAddress(owner, folder, core.MailboxPrivate)
	if err != nil {
		t.Fatalf("mailbox address: %v", err)
	}
	mailbox, err := store.GetOrCreateMailbox(context.Background(), addr, max, 30, nil)
	if err != nil {
		t.Fatalf("create mailbox: %v", err)
	}
	return mailbox
}

func testMessage(owner peer.ID, mailboxID int64, i int) *core.Message {
	return &core.Message{
		MessageID:        fmt.Sprintf("count-%d-%d", mailboxID, i),
		RecipientPeerID:  owner.String(),
		SenderPeerID:     owner.String(),
		Payload:          []byte("x"),
		Priority:         core.PriorityNormal,
		CreatedTimestamp: time.Now().UnixMilli(),
		ExpiryTimestamp:  time.Now().Add(time.Hour).UnixMilli(),
	}
}

func rowCount(t *testing.T, store *PostgresStorage, mailboxID int64) int {
	t.Helper()
	var n int
	if err := store.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM stored_messages WHERE mailbox_id = $1`, mailboxID).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func counter(t *testing.T, store *PostgresStorage, mailboxID int64) int {
	t.Helper()
	n, err := store.GetMessageCount(context.Background(), mailboxID)
	if err != nil {
		t.Fatalf("message count: %v", err)
	}
	return n
}

// The cap is enforced under the mailbox row lock, so however many deliveries
// race for the last slots, exactly max are admitted.
func TestConcurrentStoresNeverExceedCap(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	const max, senders = 5, 40
	mailbox := newCappedMailbox(t, store, owner, "inbox", max)
	ctx := context.Background()

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		admitted   int
		refused    int
		unexpected []error
		start      = make(chan struct{})
	)
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := store.StoreMessage(ctx, mailbox, testMessage(owner, mailbox.ID, i))
			mu.Lock()
			defer mu.Unlock()
			var full *storage.MailboxFullError
			switch {
			case err == nil:
				admitted++
			case errors.As(err, &full):
				refused++
				if full.Current != max || full.Max != max {
					unexpected = append(unexpected, fmt.Errorf("full error reports %d/%d", full.Current, full.Max))
				}
			default:
				unexpected = append(unexpected, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(unexpected) > 0 {
		t.Fatalf("unexpected errors: %v", unexpected)
	}
	if admitted != max || refused != senders-max {
		t.Fatalf("admitted %d, refused %d; want %d and %d", admitted, refused, max, senders-max)
	}
	if n := rowCount(t, store, mailbox.ID); n != max {
		t.Errorf("stored rows = %d, want %d", n, max)
	}
	if n := counter(t, store, mailbox.ID); n != max {
		t.Errorf("message_count = %d, want %d", n, max)
	}
}

// Every delete path lowers the counter through the statement-level trigger,
// so the cap check and the row count never disagree.
func TestMessageCountFollowsDeletes(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	mailbox := newCappedMailbox(t, store, owner, "inbox", 100)
	ctx := context.Background()

	ids := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		msg := testMessage(owner, mailbox.ID, i)
		if _, err := store.StoreMessage(ctx, mailbox, msg); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
		ids = append(ids, msg.MessageID)
	}
	expect := func(step string, want int) {
		t.Helper()
		if n := counter(t, store, mailbox.ID); n != want {
			t.Fatalf("after %s: message_count = %d, want %d", step, n, want)
		}
		if n := rowCount(t, store, mailbox.ID); n != want {
			t.Fatalf("after %s: rows = %d, want %d", step, n, want)
		}
		var counted, summed int64
		if err := store.pool.QueryRow(ctx, `
			SELECT m.message_bytes, COALESCE((SELECT SUM(octet_length(payload)) FROM stored_messages WHERE mailbox_id = m.id), 0)
			FROM mailboxes m WHERE m.id = $1`, mailbox.ID).Scan(&counted, &summed); err != nil {
			t.Fatalf("after %s: bytes: %v", step, err)
		}
		if counted != summed {
			t.Fatalf("after %s: message_bytes = %d, rows sum to %d", step, counted, summed)
		}
	}
	expect("six stores", 6)

	if err := store.DeleteMessages(ctx, ids[:1]); err != nil {
		t.Fatalf("delete: %v", err)
	}
	expect("DeleteMessages", 5)

	if _, err := store.DeleteOwnedMessages(ctx, owner, ids[1:2]); err != nil {
		t.Fatalf("delete owned: %v", err)
	}
	expect("DeleteOwnedMessages", 4)

	if _, err := store.pool.Exec(ctx,
		`UPDATE stored_messages SET expires_at = NOW() - INTERVAL '1 minute' WHERE message_id = $1`, ids[2]); err != nil {
		t.Fatalf("expire: %v", err)
	}
	if _, err := store.DeleteExpiredMessages(ctx); err != nil {
		t.Fatalf("delete expired: %v", err)
	}
	expect("DeleteExpiredMessages", 3)

	if _, err := store.pool.Exec(ctx,
		`UPDATE stored_messages SET flags_bitmap = flags_bitmap | $1 WHERE message_id = $2`,
		uint32(core.MsgFlagDeleted), ids[3]); err != nil {
		t.Fatalf("flag deleted: %v", err)
	}
	if _, err := store.ExpungeMailbox(ctx, mailbox.ID); err != nil {
		t.Fatalf("expunge: %v", err)
	}
	expect("ExpungeMailbox", 2)

	one := 1
	mailbox.RetentionCount = &one
	if err := store.EnforceRetentionPolicy(ctx, mailbox); err != nil {
		t.Fatalf("retention: %v", err)
	}
	expect("EnforceRetentionPolicy", 1)

	if _, err := store.EnforceAllRetention(ctx); err != nil {
		t.Fatalf("all retention: %v", err)
	}
	expect("EnforceAllRetention", 1)

	// The counter is what admits the next store, so a delete must reopen
	// a full mailbox.
	small := newCappedMailbox(t, store, owner, "small", 1)
	first := testMessage(owner, small.ID, 0)
	if _, err := store.StoreMessage(ctx, small, first); err != nil {
		t.Fatalf("fill: %v", err)
	}
	var full *storage.MailboxFullError
	if _, err := store.StoreMessage(ctx, small, testMessage(owner, small.ID, 1)); !errors.As(err, &full) {
		t.Fatalf("second store into a cap of one: err = %v, want full", err)
	}
	if err := store.DeleteMessages(ctx, []string{first.MessageID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.StoreMessage(ctx, small, testMessage(owner, small.ID, 2)); err != nil {
		t.Fatalf("store after delete: %v", err)
	}
}

// The insert trigger that recorded the access is gone; the sequence UPDATE
// records it instead.
func TestStoreMessageAdvancesLastAccess(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	mailbox := newCappedMailbox(t, store, owner, "inbox", 10)
	ctx := context.Background()

	if _, err := store.pool.Exec(ctx,
		`UPDATE mailboxes SET last_access_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, mailbox.ID); err != nil {
		t.Fatalf("age mailbox: %v", err)
	}
	if _, err := store.StoreMessage(ctx, mailbox, testMessage(owner, mailbox.ID, 0)); err != nil {
		t.Fatalf("store: %v", err)
	}
	got, err := store.FindMailbox(ctx, owner, "inbox")
	if err != nil || got == nil {
		t.Fatalf("find mailbox: %v", err)
	}
	if age := time.Since(got.LastAccessAt); age > time.Minute {
		t.Errorf("last_access_at is %v old after a store; want just now", age)
	}
	if got.MessageCount != 1 {
		t.Errorf("record MessageCount = %d, want 1", got.MessageCount)
	}
}

// Uncapped mailboxes are not refused however much they hold.
func TestZeroCapMeansUncapped(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	mailbox := newCappedMailbox(t, store, owner, "inbox", 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := store.StoreMessage(ctx, mailbox, testMessage(owner, mailbox.ID, i)); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
}

// The maintenance sweep repairs a counter that has drifted from the rows.
func TestReconcileMessageCountsRepairsDrift(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	mailbox := newCappedMailbox(t, store, owner, "inbox", 10)
	empty := newCappedMailbox(t, store, owner, "empty", 10)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := store.StoreMessage(ctx, mailbox, testMessage(owner, mailbox.ID, i)); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	if _, err := store.pool.Exec(ctx,
		`UPDATE mailboxes SET message_count = 99 WHERE id = ANY($1)`, []int64{mailbox.ID, empty.ID}); err != nil {
		t.Fatalf("corrupt counters: %v", err)
	}

	drifted, err := store.ReconcileMessageCounts(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if drifted < 2 {
		t.Errorf("reconcile repaired %d mailboxes, want at least the 2 corrupted here", drifted)
	}
	if n := counter(t, store, mailbox.ID); n != 2 {
		t.Errorf("message_count = %d, want 2", n)
	}
	if n := counter(t, store, empty.ID); n != 0 {
		t.Errorf("empty mailbox message_count = %d, want 0", n)
	}
}
