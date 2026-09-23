package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// The first delivery to a mailbox creates it. When several first deliveries
// arrive together, they used to race a SELECT against an INSERT: every one
// found nothing, every one inserted, and all but the winner failed on the
// unique constraint, so the sender saw a refused submission for a mailbox
// that now existed. The create is now one upsert, and every caller gets the
// same row.
func TestConcurrentFirstDeliveriesShareOneMailbox(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	addr, err := core.NewMailboxAddress(owner, "race", core.MailboxPrivate)
	if err != nil {
		t.Fatal(err)
	}

	const callers = 24
	ids := make([]int64, callers)
	errs := make([]error, callers)
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer done.Done()
			start.Wait()
			rec, err := store.GetOrCreateMailbox(context.Background(), addr, 1000, 30, nil)
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = rec.ID
		}(i)
	}
	start.Done()
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	for i, id := range ids {
		if id != ids[0] {
			t.Errorf("caller %d got mailbox %d, caller 0 got %d", i, id, ids[0])
		}
	}

	var rows int
	if err := store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM mailboxes WHERE owner_peer_id = $1 AND folder_path = $2`,
		owner.String(), "race").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("%d mailbox rows, want 1", rows)
	}
}

// The settings passed to a later call must not overwrite the ones the
// mailbox was created with; the upsert's no-op update has to stay a no-op.
func TestGetOrCreateKeepsExistingSettings(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	addr, err := core.NewMailboxAddress(owner, "keep", core.MailboxPrivate)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.GetOrCreateMailbox(context.Background(), addr, 500, 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.GetOrCreateMailbox(context.Background(), addr, 9999, 99, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID || again.MaxMessages != 500 || again.RetentionDays != 7 {
		t.Errorf("second call changed the mailbox: %+v", again)
	}
}
