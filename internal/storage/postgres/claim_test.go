package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// Concurrent claims take different messages: every stored message is handed
// out exactly once, the rest find nothing, and the counters end at zero.
func TestConcurrentClaimsNeverShareAMessage(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	addr, err := core.NewMailboxAddress(owner, "signal/otk", core.MailboxClaim)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rec, err := store.GetOrCreateMailbox(ctx, addr, 1000, 30, nil)
	if err != nil {
		t.Fatal(err)
	}

	const items, claimers = 20, 40
	for i := 0; i < items; i++ {
		msg := &core.Message{
			MessageID:       fmt.Sprintf("%s-otk-%d", owner, i),
			SenderPeerID:    owner.String(),
			RecipientPeerID: owner.String(),
			FolderPath:      "signal/otk",
			Payload:         []byte{byte(i)},
			ExpiryTimestamp: time.Now().Add(time.Hour).UnixMilli(),
		}
		if _, err := store.StoreMessage(ctx, rec, msg); err != nil {
			t.Fatal(err)
		}
	}

	got := make([]*core.Message, claimers)
	errs := make([]error, claimers)
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(claimers)
	for i := 0; i < claimers; i++ {
		go func(i int) {
			defer done.Done()
			start.Wait()
			got[i], errs[i] = store.ClaimMessage(ctx, rec.ID)
		}(i)
	}
	start.Done()
	done.Wait()

	seen := map[string]bool{}
	for i, msg := range got {
		if errs[i] != nil {
			t.Fatalf("claimer %d: %v", i, errs[i])
		}
		if msg == nil {
			continue
		}
		if seen[msg.MessageID] {
			t.Fatalf("message %s claimed twice", msg.MessageID)
		}
		seen[msg.MessageID] = true
	}
	if len(seen) != items {
		t.Fatalf("%d messages claimed, want %d", len(seen), items)
	}
	if n, err := store.GetMessageCount(ctx, rec.ID); err != nil || n != 0 {
		t.Fatalf("message count %d, %v after claiming all; want 0", n, err)
	}
}
