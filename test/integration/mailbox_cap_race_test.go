package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// The cap is enforced under the mailbox row lock. It used to be a count read
// before the store, so deliveries racing for the last slots were all admitted
// and a mailbox could hold more than its cap.
func TestParallelDeliveriesNeverExceedCap(t *testing.T) {
	const cap, senders = 5, 40

	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.MaxMessagesPerMailbox = cap
	})
	cl := newTestClient(t, server)
	self := cl.PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		admitted int
		refused  int
		other    []error
		start    = make(chan struct{})
	)
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := cl.SendMessage(ctx, self, []byte(fmt.Sprintf("racer %d", i)))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				other = append(other, err)
				return
			}
			if res.Success {
				admitted++
				return
			}
			var full *client.MailboxFullError
			if errors.As(res.Err(), &full) {
				refused++
			} else {
				other = append(other, res.Err())
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	if admitted != cap {
		t.Errorf("admitted %d messages into a mailbox capped at %d (refused %d)", admitted, cap, refused)
	}

	msgs, err := cl.RetrieveMessages(ctx, client.WithMaxMessages(senders))
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != cap {
		t.Errorf("mailbox holds %d messages, want %d", len(msgs), cap)
	}
}

// A mailbox with a retention_count below its cap is a rolling window. Pruning
// moved from every store into maintenance, so a busy window reaches the cap
// between sweeps; the full path prunes it and the store goes through.
func TestRollingWindowMailboxNeverFills(t *testing.T) {
	const cap, keep, sends = 4, 2, 12

	server := newTestServer(t)
	cl := newTestClient(t, server)
	self := cl.PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := cl.CreateMailbox(ctx, "window", wire.MailboxPrivate,
		client.WithMailboxMaxMessages(cap), client.WithRetentionCount(keep)); err != nil {
		t.Fatalf("create mailbox: %v", err)
	}

	for i := 0; i < sends; i++ {
		res, err := cl.SendMessage(ctx, self, []byte(fmt.Sprintf("post %d", i)), client.WithFolderPath("window"))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if !res.Success {
			t.Fatalf("send %d into a rolling window was refused: %s", i, res.ErrorMessage)
		}
	}

	msgs, err := cl.RetrieveMessages(ctx, client.WithRetrieveFolderPath("window"), client.WithMaxMessages(sends))
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) > cap {
		t.Errorf("window holds %d messages, cap is %d", len(msgs), cap)
	}
	if len(msgs) == 0 {
		t.Error("window is empty after twelve posts")
	}
}
