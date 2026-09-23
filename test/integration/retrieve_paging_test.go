package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

// Retrieval used to delete every non-persistent message it returned, before
// the response was written. A client that disconnected mid-read, or whose
// page did not fit in a frame, lost its mail. Now reading changes nothing:
// the same messages come back until the owner marks them delivered.
func TestRetrieveIsNotConsumption(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("still here")); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	first, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("first retrieve: %v", err)
	}
	second, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("second retrieve: %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("retrieves returned %d then %d messages; want 1 and 1 — reading consumed the message", len(first), len(second))
	}
	if first[0].MessageID != second[0].MessageID {
		t.Errorf("second retrieve returned a different message")
	}
}

// Marking delivered is the acknowledgement that consumes a non-persistent
// message; a persistent one is kept and flagged \Seen instead.
func TestMarkDeliveredConsumesNonPersistentAndFlagsPersistent(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queued, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("queue entry"))
	if err != nil {
		t.Fatalf("send non-persistent: %v", err)
	}
	kept, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("keep me"), client.WithPersistent(true))
	if err != nil {
		t.Fatalf("send persistent: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	ack, err := recipient.MarkDelivered(ctx, []string{queued.MessageID, kept.MessageID})
	if err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	if ack.UpdatedCount != 2 {
		t.Errorf("updated count = %d; want 2 (one consumed, one flagged)", ack.UpdatedCount)
	}

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve after ack: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages after ack; want only the persistent one", len(msgs))
	}
	if msgs[0].MessageID != kept.MessageID {
		t.Errorf("surviving message is %s; want the persistent one %s", msgs[0].MessageID, kept.MessageID)
	}
	if msgs[0].MsgFlags&core.MsgFlagSeen == 0 {
		t.Errorf("persistent message flags = %v; want \\Seen set", msgs[0].MsgFlags)
	}
}

// A page is cut at the requested count and says so, and the next page picks
// up where it left off.
func TestRetrievePagesByCount(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const total = 5
	for i := 0; i < total; i++ {
		if _, err := sender.SendMessage(ctx, recipient.PeerID(), []byte{byte(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	seen := map[string]bool{}
	var from uint64
	pages := 0
	for {
		page, err := recipient.RetrievePage(ctx, client.WithMaxMessages(2), client.WithFromSequence(from))
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		if len(page.Messages) > 2 {
			t.Fatalf("page %d has %d messages; asked for 2", pages, len(page.Messages))
		}
		for _, m := range page.Messages {
			if seen[m.MessageID] {
				t.Fatalf("message %s returned twice across pages", m.MessageID)
			}
			seen[m.MessageID] = true
			from = m.SequenceNumber + 1
		}
		if !page.HasMore {
			break
		}
		if pages > total {
			t.Fatalf("hasMore never cleared after %d pages", pages)
		}
	}
	if len(seen) != total {
		t.Errorf("paged through %d distinct messages; want %d", len(seen), total)
	}
	if pages != 3 {
		t.Errorf("took %d pages of 2 for %d messages; want 3", pages, total)
	}
}

// A page is also cut at the frame size. Two messages that together exceed
// the 10 MB frame used to produce a response the writer refused — after the
// messages had already been deleted. Now the first page carries what fits and
// flags that more remain.
func TestRetrievePagesByFrameSize(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 4 MB raw is ~5.4 MB once base64'd into JSON; two of them do not fit in
	// one 10 MB frame, and each on its own does.
	payload := bytes.Repeat([]byte{0xAB}, 4*1024*1024)
	for i := 0; i < 2; i++ {
		if _, err := sender.SendMessage(ctx, recipient.PeerID(), payload); err != nil {
			t.Fatalf("send large %d: %v", i, err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	first, err := recipient.RetrievePage(ctx)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Messages) != 1 {
		t.Fatalf("first page has %d messages; want 1 (the second does not fit the frame)", len(first.Messages))
	}
	if !first.HasMore {
		t.Fatalf("first page does not report more, but a second message is waiting")
	}
	if !bytes.Equal(first.Messages[0].Payload, payload) {
		t.Errorf("first payload corrupted: got %d bytes", len(first.Messages[0].Payload))
	}

	second, err := recipient.RetrievePage(ctx, client.WithFromSequence(first.Messages[0].SequenceNumber+1))
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Messages) != 1 || second.HasMore {
		t.Fatalf("second page: %d messages, hasMore=%v; want 1 and false", len(second.Messages), second.HasMore)
	}
	if second.Messages[0].MessageID == first.Messages[0].MessageID {
		t.Errorf("second page repeated the first message")
	}
}
