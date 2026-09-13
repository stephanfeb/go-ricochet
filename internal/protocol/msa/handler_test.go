package msa_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/protocoltest"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
)

func encode(t *testing.T, msg *core.Message) []byte {
	t.Helper()
	data, err := msg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSubmitDeliversToTheRecipientsFolder(t *testing.T) {
	env := protocoltest.New(t)
	p := msa.NewPipeline(env.Logger, env.Pool, env.Registry)
	sender, recipient := protocoltest.PeerID(t), protocoltest.PeerID(t)

	msg := core.NewMessageWithDefaultExpiry(sender, recipient, []byte("hello"))
	msg.FolderPath = "work"
	ack := protocoltest.Decode[core.StoreAck](t, protocoltest.Exchange(t, p, sender, encode(t, msg)))
	if !ack.Success || ack.MessageID != msg.MessageID {
		t.Fatalf("ack = %+v, want success for %s", ack, msg.MessageID)
	}

	stored := env.Store.Messages(env.Mailbox(t, recipient, "work").ID)
	if len(stored) != 1 || stored[0].MessageID != msg.MessageID || stored[0].SequenceNumber != 1 {
		t.Errorf("stored = %+v, want the submitted message at sequence 1", stored)
	}
}

// A submission whose sender is not the connected peer is a forwarded one,
// and forwarding is off by default: refused as a 403 in the ack, not
// delivered with a sender the server never verified.
func TestSubmitRefusesAForgedSender(t *testing.T) {
	env := protocoltest.New(t)
	p := msa.NewPipeline(env.Logger, env.Pool, env.Registry)
	sender, recipient, forger := protocoltest.PeerID(t), protocoltest.PeerID(t), protocoltest.PeerID(t)

	msg := core.NewMessageWithDefaultExpiry(sender, recipient, []byte("forged"))
	ack := protocoltest.Decode[core.StoreAck](t, protocoltest.Exchange(t, p, forger, encode(t, msg)))
	if ack.Success || ack.Status != wire.StatusForbidden {
		t.Errorf("ack = %+v, want a 403 refusal", ack)
	}
	if n := len(env.Store.AllMessages()); n != 0 {
		t.Errorf("%d messages stored from a forged submission", n)
	}
}

func TestSubmitClassifiesRefusals(t *testing.T) {
	env := protocoltest.New(t, func(cfg *core.ServerConfig) { cfg.MaxMessagesPerMailbox = 1 })
	p := msa.NewPipeline(env.Logger, env.Pool, env.Registry)
	sender, recipient := protocoltest.PeerID(t), protocoltest.PeerID(t)

	submit := func(msg *core.Message) *core.StoreAck {
		return protocoltest.Decode[core.StoreAck](t, protocoltest.Exchange(t, p, sender, encode(t, msg)))
	}

	if ack := submit(core.NewMessageWithDefaultExpiry(sender, recipient, []byte("first"))); !ack.Success {
		t.Fatalf("first submission refused: %+v", ack)
	}
	if ack := submit(core.NewMessageWithDefaultExpiry(sender, recipient, []byte("second"))); ack.Success || ack.Status != wire.StatusInsufficientStorage {
		t.Errorf("submission into a full mailbox: ack = %+v, want 507", ack)
	}

	expired := core.NewMessageWithDefaultExpiry(sender, recipient, []byte("stale"))
	expired.ExpiryTimestamp = time.Now().Add(-time.Hour).UnixMilli()
	if ack := submit(expired); ack.Success || ack.Status != wire.StatusBadRequest {
		t.Errorf("expired submission: ack = %+v, want 400", ack)
	}

	if ack := protocoltest.Decode[core.StoreAck](t, protocoltest.Exchange(t, p, sender, []byte("not json"))); ack.Success || ack.Status != wire.StatusBadRequest {
		t.Errorf("unparseable submission: ack = %+v, want 400", ack)
	}
}

func TestBatchSubmitAnswersEveryMessage(t *testing.T) {
	env := protocoltest.New(t, func(cfg *core.ServerConfig) { cfg.MaxMessagesPerMailbox = 2 })
	p := msa.NewBatchPipeline(env.Logger, env.Pool, env.Registry)
	sender, recipient, other := protocoltest.PeerID(t), protocoltest.PeerID(t), protocoltest.PeerID(t)

	forged := core.NewMessageWithDefaultExpiry(other, recipient, []byte("forged"))
	msgs := []json.RawMessage{
		encode(t, core.NewMessageWithDefaultExpiry(sender, recipient, []byte("one"))),
		encode(t, forged),
		encode(t, core.NewMessageWithDefaultExpiry(sender, recipient, []byte("two"))),
		encode(t, core.NewMessageWithDefaultExpiry(sender, recipient, []byte("three"))),
		json.RawMessage(`[]`),
	}
	resp := protocoltest.Decode[msa.BatchSubmitResponse](t, protocoltest.Exchange(t, p, sender, msa.BatchSubmitRequest{Messages: msgs}))

	if resp.Total != 5 || resp.Accepted != 2 || len(resp.Acks) != 5 {
		t.Fatalf("resp = total %d accepted %d acks %d, want 5/2/5", resp.Total, resp.Accepted, len(resp.Acks))
	}
	want := []struct {
		success bool
		status  int
	}{{true, 0}, {false, wire.StatusForbidden}, {true, 0}, {false, wire.StatusInsufficientStorage}, {false, 0}}
	for i, w := range want {
		ack := resp.Acks[i]
		if ack.Success != w.success || (w.status != 0 && ack.Status != w.status) {
			t.Errorf("ack %d = %+v, want success %v status %d", i, ack, w.success, w.status)
		}
	}
	if resp.StatusCode() != wire.StatusMultiStatus {
		t.Errorf("status = %d, want 207 for a mixed batch", resp.StatusCode())
	}
}

func TestBatchSubmitRefusesTheWholeBatchWhenMalformed(t *testing.T) {
	env := protocoltest.New(t)
	p := msa.NewBatchPipeline(env.Logger, env.Pool, env.Registry)
	sender := protocoltest.PeerID(t)

	cases := map[string]any{
		"empty":     msa.BatchSubmitRequest{},
		"oversized": msa.BatchSubmitRequest{Messages: make([]json.RawMessage, 101)},
		"garbage":   []byte("nope"),
	}
	for name, req := range cases {
		if r, ok := req.(msa.BatchSubmitRequest); ok {
			for i := range r.Messages {
				r.Messages[i] = json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))
			}
		}
		resp := protocoltest.Decode[msa.BatchSubmitResponse](t, protocoltest.Exchange(t, p, sender, req))
		if resp.Total != 0 || len(resp.Acks) != 1 || resp.Acks[0].Success {
			t.Errorf("%s: resp = %+v, want one refusal and no total", name, resp)
		}
	}
}
