package maa_test

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/protocol/maa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/protocoltest"
	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
)

func setup(t *testing.T) (*protocoltest.Env, func(from peer.ID, req any) []byte, peer.ID, peer.ID) {
	t.Helper()
	env := protocoltest.New(t)
	p := maa.NewPipeline(env.Logger, env.Pool, env.Registry)
	owner, sender := protocoltest.PeerID(t), protocoltest.PeerID(t)
	call := func(from peer.ID, req any) []byte { return protocoltest.Exchange(t, p, from, req) }
	return env, call, owner, sender
}

func TestRetrieveReturnsTheOwnersInbox(t *testing.T) {
	env, call, owner, sender := setup(t)
	env.Deliver(t, sender, owner, "inbox", []byte("first"))
	env.Deliver(t, sender, owner, "inbox", []byte("second"))
	env.Deliver(t, sender, owner, "other", []byte("elsewhere"))

	// The operation defaults to retrieve for clients that predate the
	// operationType field.
	page := protocoltest.DecodeRetrieve(t, call(owner, core.RetrieveRequest{PeerID: owner.String()}))
	if page.MessageCount != 2 || len(page.Messages) != 2 || page.HasMore {
		t.Fatalf("page = %d messages, hasMore %v; want the two in inbox", page.MessageCount, page.HasMore)
	}
	if string(page.Messages[0].Payload) != "first" || page.Messages[0].SequenceNumber != 1 {
		t.Errorf("first message = %q at %d, want \"first\" at 1", page.Messages[0].Payload, page.Messages[0].SequenceNumber)
	}

	one := 1
	from := uint64(2)
	page = protocoltest.DecodeRetrieve(t, call(owner, core.RetrieveRequest{PeerID: owner.String(), MaxMessages: &one}))
	if len(page.Messages) != 1 || !page.HasMore {
		t.Errorf("page of one: %d messages, hasMore %v", len(page.Messages), page.HasMore)
	}
	page = protocoltest.DecodeRetrieve(t, call(owner, core.RetrieveRequest{PeerID: owner.String(), FromSequence: &from}))
	if len(page.Messages) != 1 || string(page.Messages[0].Payload) != "second" {
		t.Errorf("from sequence 2: %d messages", len(page.Messages))
	}
	page = protocoltest.DecodeRetrieve(t, call(owner, core.RetrieveRequest{PeerID: owner.String(), FolderPath: "other"}))
	if len(page.Messages) != 1 || string(page.Messages[0].Payload) != "elsewhere" {
		t.Errorf("folder other: %d messages", len(page.Messages))
	}
}

func TestRetrieveOfAnotherPeersInboxIsRefusedOrNotFound(t *testing.T) {
	env, call, owner, sender := setup(t)
	stranger := protocoltest.PeerID(t)

	// The owner reading an inbox nothing has been delivered to gets an
	// empty page; a stranger gets a 404 rather than a mailbox created on
	// their behalf.
	page := protocoltest.DecodeRetrieve(t, call(owner, core.RetrieveRequest{PeerID: owner.String()}))
	if page.MessageCount != 0 {
		t.Errorf("fresh inbox holds %d messages", page.MessageCount)
	}
	resp := protocoltest.Decode[maa.ErrorResponse](t, call(stranger, core.RetrieveRequest{PeerID: owner.String()}))
	if resp.Status != wire.StatusNotFound {
		t.Errorf("stranger reading an absent inbox: %+v, want 404", resp)
	}
	if env.Store.MailboxCount() != 0 {
		t.Errorf("a read created %d mailboxes", env.Store.MailboxCount())
	}

	env.Deliver(t, sender, owner, "inbox", []byte("private"))
	resp = protocoltest.Decode[maa.ErrorResponse](t, call(stranger, core.RetrieveRequest{PeerID: owner.String()}))
	if resp.Status != wire.StatusForbidden {
		t.Errorf("stranger reading a private inbox: %+v, want 403", resp)
	}
}

func TestMarkDeliveredConsumesNonPersistentMessages(t *testing.T) {
	env, call, owner, sender := setup(t)
	kept := env.Deliver(t, sender, owner, "inbox", []byte("keep me"))
	gone := env.Deliver(t, sender, owner, "inbox", []byte("consume me"))
	mailboxID := env.Mailbox(t, owner, "inbox").ID
	env.Store.Messages(mailboxID)[0].Persistent = true

	ack := protocoltest.Decode[core.MarkDeliveredAck](t, call(owner, core.MarkDeliveredRequest{
		OperationType: "markDelivered", MessageIDs: []string{kept.MessageID, gone.MessageID}}))
	if !ack.Success || ack.UpdatedCount != 2 {
		t.Fatalf("ack = %+v, want 2 updated", ack)
	}
	left := env.Store.Messages(mailboxID)
	if len(left) != 1 || left[0].MessageID != kept.MessageID || !left[0].MsgFlags.HasFlag(core.MsgFlagSeen) {
		t.Errorf("after markDelivered: %+v, want only the persistent one, flagged seen", left)
	}

	// Another peer naming the same IDs touches nothing.
	ack = protocoltest.Decode[core.MarkDeliveredAck](t, call(protocoltest.PeerID(t), core.MarkDeliveredRequest{
		OperationType: "markDelivered", MessageIDs: []string{kept.MessageID}}))
	if ack.UpdatedCount != 0 || len(env.Store.Messages(mailboxID)) != 1 {
		t.Errorf("a stranger's markDelivered changed the owner's mailbox: %+v", ack)
	}
}

func TestUpdateFlagsIsOwnerScoped(t *testing.T) {
	env, call, owner, sender := setup(t)
	msg := env.Deliver(t, sender, owner, "inbox", []byte("flag me"))

	ack := protocoltest.Decode[core.UpdateFlagsAck](t, call(owner, core.UpdateFlagsRequest{
		OperationType: "updateFlags", MessageID: msg.MessageID, AddFlags: uint32(core.MsgFlagSeen | core.MsgFlagFlagged)}))
	if !ack.Success || ack.NewFlags == nil || *ack.NewFlags != uint32(core.MsgFlagSeen|core.MsgFlagFlagged) {
		t.Fatalf("add flags: ack = %+v", ack)
	}
	ack = protocoltest.Decode[core.UpdateFlagsAck](t, call(owner, core.UpdateFlagsRequest{
		OperationType: "updateFlags", MessageID: msg.MessageID, RemoveFlags: uint32(core.MsgFlagFlagged)}))
	if !ack.Success || *ack.NewFlags != uint32(core.MsgFlagSeen) {
		t.Errorf("remove flags: ack = %+v", ack)
	}

	// A message the caller does not own reads as not found: "exists but is
	// not yours" would confirm a guessed ID.
	ack = protocoltest.Decode[core.UpdateFlagsAck](t, call(protocoltest.PeerID(t), core.UpdateFlagsRequest{
		OperationType: "updateFlags", MessageID: msg.MessageID, AddFlags: uint32(core.MsgFlagDeleted)}))
	if ack.Success || ack.ErrorMessage != "message not found" {
		t.Errorf("stranger's updateFlags: ack = %+v, want not found", ack)
	}
}

func TestExpungeRemovesOnlyDeletedFlaggedMessages(t *testing.T) {
	env, call, owner, sender := setup(t)
	doomed := env.Deliver(t, sender, owner, "inbox", []byte("doomed"))
	env.Deliver(t, sender, owner, "inbox", []byte("stays"))
	env.Deliver(t, sender, owner, "archive", []byte("also stays"))
	env.Store.Messages(env.Mailbox(t, owner, "inbox").ID)[0].MsgFlags = core.MsgFlagDeleted

	ack := protocoltest.Decode[core.ExpungeAck](t, call(owner, core.ExpungeRequest{OperationType: "expunge", PeerID: owner.String()}))
	if !ack.Success || ack.DeletedCount != 1 {
		t.Fatalf("expunge: ack = %+v, want 1 deleted", ack)
	}
	for _, m := range env.Store.AllMessages() {
		if m.MessageID == doomed.MessageID {
			t.Error("the \\Deleted message is still stored")
		}
	}
	if n := len(env.Store.AllMessages()); n != 2 {
		t.Errorf("%d messages remain, want 2", n)
	}

	resp := protocoltest.Decode[maa.ErrorResponse](t, call(protocoltest.PeerID(t), core.ExpungeRequest{OperationType: "expunge", PeerID: owner.String()}))
	if resp.Status != wire.StatusForbidden {
		t.Errorf("expunge of another peer's mailboxes: %+v, want 403", resp)
	}
}

func TestDeleteMessagesIsOwnerScoped(t *testing.T) {
	env, call, owner, sender := setup(t)
	a := env.Deliver(t, sender, owner, "inbox", []byte("a"))
	b := env.Deliver(t, sender, owner, "inbox", []byte("b"))

	ack := protocoltest.Decode[core.DeleteMessagesAck](t, call(protocoltest.PeerID(t), core.DeleteMessagesRequest{
		OperationType: "deleteMessages", MessageIDs: []string{a.MessageID}}))
	if ack.DeletedCount != 0 {
		t.Errorf("stranger deleted %d of the owner's messages", ack.DeletedCount)
	}
	ack = protocoltest.Decode[core.DeleteMessagesAck](t, call(owner, core.DeleteMessagesRequest{
		OperationType: "deleteMessages", MessageIDs: []string{a.MessageID, "no-such-id"}}))
	if !ack.Success || ack.DeletedCount != 1 {
		t.Errorf("owner delete: ack = %+v, want 1 deleted", ack)
	}
	left := env.Store.AllMessages()
	if len(left) != 1 || left[0].MessageID != b.MessageID {
		t.Errorf("remaining = %+v, want only b", left)
	}
}

func TestUnknownOperationIsTheClientsMistake(t *testing.T) {
	_, call, owner, _ := setup(t)
	resp := protocoltest.Decode[maa.ErrorResponse](t, call(owner, map[string]any{"operationType": "teleport"}))
	if resp.Status != wire.StatusBadRequest {
		t.Errorf("unknown operation: %+v, want 400", resp)
	}
}
