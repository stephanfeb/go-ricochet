package mda

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/mda/mailboxes"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

func claimSetup(t *testing.T, mailboxType core.MailboxType, items int) (*MailboxServer, *core.MailboxAddress) {
	t.Helper()
	server := NewMailboxServer(storagetest.New(), MailboxDefaults{MaxMessages: 1000, RetentionDays: 30}, quietLogger())
	owner := test.RandPeerIDFatal(t)
	addr := &core.MailboxAddress{OwnerID: owner, Type: mailboxType, FolderPath: "signal/otk"}
	if err := server.CreateMailbox(context.Background(), addr, 1000, 30, nil); err != nil {
		t.Fatalf("create mailbox: %v", err)
	}
	for i := 0; i < items; i++ {
		msg := &core.Message{
			MessageID:       fmt.Sprintf("otk-%d", i),
			SenderPeerID:    owner.String(),
			RecipientPeerID: owner.String(),
			FolderPath:      addr.FolderPath,
			Payload:         []byte(fmt.Sprintf("key %d", i)),
		}
		if _, err := server.DeliverLocal(context.Background(), msg); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	return server, addr
}

// Each claim hands out a different item, oldest first, and an emptied
// mailbox answers with nothing rather than an error.
func TestClaimHandsOutEachItemOnce(t *testing.T) {
	server, addr := claimSetup(t, core.MailboxClaim, 3)
	claimer := test.RandPeerIDFatal(t)

	for i := 0; i < 3; i++ {
		msg, err := server.Claim(context.Background(), addr, claimer)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if msg == nil || msg.MessageID != fmt.Sprintf("otk-%d", i) {
			t.Fatalf("claim %d gave %v, want otk-%d", i, msg, i)
		}
	}
	msg, err := server.Claim(context.Background(), addr, claimer)
	if err != nil || msg != nil {
		t.Fatalf("claim from an empty mailbox gave %v, %v; want nothing", msg, err)
	}
}

// Claiming deletes: a public mailbox is not claimable, or anyone could
// delete its owner's posts.
func TestClaimRefusesOtherMailboxTypes(t *testing.T) {
	for _, mailboxType := range []core.MailboxType{core.MailboxPublic, core.MailboxShared, core.MailboxPrivate} {
		t.Run(mailboxType.String(), func(t *testing.T) {
			server, addr := claimSetup(t, mailboxType, 0)
			_, err := server.Claim(context.Background(), addr, test.RandPeerIDFatal(t))
			var unauthorized *mailboxes.UnauthorizedError
			if !errors.As(err, &unauthorized) {
				t.Fatalf("claim from a %s mailbox: %v, want unauthorized", mailboxType, err)
			}
		})
	}
}

func TestClaimFromAMissingMailboxIsNotFound(t *testing.T) {
	server := NewMailboxServer(storagetest.New(), MailboxDefaults{}, quietLogger())
	addr := &core.MailboxAddress{OwnerID: test.RandPeerIDFatal(t), FolderPath: "signal/otk"}
	_, err := server.Claim(context.Background(), addr, test.RandPeerIDFatal(t))
	var notFound *mailboxes.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("claim from a missing mailbox: %v, want not found", err)
	}
}

// Only the owner lists or fills a claim mailbox.
func TestClaimMailboxIsTheOwnersToListAndFill(t *testing.T) {
	server, addr := claimSetup(t, core.MailboxClaim, 2)
	stranger := test.RandPeerIDFatal(t)

	_, _, err := server.Retrieve(context.Background(), addr, stranger, RetrieveOpts{})
	var unauthorized *mailboxes.UnauthorizedError
	if !errors.As(err, &unauthorized) {
		t.Fatalf("stranger listing: %v, want unauthorized", err)
	}
	msgs, _, err := server.Retrieve(context.Background(), addr, addr.OwnerID, RetrieveOpts{})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("owner listing gave %d messages, %v; want 2", len(msgs), err)
	}

	_, err = server.DeliverLocal(context.Background(), &core.Message{
		MessageID:       "planted",
		SenderPeerID:    stranger.String(),
		RecipientPeerID: addr.OwnerID.String(),
		FolderPath:      addr.FolderPath,
		Payload:         []byte("not the owner's key"),
	})
	if !errors.As(err, &unauthorized) {
		t.Fatalf("stranger storing: %v, want unauthorized", err)
	}
}

// Past the per-mailbox rate a claim finds nothing, whoever asks: peer IDs
// are free, so a per-claimer bound would not stop one party emptying it.
func TestClaimRateIsPerMailbox(t *testing.T) {
	server, addr := claimSetup(t, core.MailboxClaim, 10)
	server.WithClaimsPerHour(4)

	got := 0
	for i := 0; i < 10; i++ {
		var claimer peer.ID = test.RandPeerIDFatal(t)
		msg, err := server.Claim(context.Background(), addr, claimer)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if msg != nil {
			got++
		}
	}
	if got != 4 {
		t.Fatalf("%d claims succeeded, want 4", got)
	}
}
