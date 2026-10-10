package mda

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/protocol/notify"
	"github.com/stephanfeb/go-ricochet/internal/storage/storagetest"
)

// publishingNode remembers the topics published to.
type publishingNode struct {
	recordingNode
	mu        sync.Mutex
	published []string
}

func (p *publishingNode) Publish(_ context.Context, topic string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, topic)
	return nil
}

func (p *publishingNode) topics() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.published...)
}

// A message delivered to a public or shared mailbox is announced on the
// mailbox's topic. Delivery finds the mailbox through an address it builds
// as private, and used to notify with that address: the owner got a direct
// notification and the topic, which everyone else follows, heard nothing.
func TestDeliveryToPublicOrSharedMailboxIsAnnouncedOnItsTopic(t *testing.T) {
	for _, mailboxType := range []core.MailboxType{core.MailboxPublic, core.MailboxShared} {
		t.Run(mailboxType.String(), func(t *testing.T) {
			node := &publishingNode{recordingNode: *newRecordingNode()}
			n := newTopicNotifier(t, &node.recordingNode)
			n.node = node
			server := NewMailboxServer(storagetest.New(), MailboxDefaults{MaxMessages: 100, RetentionDays: 1}, quietLogger())
			server.SetNotifier(n)

			owner := n.host.ID()
			addr := &core.MailboxAddress{OwnerID: owner, Type: mailboxType, FolderPath: "spaces/m1/shares"}
			if err := server.CreateMailbox(context.Background(), addr, 100, 1, nil); err != nil {
				t.Fatalf("create mailbox: %v", err)
			}

			msg := &core.Message{
				MessageID:       "m1",
				SenderPeerID:    owner.String(),
				RecipientPeerID: owner.String(),
				FolderPath:      addr.FolderPath,
				Payload:         []byte("hello"),
			}
			if _, err := server.DeliverLocal(context.Background(), msg); err != nil {
				t.Fatalf("deliver: %v", err)
			}

			want := notify.MailboxTopic(owner.String(), addr.FolderPath)
			deadline := time.Now().Add(2 * time.Second)
			for len(node.topics()) == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if got := node.topics(); len(got) != 1 || got[0] != want {
				t.Fatalf("published to %v, want [%s]", got, want)
			}
		})
	}
}
