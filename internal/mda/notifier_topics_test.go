package mda

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/protocol/notify"
)

// recordingNode remembers which topics were joined and left.
type recordingNode struct {
	mu     sync.Mutex
	joined map[string]int
	left   map[string]int
}

func newRecordingNode() *recordingNode {
	return &recordingNode{joined: map[string]int{}, left: map[string]int{}}
}

func (r *recordingNode) JoinTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.joined[name]++
	return nil
}

func (r *recordingNode) LeaveTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.left[name]++
	return nil
}

func (r *recordingNode) Publish(context.Context, string, []byte) error { return nil }

func (r *recordingNode) counts(name string) (joined, left int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.joined[name], r.left[name]
}

func newTopicNotifier(t *testing.T, rec *recordingNode) *Notifier {
	t.Helper()
	n := NewNotifier(newTestHost(t), nil, nil, quietLogger())
	n.node = rec
	return n
}

func notifyShared(n *Notifier, addr *core.MailboxAddress) {
	n.NotifyNewMessage(context.Background(), addr, &core.Message{MessageID: "m"})
}

func waitJoined(t *testing.T, n *Notifier, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for n.JoinedTopics() != want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := n.JoinedTopics(); got != want {
		t.Fatalf("joined topics = %d, want %d", got, want)
	}
}

// Deleting a shared mailbox leaves its notification topic.
func TestNotifierLeavesTopicOnMailboxDelete(t *testing.T) {
	rec := newRecordingNode()
	n := newTopicNotifier(t, rec)
	addr := &core.MailboxAddress{OwnerID: n.host.ID(), Type: core.MailboxShared, FolderPath: "team"}
	topic := notify.MailboxTopic(addr.OwnerID.String(), addr.FolderPath)

	notifyShared(n, addr)
	waitJoined(t, n, 1)

	n.MailboxDeleted(addr)
	if joined, left := rec.counts(topic); joined != 1 || left != 1 {
		t.Fatalf("topic joined %d times and left %d, want 1 and 1", joined, left)
	}
	if n.JoinedTopics() != 0 {
		t.Fatal("deleted mailbox topic still counted as joined")
	}

	// Deleting it again, or a private mailbox, leaves nothing.
	n.MailboxDeleted(addr)
	n.MailboxDeleted(&core.MailboxAddress{OwnerID: n.host.ID(), Type: core.MailboxPrivate, FolderPath: "inbox"})
	if _, left := rec.counts(topic); left != 1 {
		t.Fatalf("topic left %d times, want 1", left)
	}
}

// A topic with no notification for longer than the idle limit is left; one
// still in use is kept and is not re-joined.
func TestNotifierLeavesIdleTopics(t *testing.T) {
	rec := newRecordingNode()
	n := newTopicNotifier(t, rec).WithTopicIdle(50 * time.Millisecond)
	owner := n.host.ID()
	quiet := &core.MailboxAddress{OwnerID: owner, Type: core.MailboxPublic, FolderPath: "quiet"}
	busy := &core.MailboxAddress{OwnerID: owner, Type: core.MailboxShared, FolderPath: "busy"}
	quietTopic := notify.MailboxTopic(owner.String(), "quiet")
	busyTopic := notify.MailboxTopic(owner.String(), "busy")

	notifyShared(n, quiet)
	notifyShared(n, busy)
	waitJoined(t, n, 2)

	time.Sleep(60 * time.Millisecond)
	notifyShared(n, busy) // refreshes busy just before the sweep
	time.Sleep(20 * time.Millisecond)
	n.leaveIdleTopics(time.Now())

	if _, left := rec.counts(quietTopic); left != 1 {
		t.Fatalf("idle topic left %d times, want 1", left)
	}
	if joined, left := rec.counts(busyTopic); left != 0 || joined != 2 {
		t.Fatalf("busy topic left %d times and joined %d, want 0 and 2", left, joined)
	}
	if n.JoinedTopics() != 1 {
		t.Fatalf("joined topics = %d, want 1", n.JoinedTopics())
	}

	// The next notification on the quiet mailbox joins it again.
	notifyShared(n, quiet)
	waitJoined(t, n, 2)
	if joined, _ := rec.counts(quietTopic); joined != 2 {
		t.Fatalf("quiet topic joined %d times, want 2", joined)
	}
}
