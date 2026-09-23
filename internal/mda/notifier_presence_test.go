package mda

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/presence"
	"github.com/stephanfeb/go-ricochet/internal/protocol/notify"
)

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// fixedPresence answers every check with one state.
type fixedPresence struct {
	state atomic.Int32
	calls atomic.Int64
}

func (f *fixedPresence) set(state presence.PresenceState) { f.state.Store(int32(state)) }

func (f *fixedPresence) CheckPresence(_ context.Context, id peer.ID) *presence.PresenceStatus {
	f.calls.Add(1)
	return &presence.PresenceStatus{PeerID: id, State: presence.PresenceState(f.state.Load())}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A recipient the presence checker reports offline is not dialled; one it
// reports online gets the notification.
func TestNotifierSkipsOfflineRecipient(t *testing.T) {
	server := newTestHost(t)
	client := newTestHost(t)
	server.Peerstore().AddAddrs(client.ID(), client.Addrs(), peerstore.PermanentAddrTTL)

	var received atomic.Int64
	client.SetStreamHandler(notify.ProtocolID, func(s network.Stream) {
		received.Add(1)
		_ = s.Close()
	})

	addr := &core.MailboxAddress{OwnerID: client.ID(), Type: core.MailboxPrivate, FolderPath: "inbox"}
	msg := &core.Message{MessageID: "m1"}

	checker := &fixedPresence{}
	checker.set(presence.Offline)
	n := NewNotifier(server, nil, checker, quietLogger())
	n.NotifyNewMessage(context.Background(), addr, msg)
	time.Sleep(300 * time.Millisecond)

	if checker.calls.Load() == 0 {
		t.Fatal("presence was never checked")
	}
	if got := received.Load(); got != 0 {
		t.Fatalf("offline recipient received %d notifications, want 0", got)
	}

	checker.set(presence.Online)
	n.NotifyNewMessage(context.Background(), addr, msg)
	deadline := time.Now().Add(5 * time.Second)
	for received.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := received.Load(); got != 1 {
		t.Fatalf("online recipient received %d notifications, want 1", got)
	}
}
