package relay

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const topic = "/overmedia/service-announce"

func newPeer(t *testing.T, ctx context.Context) (host.Host, *pubsub.PubSub) {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	return h, ps
}

func connect(t *testing.T, ctx context.Context, a, b host.Host) {
	t.Helper()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatal(err)
	}
}

// received publishes from pub until sub receives a message or the window ends.
func received(t *testing.T, ctx context.Context, pub *pubsub.Topic, sub *pubsub.Subscription, window time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			_ = pub.Publish(ctx, []byte("announcement"))
			time.Sleep(200 * time.Millisecond)
		}
	}()
	_, err := sub.Next(ctx)
	return err == nil
}

// A service and an app connected only through the server: messages pass
// only while the server relays the topic.
func TestRelayCarriesTopicBetweenPeersOfTheServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	serviceHost, servicePS := newPeer(t, ctx)
	serverHost, serverPS := newPeer(t, ctx)
	appHost, appPS := newPeer(t, ctx)
	connect(t, ctx, serviceHost, serverHost)
	connect(t, ctx, appHost, serverHost)

	pub, err := servicePS.Join(topic)
	if err != nil {
		t.Fatal(err)
	}
	appTopic, err := appPS.Join(topic)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := appTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	if received(t, ctx, pub, sub, 3*time.Second) {
		t.Fatal("message arrived without the server relaying the topic")
	}

	r := New(serverPS, []string{topic}, logger)
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if got := r.Topics(); len(got) != 1 || got[0] != topic {
		t.Fatalf("Topics() = %v", got)
	}
	if !received(t, ctx, pub, sub, 10*time.Second) {
		t.Fatal("message did not arrive through the relaying server")
	}
}

// A topic the server already uses is forwarded anyway; it is skipped, and the
// other topics are still relayed.
func TestRelaySkipsTopicsTheServerAlreadyJoined(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, ps := newPeer(t, ctx)
	if _, err := ps.Join("/sf-network/services/announce"); err != nil {
		t.Fatal(err)
	}

	r := New(ps, []string{"/sf-network/services/announce", topic}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := r.Start(); err == nil {
		t.Error("expected an error naming the skipped topic")
	}
	defer r.Stop()
	if got := r.Topics(); len(got) != 1 || got[0] != topic {
		t.Errorf("Topics() = %v, want [%s]", got, topic)
	}
}

func TestRelayWithoutPubSub(t *testing.T) {
	if err := New(nil, []string{topic}, slog.New(slog.NewTextHandler(io.Discard, nil))).Start(); err == nil {
		t.Error("expected an error without pubsub")
	}
}
