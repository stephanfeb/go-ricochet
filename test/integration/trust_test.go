package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/trust"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

func keyAndID(t *testing.T) (crypto.PrivKey, peer.ID) {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return priv, id
}

// enable_authentication used to be a documented knob that read nothing. It
// is now a connection gate: with it on, only trusted peers get past the
// handshake. The gate is the same object the server installs, on a host
// built the way the test hosts are.
func TestAuthenticationAdmitsOnlyTrustedPeers(t *testing.T) {
	trustedKey, trustedID := keyAndID(t)
	strangerKey, _ := keyAndID(t)
	serverKey, _ := keyAndID(t)

	peers, err := trust.Parse([]string{trustedID.String()})
	if err != nil {
		t.Fatal(err)
	}
	server, err := libp2p.New(
		libp2p.Identity(serverKey),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.ConnectionGater(trust.NewGater(peers, slog.New(slog.NewTextHandler(io.Discard, nil)))),
		libp2p.DisableRelay(),
	)
	if err != nil {
		t.Fatalf("create gated host: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	target := peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The gate decides once the Noise handshake has authenticated the
	// peer, and the muxer is negotiated inside that handshake, so from the
	// dialer's side Connect can return before the server hangs up. What
	// matters is whether the connection is still there a moment later.
	settle := func(h host.Host) network.Connectedness {
		time.Sleep(300 * time.Millisecond)
		return h.Network().Connectedness(server.ID())
	}

	trusted := createHost(t, trustedKey)
	t.Cleanup(func() { trusted.Close() })
	if err := trusted.Connect(ctx, target); err != nil {
		t.Fatalf("trusted peer refused: %v", err)
	}
	if settle(trusted) != network.Connected {
		t.Fatal("trusted peer's connection did not survive the gate")
	}

	stranger := createHost(t, strangerKey)
	t.Cleanup(func() { stranger.Close() })
	connectCtx, cancelConnect := context.WithTimeout(ctx, 5*time.Second)
	defer cancelConnect()
	_ = stranger.Connect(connectCtx, target)
	if settle(stranger) == network.Connected {
		t.Fatal("untrusted peer holds a connection through the authentication gate")
	}
}

// enable_forwarding used to read nothing either. A submission whose sender
// is not the peer on the connection is now accepted only from a trusted
// forwarder with forwarding on, and arrives marked forwarded with its hop
// count advanced.
func TestForwardedSubmissionRequiresTrustAndForwarding(t *testing.T) {
	forwarderKey, forwarderID := keyAndID(t)
	server := newTestServer(t, func(c *core.ServerConfig) {
		c.EnableForwarding = true
		c.TrustedPeers = []string{forwarderID.String()}
	})
	forwarder := newTestClientWithKey(t, server, forwarderKey)
	stranger := newTestClient(t, server)
	origin := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := core.NewMessageWithDefaultExpiry(origin.PeerID(), recipient.PeerID(), []byte("relayed"))

	res, err := forwarder.ForwardMessage(ctx, msg)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if !res.Success {
		t.Fatalf("trusted forwarder refused: status %d: %s", res.Status, res.ErrorMessage)
	}

	if got := forwardedStatus(t, ctx, stranger, msg); got != 403 {
		t.Errorf("untrusted forwarder refused with %d; want 403", got)
	}

	time.Sleep(100 * time.Millisecond)
	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("recipient has %d messages; want the one forwarded", len(msgs))
	}
	got := msgs[0]
	if got.SenderPeerID != origin.PeerID().String() {
		t.Errorf("sender = %s; want the original sender %s", got.SenderPeerID, origin.PeerID())
	}
	if !got.Flags.IsForwarded() {
		t.Errorf("forwarded flag not set: %v", got.Flags)
	}
	if got.HopCount != 1 {
		t.Errorf("hop count = %d; want 1", got.HopCount)
	}
}

// With forwarding off, even a trusted peer cannot submit on another's behalf.
func TestForwardingOffRefusesTrustedForwarders(t *testing.T) {
	forwarderKey, forwarderID := keyAndID(t)
	server := newTestServer(t, func(c *core.ServerConfig) {
		c.EnableForwarding = false
		c.TrustedPeers = []string{forwarderID.String()}
	})
	forwarder := newTestClientWithKey(t, server, forwarderKey)
	origin := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := core.NewMessageWithDefaultExpiry(origin.PeerID(), recipient.PeerID(), []byte("relayed"))
	if got := forwardedStatus(t, ctx, forwarder, msg); got != 403 {
		t.Errorf("refused with %d; want 403", got)
	}
}

// A forwarded message that has already travelled the maximum number of hops
// is refused rather than passed on again.
func TestForwardingRefusesPastTheHopLimit(t *testing.T) {
	forwarderKey, forwarderID := keyAndID(t)
	server := newTestServer(t, func(c *core.ServerConfig) {
		c.EnableForwarding = true
		c.TrustedPeers = []string{forwarderID.String()}
	})
	forwarder := newTestClientWithKey(t, server, forwarderKey)
	origin := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := core.NewMessageWithDefaultExpiry(origin.PeerID(), recipient.PeerID(), []byte("looping"))
	msg.HopCount = core.MaxHopCount
	if got := forwardedStatus(t, ctx, forwarder, msg); got == 0 || got == 200 {
		t.Errorf("message at the hop limit was forwarded again (status %d)", got)
	}
}

func forwardedStatus(t *testing.T, ctx context.Context, cl *client.Client, msg *core.Message) int {
	t.Helper()
	res, err := cl.ForwardMessage(ctx, msg)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if res.Success {
		t.Fatal("server accepted a forwarded submission it should have refused")
	}
	return res.Status
}
