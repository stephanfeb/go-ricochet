// Command probe checks what a Ricochet server's relay does for two clients
// behind NAT. run.sh runs it in the mailbox example's Docker setup.
//
//	probe listen          reserve a relay slot, print this peer's ID, accept streams
//	probe punch <peer>    connect to peer through the relay; exit 0 once a
//	                      hole punch gives a direct connection
//	probe bulk <peer>     send 1 MiB to peer through the relay, without
//	                      hole punching; exit 0 once peer confirms every byte
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	ma "github.com/multiformats/go-multiaddr"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"
)

const (
	protocolID = "/ricochet-nat-probe/1.0.0"
	punchWait  = 30 * time.Second
	// bulkSize is eight times the 128 KiB a go-libp2p relay carries by
	// default before it cuts the connection.
	bulkSize = 1 << 20
)

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: probe listen | punch <peer> | bulk <peer>"))
	}
	raw, err := os.ReadFile("/shared/server.addr")
	if err != nil {
		fail(err)
	}
	serverAddr := strings.TrimSpace(string(raw))
	server, err := peer.AddrInfoFromString(serverAddr)
	if err != nil {
		fail(err)
	}

	h, err := newHost(os.Args[1] != "bulk")
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	if err := h.Connect(ctx, *server); err != nil {
		fail(fmt.Errorf("connect to server: %w", err))
	}

	switch os.Args[1] {
	case "listen":
		listen(ctx, h, *server)
	case "punch":
		target := targetArg()
		connectThroughRelay(ctx, h, serverAddr, target)
		if !waitForDirect(h, target) {
			fail(fmt.Errorf("no direct connection %s after the relayed one", punchWait))
		}
	case "bulk":
		target := targetArg()
		connectThroughRelay(ctx, h, serverAddr, target)
		bulk(ctx, h, target)
	default:
		fail(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

// newHost speaks what a Ricochet server speaks, with the relay client and,
// when punch is set, go-libp2p's hole puncher on, as a mobile client would
// have them. Without the hole puncher a connection stays on the relay.
func newHost(punch bool) (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.EnableRelay(),
	}
	if punch {
		opts = append(opts, libp2p.EnableHolePunching(holepunch.WithTracer(tracer{})))
	}
	return libp2p.New(opts...)
}

func listen(ctx context.Context, h host.Host, server peer.AddrInfo) {
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		n, err := io.Copy(io.Discard, s)
		logf("stream over %s ended after %d bytes: %v", s.Conn().RemoteMultiaddr(), n, err)
		fmt.Fprintf(s, "%d", n)
		s.Close()
	})
	rsvp, err := client.Reserve(ctx, h, server)
	if err != nil {
		fail(fmt.Errorf("reserve: %w", err))
	}
	limit := "none"
	if rsvp.LimitDuration > 0 || rsvp.LimitData > 0 {
		limit = fmt.Sprintf("%s or %d bytes", rsvp.LimitDuration, rsvp.LimitData)
	}
	logf("reserved a relay slot; per-connection limit: %s", limit)
	fmt.Printf("PEER %s\n", h.ID())
	select {}
}

func targetArg() peer.ID {
	if len(os.Args) < 3 {
		fail(fmt.Errorf("%s needs the listener's peer ID", os.Args[1]))
	}
	id, err := peer.Decode(os.Args[2])
	if err != nil {
		fail(err)
	}
	return id
}

func connectThroughRelay(ctx context.Context, h host.Host, serverAddr string, target peer.ID) {
	circuit, err := ma.NewMultiaddr(serverAddr + "/p2p-circuit/p2p/" + target.String())
	if err != nil {
		fail(err)
	}
	if err := h.Connect(ctx, peer.AddrInfo{ID: target, Addrs: []ma.Multiaddr{circuit}}); err != nil {
		fail(fmt.Errorf("connect through the relay: %w", err))
	}
	logf("connected to %s through the relay", target)
}

func waitForDirect(h host.Host, target peer.ID) bool {
	deadline := time.Now().Add(punchWait)
	for time.Now().Before(deadline) {
		for _, c := range h.Network().ConnsToPeer(target) {
			if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
				logf("direct connection: %s", c.RemoteMultiaddr())
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// bulk sends bulkSize bytes over the relayed connection and fails unless
// the listener reports receiving all of them.
func bulk(ctx context.Context, h host.Host, target peer.ID) {
	s, err := h.NewStream(network.WithAllowLimitedConn(ctx, "probe"), target, protocolID)
	if err != nil {
		fail(fmt.Errorf("open stream: %w", err))
	}
	if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
		fail(fmt.Errorf("stream is not on the relay: %s", s.Conn().RemoteMultiaddr()))
	}
	start := time.Now()
	if _, err := io.Copy(s, io.LimitReader(rand.Reader, bulkSize)); err != nil {
		fail(fmt.Errorf("relay cut the stream while sending: %w", err))
	}
	if err := s.CloseWrite(); err != nil {
		fail(err)
	}
	reply, err := io.ReadAll(s)
	if err != nil {
		fail(fmt.Errorf("relay cut the stream before the listener replied: %w", err))
	}
	got, err := strconv.Atoi(string(bytes.TrimSpace(reply)))
	if err != nil || got != bulkSize {
		fail(fmt.Errorf("listener received %q bytes, want %d", reply, bulkSize))
	}
	logf("relay carried %d bytes in %s", got, time.Since(start).Round(100*time.Millisecond))
}

type tracer struct{}

func (tracer) Trace(evt *holepunch.Event) {
	logf("holepunch %s: %+v", evt.Type, evt.Evt)
}

func logf(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().Format("15:04:05.0"), fmt.Sprintf(format, args...))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "probe:", err)
	os.Exit(1)
}
