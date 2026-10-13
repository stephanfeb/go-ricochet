package transports

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	forgehost "github.com/stephanfeb/go-p2p-forge/host"
)

const echoProtocol = "/ricochet/test/echo/1.0.0"

// newHost builds a host the way the server does: forge's host with the
// options Options returns for one transport.
func newHost(t *testing.T, transport, listen string) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := Options([]string{transport})
	if err != nil {
		t.Fatal(err)
	}
	cfg := forgehost.DefaultConfig()
	cfg.ListenAddresses = []string{listen}
	cfg.EnableRelay = false
	cfg.EnableAutoNAT = false
	h, err := forgehost.Create(cfg, priv, slog.New(slog.DiscardHandler), opts...)
	if err != nil {
		t.Fatalf("create %s host: %v", transport, err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// Every supported transport carries a stream between two hosts built the
// way the server builds its own.
func TestEveryTransportCarriesAStream(t *testing.T) {
	for transport, listen := range map[string]string{
		UDX:          "/ip4/127.0.0.1/udp/0/udx",
		TCP:          "/ip4/127.0.0.1/tcp/0",
		QUIC:         "/ip4/127.0.0.1/udp/0/quic-v1",
		WebSocket:    "/ip4/127.0.0.1/tcp/0/ws",
		WebTransport: "/ip4/127.0.0.1/udp/0/quic-v1/webtransport",
	} {
		t.Run(transport, func(t *testing.T) {
			server := newHost(t, transport, listen)
			server.SetStreamHandler(echoProtocol, func(s network.Stream) {
				defer s.Close()
				io.Copy(s, s)
			})
			client := newHost(t, transport, listen)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
				t.Fatalf("connect over %s to %v: %v", transport, server.Addrs(), err)
			}
			s, err := client.NewStream(ctx, server.ID(), echoProtocol)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer s.Close()

			want := []byte("store and forward over " + transport)
			if _, err := s.Write(want); err != nil {
				t.Fatalf("write: %v", err)
			}
			s.CloseWrite()
			got, err := io.ReadAll(s)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(got) != string(want) {
				t.Fatalf("echo = %q, want %q", got, want)
			}

			conns := client.Network().ConnsToPeer(server.ID())
			if len(conns) == 0 {
				t.Fatal("no connection to the server")
			}
			if got, err := ForAddress(conns[0].RemoteMultiaddr()); err != nil || got != transport {
				t.Fatalf("connection runs over %q (%v), want %q", got, err, transport)
			}
		})
	}
}

// TCP and WebSocket listen on one port, and a client of each reaches the
// server through it.
func TestTCPAndWebSocketShareAPort(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := Options([]string{TCP, WebSocket})
	if err != nil {
		t.Fatal(err)
	}
	port := freeTCPPort(t)
	cfg := forgehost.DefaultConfig()
	cfg.ListenAddresses = []string{
		fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port),
		fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/ws", port),
	}
	cfg.EnableRelay = false
	cfg.EnableAutoNAT = false
	server, err := forgehost.Create(cfg, priv, slog.New(slog.DiscardHandler), opts...)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	server.SetStreamHandler(echoProtocol, func(s network.Stream) {
		defer s.Close()
		io.Copy(s, s)
	})

	for _, transport := range []string{TCP, WebSocket} {
		client := newHost(t, transport, map[string]string{TCP: "/ip4/127.0.0.1/tcp/0", WebSocket: "/ip4/127.0.0.1/tcp/0/ws"}[transport])
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
			cancel()
			t.Fatalf("connect over %s: %v", transport, err)
		}
		s, err := client.NewStream(ctx, server.ID(), echoProtocol)
		cancel()
		if err != nil {
			t.Fatalf("stream over %s: %v", transport, err)
		}
		s.Write([]byte("ping"))
		s.CloseWrite()
		if got, _ := io.ReadAll(s); string(got) != "ping" {
			t.Fatalf("echo over %s = %q", transport, got)
		}
		remote := client.Network().ConnsToPeer(server.ID())[0].RemoteMultiaddr()
		if got, _ := ForAddress(remote); got != transport {
			t.Fatalf("connection over %s reached %s", transport, remote)
		}
		if !strings.Contains(remote.String(), fmt.Sprintf("/tcp/%d", port)) {
			t.Fatalf("connection over %s reached %s, not the shared port", transport, remote)
		}
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
