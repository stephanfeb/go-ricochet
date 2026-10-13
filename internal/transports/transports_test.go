package transports

import (
	"net"
	"strings"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestForAddressNamesTheTransport(t *testing.T) {
	for addr, want := range map[string]string{
		"/ip4/0.0.0.0/udp/55223/udx":                  UDX,
		"/ip4/0.0.0.0/tcp/55223":                      TCP,
		"/ip6/::/tcp/55223":                           TCP,
		"/ip4/0.0.0.0/udp/55224/quic-v1":              QUIC,
		"/ip4/0.0.0.0/udp/55224/quic-v1/webtransport": WebTransport,
		"/ip4/0.0.0.0/udp/55225/webrtc-direct":        WebRTCDirect,
		"/ip4/0.0.0.0/tcp/8080/ws":                    WebSocket,
		"/dns4/example.com/tcp/443/wss":               WebSocket,
		"/dns4/example.com/tcp/443/tls/sni/x.com/ws":  WebSocket,
		"/dns4/example.com/udp/55223/udx":             UDX,
	} {
		got, err := ForAddress(ma.StringCast(addr))
		if err != nil || got != want {
			t.Errorf("ForAddress(%s) = %q, %v; want %q", addr, got, err, want)
		}
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name     string
		names    []string
		listen   []string
		external []string
		wssCert  bool
		wantErr  string // empty: must pass
	}{
		{name: "the default", names: []string{UDX},
			listen: []string{"/ip4/0.0.0.0/udp/55223/udx"}},
		{name: "every transport on its own port", names: Known(),
			listen: []string{
				"/ip4/0.0.0.0/udp/55223/udx",
				"/ip4/0.0.0.0/tcp/55223",
				"/ip4/0.0.0.0/udp/55224/quic-v1",
				"/ip4/0.0.0.0/udp/55224/quic-v1/webtransport",
				"/ip4/0.0.0.0/tcp/55225/ws",
			},
			external: []string{"/ip4/203.0.113.10/udp/55223/udx", "/dns4/example.com/tcp/443/wss"}},
		{name: "a dial-only transport", names: []string{UDX, TCP},
			listen: []string{"/ip4/0.0.0.0/udp/55223/udx"}},
		{name: "UDX and QUIC on different addresses of one port", names: []string{UDX, QUIC},
			listen: []string{"/ip4/127.0.0.1/udp/55223/udx", "/ip4/10.0.0.1/udp/55223/quic-v1"}},
		{name: "IPv4 and IPv6 are separate sockets", names: []string{UDX, QUIC},
			listen: []string{"/ip4/0.0.0.0/udp/55223/udx", "/ip6/::1/udp/55223/quic-v1"}},
		{name: "port 0 never clashes", names: []string{UDX, QUIC},
			listen: []string{"/ip4/0.0.0.0/udp/0/udx", "/ip4/0.0.0.0/udp/0/quic-v1"}},
		{name: "relay external addresses are skipped", names: []string{UDX},
			listen:   []string{"/ip4/0.0.0.0/udp/55223/udx"},
			external: []string{"/ip4/203.0.113.10/udp/55223/udx/p2p/12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA/p2p-circuit"}},

		{name: "no transports", names: nil, wantErr: "at least one"},
		{name: "unknown name", names: []string{"carrier-pigeon"}, wantErr: "unknown transport"},
		{name: "listed twice", names: []string{UDX, UDX}, wantErr: "twice"},
		{name: "listen address without its transport", names: []string{UDX},
			listen: []string{"/ip4/0.0.0.0/tcp/55223"}, wantErr: `needs transport "tcp"`},
		{name: "external address without its transport", names: []string{UDX},
			listen:   []string{"/ip4/0.0.0.0/udp/55223/udx"},
			external: []string{"/ip4/203.0.113.10/udp/55224/quic-v1"}, wantErr: `needs transport "quic-v1"`},
		{name: "UDX and QUIC share a UDP port", names: []string{UDX, QUIC},
			listen:  []string{"/ip4/0.0.0.0/udp/55223/udx", "/ip4/0.0.0.0/udp/55223/quic-v1"},
			wantErr: "udp port 55223"},
		{name: "unspecified overlaps a specific address", names: []string{UDX, WebTransport},
			listen:  []string{"/ip4/0.0.0.0/udp/55223/udx", "/ip4/127.0.0.1/udp/55223/quic-v1/webtransport"},
			wantErr: "udp port 55223"},
		{name: "TCP and WebSocket share a port", names: []string{TCP, WebSocket},
			listen: []string{"/ip4/0.0.0.0/tcp/55223", "/ip4/0.0.0.0/tcp/55223/ws"}},
		{name: "secure WebSocket listener without a certificate", names: []string{WebSocket},
			listen: []string{"/ip4/0.0.0.0/tcp/443/wss"}, wantErr: "server.websocket_tls"},
		{name: "secure WebSocket listener with a certificate", names: []string{WebSocket},
			listen: []string{"/ip4/0.0.0.0/tcp/443/tls/ws"}, wssCert: true},
		{name: "webrtc-direct by name", names: []string{UDX, WebRTCDirect}, wantErr: "GO-2026-4479"},
		{name: "webrtc-direct by address", names: []string{UDX},
			listen: []string{"/ip4/0.0.0.0/udp/55225/webrtc-direct"}, wantErr: "GO-2026-4479"},
		{name: "QUIC draft-29", names: []string{QUIC},
			listen: []string{"/ip4/0.0.0.0/udp/55223/quic"}, wantErr: "draft-29"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.names, tc.listen, tc.external, tc.wssCert)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("passed; want an error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestOptionsCoverEveryKnownTransport(t *testing.T) {
	opts, err := Options(Known())
	if err != nil {
		t.Fatal(err)
	}
	// One per transport, and one more to share TCP listeners with WebSocket.
	if len(opts) != len(Known())+1 {
		t.Fatalf("got %d options for %d transports", len(opts), len(Known()))
	}
	if opts, _ := Options([]string{UDX, TCP}); len(opts) != 2 {
		t.Fatalf("got %d options for udx and tcp; sharing needs WebSocket", len(opts))
	}
	if _, err := Options([]string{"smoke-signals"}); err == nil {
		t.Fatal("an unknown transport produced an option")
	}
}

func TestOnIPAdvertisesEveryReachableListener(t *testing.T) {
	listen := []string{
		"/ip4/0.0.0.0/udp/55223/udx",
		"/ip4/0.0.0.0/tcp/55223",
		"/ip4/10.0.0.5/udp/55224/quic-v1/webtransport",
		"/ip4/127.0.0.1/tcp/55225/ws",
		"/ip6/::/udp/55223/udx",
	}
	got, err := OnIP(listen, net.ParseIP("203.0.113.10"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/ip4/203.0.113.10/udp/55223/udx",
		"/ip4/203.0.113.10/tcp/55223",
		"/ip4/203.0.113.10/udp/55224/quic-v1/webtransport",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("OnIP = %v\nwant %v", got, want)
	}

	got, err = OnIP(listen, net.ParseIP("2001:db8::1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "/ip6/2001:db8::1/udp/55223/udx" {
		t.Fatalf("OnIP over IPv6 = %v", got)
	}
}
