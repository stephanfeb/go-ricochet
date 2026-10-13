// Package transports maps the transport names an operator writes in the
// config to the go-libp2p transports the host runs, and checks that a set
// of listen and external addresses is one those transports can serve.
//
// UDX is the transport the mobile clients speak and the default. The rest
// are go-libp2p's own: TCP, QUIC, WebSocket and WebTransport. TCP and
// WebSocket run Noise and yamux over the connection; QUIC and WebTransport
// bring their own encryption and stream multiplexing, so the yamux keepalive and write timeout do not apply to
// them (quic-go keeps a connection alive on its own 15-second period).
package transports

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"

	"github.com/libp2p/go-libp2p"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	webtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	ma "github.com/multiformats/go-multiaddr"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"
)

// Transport names, as written in server.transports.
const (
	UDX          = "udx"
	TCP          = "tcp"
	QUIC         = "quic-v1"
	WebSocket    = "websocket"
	WebTransport = "webtransport"

	// WebRTCDirect is recognised so that it can be refused by name.
	// go-libp2p's WebRTC transport runs pion/dtls/v2, which carries
	// GO-2026-4479 (AES-GCM nonce reuse) with no upstream fix; the server
	// links it but, while this transport stays off, never reaches it.
	WebRTCDirect = "webrtc-direct"
)

const webrtcRefusal = "webrtc-direct is not supported: its DTLS library carries GO-2026-4479, " +
	"which has no fix; use webtransport for browsers"

// constructors holds every supported transport, in the order Known lists them.
var constructors = []struct {
	name string
	ctor any
}{
	{UDX, udxtransport.NewTransport},
	{TCP, tcp.NewTCPTransport},
	{QUIC, quic.NewTransport},
	{WebSocket, websocket.New},
	{WebTransport, webtransport.New},
}

// Known returns every transport name the server accepts.
func Known() []string {
	names := make([]string, len(constructors))
	for i, c := range constructors {
		names[i] = c.name
	}
	return names
}

// Option adjusts the transports Options builds.
type Option func(*settings)

type settings struct {
	wsServerTLS *tls.Config
	wsClientTLS *tls.Config
}

// WebSocketServerTLS is the TLS configuration the WebSocket transport
// listens for secure WebSocket (/tls/ws, /wss) with.
func WebSocketServerTLS(c *tls.Config) Option {
	return func(s *settings) { s.wsServerTLS = c }
}

// WebSocketClientTLS is the TLS configuration the WebSocket transport dials
// secure WebSocket with; unset, the system roots verify the server.
func WebSocketClientTLS(c *tls.Config) Option {
	return func(s *settings) { s.wsClientTLS = c }
}

// Options returns one libp2p transport option per name. The names must
// already have passed Validate.
//
// With both TCP and WebSocket enabled, the two share their TCP listeners,
// so they may listen on one port: go-libp2p tells the connections apart by
// their first bytes. go-libp2p means to make this its default.
func Options(names []string, with ...Option) ([]libp2p.Option, error) {
	var set settings
	for _, o := range with {
		o(&set)
	}
	opts := make([]libp2p.Option, 0, len(names)+1)
	hasTCP, hasWS := false, false
	for _, name := range names {
		hasTCP = hasTCP || name == TCP
		hasWS = hasWS || name == WebSocket
		ctor, ok := constructorFor(name)
		if !ok {
			return nil, fmt.Errorf("unknown transport %q (known: %s)", name, strings.Join(Known(), ", "))
		}
		var ctorOpts []any
		if name == WebSocket {
			if set.wsServerTLS != nil {
				ctorOpts = append(ctorOpts, websocket.WithTLSConfig(set.wsServerTLS))
			}
			if set.wsClientTLS != nil {
				ctorOpts = append(ctorOpts, websocket.WithTLSClientConfig(set.wsClientTLS))
			}
		}
		opts = append(opts, libp2p.Transport(ctor, ctorOpts...))
	}
	if hasTCP && hasWS {
		opts = append(opts, libp2p.ShareTCPListener())
	}
	return opts, nil
}

func constructorFor(name string) (any, bool) {
	for _, c := range constructors {
		if c.name == name {
			return c.ctor, true
		}
	}
	return nil, false
}

// errRelayed marks a circuit relay address, which no transport listens on.
var errRelayed = fmt.Errorf("circuit relay address")

// ForAddress reports which transport serves a multiaddr.
func ForAddress(addr ma.Multiaddr) (string, error) {
	has := map[int]bool{}
	for _, c := range addr {
		has[c.Protocol().Code] = true
	}
	switch {
	case has[ma.P_CIRCUIT]:
		return "", errRelayed
	case has[ma.P_WEBRTC_DIRECT]:
		return WebRTCDirect, nil
	case has[ma.P_WEBTRANSPORT]:
		return WebTransport, nil
	case has[ma.P_QUIC_V1]:
		return QUIC, nil
	case has[ma.P_QUIC]:
		return "", fmt.Errorf("%s: QUIC draft-29 is not supported; use /quic-v1", addr)
	case has[udxtransport.P_UDX]:
		return UDX, nil
	case has[ma.P_WS] || has[ma.P_WSS]:
		return WebSocket, nil
	case has[ma.P_TCP]:
		return TCP, nil
	}
	return "", fmt.Errorf("%s: no supported transport serves this address", addr)
}

// isSecureWebSocket reports whether an address is /wss or /tls/.../ws.
func isSecureWebSocket(addr ma.Multiaddr) bool {
	for _, c := range addr {
		switch c.Protocol().Code {
		case ma.P_WSS, ma.P_TLS:
			return true
		}
	}
	return false
}

// Validate checks a transport list against the addresses the host will
// listen on and advertise. secureWebSocket says whether the server holds a
// certificate to listen for secure WebSocket with.
//
// Every listen address needs its transport enabled, or the host fails to
// start; every external address needs its transport enabled, or the server
// advertises an address it cannot answer on. Relay addresses are skipped:
// circuit relay is configured separately.
//
// Two listeners must not claim the same port. UDX holds its own UDP socket,
// while QUIC and WebTransport share one through go-libp2p's
// quicreuse; with SO_REUSEPORT on both, the kernel would split one port's
// datagrams between two transports that cannot read each other's. TCP and
// WebSocket share a listener (see Options), and TCP and UDP may share a
// number.
func Validate(names, listen, external []string, secureWebSocket bool) error {
	if len(names) == 0 {
		return fmt.Errorf("server.transports must name at least one transport (known: %s)", strings.Join(Known(), ", "))
	}
	enabled := make(map[string]bool, len(names))
	for _, name := range names {
		if name == WebRTCDirect {
			return fmt.Errorf("server.transports: %s", webrtcRefusal)
		}
		if _, ok := constructorFor(name); !ok {
			return fmt.Errorf("server.transports: unknown transport %q (known: %s)", name, strings.Join(Known(), ", "))
		}
		if enabled[name] {
			return fmt.Errorf("server.transports lists %q twice", name)
		}
		enabled[name] = true
	}

	var bound []binding
	for _, s := range listen {
		addr, err := ma.NewMultiaddr(s)
		if err != nil {
			return fmt.Errorf("listen address %q: %w", s, err)
		}
		name, err := ForAddress(addr)
		if err == errRelayed {
			return fmt.Errorf("listen address %q: a relay address cannot be listened on; enable relay instead", s)
		}
		if err != nil {
			return fmt.Errorf("listen address: %w", err)
		}
		if name == WebRTCDirect {
			return fmt.Errorf("listen address %q: %s", s, webrtcRefusal)
		}
		if !enabled[name] {
			return fmt.Errorf("listen address %q needs transport %q; add it to server.transports", s, name)
		}
		if name == WebSocket && isSecureWebSocket(addr) && !secureWebSocket {
			return fmt.Errorf("listen address %q: secure WebSocket needs a certificate; set server.websocket_tls, "+
				"or terminate TLS in a reverse proxy, listen on /ws and advertise the /wss address in external_addresses", s)
		}
		b, ok := bindingOf(addr, name)
		if !ok {
			continue
		}
		for _, other := range bound {
			if b.clashes(other) {
				return fmt.Errorf("listen addresses %q and %q both claim %s port %d; give %s and %s different ports",
					other.addr, s, b.network, b.port, other.transport, name)
			}
		}
		bound = append(bound, b)
	}

	for _, s := range external {
		addr, err := ma.NewMultiaddr(s)
		if err != nil {
			return fmt.Errorf("external address %q: %w", s, err)
		}
		name, err := ForAddress(addr)
		if err == errRelayed {
			continue
		}
		if err != nil {
			return fmt.Errorf("external address: %w", err)
		}
		if name == WebRTCDirect {
			return fmt.Errorf("external address %q: %s", s, webrtcRefusal)
		}
		if !enabled[name] {
			return fmt.Errorf("external address %q needs transport %q; add it to server.transports", s, name)
		}
	}
	return nil
}

// binding is the socket a listen address claims.
type binding struct {
	addr      string
	transport string
	family    int // ma.P_IP4 or ma.P_IP6
	ip        net.IP
	network   string // "udp" or "tcp"
	port      int
}

// bindingOf extracts the socket of an IP listen address. Addresses with a
// DNS host or port 0 claim nothing checkable and report false.
func bindingOf(addr ma.Multiaddr, transport string) (binding, bool) {
	b := binding{addr: addr.String(), transport: transport}
	for _, c := range addr {
		switch c.Protocol().Code {
		case ma.P_IP4, ma.P_IP6:
			if b.family == 0 {
				b.family = c.Protocol().Code
				b.ip = net.IP(c.RawValue())
			}
		case ma.P_UDP, ma.P_TCP:
			if b.network == "" {
				b.network = c.Protocol().Name
				fmt.Sscanf(c.Value(), "%d", &b.port)
			}
		}
	}
	if b.family == 0 || b.network == "" || b.port == 0 {
		return binding{}, false
	}
	return b, true
}

// socketGroup names the transports that share one socket per port.
func socketGroup(transport string) string {
	switch transport {
	case QUIC, WebTransport:
		return "quic"
	case TCP, WebSocket:
		return "tcp"
	}
	return transport
}

func (b binding) clashes(o binding) bool {
	if b.network != o.network || b.port != o.port || b.family != o.family {
		return false
	}
	if !b.ip.Equal(o.ip) && !b.ip.IsUnspecified() && !o.ip.IsUnspecified() {
		return false
	}
	return socketGroup(b.transport) != socketGroup(o.transport)
}

// OnIP returns each listen address as reached on ip: the address a peer
// outside the host dials for it. Listen addresses of the other IP family,
// and those bound to loopback, are left out, since nothing outside the
// host reaches them; so are DNS addresses, which already name the host.
func OnIP(listen []string, ip net.IP) ([]string, error) {
	family := ma.P_IP6
	if ip.To4() != nil {
		family = ma.P_IP4
	}
	ipComp, err := ma.NewComponent(ma.ProtocolWithCode(family).Name, ip.String())
	if err != nil {
		return nil, err
	}
	var out []string
	for _, s := range listen {
		addr, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("listen address %q: %w", s, err)
		}
		if len(addr) == 0 || addr[0].Protocol().Code != family || net.IP(addr[0].RawValue()).IsLoopback() {
			continue
		}
		onIP := append(ma.Multiaddr{*ipComp}, addr[1:]...)
		out = append(out, onIP.String())
	}
	return out, nil
}
