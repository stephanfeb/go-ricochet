package p2p

import (
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	multiaddr "github.com/multiformats/go-multiaddr"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/twostack/go-ricochet/internal/core"
)

// CreateHost creates a libp2p host configured for Ricochet.
func CreateHost(cfg *core.ServerConfig, priv crypto.PrivKey, logger *slog.Logger) (host.Host, error) {
	port := cfg.Port
	if port == 0 {
		port = 55223
	}

	// Use configured listen addresses, falling back to IPv4 wildcard.
	// NOTE: On Linux, binding IPv6 [::] with default IPV6_V6ONLY=0 claims the
	// port for both families, causing a subsequent IPv4 0.0.0.0 bind on the
	// same port to fail with "address already in use". Bind IPv4 only.
	listenAddrs := cfg.ListenAddresses
	if len(listenAddrs) == 0 {
		listenAddrs = []string{
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/udx", port),
		}
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	}

	// Advertise external (public) addresses so that Identify reports them
	// to connecting peers. Without this, a server behind NAT only reports
	// its private IPs, which remote clients filter out, leaving the
	// peerstore empty and preventing reconnection.
	if len(cfg.ExternalAddresses) > 0 {
		extMAs := make([]multiaddr.Multiaddr, 0, len(cfg.ExternalAddresses))
		for _, addr := range cfg.ExternalAddresses {
			ma, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				logger.Warn("invalid external address, skipping", "addr", addr, "error", err)
				continue
			}
			extMAs = append(extMAs, ma)
		}
		if len(extMAs) > 0 {
			opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return append(addrs, extMAs...)
			}))
			logger.Info("advertising external addresses", "count", len(extMAs))
		}
	}

	// AutoNAT v2 configuration — enables dial-back service for clients.
	if cfg.EnableAutoNAT {
		opts = append(opts, libp2p.EnableAutoNATv2())
	}

	// Relay configuration.
	if cfg.EnableRelay {
		opts = append(opts, libp2p.EnableRelay())
		if cfg.EnableHolePunching {
			opts = append(opts, libp2p.EnableHolePunching())
		}
		if cfg.EnableAutoRelay && len(cfg.BootstrapPeers) > 0 {
			relayPeers := make([]peer.AddrInfo, 0, len(cfg.BootstrapPeers))
			for _, addr := range cfg.BootstrapPeers {
				ma, err := multiaddr.NewMultiaddr(addr)
				if err != nil {
					logger.Warn("invalid bootstrap peer for relay", "addr", addr, "error", err)
					continue
				}
				ai, err := peer.AddrInfoFromP2pAddr(ma)
				if err != nil {
					logger.Warn("cannot parse bootstrap peer info", "addr", addr, "error", err)
					continue
				}
				relayPeers = append(relayPeers, *ai)
			}
			if len(relayPeers) > 0 {
				opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(relayPeers))
			}
		}
	} else {
		opts = append(opts, libp2p.DisableRelay())
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("create host: %w", err)
	}

	logger.Info("created libp2p host",
		"peer_id", h.ID().String(),
		"port", port,
	)

	return h, nil
}
