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

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/udx", port)),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
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
