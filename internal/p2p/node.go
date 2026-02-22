package p2p

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"

	"github.com/twostack/go-ricochet/internal/core"
)

// Node wraps a libp2p host with DHT and GossipSub.
type Node struct {
	host   host.Host
	dht    *dht.IpfsDHT
	pubsub *pubsub.PubSub
	topics map[string]*pubsub.Topic
	subs   map[string]*pubsub.Subscription
	logger *slog.Logger
	mu     sync.RWMutex
	cancel context.CancelFunc
}

// NewNode creates and starts a new P2P node.
func NewNode(ctx context.Context, cfg *core.ServerConfig, priv interface{ Raw() ([]byte, error) }, h host.Host, logger *slog.Logger) (*Node, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Create DHT in server mode
	kadDHT, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.AddressFilter(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrs
		}),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create dht: %w", err)
	}
	logger.Info("DHT initialized", "mode", "server")

	// Bootstrap DHT
	if err := kadDHT.Bootstrap(ctx); err != nil {
		kadDHT.Close()
		cancel()
		return nil, fmt.Errorf("bootstrap dht: %w", err)
	}

	// Connect to bootstrap peers
	for _, peerAddr := range cfg.BootstrapPeers {
		maddr, err := multiaddr.NewMultiaddr(peerAddr)
		if err != nil {
			logger.Warn("invalid bootstrap peer", "addr", peerAddr, "error", err)
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			logger.Warn("invalid bootstrap peer info", "addr", peerAddr, "error", err)
			continue
		}
		if err := h.Connect(ctx, *info); err != nil {
			logger.Warn("failed to connect to bootstrap peer", "peer", info.ID, "error", err)
		} else {
			logger.Info("connected to bootstrap peer", "peer", info.ID)
		}
	}

	// Create GossipSub with strict message signing (required by Dart client)
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		kadDHT.Close()
		cancel()
		return nil, fmt.Errorf("create gossipsub: %w", err)
	}
	logger.Info("GossipSub initialized")

	node := &Node{
		host:   h,
		dht:    kadDHT,
		pubsub: ps,
		topics: make(map[string]*pubsub.Topic),
		subs:   make(map[string]*pubsub.Subscription),
		logger: logger,
		cancel: cancel,
	}

	return node, nil
}

// PeerID returns this node's peer ID.
func (n *Node) PeerID() peer.ID {
	return n.host.ID()
}

// Addrs returns this node's listen addresses.
func (n *Node) Addrs() []multiaddr.Multiaddr {
	return n.host.Addrs()
}

// Host returns the underlying libp2p host.
func (n *Node) Host() host.Host {
	return n.host
}

// DHT returns the underlying Kademlia DHT.
func (n *Node) DHT() *dht.IpfsDHT {
	return n.dht
}

// PubSub returns the underlying GossipSub instance.
func (n *Node) PubSub() *pubsub.PubSub {
	return n.pubsub
}

// JoinTopic joins a GossipSub topic.
func (n *Node) JoinTopic(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.topics[name]; exists {
		return nil
	}

	topic, err := n.pubsub.Join(name)
	if err != nil {
		return fmt.Errorf("join topic %s: %w", name, err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribe topic %s: %w", name, err)
	}

	n.topics[name] = topic
	n.subs[name] = sub
	n.logger.Info("joined topic", "topic", name)

	return nil
}

// Publish publishes data to a topic.
func (n *Node) Publish(ctx context.Context, topic string, data []byte) error {
	n.mu.RLock()
	t, exists := n.topics[topic]
	n.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s not joined", topic)
	}

	return t.Publish(ctx, data)
}

// Subscribe returns a subscription for a topic.
func (n *Node) Subscribe(topic string) *pubsub.Subscription {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.subs[topic]
}

// Close shuts down the P2P node.
func (n *Node) Close() error {
	n.cancel()

	n.mu.Lock()
	for _, sub := range n.subs {
		sub.Cancel()
	}
	for _, topic := range n.topics {
		topic.Close()
	}
	n.mu.Unlock()

	if err := n.dht.Close(); err != nil {
		n.logger.Warn("error closing dht", "error", err)
	}

	return n.host.Close()
}
