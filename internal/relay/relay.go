// Package relay keeps the server in the GossipSub mesh of topics it does not
// use itself, so that peers connected only through this server can still
// reach each other on them.
//
// GossipSub forwards a topic only through peers that announce it. A
// bootstrap server that most peers connect to, and that does not announce a
// topic, is a dead end for it: a service publishing on the topic and an app
// subscribed to it, each connected only to the bootstrap, never exchange a
// message. Relaying announces the topic and forwards its messages without
// delivering them to this server.
package relay

import (
	"errors"
	"fmt"
	"log/slog"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Relay relays a fixed set of topics.
type Relay struct {
	ps     *pubsub.PubSub
	topics []string
	logger *slog.Logger

	relays []relayed
}

type relayed struct {
	name   string
	topic  *pubsub.Topic
	cancel pubsub.RelayCancelFunc
}

// New returns a Relay for topics on ps. Nothing happens until Start.
func New(ps *pubsub.PubSub, topics []string, logger *slog.Logger) *Relay {
	return &Relay{ps: ps, topics: topics, logger: logger}
}

// Start joins and relays every topic. A topic this server has already
// joined (it uses it itself, so it already forwards it) is skipped with a
// warning, as is one that cannot be joined; the others are still relayed.
// The error, if any, names every topic that is not relayed.
func (r *Relay) Start() error {
	if r.ps == nil {
		return errors.New("relay: pubsub is not enabled")
	}
	var errs []error
	for _, name := range r.topics {
		topic, err := r.ps.Join(name)
		if err != nil {
			r.logger.Warn("not relaying topic", "topic", name, "error", err)
			errs = append(errs, fmt.Errorf("relay %s: %w", name, err))
			continue
		}
		cancel, err := topic.Relay()
		if err != nil {
			_ = topic.Close()
			r.logger.Warn("not relaying topic", "topic", name, "error", err)
			errs = append(errs, fmt.Errorf("relay %s: %w", name, err))
			continue
		}
		r.relays = append(r.relays, relayed{name: name, topic: topic, cancel: cancel})
		r.logger.Info("relaying topic", "topic", name)
	}
	return errors.Join(errs...)
}

// Topics returns the topics being relayed.
func (r *Relay) Topics() []string {
	out := make([]string, len(r.relays))
	for i, rl := range r.relays {
		out[i] = rl.name
	}
	return out
}

// Stop stops relaying. The topics are left to close with the pubsub
// instance: closing one right after cancelling its relay can race the
// cancellation and fail.
func (r *Relay) Stop() {
	for _, rl := range r.relays {
		rl.cancel()
	}
	r.relays = nil
}
