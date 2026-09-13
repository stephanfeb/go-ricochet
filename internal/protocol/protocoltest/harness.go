// Package protocoltest runs a protocol pipeline against in-memory storage.
//
// A handler test used to need PostgreSQL and a libp2p host: the integration
// suite. This harness drives a pipeline through forgetest's mock stream, so
// a test names a caller, sends one request and reads one reply, with the
// MDA, MTA and storage wired as server.go wires them but nothing on the
// network.
package protocoltest

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/forgetest"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/ratelimit"
	"github.com/twostack/go-ricochet/internal/storage"
	"github.com/twostack/go-ricochet/internal/storage/storagetest"
	"github.com/twostack/go-ricochet/internal/trust"
)

// Env is one server's worth of services for a pipeline under test.
type Env struct {
	Store    *storagetest.Fake
	MDA      *mda.MailboxServer
	MTA      *mta.Router
	Config   *core.ServerConfig
	Registry *forge.Registry
	Pool     *codec.BufferPool
	Logger   *slog.Logger
}

// New builds an environment from the default config, adjusted by configure.
// Rate limits are the defaults, admission control and capacity gating are
// off (their registry entries are absent, which admits everything).
func New(t *testing.T, configure ...func(*core.ServerConfig)) *Env {
	t.Helper()

	cfg := core.DefaultConfig()
	for _, c := range configure {
		c(cfg)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storagetest.New()

	limiters := ratelimit.Default()
	t.Cleanup(limiters.Close)

	mdaSrv := mda.NewMailboxServer(store, mda.DefaultsFromConfig(cfg), logger).
		WithCacheSize(cfg.MailboxCacheSize)
	trusted, err := trust.Parse(cfg.TrustedPeers)
	if err != nil {
		t.Fatalf("trusted peers: %v", err)
	}
	router := mta.NewRouter(mdaSrv, limiters.MTA, logger).
		WithForwarding(cfg.EnableForwarding, trusted)

	reg := forge.NewRegistry()
	reg.Provide("storage", storage.Storage(store))
	reg.Provide("mta", router)
	reg.Provide("mda", mdaSrv)
	reg.Provide("config", cfg)
	reg.Provide(ratelimit.RegistryKey, limiters)
	reg.Provide(trust.RegistryKey, trusted)

	return &Env{
		Store:    store,
		MDA:      mdaSrv,
		MTA:      router,
		Config:   cfg,
		Registry: reg,
		Pool:     codec.NewBufferPool(),
		Logger:   logger,
	}
}

// PeerID returns a fresh Ed25519 identity.
func PeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}
	return id
}

// Exchange sends one request on a mock stream from the given peer and
// returns the reply frame. A []byte request is sent as is; anything else is
// marshalled to JSON.
func Exchange(t *testing.T, p *forge.Pipeline, from peer.ID, req any) []byte {
	t.Helper()
	payload, ok := req.([]byte)
	if !ok {
		var err error
		payload, err = json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
	}
	stream := forgetest.NewMockStreamWithFrame(from, payload)
	p.StreamHandler()(stream)
	reply, err := stream.ReadResponseFrame()
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return reply
}

// Decode unmarshals a reply into T.
func Decode[T any](t *testing.T, reply []byte) *T {
	t.Helper()
	var out T
	if err := json.Unmarshal(reply, &out); err != nil {
		t.Fatalf("decode %T from %s: %v", out, reply, err)
	}
	return &out
}

// RetrievePage is the decoded form of the MAA compound retrieve reply.
type RetrievePage struct {
	MessageCount int
	HasMore      bool
	Messages     []*core.Message
}

// DecodeRetrieve parses the compound retrieve reply: a length-prefixed
// metadata object followed by one length-prefixed message each.
func DecodeRetrieve(t *testing.T, reply []byte) *RetrievePage {
	t.Helper()
	next := func() []byte {
		if len(reply) < 4 {
			t.Fatalf("compound reply truncated at %d bytes", len(reply))
		}
		n := int(binary.BigEndian.Uint32(reply[:4]))
		if len(reply) < 4+n {
			t.Fatalf("compound frame claims %d bytes, %d remain", n, len(reply)-4)
		}
		part := reply[4 : 4+n]
		reply = reply[4+n:]
		return part
	}
	var meta struct {
		MessageCount int  `json:"messageCount"`
		HasMore      bool `json:"hasMore"`
	}
	if err := json.Unmarshal(next(), &meta); err != nil {
		t.Fatalf("decode retrieve metadata: %v", err)
	}
	page := &RetrievePage{MessageCount: meta.MessageCount, HasMore: meta.HasMore}
	for i := 0; i < meta.MessageCount; i++ {
		msg, err := core.MessageFromJSON(next())
		if err != nil {
			t.Fatalf("decode message %d: %v", i, err)
		}
		page.Messages = append(page.Messages, msg)
	}
	if len(reply) != 0 {
		t.Fatalf("%d trailing bytes after the compound reply", len(reply))
	}
	return page
}

// Deliver stores a message in the recipient's folder through the MDA, the
// way a submission would land, and returns its sequence number.
func (e *Env) Deliver(t *testing.T, sender, recipient peer.ID, folder string, payload []byte) *core.Message {
	t.Helper()
	msg := core.NewMessageWithDefaultExpiry(sender, recipient, payload)
	msg.FolderPath = folder
	if _, err := e.MDA.DeliverLocal(t.Context(), msg); err != nil {
		t.Fatalf("deliver to %s/%s: %v", recipient, folder, err)
	}
	return msg
}

// Mailbox returns the stored record for a folder, failing if it is absent.
func (e *Env) Mailbox(t *testing.T, owner peer.ID, folder string) *storage.MailboxRecord {
	t.Helper()
	rec, err := e.Store.FindMailbox(t.Context(), owner, folder)
	if err != nil {
		t.Fatalf("find mailbox: %v", err)
	}
	if rec == nil {
		t.Fatalf("mailbox %s/%s does not exist", owner, folder)
	}
	return rec
}

// Base64 encodes a request body the way the store protocols carry it.
func Base64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// FromBase64 decodes a reply body.
func FromBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return b
}
