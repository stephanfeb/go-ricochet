package client

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/notify"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
	"github.com/twostack/go-ricochet/pkg/wire"
)

// newHost is a loopback TCP host: enough to exercise the client's dialling
// and reply handling without a server, storage or the UDX transport.
func newHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableRelay())
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// serve answers every request on pid with whatever respond returns for it.
func serve(h host.Host, pid protocol.ID, respond func(req []byte) []byte) {
	h.SetStreamHandler(pid, func(s network.Stream) {
		defer s.Close()
		req, err := frame.ReadFrame(s)
		if err != nil {
			return
		}
		_ = frame.WriteFrame(s, respond(req))
	})
}

func jsonReply(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ackingServer accepts every submission.
func ackingServer(t *testing.T) host.Host {
	t.Helper()
	h := newHost(t)
	serve(h, msa.ProtocolID, func(req []byte) []byte {
		msg, err := frame.DecodeMessage(req)
		if err != nil {
			t.Errorf("server decode: %v", err)
			return nil
		}
		ack, _ := frame.EncodeStoreAck(&core.StoreAck{Success: true, MessageID: msg.MessageID})
		return ack
	})
	return h
}

func newClient(t *testing.T, servers ...host.Host) (*Client, host.Host) {
	t.Helper()
	h := newHost(t)
	cfg := Config{ConnectionTimeout: 5 * time.Second, MessageTimeout: 5 * time.Second}
	for i, srv := range servers {
		h.Peerstore().AddAddrs(srv.ID(), srv.Addrs(), time.Hour)
		cfg.PreferredServers = append(cfg.PreferredServers, ServerPreference{PeerID: srv.ID(), Priority: i + 1, Weight: 1})
	}
	return New(h, cfg), h
}

// deadPeer is an identity whose only address is a closed port, so dialling
// it fails the way an unreachable server does.
func deadPeer(t *testing.T, h host.Host) peer.ID {
	t.Helper()
	ghost := newHost(t)
	id := ghost.ID()
	ghost.Close()
	addr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	h.Peerstore().AddAddrs(id, []ma.Multiaddr{addr}, time.Hour)
	return id
}

func TestDeleteOutcomeIsHonest(t *testing.T) {
	cases := []struct {
		status  int
		deleted bool
		fails   bool
		want    error
	}{
		{wire.StatusNotFound, false, false, nil},
		{204, true, false, nil},
		{wire.StatusOK, true, false, nil},
		{wire.StatusForbidden, false, true, ErrForbidden},
		{wire.StatusInternalError, false, true, nil},
	}
	for _, tc := range cases {
		deleted, err := deleteOutcome("delete", tc.status, map[string]any{"Error": "because"})
		if deleted != tc.deleted {
			t.Errorf("status %d: deleted = %v, want %v", tc.status, deleted, tc.deleted)
		}
		if (err != nil) != tc.fails {
			t.Errorf("status %d: err = %v, want failure %v", tc.status, err, tc.fails)
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Errorf("status %d: err = %v, want %v", tc.status, err, tc.want)
		}
	}
}

// The store protocols answer with a status in the envelope. Every delete,
// the patch and the head used to read a refusal or a fault as success or as
// an untyped string; they now return the typed error for the status.
func TestFailuresOnTheStoreProtocolsAreTypedErrors(t *testing.T) {
	srv := newHost(t)
	// The path (or key) names the status the fake server answers with.
	statusOf := func(req map[string]any) int {
		for _, k := range []string{"key", "path"} {
			if s, ok := req[k].(string); ok {
				switch {
				case strings.HasSuffix(s, "forbidden"):
					return wire.StatusForbidden
				case strings.HasSuffix(s, "missing"):
					return wire.StatusNotFound
				case strings.HasSuffix(s, "conflict"):
					return wire.StatusConflict
				case strings.HasSuffix(s, "broken"):
					return wire.StatusInternalError
				}
			}
		}
		return 204
	}
	respond := func(req []byte) []byte {
		var m map[string]any
		_ = json.Unmarshal(req, &m)
		status := statusOf(m)
		headers := map[string]any{"Error": "as requested"}
		if status == wire.StatusConflict {
			headers["Actual-ETag"] = "sha256:current"
		}
		return jsonReply(t, map[string]any{"status": status, "headers": headers})
	}
	serve(srv, sda.ProtocolID, respond)
	serve(srv, sca.ProtocolID, respond)
	serve(srv, sfa.ProtocolID, respond)

	cl, _ := newClient(t, srv)
	ctx := context.Background()
	owner := cl.PeerID()

	deleted, err := cl.DeleteDocument(ctx, owner, "doc-forbidden")
	if deleted || !errors.Is(err, ErrForbidden) {
		t.Errorf("forbidden document delete: deleted %v err %v, want false and ErrForbidden", deleted, err)
	}
	deleted, err = cl.DeleteDocument(ctx, owner, "doc-missing")
	if deleted || err != nil {
		t.Errorf("missing document delete: deleted %v err %v, want false and nil", deleted, err)
	}
	deleted, err = cl.DeleteDocument(ctx, owner, "doc-gone")
	if !deleted || err != nil {
		t.Errorf("document delete: deleted %v err %v, want true", deleted, err)
	}

	deleted, err = cl.DeleteCollection(ctx, "coll-broken")
	var pe *ProtocolError
	if deleted || !errors.As(err, &pe) || pe.Status != wire.StatusInternalError {
		t.Errorf("broken collection delete: deleted %v err %v, want false and a 500 ProtocolError", deleted, err)
	}
	deleted, err = cl.DeleteCollectionItem(ctx, "coll", "item-forbidden")
	if deleted || !errors.Is(err, ErrForbidden) {
		t.Errorf("forbidden item delete: deleted %v err %v", deleted, err)
	}
	deleted, err = cl.DeleteFeed(ctx, "feed-forbidden")
	if deleted || !errors.Is(err, ErrForbidden) {
		t.Errorf("forbidden feed delete: deleted %v err %v", deleted, err)
	}

	_, err = cl.PatchDocument(ctx, owner, "doc-conflict", map[string]any{"a": 1})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.ActualETag != "sha256:current" {
		t.Errorf("patch conflict: err = %v, want ConflictError carrying the server's ETag", err)
	}
	if _, err := cl.PatchDocument(ctx, owner, "doc-forbidden", map[string]any{}); !errors.Is(err, ErrForbidden) {
		t.Errorf("forbidden patch: err = %v", err)
	}
	if _, err := cl.HeadDocument(ctx, owner, "doc-missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("head of a missing document: err = %v, want ErrNotFound", err)
	}
	if _, err := cl.QueryCollection(ctx, owner, "coll-missing", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("query of a missing collection: err = %v, want ErrNotFound", err)
	}
	if _, _, err := cl.ListCollectionKeys(ctx, owner, "coll-missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("keys of a missing collection: err = %v, want ErrNotFound", err)
	}
}

func TestDialFailsOverToTheNextPreferredServer(t *testing.T) {
	backup := ackingServer(t)
	cl, h := newClient(t, backup)
	dead := deadPeer(t, h)
	// The dead server is preferred; the backup is what keeps the client
	// working.
	cl.cfg.PreferredServers = append([]ServerPreference{{PeerID: dead, Priority: 0, Weight: 1}}, cl.cfg.PreferredServers...)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := cl.SendMessage(ctx, cl.PeerID(), []byte("hello"))
	if err != nil {
		t.Fatalf("send with the preferred server down: %v", err)
	}
	if !res.Success || res.StoredAtServer != backup.ID() {
		t.Errorf("result = %+v, want success at the backup %s", res, backup.ID())
	}

	// An explicit server is never substituted: naming a dead one fails.
	if _, err := cl.GetDocument(ctx, cl.PeerID(), "x", WithDocServer(dead)); err == nil {
		t.Error("an explicitly named dead server was silently replaced")
	}

	// With every server down the error names each attempt.
	backup.Close()
	_, err = cl.SendMessage(ctx, cl.PeerID(), []byte("hello"))
	if err == nil || !strings.Contains(err.Error(), "no preferred server reachable") {
		t.Errorf("send with every server down: err = %v", err)
	} else if !strings.Contains(err.Error(), dead.String()[:12]) || !strings.Contains(err.Error(), backup.ID().String()[:12]) {
		t.Errorf("err = %v, want both servers named", err)
	}
}

func TestServersAreTriedByPriorityThenWeight(t *testing.T) {
	a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
	cl := &Client{cfg: Config{PreferredServers: []ServerPreference{
		{PeerID: c, Priority: 2, Weight: 1},
		{PeerID: a, Priority: 1, Weight: 1},
		{PeerID: b, Priority: 1, Weight: 5},
	}}}
	if got := cl.servers(); !slices.Equal(got, []peer.ID{b, a, c}) {
		t.Errorf("order = %v, want [b a c]", got)
	}
	if got := (&Client{}).servers(); len(got) != 0 {
		t.Errorf("no servers configured yielded %v", got)
	}
}

func TestCloseRefusesFurtherRequestsAndUnregistersHandlers(t *testing.T) {
	srv := ackingServer(t)
	cl, h := newClient(t, srv)
	cl.RegisterNotificationHandler(func(*wire.Notification) {})
	if !slices.Contains(h.Mux().Protocols(), notify.ProtocolID) {
		t.Fatal("notification handler not registered")
	}

	if err := cl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
	if slices.Contains(h.Mux().Protocols(), notify.ProtocolID) {
		t.Error("notification handler still registered after Close")
	}
	if _, err := cl.SendMessage(context.Background(), cl.PeerID(), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Errorf("send after close: err = %v, want ErrClosed", err)
	}
	if _, err := cl.GetDocument(context.Background(), cl.PeerID(), "x"); !errors.Is(err, ErrClosed) {
		t.Errorf("get after close: err = %v, want ErrClosed", err)
	}
	// The host is the caller's and stays usable.
	if h.Network().ListenAddresses() == nil {
		t.Error("Close took the caller's host down")
	}
}
