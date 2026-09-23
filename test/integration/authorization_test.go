package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/stephanfeb/go-ricochet/internal/protocol/frame"
	"github.com/stephanfeb/go-ricochet/internal/protocol/maa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/mma"
	"github.com/stephanfeb/go-ricochet/internal/protocol/msa"
	"github.com/stephanfeb/go-ricochet/internal/protocol/sca"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// rawExchange sends one JSON frame on a fresh stream as the given identity
// and returns the first frame of the reply. It exists for the requests the
// Go client will not build: ones that name an identity other than its own.
func rawExchange(t *testing.T, ctx context.Context, server *testServer, priv crypto.PrivKey, pid protocol.ID, req any) []byte {
	t.Helper()
	h := createHost(t, priv)
	t.Cleanup(func() { h.Close() })
	h.Peerstore().AddAddrs(server.PeerID, server.Host.Addrs(), time.Hour)

	s, err := h.NewStream(ctx, server.PeerID, pid)
	if err != nil {
		t.Fatalf("open %s stream: %v", pid, err)
	}
	defer s.Close()

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := frame.WriteFrame(s, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	resp, err := frame.ReadFrame(s)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return resp
}

// Every protocol used to refuse an unauthorized request in its own way: a
// bare {"error": ...} map from MAA, an admin response with a message and no
// status, a submit ack with a message and no status, a 403 from the three
// store protocols. A client had four shapes to recognise for one fact.
//
// Every refusal is now a 403 whose message starts "unauthorized:", in the
// envelope the protocol already uses for its other failures.
func TestUnauthorizedRequestsShareOneShape(t *testing.T) {
	server := newTestServer(t)
	owner := newTestClient(t, server)
	ownerID := owner.PeerID()

	strangerKey, _ := keyAndID(t)
	stranger := newTestClientWithKey(t, server, strangerKey)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Resources the stranger will try to touch.
	if err := owner.CreateMailbox(ctx, "inbox", wire.MailboxPrivate); err != nil {
		t.Fatalf("owner create mailbox: %v", err)
	}
	if err := owner.CreateFeed(ctx, "news", "News", ""); err != nil {
		t.Fatalf("owner create feed: %v", err)
	}
	if err := owner.CreateCollection(ctx, "products", "Products"); err != nil {
		t.Fatalf("owner create collection: %v", err)
	}

	assertRefusal := func(t *testing.T, site string, status int, message string) {
		t.Helper()
		if status != wire.StatusForbidden {
			t.Errorf("%s: status %d, want 403", site, status)
		}
		if !strings.HasPrefix(message, "unauthorized: ") {
			t.Errorf("%s: message %q, want it to start with \"unauthorized: \"", site, message)
		}
	}

	// Refusals the client library surfaces as ForbiddenError.
	viaClient := []struct {
		site string
		call func() error
	}{
		{"sda put as non-owner", func() error {
			_, err := stranger.PutDocument(ctx, ownerID, "notes/a", []byte("x"))
			return err
		}},
		{"sfa append to a non-collaborative feed", func() error {
			_, err := stranger.AppendToFeed(ctx, ownerID, "news", []byte(`{"t":1}`), "post")
			return err
		}},
		{"maa retrieve of a foreign private inbox", func() error {
			_, err := stranger.RetrieveMessages(ctx, client.WithTargetPeer(ownerID))
			return err
		}},
	}
	for _, tc := range viaClient {
		err := tc.call()
		if err == nil {
			t.Errorf("%s: allowed", tc.site)
			continue
		}
		var fb *client.ForbiddenError
		if !errors.As(err, &fb) {
			t.Errorf("%s: err = %v, want ForbiddenError", tc.site, err)
			continue
		}
		assertRefusal(t, tc.site, fb.Status, fb.Message)
	}

	// Refusals of a request that names another identity, which the client
	// library never sends.
	t.Run("msa batch with a forged sender", func(t *testing.T) {
		forged := wire.NewMessageWithDefaultExpiry(ownerID, ownerID, []byte("from the owner, honest"))
		raw, err := frame.EncodeMessage(forged)
		if err != nil {
			t.Fatal(err)
		}
		reply := rawExchange(t, ctx, server, strangerKey, msa.BatchProtocolID,
			msa.BatchSubmitRequest{Messages: []json.RawMessage{raw}})
		var resp msa.BatchSubmitResponse
		if err := json.Unmarshal(reply, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(resp.Acks) != 1 || resp.Acks[0].Success {
			t.Fatalf("acks = %+v, want one refusal", resp.Acks)
		}
		assertRefusal(t, "msa batch", resp.Acks[0].Status, resp.Acks[0].ErrorMessage)
	})

	t.Run("maa expunge of another peer's mailboxes", func(t *testing.T) {
		reply := rawExchange(t, ctx, server, strangerKey, maa.ProtocolID,
			wire.ExpungeRequest{OperationType: "expunge", PeerID: ownerID.String()})
		var resp maa.ErrorResponse
		if err := json.Unmarshal(reply, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		assertRefusal(t, "maa expunge", resp.Status, resp.Error)
	})

	t.Run("mma create under another owner", func(t *testing.T) {
		reply := rawExchange(t, ctx, server, strangerKey, mma.ProtocolID,
			mma.AdminRequest{OperationType: mma.OpCreateMailbox, OwnerPeerID: ownerID.String(), FolderPath: "planted"})
		var resp mma.AdminResponse
		if err := json.Unmarshal(reply, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Success {
			t.Fatal("a stranger created a mailbox under the owner")
		}
		assertRefusal(t, "mma createMailbox", resp.Status, resp.ErrorMessage)
	})

	t.Run("sca put into another owner's collection", func(t *testing.T) {
		reply := rawExchange(t, ctx, server, strangerKey, sca.ProtocolID,
			sca.CollectionRequest{Operation: sca.OpPUT, OwnerPeerID: ownerID.String(), Path: "products",
				Key: "sku-1", Body: base64.StdEncoding.EncodeToString([]byte(`{"name":"Drill"}`))})
		var resp sca.CollectionResponse
		if err := json.Unmarshal(reply, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		msg, _ := resp.Headers["Error"].(string)
		assertRefusal(t, "sca put", resp.Status, msg)
	})
}
