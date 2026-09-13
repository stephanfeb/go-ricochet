package mma_test

import (
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/protocol/mma"
	"github.com/twostack/go-ricochet/internal/protocol/protocoltest"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
)

func setup(t *testing.T, configure ...func(*core.ServerConfig)) (*protocoltest.Env, func(from peer.ID, req mma.AdminRequest) *mma.AdminResponse, peer.ID) {
	t.Helper()
	env := protocoltest.New(t, configure...)
	p := mma.NewPipeline(env.Logger, env.Pool, env.Registry)
	owner := protocoltest.PeerID(t)
	call := func(from peer.ID, req mma.AdminRequest) *mma.AdminResponse {
		return protocoltest.Decode[mma.AdminResponse](t, protocoltest.Exchange(t, p, from, req))
	}
	return env, call, owner
}

func intp(n int) *int { return &n }

func TestCreateListAndDeleteMailbox(t *testing.T) {
	env, call, owner := setup(t)

	resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "work", MailboxType: "shared",
		MaxMessages: intp(50), RetentionDays: intp(7), RetentionCount: intp(10)})
	if !resp.Success {
		t.Fatalf("create: %+v", resp)
	}
	rec := env.Mailbox(t, owner, "work")
	if rec.Type != core.MailboxShared || rec.MaxMessages != 50 || rec.RetentionDays != 7 || rec.RetentionCount == nil || *rec.RetentionCount != 10 {
		t.Errorf("stored record = %+v, want shared, 50, 7 days, keep 10", rec)
	}

	// Creating it again is idempotent and keeps the original settings.
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "work", MaxMessages: intp(5)}); !resp.Success {
		t.Errorf("second create: %+v", resp)
	}
	if env.Mailbox(t, owner, "work").MaxMessages != 50 {
		t.Error("a second create changed the settings")
	}

	resp = call(owner, mma.AdminRequest{OperationType: mma.OpListMailboxes})
	if !resp.Success || len(resp.Mailboxes) != 1 || resp.Mailboxes[0].FolderPath != "work" || resp.Mailboxes[0].Type != "shared" {
		t.Errorf("list: %+v", resp)
	}

	if resp := call(protocoltest.PeerID(t), mma.AdminRequest{OperationType: mma.OpDeleteMailbox, FolderPath: "work"}); resp.Success {
		t.Error("a stranger deleted the owner's mailbox")
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpDeleteMailbox, FolderPath: "work"}); !resp.Success {
		t.Errorf("delete: %+v", resp)
	}
	if env.Store.MailboxCount() != 0 {
		t.Error("mailbox still exists after delete")
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpDeleteMailbox, FolderPath: "work"}); resp.Success {
		t.Error("deleting an absent mailbox reported success")
	}
}

func TestCreateClampsSettingsToTheServerDefaults(t *testing.T) {
	env, call, owner := setup(t, func(cfg *core.ServerConfig) { cfg.MaxMessagesPerMailbox = 100 })

	resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "big", MaxMessages: intp(1_000_000)})
	if !resp.Success {
		t.Fatalf("create: %+v", resp)
	}
	if got := env.Mailbox(t, owner, "big").MaxMessages; got != 100 {
		t.Errorf("max_messages stored as %d, want clamped to the server's 100", got)
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "bad", MailboxType: "royal"}); resp.Success {
		t.Error("an unknown mailbox type was accepted")
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "/absolute"}); resp.Success {
		t.Error("a folder path starting with / was accepted")
	}
}

func TestMailboxQuotaRefusesWith507(t *testing.T) {
	_, call, owner := setup(t, func(cfg *core.ServerConfig) { cfg.MaxMailboxesPerOwner = 1 })
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "one"}); !resp.Success {
		t.Fatalf("first create: %+v", resp)
	}
	resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "two"})
	if resp.Success || resp.Status != wire.StatusInsufficientStorage {
		t.Errorf("create past the per-owner quota: %+v, want 507", resp)
	}
}

func TestGrantRevokeAndListACL(t *testing.T) {
	env, call, owner := setup(t)
	grantee := protocoltest.PeerID(t)
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "shared", MailboxType: "shared"}); !resp.Success {
		t.Fatal(resp)
	}

	resp := call(owner, mma.AdminRequest{OperationType: mma.OpGrantAccess, FolderPath: "shared", GranteePeerID: grantee.String(), AccessMode: "readwrite"})
	if !resp.Success {
		t.Fatalf("grant: %+v", resp)
	}
	ok, err := env.Store.CheckAccess(t.Context(), env.Mailbox(t, owner, "shared").ID, grantee, core.AccessReadWrite)
	if err != nil || !ok {
		t.Errorf("grant not stored: ok %v err %v", ok, err)
	}

	resp = call(owner, mma.AdminRequest{OperationType: mma.OpListACL, FolderPath: "shared"})
	if !resp.Success || len(resp.ACL) != 1 || resp.ACL[0].PeerID != grantee.String() {
		t.Errorf("list ACL: %+v", resp)
	}

	if resp := call(grantee, mma.AdminRequest{OperationType: mma.OpGrantAccess, FolderPath: "shared", OwnerPeerID: owner.String(),
		GranteePeerID: grantee.String(), AccessMode: "readwrite"}); resp.Success || resp.Status != wire.StatusForbidden {
		t.Errorf("grantee granting on the owner's behalf: %+v, want 403", resp)
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpGrantAccess, FolderPath: "shared", GranteePeerID: "not a peer id", AccessMode: "read"}); resp.Success {
		t.Error("an unparseable grantee was accepted")
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpGrantAccess, FolderPath: "missing", GranteePeerID: grantee.String(), AccessMode: "read"}); resp.Success {
		t.Error("a grant on an absent mailbox succeeded")
	}

	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpRevokeAccess, FolderPath: "shared", GranteePeerID: grantee.String()}); !resp.Success {
		t.Errorf("revoke: %+v", resp)
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpListACL, FolderPath: "shared"}); len(resp.ACL) != 0 {
		t.Errorf("ACL after revoke: %+v", resp.ACL)
	}
}

func TestUpdateConfigAndMailboxInfo(t *testing.T) {
	env, call, owner := setup(t, func(cfg *core.ServerConfig) { cfg.MaxMessagesPerMailbox = 100 })
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpCreateMailbox, FolderPath: "inbox", RetentionDays: intp(30)}); !resp.Success {
		t.Fatal(resp)
	}
	env.Deliver(t, protocoltest.PeerID(t), owner, "inbox", []byte("one"))

	resp := call(owner, mma.AdminRequest{OperationType: mma.OpUpdateConfig, FolderPath: "inbox", RetentionDays: intp(1), MaxMessages: intp(5000)})
	if !resp.Success {
		t.Fatalf("update: %+v", resp)
	}
	rec := env.Mailbox(t, owner, "inbox")
	if rec.RetentionDays != 1 || rec.MaxMessages != 100 {
		t.Errorf("after update: retention %d max %d, want 1 and the clamped 100", rec.RetentionDays, rec.MaxMessages)
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpUpdateConfig, FolderPath: "nowhere", RetentionDays: intp(1)}); resp.Success {
		t.Error("update of an absent mailbox succeeded")
	}

	p := mma.NewPipeline(env.Logger, env.Pool, env.Registry)
	raw := protocoltest.Exchange(t, p, owner, mma.AdminRequest{OperationType: mma.OpGetMailboxInfo, FolderPath: "inbox"})
	var info struct {
		Success bool `json:"success"`
		Data    struct {
			Address       string `json:"address"`
			Type          string `json:"type"`
			MessageCount  int    `json:"messageCount"`
			RetentionDays int    `json:"retentionDays"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		t.Fatal(err)
	}
	if !info.Success || info.Data.MessageCount != 1 || info.Data.RetentionDays != 1 || info.Data.Type != "private" {
		t.Errorf("info = %+v", info)
	}
	if resp := call(protocoltest.PeerID(t), mma.AdminRequest{OperationType: mma.OpGetMailboxInfo, FolderPath: "inbox"}); resp.Success {
		t.Error("a stranger read the owner's mailbox info")
	}
}

func TestQueryCapacityBeforeAnySampleIsUnavailable(t *testing.T) {
	_, call, owner := setup(t)
	resp := call(owner, mma.AdminRequest{OperationType: mma.OpQueryCapacity})
	if resp.Success || resp.Status != wire.StatusServiceUnavailable {
		t.Errorf("capacity with no sampler: %+v, want 503", resp)
	}
}

func TestMalformedAdminRequests(t *testing.T) {
	env, call, owner := setup(t)
	if resp := call(owner, mma.AdminRequest{OperationType: "selfDestruct"}); resp.Success || resp.Status != wire.StatusBadRequest {
		t.Errorf("unknown operation: %+v, want 400", resp)
	}
	if resp := call(owner, mma.AdminRequest{OperationType: mma.OpGrantAccess}); resp.Success {
		t.Error("grant with no folder succeeded")
	}
	p := mma.NewPipeline(env.Logger, env.Pool, env.Registry)
	resp := protocoltest.Decode[mma.AdminResponse](t, protocoltest.Exchange(t, p, owner, []byte("{not json")))
	if resp.Success || resp.Status != wire.StatusBadRequest {
		t.Errorf("unparseable request: %+v, want 400", resp)
	}
}
