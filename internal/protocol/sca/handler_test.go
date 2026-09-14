package sca_test

import (
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/protocol/protocoltest"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
)

func setup(t *testing.T) (func(from peer.ID, req sca.CollectionRequest) *sca.CollectionResponse, peer.ID) {
	t.Helper()
	env := protocoltest.New(t)
	p := sca.NewPipeline(env.Logger, env.Pool, env.Registry)
	owner := protocoltest.PeerID(t)
	call := func(from peer.ID, req sca.CollectionRequest) *sca.CollectionResponse {
		if req.OwnerPeerID == "" {
			req.OwnerPeerID = owner.String()
		}
		return protocoltest.Decode[sca.CollectionResponse](t, protocoltest.Exchange(t, p, from, req))
	}
	return call, owner
}

func intp(n int) *int { return &n }

func put(t *testing.T, call func(peer.ID, sca.CollectionRequest) *sca.CollectionResponse, owner peer.ID, key, body string) *sca.CollectionResponse {
	t.Helper()
	resp := call(owner, sca.CollectionRequest{Operation: sca.OpPUT, Path: "products", Key: key, Body: protocoltest.Base64([]byte(body))})
	if resp.Status != sca.StatusCreated && resp.Status != sca.StatusOK {
		t.Fatalf("put %s: %+v", key, resp)
	}
	return resp
}

func TestCollectionLifecycle(t *testing.T) {
	call, owner := setup(t)

	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "products", Name: "Products", Visibility: "public"}); resp.Status != sca.StatusCreated {
		t.Fatalf("create: %+v", resp)
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "products", Name: "Again"}); resp.Status == sca.StatusCreated {
		t.Errorf("duplicate create: %+v, want a refusal", resp)
	}

	resp := put(t, call, owner, "sku-1", `{"name":"Drill","price":30}`)
	if resp.Status != sca.StatusCreated || resp.Headers["X-Version"] != float64(1) {
		t.Errorf("first put: %+v, want 201 version 1", resp)
	}
	etag := resp.Headers["ETag"].(string)
	resp = put(t, call, owner, "sku-1", `{"name":"Drill","price":25}`)
	if resp.Status != sca.StatusOK || resp.Headers["X-Version"] != float64(2) || resp.Headers["ETag"] == etag {
		t.Errorf("second put: %+v, want 200 version 2 and a new ETag", resp)
	}

	resp = call(owner, sca.CollectionRequest{Operation: sca.OpPUT, Path: "products", Key: "sku-1",
		Body: protocoltest.Base64([]byte(`{}`)), Headers: map[string]string{"If-Match": etag}})
	if resp.Status != sca.StatusConflict {
		t.Errorf("stale If-Match: %+v, want 409", resp)
	}

	resp = call(protocoltest.PeerID(t), sca.CollectionRequest{Operation: sca.OpGET, Path: "products", Key: "sku-1"})
	if resp.Status != sca.StatusOK || string(resp.Data) != `{"name":"Drill","price":25}` || resp.Headers["X-Version"] != float64(2) {
		t.Errorf("get item of a public collection by anyone: %+v", resp)
	}
	resp = call(owner, sca.CollectionRequest{Operation: sca.OpGET, Path: "products"})
	if resp.Status != sca.StatusOK || resp.Headers["X-Record-Count"] != float64(1) {
		t.Errorf("get collection: %+v, want record count 1", resp)
	}

	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpDELETE, Path: "products", Key: "sku-1"}); resp.Status != sca.StatusNoContent {
		t.Errorf("delete item: %+v, want 204", resp)
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpGET, Path: "products", Key: "sku-1"}); resp.Status != sca.StatusNotFound {
		t.Errorf("get deleted item: %+v, want 404", resp)
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpDELETE, Path: "products"}); resp.Status != sca.StatusNoContent {
		t.Errorf("delete collection: %+v, want 204", resp)
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpGET, Path: "products"}); resp.Status != sca.StatusNotFound {
		t.Errorf("get deleted collection: %+v, want 404", resp)
	}
}

func TestListKeysAndQuery(t *testing.T) {
	call, owner := setup(t)
	call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "products", Name: "Products", Visibility: "public"})
	put(t, call, owner, "c", `{"name":"Saw","price":40,"tag":"tool"}`)
	put(t, call, owner, "a", `{"name":"Drill","price":30,"tag":"tool"}`)
	put(t, call, owner, "b", `{"name":"Glue","price":5,"tag":"supply"}`)

	resp := call(owner, sca.CollectionRequest{Operation: sca.OpLIST, Path: "products", Limit: intp(2)})
	if resp.Status != sca.StatusOK || resp.Headers["X-Total-Count"] != float64(3) || resp.Headers["X-Has-More"] != true {
		t.Fatalf("list keys: %+v, want total 3 and more", resp)
	}
	var keyPage struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &keyPage); err != nil {
		t.Fatal(err)
	}
	if keys := keyPage.Keys; len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("keys = %v, want [a b]", keys)
	}

	resp = call(protocoltest.PeerID(t), sca.CollectionRequest{Operation: sca.OpQUERY, Path: "products",
		Filter: map[string]any{"tag": "tool", "price": map[string]any{"$gte": 35}}})
	if resp.Status != sca.StatusOK {
		t.Fatalf("query: %+v", resp)
	}
	var result struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatal(err)
	}
	if items := result.Items; len(items) != 1 || items[0].Key != "c" {
		t.Errorf("query = %+v, want only the saw", items)
	}

	// Sorting compares the field as text, so it is exercised on names.
	resp = call(owner, sca.CollectionRequest{Operation: sca.OpQUERY, Path: "products", SortField: "name", SortAsc: boolp(false), Limit: intp(2)})
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatal(err)
	}
	if items := result.Items; len(items) != 2 || items[0].Key != "c" || items[1].Key != "b" || resp.Headers["X-Has-More"] != true {
		t.Errorf("sorted query = %+v (headers %v), want saw then glue with more", items, resp.Headers)
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpQUERY, Path: "products", Filter: map[string]any{"price": map[string]any{"$near": 1}}}); resp.Status != sca.StatusBadRequest {
		t.Errorf("unknown operator: %+v, want 400", resp)
	}
}

func TestCollectionRequestValidation(t *testing.T) {
	call, owner := setup(t)
	stranger := protocoltest.PeerID(t)
	call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "products", Name: "Products"})

	if resp := call(stranger, sca.CollectionRequest{Operation: sca.OpPUT, Path: "products", Key: "k", Body: protocoltest.Base64([]byte(`{}`))}); resp.Status != sca.StatusForbidden {
		t.Errorf("stranger put: %+v, want 403", resp)
	}
	if resp := call(stranger, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "planted", Name: "x"}); resp.Status != sca.StatusForbidden {
		t.Errorf("stranger create: %+v, want 403", resp)
	}
	cases := map[string]sca.CollectionRequest{
		"bad owner":  {Operation: sca.OpGET, OwnerPeerID: "nope", Path: "products"},
		"bad path":   {Operation: sca.OpGET, Path: "../products"},
		"unknown op": {Operation: "MERGE", Path: "products"},
		"no key":     {Operation: sca.OpPUT, Path: "products", Body: protocoltest.Base64([]byte(`{}`))},
		"bad body":   {Operation: sca.OpPUT, Path: "products", Key: "k", Body: "!!"},
		"not json":   {Operation: sca.OpPUT, Path: "products", Key: "k", Body: protocoltest.Base64([]byte("plain"))},
		"long key":   {Operation: sca.OpPUT, Path: "products", Key: string(make([]byte, wire.MaxCollectionKeyLength+1)), Body: protocoltest.Base64([]byte(`{}`))},
	}
	for name, req := range cases {
		if resp := call(owner, req); resp.Status != sca.StatusBadRequest {
			t.Errorf("%s: %+v, want 400", name, resp)
		}
	}
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpPUT, Path: "absent", Key: "k", Body: protocoltest.Base64([]byte(`{}`))}); resp.Status != sca.StatusNotFound {
		t.Errorf("put into an absent collection: %+v, want 404", resp)
	}
}

func boolp(b bool) *bool { return &b }

// A collection is private by default: its metadata, items, keys and queries
// are the owner's until the owner shares it with a reader list or makes it
// public. The check is one per operation, before any item is fetched.
func TestCollectionVisibility(t *testing.T) {
	call, owner := setup(t)
	stranger := protocoltest.PeerID(t)
	friend := protocoltest.PeerID(t)

	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "products", Name: "Products"}); resp.Status != sca.StatusCreated {
		t.Fatalf("create: %+v", resp)
	}
	put(t, call, owner, "jan", `{"total":10}`)
	call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "menu", Name: "Menu", Visibility: "public"})
	if resp := call(owner, sca.CollectionRequest{Operation: sca.OpCREATE, Path: "odd", Name: "Odd", Visibility: "secret"}); resp.Status != sca.StatusBadRequest {
		t.Errorf("create with an unknown visibility: %+v, want 400", resp)
	}

	reads := map[string]sca.CollectionRequest{
		"GET":   {Operation: sca.OpGET, Path: "products"},
		"item":  {Operation: sca.OpGET, Path: "products", Key: "jan"},
		"keys":  {Operation: sca.OpLIST, Path: "products"},
		"QUERY": {Operation: sca.OpQUERY, Path: "products", Filter: map[string]any{"total": 10}},
	}
	for name, req := range reads {
		if resp := call(stranger, req); resp.Status != sca.StatusForbidden || resp.Body != "" {
			t.Errorf("stranger %s of a private collection: %+v, want 403 and no body", name, resp)
		}
		if resp := call(owner, req); resp.Status != sca.StatusOK {
			t.Errorf("owner %s of a private collection: %+v, want 200", name, resp)
		}
	}

	listPaths := func(from peer.ID) []string {
		resp := call(from, sca.CollectionRequest{Operation: sca.OpLIST})
		if resp.Status != sca.StatusOK {
			t.Fatalf("list: %+v", resp)
		}
		var entries []struct {
			Path       string `json:"path"`
			Visibility string `json:"visibility"`
		}
		if err := json.Unmarshal(protocoltest.FromBase64(t, resp.Body), &entries); err != nil {
			t.Fatal(err)
		}
		paths := make([]string, 0, len(entries))
		for _, e := range entries {
			paths = append(paths, e.Path+":"+e.Visibility)
		}
		return paths
	}
	if got := listPaths(stranger); len(got) != 1 || got[0] != "menu:public" {
		t.Errorf("stranger list = %v, want only the public collection", got)
	}
	if got := listPaths(owner); len(got) != 2 {
		t.Errorf("owner list = %v, want both", got)
	}

	if resp := call(stranger, sca.CollectionRequest{Operation: sca.OpACCESS, Path: "products", AccessAction: "set", Visibility: "public"}); resp.Status != sca.StatusForbidden {
		t.Errorf("stranger ACCESS set: %+v, want 403", resp)
	}
	call(owner, sca.CollectionRequest{Operation: sca.OpACCESS, Path: "products", AccessAction: "grant", ReaderPeerID: friend.String()})
	resp := call(owner, sca.CollectionRequest{Operation: sca.OpACCESS, Path: "products", AccessAction: "set", Visibility: "shared"})
	if resp.Status != sca.StatusOK || resp.Headers["Visibility"] != "shared" {
		t.Fatalf("set shared: %+v", resp)
	}
	for name, req := range reads {
		if resp := call(friend, req); resp.Status != sca.StatusOK {
			t.Errorf("friend %s of a shared collection: %+v, want 200", name, resp)
		}
		if resp := call(stranger, req); resp.Status != sca.StatusForbidden {
			t.Errorf("stranger %s of a shared collection: %+v, want 403", name, resp)
		}
	}
	if got := listPaths(friend); len(got) != 2 || got[1] != "products:shared" {
		t.Errorf("friend list = %v, want the shared collection too", got)
	}
	call(owner, sca.CollectionRequest{Operation: sca.OpACCESS, Path: "products", AccessAction: "revoke", ReaderPeerID: friend.String()})
	if resp := call(friend, sca.CollectionRequest{Operation: sca.OpGET, Path: "products", Key: "jan"}); resp.Status != sca.StatusForbidden {
		t.Errorf("revoked reader: %+v, want 403", resp)
	}
	call(owner, sca.CollectionRequest{Operation: sca.OpACCESS, Path: "products", AccessAction: "set", Visibility: "public"})
	for name, req := range reads {
		if resp := call(stranger, req); resp.Status != sca.StatusOK {
			t.Errorf("stranger %s of a public collection: %+v, want 200", name, resp)
		}
	}
	// Reads never became writes.
	if resp := call(stranger, sca.CollectionRequest{Operation: sca.OpPUT, Path: "products", Key: "feb", Body: protocoltest.Base64([]byte(`{}`))}); resp.Status != sca.StatusForbidden {
		t.Errorf("stranger put into a public collection: %+v, want 403", resp)
	}
}
