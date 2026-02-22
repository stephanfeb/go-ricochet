package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/pkg/client"
)

func TestCreateAndGetCollection(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a collection.
	err := cl.CreateCollection(ctx, "products", "Product Catalog")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Retrieve metadata.
	info, err := cl.GetCollection(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info == nil {
		t.Fatal("expected collection info, got nil")
	}
	if info.Name != "Product Catalog" {
		t.Fatalf("expected name 'Product Catalog', got %q", info.Name)
	}
	if info.RecordCount != 0 {
		t.Fatalf("expected recordCount 0, got %d", info.RecordCount)
	}
}

func TestPutAndGetCollectionItem(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	content := []byte(`{"name":"Drill","price":49.99,"category":"tools"}`)
	result, err := cl.PutCollectionItem(ctx, "products", "sku-001", content)
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	if !result.Created {
		t.Fatal("expected item to be created")
	}
	if result.Version != 1 {
		t.Fatalf("expected version 1, got %d", result.Version)
	}
	if result.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// Get the item back.
	item, err := cl.GetCollectionItem(ctx, cl.PeerID(), "products", "sku-001")
	if err != nil {
		t.Fatalf("GetCollectionItem: %v", err)
	}
	if item == nil {
		t.Fatal("expected item, got nil")
	}
	if item.Version != 1 {
		t.Fatalf("expected version 1, got %d", item.Version)
	}

	// Verify content roundtrips correctly.
	var parsed map[string]any
	if err := json.Unmarshal(item.Content, &parsed); err != nil {
		t.Fatalf("unmarshal content: %v", err)
	}
	if parsed["name"] != "Drill" {
		t.Fatalf("expected name 'Drill', got %v", parsed["name"])
	}

	// Check recordCount incremented.
	info, err := cl.GetCollection(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info.RecordCount != 1 {
		t.Fatalf("expected recordCount 1, got %d", info.RecordCount)
	}
}

func TestPutUpdateItem(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Insert
	content1 := []byte(`{"name":"Drill","price":49.99}`)
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", content1)
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Update same key
	content2 := []byte(`{"name":"Drill","price":39.99}`)
	result, err := cl.PutCollectionItem(ctx, "products", "sku-001", content2)
	if err != nil {
		t.Fatalf("PutCollectionItem (update): %v", err)
	}
	if result.Created {
		t.Fatal("expected update, not create")
	}
	if result.Version != 2 {
		t.Fatalf("expected version 2, got %d", result.Version)
	}

	// recordCount should still be 1
	info, err := cl.GetCollection(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info.RecordCount != 1 {
		t.Fatalf("expected recordCount 1, got %d", info.RecordCount)
	}
}

func TestOptimisticLocking(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Insert item and get its ETag
	content := []byte(`{"name":"Drill","price":49.99}`)
	result, err := cl.PutCollectionItem(ctx, "products", "sku-001", content)
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	etag := result.ETag

	// Update with correct If-Match should succeed
	content2 := []byte(`{"name":"Drill","price":39.99}`)
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", content2,
		client.WithCollectionIfMatch(etag))
	if err != nil {
		t.Fatalf("PutCollectionItem with correct If-Match: %v", err)
	}

	// Update with old (now wrong) If-Match should fail
	content3 := []byte(`{"name":"Drill","price":29.99}`)
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", content3,
		client.WithCollectionIfMatch(etag))
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

func TestDeleteCollectionItem(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	content := []byte(`{"name":"Drill","price":49.99}`)
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", content)
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Delete the item
	deleted, err := cl.DeleteCollectionItem(ctx, "products", "sku-001")
	if err != nil {
		t.Fatalf("DeleteCollectionItem: %v", err)
	}
	if !deleted {
		t.Fatal("expected item to be deleted")
	}

	// Verify it's gone
	item, err := cl.GetCollectionItem(ctx, cl.PeerID(), "products", "sku-001")
	if err != nil {
		t.Fatalf("GetCollectionItem: %v", err)
	}
	if item != nil {
		t.Fatal("expected nil after delete")
	}

	// Verify recordCount decremented
	info, err := cl.GetCollection(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info.RecordCount != 0 {
		t.Fatalf("expected recordCount 0, got %d", info.RecordCount)
	}
}

func TestDeleteCollection(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Add some items
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", []byte(`{"name":"Drill"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-002", []byte(`{"name":"Hammer"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Delete the whole collection
	deleted, err := cl.DeleteCollection(ctx, "products")
	if err != nil {
		t.Fatalf("DeleteCollection: %v", err)
	}
	if !deleted {
		t.Fatal("expected collection to be deleted")
	}

	// Verify it's gone
	info, err := cl.GetCollection(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestListCollectionKeys(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Add items
	for _, key := range []string{"sku-003", "sku-001", "sku-002"} {
		_, err = cl.PutCollectionItem(ctx, "products", key, []byte(`{"name":"Item"}`))
		if err != nil {
			t.Fatalf("PutCollectionItem(%s): %v", key, err)
		}
	}

	// List keys (should be alphabetical)
	keys, total, err := cl.ListCollectionKeys(ctx, cl.PeerID(), "products")
	if err != nil {
		t.Fatalf("ListCollectionKeys: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected total 3, got %d", total)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "sku-001" || keys[1] != "sku-002" || keys[2] != "sku-003" {
		t.Fatalf("expected sorted keys, got %v", keys)
	}
}

func TestQueryEquality(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Add items with different categories
	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", []byte(`{"name":"Drill","category":"tools"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-002", []byte(`{"name":"Paint","category":"supplies"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-003", []byte(`{"name":"Hammer","category":"tools"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Query for tools only
	result, err := cl.QueryCollection(ctx, cl.PeerID(), "products",
		map[string]any{"category": "tools"})
	if err != nil {
		t.Fatalf("QueryCollection: %v", err)
	}
	if result.TotalCount != 2 {
		t.Fatalf("expected 2 results, got %d", result.TotalCount)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result.Items))
	}
}

func TestQueryComparison(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", []byte(`{"name":"Drill","price":49.99}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-002", []byte(`{"name":"Paint","price":29.99}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-003", []byte(`{"name":"Hammer","price":19.99}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Query for items under $30
	result, err := cl.QueryCollection(ctx, cl.PeerID(), "products",
		map[string]any{"price": map[string]any{"$lt": 30}})
	if err != nil {
		t.Fatalf("QueryCollection: %v", err)
	}
	if result.TotalCount != 2 {
		t.Fatalf("expected 2 results, got %d", result.TotalCount)
	}
}

func TestQueryCompound(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	_, err = cl.PutCollectionItem(ctx, "products", "sku-001", []byte(`{"name":"Drill","price":49.99,"category":"tools"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-002", []byte(`{"name":"Hammer","price":19.99,"category":"tools"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}
	_, err = cl.PutCollectionItem(ctx, "products", "sku-003", []byte(`{"name":"Paint","price":29.99,"category":"supplies"}`))
	if err != nil {
		t.Fatalf("PutCollectionItem: %v", err)
	}

	// Query: tools AND price < 30
	result, err := cl.QueryCollection(ctx, cl.PeerID(), "products",
		map[string]any{
			"$and": []any{
				map[string]any{"category": "tools"},
				map[string]any{"price": map[string]any{"$lt": 30}},
			},
		})
	if err != nil {
		t.Fatalf("QueryCollection: %v", err)
	}
	if result.TotalCount != 1 {
		t.Fatalf("expected 1 result, got %d", result.TotalCount)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result.Items))
	}
	if result.Items[0].Key != "sku-002" {
		t.Fatalf("expected sku-002, got %s", result.Items[0].Key)
	}
}

func TestListCollections(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create multiple collections
	err := cl.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	err = cl.CreateCollection(ctx, "contacts", "Contact List")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	collections, err := cl.ListCollections(ctx, cl.PeerID())
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(collections) < 2 {
		t.Fatalf("expected at least 2 collections, got %d", len(collections))
	}

	// Verify ordering (alphabetical by path)
	found := map[string]bool{}
	for _, c := range collections {
		found[c.Path] = true
	}
	if !found["contacts"] || !found["products"] {
		t.Fatalf("expected both 'contacts' and 'products' in list, got %v", collections)
	}
}

func TestOwnerOnlyWrite(t *testing.T) {
	server := newTestServer(t)
	owner := newTestClient(t, server)
	other := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Owner creates collection
	err := owner.CreateCollection(ctx, "products", "Products")
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Other peer tries to PUT (should fail with 403)
	_, err = other.PutCollectionItem(ctx, "products", "sku-001", []byte(`{"name":"Drill"}`))
	if err == nil {
		t.Fatal("expected error for non-owner write, got nil")
	}
}
