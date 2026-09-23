package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

// A client pages a query with the cursor each page hands back, and sees
// every item once. The total arrives with the first page only.
func TestCollectionQueryPagesByCursor(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cl.CreateCollection(ctx, "paged", "Paged"); err != nil {
		t.Fatalf("create: %v", err)
	}
	const n = 7
	for i := 0; i < n; i++ {
		if _, err := cl.PutCollectionItem(ctx, "paged", fmt.Sprintf("item-%d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	seen := map[string]int{}
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		opts := []client.CollectionQueryOption{client.WithQueryLimit(3)}
		if cursor != "" {
			opts = append(opts, client.WithQueryCursor(cursor))
		}
		res, err := cl.QueryCollection(ctx, cl.PeerID(), "paged", nil, opts...)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		if pages == 0 && res.TotalCount != n {
			t.Errorf("first page total = %d, want %d", res.TotalCount, n)
		}
		if pages > 0 && res.TotalCount != -1 {
			t.Errorf("page %d total = %d, want -1", pages, res.TotalCount)
		}
		for _, it := range res.Items {
			seen[it.Key]++
		}
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if len(seen) != n {
		t.Errorf("saw %d distinct keys, want %d: %v", len(seen), n, seen)
	}
	for k, c := range seen {
		if c != 1 {
			t.Errorf("%s returned %d times", k, c)
		}
	}

	keys, err := cl.ListCollectionKeysPage(ctx, cl.PeerID(), "paged", client.WithQueryLimit(4))
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if len(keys.Keys) != 4 || !keys.HasMore || keys.NextCursor == "" || keys.TotalCount != n {
		t.Errorf("first keys page = %+v, want 4 keys, more, a cursor, total %d", keys, n)
	}
	rest, err := cl.ListCollectionKeysPage(ctx, cl.PeerID(), "paged", client.WithQueryLimit(4), client.WithQueryCursor(keys.NextCursor))
	if err != nil {
		t.Fatalf("second keys page: %v", err)
	}
	if len(rest.Keys) != 3 || rest.HasMore || rest.TotalCount != -1 {
		t.Errorf("second keys page = %+v, want the last 3 keys and no more", rest)
	}
}
