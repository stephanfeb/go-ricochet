package integration_test

import (
	"context"
	"testing"
	"time"
)

// TestGetFeedEntriesWithNoRange is N21: with no from, to or limit the request
// was the server's feed-metadata request, and the client parsed the metadata
// as an empty page with no error.
func TestGetFeedEntriesWithNoRange(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cl.CreateFeed(ctx, "news", "News", ""); err != nil {
		t.Fatalf("create feed: %v", err)
	}
	for _, body := range []string{"one", "two", "three"} {
		if _, err := cl.AppendFeedEntry(ctx, "news", []byte(body), "post"); err != nil {
			t.Fatalf("append %s: %v", body, err)
		}
	}

	entries, _, err := cl.GetFeedEntries(ctx, cl.PeerID(), "news")
	if err != nil {
		t.Fatalf("get entries: %v", err)
	}
	if len(entries) != 3 || string(entries[0].Content) != "one" || string(entries[2].Content) != "three" {
		t.Fatalf("got %d entries, want one, two, three", len(entries))
	}
}
