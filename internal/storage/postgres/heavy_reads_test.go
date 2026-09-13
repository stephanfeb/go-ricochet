package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage"
)

// HEAD reports the size without loading the body.
func TestHeadDocumentReportsSizeWithoutBody(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	ctx := context.Background()

	body := make([]byte, 50_000)
	if _, err := store.PutDocument(ctx, owner, "big/doc", body, "application/octet-stream", owner, nil, nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	head, err := store.HeadDocument(ctx, owner, "big/doc")
	if err != nil || head == nil {
		t.Fatalf("head: %v", err)
	}
	if head.Content != nil {
		t.Errorf("HeadDocument loaded %d bytes of body", len(head.Content))
	}
	if head.ContentLength != len(body) {
		t.Errorf("ContentLength = %d, want %d", head.ContentLength, len(body))
	}
	full, err := store.GetDocument(ctx, owner, "big/doc")
	if err != nil || full == nil {
		t.Fatalf("get: %v", err)
	}
	if full.ContentHash != head.ContentHash || full.VersionNumber != head.VersionNumber || full.ContentLength != head.ContentLength {
		t.Errorf("head and get disagree: %+v vs %+v", head, full)
	}
	if missing, err := store.HeadDocument(ctx, owner, "big/absent"); err != nil || missing != nil {
		t.Errorf("absent document: rec=%v err=%v, want nil, nil", missing, err)
	}
}

// The capacity and operator figures come from the counters on the mailbox
// row, and those counters agree with the rows.
func TestUsageFiguresComeFromCounters(t *testing.T) {
	store := newTestStorage(t)
	alice, bob := newTestPeer(t), newTestPeer(t)
	ctx := context.Background()

	inbox := newCappedMailbox(t, store, alice, "inbox", 10)
	work := newCappedMailbox(t, store, alice, "work", 10)
	bobs := newCappedMailbox(t, store, bob, "inbox", 10)

	put := func(mb *storage.MailboxRecord, owner peer.ID, n, size int) {
		t.Helper()
		for i := 0; i < n; i++ {
			msg := testMessage(owner, mb.ID, i)
			msg.Payload = make([]byte, size)
			if _, err := store.StoreMessage(ctx, mb, msg); err != nil {
				t.Fatalf("store into %s: %v", mb.FullPath(), err)
			}
		}
	}
	put(inbox, alice, 3, 100)
	put(work, alice, 2, 1000)
	put(bobs, bob, 1, 10)

	// Before comparing, the raw rows say what the counters must say.
	var rows, bytes int64
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(octet_length(payload)), 0) FROM stored_messages
		WHERE mailbox_id = ANY($1)`, []int64{inbox.ID, work.ID, bobs.ID}).Scan(&rows, &bytes); err != nil {
		t.Fatalf("raw sums: %v", err)
	}
	if rows != 6 || bytes != 3*100+2*1000+10 {
		t.Fatalf("raw rows=%d bytes=%d, test setup is off", rows, bytes)
	}

	usage, err := store.ListMailboxUsage(ctx, storage.MailboxUsageQuery{Owner: alice.String(), Limit: 10})
	if err != nil {
		t.Fatalf("list mailbox usage: %v", err)
	}
	got := map[string]*storage.MailboxUsage{}
	for _, u := range usage {
		got[u.FolderPath] = u
	}
	if u := got["inbox"]; u == nil || u.MessageCount != 3 || u.MessageBytes != 300 || u.LastMessageAt == nil {
		t.Errorf("inbox usage = %+v, want 3 messages, 300 bytes, a last message time", u)
	}
	if u := got["work"]; u == nil || u.MessageCount != 2 || u.MessageBytes != 2000 {
		t.Errorf("work usage = %+v, want 2 messages, 2000 bytes", u)
	}
	if got["inbox"] != nil && got["inbox"].FillRatio != 0.3 {
		t.Errorf("inbox fill = %v, want 0.3", got["inbox"].FillRatio)
	}

	owners, err := store.ListOwnerUsage(ctx, 100, 0)
	if err != nil {
		t.Fatalf("list owner usage: %v", err)
	}
	var aliceUsage, bobUsage *storage.OwnerUsage
	for _, o := range owners {
		switch o.OwnerPeerID {
		case alice.String():
			aliceUsage = o
		case bob.String():
			bobUsage = o
		}
	}
	if aliceUsage == nil || aliceUsage.Mailboxes != 2 || aliceUsage.Messages != 5 || aliceUsage.MessageBytes != 2300 {
		t.Errorf("alice usage = %+v, want 2 mailboxes, 5 messages, 2300 bytes", aliceUsage)
	}
	if bobUsage == nil || bobUsage.Mailboxes != 1 || bobUsage.Messages != 1 || bobUsage.MessageBytes != 10 {
		t.Errorf("bob usage = %+v, want 1 mailbox, 1 message, 10 bytes", bobUsage)
	}

	// Server-wide figures include other tests' rows on a shared database, so
	// compare deltas around a store into a fresh mailbox.
	before, err := store.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("server stats: %v", err)
	}
	extra := newCappedMailbox(t, store, bob, "extra", 10)
	put(extra, bob, 4, 250)
	after, err := store.ServerStats(ctx, 0.9)
	if err != nil {
		t.Fatalf("server stats: %v", err)
	}
	if after.Messages-before.Messages != 4 || after.MessageBytes-before.MessageBytes != 1000 || after.Mailboxes-before.Mailboxes != 1 {
		t.Errorf("stats delta: messages %d bytes %d mailboxes %d; want 4, 1000, 1",
			after.Messages-before.Messages, after.MessageBytes-before.MessageBytes, after.Mailboxes-before.Mailboxes)
	}
	if after.SampledAt.Before(before.SampledAt) || time.Since(after.SampledAt) > time.Minute {
		t.Errorf("SampledAt = %v, not now", after.SampledAt)
	}
}

func newTestCollection(t *testing.T, store *PostgresStorage, owner peer.ID, items map[string]string) int64 {
	t.Helper()
	ctx := context.Background()
	coll, err := store.CreateCollection(ctx, owner, "paged", "Paged", core.VisibilityPrivate)
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	for key, content := range items {
		if _, _, err := store.PutCollectionItem(ctx, coll.ID, key, []byte(content), owner, nil); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	return coll.ID
}

// Paging by cursor visits every matching item exactly once, in order, in
// either direction, and counts the total on the first page only.
func TestQueryCollectionPagesByCursor(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	ctx := context.Background()

	items := map[string]string{}
	for i := 0; i < 7; i++ {
		// Keys are deliberately out of step with the sort field so the two
		// orders differ, and two items share a rank so the key tiebreak is
		// exercised.
		key := fmt.Sprintf("k%d", (i*3)%7)
		items[key] = fmt.Sprintf(`{"rank":"%02d","kind":"x"}`, i/2)
	}
	items["odd"] = `{"kind":"y"}` // no rank at all: sorts as the empty string
	collID := newTestCollection(t, store, owner, items)

	page := func(sortField string, asc bool, cursor string) *storage.CollectionQueryResult {
		t.Helper()
		res, err := store.QueryCollection(ctx, collID, map[string]any{"kind": "x"}, sortField, asc, 3, 0, cursor)
		if err != nil {
			t.Fatalf("query (cursor %q): %v", cursor, err)
		}
		return res
	}
	walk := func(sortField string, asc bool) []string {
		t.Helper()
		var keys []string
		cursor := ""
		for pages := 0; ; pages++ {
			res := page(sortField, asc, cursor)
			if pages == 0 && res.TotalCount != 7 {
				t.Errorf("first page total = %d, want 7", res.TotalCount)
			}
			if pages > 0 && res.TotalCount != -1 {
				t.Errorf("page %d total = %d, want -1 (not counted)", pages, res.TotalCount)
			}
			for _, it := range res.Items {
				keys = append(keys, it.Key)
			}
			if !res.HasMore {
				if res.NextCursor != "" {
					t.Errorf("last page carries a cursor")
				}
				break
			}
			if res.NextCursor == "" {
				t.Fatal("HasMore without a cursor")
			}
			cursor = res.NextCursor
			if pages > 10 {
				t.Fatal("paging does not terminate")
			}
		}
		return keys
	}

	asc := walk("rank", true)
	desc := walk("rank", false)
	byKey := walk("", true)
	for name, keys := range map[string][]string{"asc": asc, "desc": desc, "byKey": byKey} {
		seen := map[string]bool{}
		for _, k := range keys {
			if seen[k] {
				t.Errorf("%s: key %s returned twice", name, k)
			}
			seen[k] = true
		}
		if len(keys) != 7 {
			t.Errorf("%s: visited %d keys, want 7: %v", name, len(keys), keys)
		}
	}
	for i := range asc {
		if asc[i] != desc[len(desc)-1-i] {
			t.Errorf("desc is not the reverse of asc: asc=%v desc=%v", asc, desc)
			break
		}
	}
	for i := 1; i < len(byKey); i++ {
		if byKey[i-1] >= byKey[i] {
			t.Errorf("key order broken at %d: %v", i, byKey)
		}
	}

	// An offset page still counts, and a garbage cursor is a 400, not a
	// silent first page.
	if res, err := store.QueryCollection(ctx, collID, map[string]any{"kind": "x"}, "", true, 3, 3, ""); err != nil || res.TotalCount != 7 || len(res.Items) != 3 {
		t.Errorf("offset page: %+v err=%v", res, err)
	}
	if _, err := store.QueryCollection(ctx, collID, nil, "", true, 3, 0, "not-a-cursor"); !errors.Is(err, storage.ErrInvalidCursor) {
		t.Errorf("bad cursor error = %v, want ErrInvalidCursor", err)
	}
}

func TestListCollectionKeysPagesByCursor(t *testing.T) {
	store := newTestStorage(t)
	owner := newTestPeer(t)
	ctx := context.Background()

	items := map[string]string{}
	for i := 0; i < 5; i++ {
		items[fmt.Sprintf("key-%d", i)] = `{}`
	}
	collID := newTestCollection(t, store, owner, items)

	var all []string
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		keys, total, next, err := store.ListCollectionKeys(ctx, collID, 2, 0, cursor)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if pages == 0 && total != 5 {
			t.Errorf("first page total = %d, want 5", total)
		}
		if pages > 0 && total != -1 {
			t.Errorf("cursor page total = %d, want -1", total)
		}
		all = append(all, keys...)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(all) != 5 {
		t.Errorf("visited %v, want all five keys once", all)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Errorf("keys out of order: %v", all)
		}
	}
}
