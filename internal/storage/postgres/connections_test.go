package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

func TestPeerConnectionsAreLoggedClosedListedAndPurged(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	who, other := newPeer(t).String(), newPeer(t).String()
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM peer_connections WHERE peer_id = ANY($1)`, []string{who, other})
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	add := func(peerID string, at time.Time, transport string) int64 {
		t.Helper()
		id, err := s.InsertPeerConnection(ctx, &storage.PeerConnection{
			PeerID: peerID, IP: "203.0.113.7", RemoteAddr: "/ip4/203.0.113.7/udp/55223/udx",
			Transport: transport, Direction: "inbound", ConnectedAt: at,
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	old := add(who, now.Add(-100*24*time.Hour), "tcp")
	first := add(who, now.Add(-2*time.Hour), "udx")
	add(who, now.Add(-time.Hour), "p2p-circuit")
	add(other, now, "udx")

	if err := s.MarkPeerDisconnected(ctx, first, now.Add(-90*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// A second close does not move the first.
	if err := s.MarkPeerDisconnected(ctx, first, now); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListPeerConnections(ctx, who, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Transport != "p2p-circuit" || got[1].ID != first || got[2].ID != old {
		t.Fatalf("listed = %+v, want three, newest first", got)
	}
	if got[0].DisconnectedAt != nil {
		t.Errorf("an open connection has a disconnect time: %v", got[0].DisconnectedAt)
	}
	if d := got[1].DisconnectedAt; d == nil || !d.Equal(now.Add(-90*time.Minute)) {
		t.Errorf("disconnected_at = %v, want the first close", d)
	}
	if g := got[1]; g.PeerID != who || g.IP != "203.0.113.7" || g.RemoteAddr == "" || g.Direction != "inbound" || !g.ConnectedAt.Equal(now.Add(-2*time.Hour)) {
		t.Errorf("row = %+v", g)
	}

	since := now.Add(-3 * time.Hour)
	if got, _ := s.ListPeerConnections(ctx, who, &since, 10); len(got) != 2 {
		t.Errorf("since = %d rows, want 2", len(got))
	}
	if got, _ := s.ListPeerConnections(ctx, who, nil, 1); len(got) != 1 || got[0].Transport != "p2p-circuit" {
		t.Errorf("limit 1 = %+v, want the newest", got)
	}

	n, err := s.PurgePeerConnections(ctx, now.Add(-90*24*time.Hour))
	if err != nil || n < 1 {
		t.Fatalf("purge = %d, %v; want at least the old row", n, err)
	}
	if got, _ := s.ListPeerConnections(ctx, who, nil, 10); len(got) != 2 {
		t.Errorf("after the purge %d rows, want 2", len(got))
	}
	// A row that is gone can still be marked: nothing happens.
	if err := s.MarkPeerDisconnected(ctx, old, now); err != nil {
		t.Errorf("marking a purged row: %v", err)
	}
}
