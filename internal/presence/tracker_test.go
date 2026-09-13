package presence

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// A paged heartbeat is applied only once every page has arrived, so the
// contacts on a page still in flight are not marked offline.
func TestTrackerReassemblesPagedHeartbeat(t *testing.T) {
	ids := randomPeerIDs(t, 3)
	server, x, y := ids[0], ids[1], ids[2]

	tracker := NewTracker(nil, NewCache(time.Hour), slog.New(slog.NewTextHandler(io.Discard, nil)))
	tracker.AddContact(x)
	tracker.AddContact(y)

	var changes []string
	tracker.SetOnChange(func(pid peer.ID, state PresenceState) {
		changes = append(changes, pid.String()+"="+state.String())
	})

	page := func(seq uint64, page, count int, online ...peer.ID) *PresenceHeartbeat {
		return &PresenceHeartbeat{
			ServerID:          server,
			HeartbeatSequence: seq,
			Page:              page,
			PageCount:         count,
			OnlineCount:       len(online),
			OnlinePeerIDs:     online,
		}
	}
	states := func() (PresenceState, PresenceState) {
		return tracker.GetPresence(x), tracker.GetPresence(y)
	}

	// First page of two: nothing is decided yet.
	tracker.processHeartbeat(page(1, 0, 2, x))
	if sx, sy := states(); sx != Unknown || sy != Unknown {
		t.Fatalf("after one page of two: x=%v y=%v, want both Unknown", sx, sy)
	}
	if len(changes) != 0 {
		t.Fatalf("a partial heartbeat reported changes: %v", changes)
	}

	// Second page completes the sequence: both online.
	tracker.processHeartbeat(page(1, 1, 2, y))
	if sx, sy := states(); sx != Online || sy != Online {
		t.Fatalf("after both pages: x=%v y=%v, want both Online", sx, sy)
	}

	// A heartbeat from before paging (no page count) is complete on its own.
	tracker.processHeartbeat(page(2, 0, 0, x))
	if sx, sy := states(); sx != Online || sy != Offline {
		t.Fatalf("after an unpaged heartbeat listing x: x=%v y=%v, want Online/Offline", sx, sy)
	}

	// A newer sequence discards a pending older one, and a straggler from
	// the older sequence cannot complete it later.
	tracker.processHeartbeat(page(3, 0, 2))
	tracker.processHeartbeat(page(4, 0, 2, x))
	tracker.processHeartbeat(page(4, 1, 2, y))
	if sx, sy := states(); sx != Online || sy != Online {
		t.Fatalf("after sequence 4: x=%v y=%v, want both Online", sx, sy)
	}
	tracker.processHeartbeat(page(3, 1, 2))
	if sx, sy := states(); sx != Online || sy != Online {
		t.Fatalf("a straggler page changed state: x=%v y=%v", sx, sy)
	}

	// Sequence numbers restart with the server, so a lower sequence is not
	// stale on its face; the straggler is simply replaced by the next one.
	tracker.processHeartbeat(page(1, 0, 2, y))
	tracker.processHeartbeat(page(1, 1, 2))
	if sx, sy := states(); sx != Offline || sy != Online {
		t.Fatalf("after a restarted sequence listing y: x=%v y=%v, want Offline/Online", sx, sy)
	}
	if len(tracker.partial) != 0 {
		t.Fatalf("%d partial sequences left pending, want 0", len(tracker.partial))
	}
}

// A presence message counts only when its verified signer is the server the
// topic belongs to and the payload names that same server.
func TestTrackerIgnoresPresenceFromAnotherPublisher(t *testing.T) {
	ids := randomPeerIDs(t, 3)
	server, impostor, contact := ids[0], ids[1], ids[2]

	tracker := NewTracker(nil, NewCache(time.Hour), slog.New(slog.NewTextHandler(io.Discard, nil)))
	tracker.AddContact(contact)

	online := func(claimedServer peer.ID) []byte {
		ev := &PresenceEvent{ServerID: claimedServer, Timestamp: time.Now(),
			Changes: []PresenceChange{{PeerID: contact, State: Online, TTLSeconds: 60}}}
		data, err := ev.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	if err := tracker.handleMessage(impostor, server, online(server)); err == nil {
		t.Fatal("event signed by another peer on the server's topic was accepted")
	}
	if err := tracker.handleMessage(server, server, online(impostor)); err == nil {
		t.Fatal("event naming another server was accepted")
	}
	if got := tracker.GetPresence(contact); got != Unknown {
		t.Fatalf("contact = %v after rejected events, want Unknown", got)
	}

	if err := tracker.handleMessage(server, server, online(server)); err != nil {
		t.Fatalf("genuine event rejected: %v", err)
	}
	if got := tracker.GetPresence(contact); got != Online {
		t.Fatalf("contact = %v after the server's own event, want Online", got)
	}

	hb := &PresenceHeartbeat{ServerID: impostor, OnlinePeerIDs: nil}
	data, _ := hb.Encode()
	if err := tracker.handleMessage(server, server, data); err == nil {
		t.Fatal("heartbeat naming another server was accepted")
	}
	if got := tracker.GetPresence(contact); got != Online {
		t.Fatalf("contact = %v after a rejected heartbeat, want Online", got)
	}
}
