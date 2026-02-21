package presence

import (
	"testing"
	"time"
)

func TestCache_SetAndGet(t *testing.T) {
	cache := NewCache(5 * time.Second)
	pid := generateTestPeerID(t)

	status := &PresenceStatus{
		PeerID:    pid,
		State:     Online,
		LastSeen:  time.Now(),
		CheckedAt: time.Now(),
	}

	cache.Set(pid, status)
	got := cache.Get(pid)

	if got == nil {
		t.Fatal("expected non-nil status")
	}
	if got.State != Online {
		t.Errorf("expected Online, got %v", got.State)
	}
	if got.PeerID != pid {
		t.Errorf("peer ID mismatch")
	}
}

func TestCache_GetExpired(t *testing.T) {
	cache := NewCache(1 * time.Millisecond)
	pid := generateTestPeerID(t)

	cache.Set(pid, &PresenceStatus{
		PeerID:    pid,
		State:     Online,
		LastSeen:  time.Now(),
		CheckedAt: time.Now(),
	})

	time.Sleep(5 * time.Millisecond)

	got := cache.Get(pid)
	if got != nil {
		t.Errorf("expected nil for expired entry, got %v", got.State)
	}
}

func TestCache_GetMissing(t *testing.T) {
	cache := NewCache(5 * time.Second)
	pid := generateTestPeerID(t)

	got := cache.Get(pid)
	if got != nil {
		t.Errorf("expected nil for missing entry")
	}
}

func TestCache_Cleanup(t *testing.T) {
	cache := NewCache(1 * time.Millisecond)
	pid1 := generateTestPeerID(t)
	pid2 := generateTestPeerID(t)

	now := time.Now()
	cache.Set(pid1, &PresenceStatus{PeerID: pid1, State: Online, CheckedAt: now})
	cache.Set(pid2, &PresenceStatus{PeerID: pid2, State: Offline, CheckedAt: now})

	time.Sleep(5 * time.Millisecond)
	cache.Cleanup()

	stats := cache.GetStats()
	total := stats["total_entries"].(int)
	if total != 0 {
		t.Errorf("expected 0 entries after cleanup, got %d", total)
	}
}

func TestCache_GetStats(t *testing.T) {
	cache := NewCache(5 * time.Second)
	pid1 := generateTestPeerID(t)
	pid2 := generateTestPeerID(t)

	now := time.Now()
	cache.Set(pid1, &PresenceStatus{PeerID: pid1, State: Online, CheckedAt: now})
	cache.Set(pid2, &PresenceStatus{PeerID: pid2, State: Offline, CheckedAt: now})

	stats := cache.GetStats()
	if stats["online"].(int) != 1 {
		t.Errorf("expected 1 online, got %d", stats["online"].(int))
	}
	if stats["offline"].(int) != 1 {
		t.Errorf("expected 1 offline, got %d", stats["offline"].(int))
	}
}
