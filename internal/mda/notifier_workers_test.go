package mda

import "testing"

// worker_threads bounds in-flight notifications; a burst past the bound is
// dropped rather than queued or run unbounded.
func TestNotifier_WorkersBoundInFlightNotifications(t *testing.T) {
	n := (&Notifier{}).WithWorkers(2)

	r1, ok1 := n.acquireWorker()
	r2, ok2 := n.acquireWorker()
	if !ok1 || !ok2 {
		t.Fatal("workers within the bound were refused")
	}
	if _, ok := n.acquireWorker(); ok {
		t.Fatal("a third worker was admitted past a bound of two")
	}
	r1()
	if _, ok := n.acquireWorker(); !ok {
		t.Fatal("a released worker was not reusable")
	}
	r2()
}

// No bound configured means every notification runs, as before.
func TestNotifier_NoBoundAdmitsAll(t *testing.T) {
	n := &Notifier{}
	for i := 0; i < 100; i++ {
		if _, ok := n.acquireWorker(); !ok {
			t.Fatalf("notification %d refused with no bound configured", i)
		}
	}
}
