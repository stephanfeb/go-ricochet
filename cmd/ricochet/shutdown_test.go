package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"syscall"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// exitRecorder stands in for os.Exit. Exiting once is the end of the process,
// so a second call is a bug in its own right.
type exitRecorder struct {
	codes chan int
}

func newExitRecorder() *exitRecorder { return &exitRecorder{codes: make(chan int, 2)} }

func (r *exitRecorder) exit(code int) { r.codes <- code }

func (r *exitRecorder) code(t *testing.T) int {
	t.Helper()
	select {
	case c := <-r.codes:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("exit was not called")
		return 0
	}
}

func (r *exitRecorder) notCalled(t *testing.T) {
	t.Helper()
	select {
	case c := <-r.codes:
		t.Fatalf("exit(%d) was called for a clean stop", c)
	case <-time.After(50 * time.Millisecond):
	}
}

// A second signal while Stop is hung ends the process with the signal's
// exit code. The old loop read one signal and then blocked on Stop, so a
// second Ctrl-C was swallowed and the operator had to find the PID.
func TestSecondSignalForcesExitWhileStopHangs(t *testing.T) {
	for _, tc := range []struct {
		sig  syscall.Signal
		code int
	}{
		{syscall.SIGINT, 130},
		{syscall.SIGTERM, 143},
	} {
		t.Run(tc.sig.String(), func(t *testing.T) {
			sigCh := make(chan os.Signal, 2)
			release := make(chan struct{})
			stopping := make(chan struct{})
			stop := func() error {
				close(stopping)
				<-release
				return nil
			}
			exit := newExitRecorder()

			done := make(chan struct{})
			go func() {
				waitForShutdown(sigCh, quietLogger(), stop, exit.exit)
				close(done)
			}()

			sigCh <- tc.sig
			select {
			case <-stopping:
			case <-time.After(5 * time.Second):
				t.Fatal("the first signal did not start the graceful stop")
			}
			sigCh <- tc.sig

			if got := exit.code(t); got != tc.code {
				t.Fatalf("forced exit code = %d, want %d", got, tc.code)
			}
			close(release)
			<-done
		})
	}
}

// A stop that finishes returns without calling exit, so main's own return
// ends the process with 0; a stop that fails exits with 1.
func TestGracefulStopReportsItsOutcome(t *testing.T) {
	t.Run("clean", func(t *testing.T) {
		sigCh := make(chan os.Signal, 2)
		exit := newExitRecorder()
		sigCh <- syscall.SIGTERM
		waitForShutdown(sigCh, quietLogger(), func() error { return nil }, exit.exit)
		exit.notCalled(t)
	})
	t.Run("failed", func(t *testing.T) {
		sigCh := make(chan os.Signal, 2)
		exit := newExitRecorder()
		sigCh <- syscall.SIGTERM
		waitForShutdown(sigCh, quietLogger(), func() error { return errors.New("pool close hung") }, exit.exit)
		if got := exit.code(t); got != 1 {
			t.Fatalf("exit code = %d, want 1", got)
		}
	})
}
