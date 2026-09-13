package main

import (
	"fmt"
	"log/slog"
	"os"
	"syscall"
)

// waitForShutdown blocks until a termination signal arrives, then runs stop.
//
// It keeps listening while stop runs. A second signal ends the process at
// once, with the exit code the shell would report for that signal: the
// operator has asked twice, and a graceful stop that has hung on a stuck
// connection or an unresponsive database must not be able to hold the
// process hostage. A stop that fails exits with 1. exit is os.Exit in
// production and a recorder in tests.
func waitForShutdown(sigCh <-chan os.Signal, logger *slog.Logger, stop func() error, exit func(int)) {
	sig := <-sigCh
	fmt.Println("\nShutting down...")
	logger.Info("shutting down", "signal", sig)

	done := make(chan error, 1)
	go func() { done <- stop() }()

	select {
	case err := <-done:
		if err != nil {
			logger.Error("error during shutdown", "error", err)
			exit(1)
		}
	case sig := <-sigCh:
		logger.Error("forced shutdown: second signal arrived before the graceful stop finished", "signal", sig)
		exit(forcedExitCode(sig))
	}
}

// forcedExitCode is 128 plus the signal number, the code a process killed by
// that signal reports, so 130 for SIGINT and 143 for SIGTERM.
func forcedExitCode(sig os.Signal) int {
	if s, ok := sig.(syscall.Signal); ok {
		return 128 + int(s)
	}
	return 130
}
