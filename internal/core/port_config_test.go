package core

import (
	"os"
	"path/filepath"
	"testing"
)

// The port reaches the listen addresses. The host listens on those and
// derives an address from Port only when the list is empty, which the
// defaults never leave it; so --port and server.port set a field nothing
// read, and the server kept listening on 55223.
func TestSetPortRewritesTheListenAddresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddresses = []string{"/ip4/0.0.0.0/udp/55223/udx", "/ip6/::/udp/55223/udx"}

	cfg.SetPort(56000)

	if cfg.Port != 56000 {
		t.Errorf("Port = %d, want 56000", cfg.Port)
	}
	want := []string{"/ip4/0.0.0.0/udp/56000/udx", "/ip6/::/udp/56000/udx"}
	for i, addr := range cfg.ListenAddresses {
		if addr != want[i] {
			t.Errorf("ListenAddresses[%d] = %q, want %q", i, addr, want[i])
		}
	}
}

// server.port in the file takes effect, and explicit listen_addresses in the
// same file still win over it, in file order of precedence: the addresses
// are the more specific setting.
func TestFilePortReachesTheListenAddresses(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	cfg := DefaultConfig()
	if err := LoadConfigFromFile(write(t, "server:\n  port: 56001\n"), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ListenAddresses; len(got) != 1 || got[0] != "/ip4/0.0.0.0/udp/56001/udx" {
		t.Errorf("listen addresses after server.port = %v", got)
	}

	cfg = DefaultConfig()
	if err := LoadConfigFromFile(write(t,
		"server:\n  port: 56001\n  listen_addresses:\n    - /ip4/127.0.0.1/udp/56002/udx\n"), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ListenAddresses; len(got) != 1 || got[0] != "/ip4/127.0.0.1/udp/56002/udx" {
		t.Errorf("explicit listen_addresses lost to server.port: %v", got)
	}
}
