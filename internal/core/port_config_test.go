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

// The port reaches TCP addresses too, including a plain one, which ends on
// its port with no trailing component.
func TestSetPortRewritesTCPAddresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddresses = []string{
		"/ip4/0.0.0.0/udp/55223/udx",
		"/ip4/0.0.0.0/tcp/55223",
		"/ip4/0.0.0.0/tcp/55223/ws",
	}

	cfg.SetPort(56000)

	want := []string{
		"/ip4/0.0.0.0/udp/56000/udx",
		"/ip4/0.0.0.0/tcp/56000",
		"/ip4/0.0.0.0/tcp/56000/ws",
	}
	for i, addr := range cfg.ListenAddresses {
		if addr != want[i] {
			t.Errorf("ListenAddresses[%d] = %q, want %q", i, addr, want[i])
		}
	}
}

// server.transports in the file reaches the config, and a listen address
// whose transport is not enabled fails validation rather than failing the
// host at start.
func TestFileTransportsAreValidated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "server:\n  transports: [udx, tcp]\n  listen_addresses:\n" +
		"    - /ip4/0.0.0.0/udp/55223/udx\n    - /ip4/0.0.0.0/tcp/55223\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	if err := LoadConfigFromFile(path, cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Transports; len(got) != 2 || got[0] != "udx" || got[1] != "tcp" {
		t.Fatalf("transports = %v", got)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	cfg.Transports = []string{"udx"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("a TCP listen address passed validation with only udx enabled")
	}
}

// external_ip advertises every reachable listen address on the public IP,
// after the explicit external addresses and without repeating them, and a
// value that is not an IP fails validation.
func TestExternalIPAdvertisesEveryListener(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Transports = []string{"udx", "tcp"}
	cfg.ListenAddresses = []string{"/ip4/0.0.0.0/udp/55223/udx", "/ip4/0.0.0.0/tcp/55223", "/ip4/127.0.0.1/tcp/55299"}
	cfg.ExternalAddresses = []string{"/ip4/203.0.113.10/udp/55223/udx"}
	cfg.ExternalIP = "203.0.113.10"

	got, err := cfg.AdvertisedAddresses()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/ip4/203.0.113.10/udp/55223/udx", "/ip4/203.0.113.10/tcp/55223"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("advertised = %v, want %v", got, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	cfg.ExternalIP = "my-server"
	if err := cfg.Validate(); err == nil {
		t.Fatal("an external_ip that is not an IP passed validation")
	}
}
