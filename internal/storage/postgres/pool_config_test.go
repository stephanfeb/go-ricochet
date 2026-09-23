package postgres

import (
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// The DSN used to be "host=%s ... password=%s sslmode=%s" formatted from the
// config. With an empty password pgx read "sslmode=disable" as the password
// and fell back to its default sslmode, so a local database with no TLS
// refused every connection. Reproduced in the release audit; this pins the
// fix without needing a database.
func TestPoolConfigWithEmptyPasswordKeepsSSLMode(t *testing.T) {
	cfg := &core.PostgresConfig{
		Host: "localhost", Port: 5432, Database: "ricochet", Username: "ricochet",
		Password: "", SSLMode: "disable", PoolSize: 7, ConnectTimeout: 3 * time.Second,
	}
	pc, err := poolConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConnConfig.Password != "" {
		t.Errorf("password = %q, want empty", pc.ConnConfig.Password)
	}
	if pc.ConnConfig.TLSConfig != nil {
		t.Errorf("sslmode=disable did not stick: TLS config is set")
	}
	if pc.MaxConns != 7 {
		t.Errorf("MaxConns = %d, want 7", pc.MaxConns)
	}
	if pc.ConnConfig.ConnectTimeout != 3*time.Second {
		t.Errorf("ConnectTimeout = %v, want 3s", pc.ConnConfig.ConnectTimeout)
	}
	if pc.ConnConfig.Host != "localhost" || pc.ConnConfig.Port != 5432 || pc.ConnConfig.Database != "ricochet" || pc.ConnConfig.User != "ricochet" {
		t.Errorf("fields did not survive: %+v", pc.ConnConfig.Config)
	}
}

func TestPoolConfigPasswordWithReservedCharacters(t *testing.T) {
	cfg := &core.PostgresConfig{
		Host: "localhost", Port: 5432, Database: "ricochet", Username: "ricochet",
		Password: "p@ss:w/rd?&#=%", SSLMode: "disable", PoolSize: 1,
	}
	pc, err := poolConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConnConfig.Password != "p@ss:w/rd?&#=%" {
		t.Errorf("password mangled: %q", pc.ConnConfig.Password)
	}
	if pc.ConnConfig.Database != "ricochet" {
		t.Errorf("database mangled: %q", pc.ConnConfig.Database)
	}
}

func TestPoolConfigRejectsBadSSLMode(t *testing.T) {
	cfg := &core.PostgresConfig{Host: "localhost", Port: 5432, Database: "d", Username: "u", SSLMode: "sometimes"}
	if _, err := poolConfig(cfg); err == nil {
		t.Fatal("invalid sslmode accepted")
	}
}
