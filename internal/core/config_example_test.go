package core

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const examplePath = "../../config.example.yaml"

func readExample(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatalf("read %s: %v", examplePath, err)
	}
	return string(data)
}

// yamlKeys walks a struct and returns every yaml key it accepts, including
// nested and map-valued ones.
func yamlKeys(typ reflect.Type, out map[string]bool) {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Map || typ.Kind() == reflect.Slice {
		yamlKeys(typ.Elem(), out)
		return
	}
	if typ.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if tag != "" && tag != "-" {
			out[tag] = true
		}
		yamlKeys(field.Type, out)
	}
}

// Every setting the parser accepts has to appear in the shipped example.
//
// A setting nobody can find is a setting nobody uses, and this file is the
// only place the full surface is written down. The check accepts a commented
// key, since a section that is off by default belongs commented out — what it
// refuses is a key that is absent entirely, which is how relay_limits,
// enable_auto_relay and enable_hole_punching went undocumented.
func TestExampleDocumentsEveryConfigKey(t *testing.T) {
	keys := map[string]bool{}
	yamlKeys(reflect.TypeOf(yamlFileConfig{}), keys)

	example := readExample(t)

	var missing []string
	for key := range keys {
		if !strings.Contains(example, key+":") {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Errorf("config.example.yaml does not mention %v", missing)
	}
}

// activeKey returns the mapping key an uncommented YAML line sets, if any.
//
// Only live lines are considered. A commented key is documentation and harms
// nobody; a live one that the parser does not read looks configured and is
// silently ignored, which is what `logging: level: INFO` was for the life of
// this file.
func activeKey(line string) (string, bool) {
	if strings.HasPrefix(strings.TrimSpace(line), "#") {
		return "", false
	}
	name, _, found := strings.Cut(strings.TrimSpace(line), ":")
	if !found {
		return "", false
	}
	// A key is lowercase with underscores. Anything else is a value, a list
	// item, or prose that happens to contain a colon.
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return "", false
		}
	}
	return name, name != ""
}

// A live key the parser does not accept is worse than an undocumented one: it
// looks configured and does nothing.
func TestExampleHasNoKeysTheParserIgnores(t *testing.T) {
	keys := map[string]bool{}
	yamlKeys(reflect.TypeOf(yamlFileConfig{}), keys)

	var unknown []string
	for _, line := range strings.Split(readExample(t), "\n") {
		name, ok := activeKey(line)
		if !ok || keys[name] {
			continue
		}
		// Protocol names under rate_limiting.protocols are map keys rather
		// than struct fields, so they are legitimate.
		if _, isProtocol := DefaultRateLimits().Protocols[name]; isProtocol {
			continue
		}
		unknown = append(unknown, name)
	}

	if len(unknown) > 0 {
		t.Errorf("config.example.yaml sets keys the parser ignores: %v\n"+
			"a setting that looks configured and does nothing is worse than an absent one",
			unknown)
	}
}

// Loading the example must actually change things.
//
// The previous version of this check loaded the example onto a default config
// and compared against the defaults, so it passed whether or not the file
// contained anything at all. Starting from values deliberately unlike the
// defaults is what makes the assertion mean something.
func TestExampleValuesActuallyApply(t *testing.T) {
	cfg := DefaultConfig()

	// Nothing here matches either the defaults or the example.
	cfg.Port = 1
	cfg.MaxMessagesPerMailbox = 7
	cfg.RetentionPolicy = 99 * time.Hour
	cfg.MaxMailboxes = 3
	cfg.MaxStorageBytes = 5
	cfg.Storage.Postgres.PoolSize = 2
	cfg.Admission.MaxInFlightPerPeer = 5
	cfg.Ops.Port = 1
	cfg.Ops.QueryTimeout = time.Second
	cfg.NearCapacityRatio = 0.11
	cfg.HealthCheckInterval = time.Second

	if err := LoadConfigFromFile(examplePath, cfg); err != nil {
		t.Fatalf("load example: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("example does not validate: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"server.port", cfg.Port, 55223},
		{"storage.max_messages_per_mailbox", cfg.MaxMessagesPerMailbox, 1000},
		{"storage.retention_days", cfg.RetentionPolicy, 30 * 24 * time.Hour},
		{"storage.max_mailboxes", cfg.MaxMailboxes, 100000},
		{"storage.max_storage_gb", cfg.MaxStorageBytes, int64(50) * 1024 * 1024 * 1024},
		{"database.pool_size", cfg.Storage.Postgres.PoolSize, 25},
		{"admission_control.max_in_flight_per_peer", cfg.Admission.MaxInFlightPerPeer, 64},
		{"ops.port", cfg.Ops.Port, 9090},
		{"ops.query_timeout", cfg.Ops.QueryTimeout, 10 * time.Second},
		{"storage.near_capacity_ratio", cfg.NearCapacityRatio, 0.9},
		{"intervals.health_check_min", cfg.HealthCheckInterval, 5 * time.Minute},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// The mailbox settings are the ones that reach delivery-created mailboxes, so
// their path from file to config is worth pinning on its own.
func TestExampleMailboxDefaultsAreTheDocumentedOnes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMessagesPerMailbox = 7
	cfg.RetentionPolicy = time.Hour

	if err := LoadConfigFromFile(examplePath, cfg); err != nil {
		t.Fatalf("load example: %v", err)
	}

	if cfg.MaxMessagesPerMailbox != 1000 {
		t.Errorf("max_messages_per_mailbox = %d, want the example's 1000",
			cfg.MaxMessagesPerMailbox)
	}
	if cfg.RetentionPolicy != 30*24*time.Hour {
		t.Errorf("retention_policy = %v, want the example's 30 days", cfg.RetentionPolicy)
	}
}
