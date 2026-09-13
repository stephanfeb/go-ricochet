package core

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// configDiff lists every leaf field that differs between two configs, as
// "Path: before -> after".
func configDiff(prefix string, a, b reflect.Value, out *[]string) {
	switch a.Kind() {
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			configDiff(prefix+"."+a.Type().Field(i).Name, a.Field(i), b.Field(i), out)
		}
	case reflect.Ptr:
		if a.IsNil() || b.IsNil() {
			if a.IsNil() != b.IsNil() {
				*out = append(*out, prefix+": one side is nil")
			}
			return
		}
		configDiff(prefix, a.Elem(), b.Elem(), out)
	default:
		if !reflect.DeepEqual(a.Interface(), b.Interface()) {
			*out = append(*out, fmt.Sprintf("%s: %v -> %v", prefix, a.Interface(), b.Interface()))
		}
	}
}

// loadedOver returns what loading the file over the preset changes.
func loadedOver(t *testing.T, path string, preset func() *ServerConfig) []string {
	t.Helper()
	before, after := preset(), preset()
	if err := LoadConfigFromFile(path, after); err != nil {
		t.Fatalf("load %s: %v", path, err)
	}
	var out []string
	configDiff("", reflect.ValueOf(before).Elem(), reflect.ValueOf(after).Elem(), &out)
	sort.Strings(out)
	return out
}

// The config the package installs changes nothing. The package runs the
// server on the production preset and loads /etc/ricochet/config.yaml over
// it, and for the life of the codebase that file was a copy of
// config.example.yaml: its live pool_size of 10 replaced production's 50,
// and admission control, which sizes itself from the pool, followed it down.
// A packaged install was a production server throttled to a tenth of its
// database connections, and nothing said so.
func TestPackagedConfigChangesNothing(t *testing.T) {
	for name, preset := range map[string]func() *ServerConfig{
		"production": ProductionConfig,
		"default":    DefaultConfig,
	} {
		if diff := loadedOver(t, "../../deploy/config.yaml", preset); len(diff) > 0 {
			t.Errorf("deploy/config.yaml changes the %s preset: %v", name, diff)
		}
	}
}

// config.example.yaml documents the defaults: every live value is the
// built-in default, except the two it declares as deployment choices. A
// live value that differs from the default reads as the default to anyone
// copying the file, which is how the example's pool_size of 10 got into
// production installs.
func TestExampleConfigDocumentsTheDefaults(t *testing.T) {
	declared := map[string]bool{
		".DataDirectory":   true,
		".MaxStorageBytes": true,
	}
	for _, line := range loadedOver(t, "../../config.example.yaml", DefaultConfig) {
		field := line[:indexByte(line, ':')]
		if !declared[field] {
			t.Errorf("config.example.yaml sets a non-default value: %s", line)
		}
	}
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return len(s)
}
