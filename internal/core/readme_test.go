package core

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// repoFile reads a file relative to the repository root.
func repoFile(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

// The README states facts the code also states: the Go version, the protocol
// IDs, the CLI flags of both binaries, the bench scenarios, the schema's
// tables, the package tree and the identity file name. Each of these drifted
// at some point (the audit of 2026-09-13 found a placeholder repository
// link, a Go version behind go.mod, four protocols missing from the table,
// four flags missing from the CLI block, a wrong identity file name and a
// package tree with a directory that no longer existed). This test pins the
// mechanical ones, so the next drift fails a build rather than a reader.
func TestReadmeMatchesTheCode(t *testing.T) {
	readme := repoFile(t, "README.md")
	missing := func(what, needle string) {
		t.Helper()
		if !strings.Contains(readme, needle) {
			t.Errorf("README does not mention %s %q", what, needle)
		}
	}

	// Go version: go.mod's directive, patch level included. A floor that
	// comes from a dependency is a real floor.
	goLine := regexp.MustCompile(`(?m)^go (\d+\.\d+(?:\.\d+)?)`).FindStringSubmatch(repoFile(t, "go.mod"))
	if goLine == nil {
		t.Fatal("go.mod has no go directive")
	}
	missing("Go version", "Go "+goLine[1]+"+")

	// Every protocol ID the handlers register.
	handlers, _ := filepath.Glob(filepath.Join("..", "..", "internal", "protocol", "*", "handler.go"))
	if len(handlers) == 0 {
		t.Fatal("no handlers found")
	}
	idPattern := regexp.MustCompile(`protocol\.ID\("([^"]+)"\)`)
	for _, h := range handlers {
		src, err := os.ReadFile(h)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range idPattern.FindAllStringSubmatch(string(src), -1) {
			missing("protocol ID", "`"+m[1]+"`")
		}
	}

	// Every flag of the server, as --name, and of the bench tool, as -name.
	flagPattern := regexp.MustCompile(`flag\.(?:Int|Bool|String|Duration|Float64)\("([a-z-]+)"`)
	for _, m := range flagPattern.FindAllStringSubmatch(repoFile(t, "cmd/ricochet/main.go"), -1) {
		missing("server flag", "--"+m[1])
	}
	bench := repoFile(t, "cmd/ricochet-bench/main.go")
	for _, m := range flagPattern.FindAllStringSubmatch(bench, -1) {
		missing("bench flag", "`-"+m[1]+"`")
	}

	// Every bench scenario.
	scenarios := regexp.MustCompile(`(?s)var validProtocols = map\[string\]string\{(.*?)\n\}`).FindStringSubmatch(bench)
	if scenarios == nil {
		t.Fatal("validProtocols not found in the bench tool")
	}
	for _, m := range regexp.MustCompile(`"([a-z-]+)":\s+"`).FindAllStringSubmatch(scenarios[1], -1) {
		missing("bench scenario", "`"+m[1]+"`")
	}

	// Every table in the schema.
	for _, m := range regexp.MustCompile(`(?m)^CREATE TABLE IF NOT EXISTS (\w+)`).FindAllStringSubmatch(repoFile(t, "schema.sql"), -1) {
		missing("schema table", "`"+m[1]+"`")
	}

	// Every package directory, in the project structure block.
	structure := regexp.MustCompile("(?s)## Project Structure\n\n```\n(.*?)```").FindStringSubmatch(readme)
	if structure == nil {
		t.Fatal("README has no Project Structure block")
	}
	var dirs []string
	for _, pattern := range []string{"internal/*", "internal/protocol/*", "internal/mda/*", "internal/storage/*", "pkg/*", "cmd/*"} {
		matches, _ := filepath.Glob(filepath.Join("..", "..", pattern))
		for _, m := range matches {
			if info, err := os.Stat(m); err == nil && info.IsDir() {
				dirs = append(dirs, filepath.Base(m)+"/")
			}
		}
	}
	sort.Strings(dirs)
	for _, d := range dirs {
		if !strings.Contains(structure[1], d) {
			t.Errorf("Project Structure omits %s", d)
		}
	}
	for _, line := range strings.Split(structure[1], "\n") {
		name := strings.TrimSpace(line)
		if i := strings.Index(name, " "); i > 0 {
			name = name[:i]
		}
		if strings.HasSuffix(name, "/") && !strings.Contains(name, "test") {
			base := filepath.Base(strings.TrimSuffix(name, "/")) + "/"
			if len(dirs) > 0 && sort.SearchStrings(dirs, base) < len(dirs) && dirs[sort.SearchStrings(dirs, base)] == base {
				continue
			}
			if _, err := os.Stat(filepath.Join("..", "..", strings.TrimSuffix(name, "/"))); err != nil && !strings.Contains(name, "/") {
				t.Errorf("Project Structure lists %s, which does not exist", name)
			}
		}
	}

	// Identity file and transport wording.
	missing("identity file", "peer_identity.key")
	if strings.Contains(readme, "UDX (unreliable") {
		t.Error("README calls UDX unreliable; it is a reliable, congestion-controlled transport")
	}
}
