package core

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// goVersion parses "1.25.7" or "1.25" into comparable parts.
func goVersion(t *testing.T, s string) []int {
	t.Helper()
	var parts []int
	for _, p := range strings.Split(s, ".") {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("bad Go version %q", s)
		}
		parts = append(parts, n)
	}
	for len(parts) < 3 {
		parts = append(parts, 0)
	}
	return parts
}

// The package build downloads a fixed Go release. go.mod's go directive is
// the floor the module's dependencies impose, so a Docker image below it
// cannot build the server, and nobody finds out until the release build.
func TestDockerBuildUsesAToolchainTheModuleAccepts(t *testing.T) {
	directive := regexp.MustCompile(`(?m)^go (\d+\.\d+(?:\.\d+)?)`).FindStringSubmatch(repoFile(t, "go.mod"))
	if directive == nil {
		t.Fatal("go.mod has no go directive")
	}
	image := regexp.MustCompile(`go\.dev/dl/go(\d+\.\d+(?:\.\d+)?)\.linux-amd64\.tar\.gz`).FindStringSubmatch(repoFile(t, "Dockerfile.build"))
	if image == nil {
		t.Fatal("Dockerfile.build does not download a Go release from go.dev/dl")
	}

	want, got := goVersion(t, directive[1]), goVersion(t, image[1])
	for i := range want {
		if got[i] > want[i] {
			return
		}
		if got[i] < want[i] {
			t.Fatalf("Dockerfile.build installs Go %s but go.mod requires %s", image[1], directive[1])
		}
	}
}
