package core

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The supervisord guide describes the deploy files and the package. It once
// told people to compile a Dart program, linked two documents that did not
// exist, opened a firewall port the server does not use, and set an
// environment variable nothing reads. This pins the guide to the files it
// describes: every repository path and document it names exists, every
// installed path it names is produced by the package build or its scripts,
// every RICOCHET_* variable is one the code reads, the ports are the real
// ones, and its excerpt of the supervisor config matches the shipped file.
func TestDeploymentGuideMatchesTheDeployFiles(t *testing.T) {
	guide := repoFile(t, "doc/SUPERVISORD_DEPLOYMENT.md")
	root := filepath.Join("..", "..")
	exists := func(rel string) bool {
		_, err := os.Stat(filepath.Join(root, rel))
		return err == nil
	}

	// Repository paths and documents.
	for _, m := range regexp.MustCompile(`(?:deploy|doc)/[A-Za-z0-9_./-]+[A-Za-z0-9]`).FindAllString(guide, -1) {
		if !exists(m) {
			t.Errorf("guide names %s, which does not exist", m)
		}
	}
	if strings.Contains(strings.ToLower(guide), "dart") {
		t.Error("guide mentions Dart; this is the Go server")
	}

	// Installed paths: produced by the package build, its maintainer scripts,
	// or the supervisor config. The .bak the manual upgrade makes is its own.
	installed := map[string]bool{}
	build := repoFile(t, "docker-build.sh")
	for _, m := range regexp.MustCompile(`cp ([^ ]+) "\$BUILD_DIR(/[^"]+)"`).FindAllStringSubmatch(build, -1) {
		dest := m[2]
		if strings.HasSuffix(dest, "/") {
			dest += filepath.Base(m[1])
		}
		installed[dest] = true
	}
	pathPattern := regexp.MustCompile(`/(?:opt|etc|var/lib|var/log)/ricochet/[A-Za-z0-9_.-]+`)
	for _, rel := range []string{"deploy/debian/postinst", "deploy/supervisor/ricochet.conf"} {
		for _, m := range pathPattern.FindAllString(repoFile(t, rel), -1) {
			installed[m] = true
		}
	}
	for _, m := range pathPattern.FindAllString(guide, -1) {
		if strings.HasSuffix(m, ".bak") || installed[m] {
			continue
		}
		t.Errorf("guide names %s, which nothing installs", m)
	}

	// Environment variables the code reads.
	known := map[string]bool{}
	for _, dir := range []string{"cmd", "internal"} {
		filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, _ := os.ReadFile(path)
			for _, v := range regexp.MustCompile(`RICOCHET_[A-Z_]+`).FindAllString(string(src), -1) {
				known[v] = true
			}
			return nil
		})
	}
	for _, v := range regexp.MustCompile(`RICOCHET_[A-Z_]+`).FindAllString(guide, -1) {
		if !known[v] {
			t.Errorf("guide sets %s, which nothing reads", v)
		}
	}

	// Ports.
	if strings.Contains(guide, "4001") {
		t.Error("guide names port 4001; the listen port is " + strconv.Itoa(DefaultConfig().Port))
	}
	for _, port := range []int{DefaultConfig().Port, DefaultOpsConfig().Port} {
		if !strings.Contains(guide, strconv.Itoa(port)) {
			t.Errorf("guide never names port %d", port)
		}
	}

	// The supervisor config excerpt: every key=value it shows is in the file.
	conf := repoFile(t, "deploy/supervisor/ricochet.conf")
	excerpt := regexp.MustCompile("(?s)```ini\n\\[program:ricochet\\]\ncommand=(.*?)```").FindStringSubmatch(guide)
	if excerpt == nil {
		t.Fatal("guide has no supervisor config excerpt starting with command=")
	}
	for _, line := range strings.Split("command="+excerpt[1], "\n") {
		if i := strings.Index(line, ";"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.Contains(conf, line+"\n") {
			t.Errorf("guide shows %q; deploy/supervisor/ricochet.conf does not", line)
		}
	}
}
