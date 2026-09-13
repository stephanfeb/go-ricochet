package main

import (
	"os"
	"sort"
	"strings"
	"testing"
)

// perOperationKeys lists the scenarios in validProtocols that are neither
// batched, sync shapes, nor the mix itself.
func perOperationKeys() []string {
	var keys []string
	for name := range validProtocols {
		if name == "mixed" || isSyncScenario(name) || strings.HasSuffix(name, "-batch") {
			continue
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	return keys
}

// "mixed" draws from every per-operation scenario. It drew from four of five
// for a long time: the setup created a collection per worker and the mix
// never touched it, so a "mixed" figure said nothing about the collection
// store. A scenario that is per-operation and absent from the mix has to be
// a deliberate choice, made here.
func TestMixedCoversEveryPerOperationScenario(t *testing.T) {
	want := perOperationKeys()
	got := append([]string(nil), perOperationScenarios...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("mixed draws from %v; per-operation scenarios are %v", got, want)
	}
	funcs := mixedFuncs(nil, nil)
	for _, name := range perOperationScenarios {
		if funcs[name] == nil {
			t.Errorf("mixed has no function for %q", name)
		}
	}
}

// Every scenario the tool offers has a row in the baselines document, so a
// new scenario cannot ship without a figure to regress against.
func TestBaselinesDocumentEveryScenario(t *testing.T) {
	data, err := os.ReadFile("../../doc/BASELINES.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)
	var names []string
	for name := range validProtocols {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.Contains(doc, "`"+name+"`") {
			t.Errorf("doc/BASELINES.md has no row for scenario `%s`", name)
		}
	}
}
