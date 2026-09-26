package licenses_test

import (
	_ "embed"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/licenses"
)

// The generator is a flagged test rather than a script, for the reason
// internal/schema's fixture generator is one: it is then compiled against the
// same code that reads it, and it cannot drift out of the build.
//
//	go test ./internal/licenses -run TestThirdPartyNoticesAreCurrent -update-notices
var update = flag.Bool("update-notices", false, "rewrite THIRD_PARTY_NOTICES.md from the module graph")

//go:embed bundled.md
var bundled string

const noticesFile = "THIRD_PARTY_NOTICES.md"

func TestThirdPartyNoticesAreCurrent(t *testing.T) {
	root := repoRoot(t)
	mods, err := licenses.Collect(root, licenses.ShippedTagSets, licenses.ShippedPatterns)
	if err != nil {
		t.Fatalf("collecting licences: %v", err)
	}
	if len(mods) == 0 {
		t.Fatal("no modules found: the scan read nothing, which is never the right answer")
	}
	want := licenses.Render(mods, bundled)

	path := filepath.Join(root, noticesFile)
	if *update {
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatalf("writing %s: %v", noticesFile, err)
		}
		t.Logf("wrote %s: %d modules", noticesFile, len(mods))
		return
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v\nrun: make notices", noticesFile, err)
	}
	if string(got) != want {
		t.Errorf("%s does not match the modules linked into the binaries.\n%s\nrun: make notices",
			noticesFile, firstDifference(string(got), want))
	}
}

// A dependency with no licence file at all is a distribution nobody can
// lawfully redistribute, and it is the failure this whole file exists to catch
// early: it is cheap to swap a dependency and expensive to discover the problem
// in somebody else's procurement review.
func TestEveryLinkedModuleCarriesALicence(t *testing.T) {
	mods, err := licenses.Collect(repoRoot(t), licenses.ShippedTagSets, licenses.ShippedPatterns)
	if err != nil {
		t.Fatalf("collecting licences: %v", err)
	}
	for _, m := range mods {
		if len(m.Files) == 0 {
			t.Errorf("%s %s has no licence file", m.Path, m.Version)
			continue
		}
		for _, f := range m.Files {
			if licenses.Kind(f.Text) == "unrecognised" && !strings.HasPrefix(filepath.Base(f.Path), "NOTICE") {
				t.Errorf("%s %s: %s is not a licence this generator recognises; "+
					"read it and decide whether it may ship", m.Path, m.Version, f.Path)
			}
		}
	}
}

// A guard that has never failed is a guard nobody has verified. This one pins
// the specific thing a naive scan gets wrong: a licence that governs one
// subtree of a module rather than the module. Pebble is BSD-3-Clause at its
// root, MIT in internal/cache and Apache-2.0 in internal/arenaskl, and all
// three are linked. A scan that only read module roots would pass every other
// check in this file while under-reporting two licences.
func TestTheScanFindsLicencesBelowAModuleRoot(t *testing.T) {
	mods, err := licenses.Collect(repoRoot(t), licenses.ShippedTagSets, licenses.ShippedPatterns)
	if err != nil {
		t.Fatalf("collecting licences: %v", err)
	}
	var pebble *licenses.Module
	for i := range mods {
		if mods[i].Path == "github.com/cockroachdb/pebble/v2" {
			pebble = &mods[i]
		}
	}
	if pebble == nil {
		t.Fatal("pebble is not in the scan, and it is linked into every binary")
	}
	want := map[string]string{
		"LICENSE":                   "BSD-3-Clause",
		"internal/cache/LICENSE":    "MIT",
		"internal/arenaskl/LICENSE": "Apache-2.0",
	}
	got := map[string]string{}
	for _, f := range pebble.Files {
		got[f.Path] = licenses.Kind(f.Text)
	}
	for path, kind := range want {
		if got[path] != kind {
			t.Errorf("pebble %s: got %q, want %q", path, got[path], kind)
		}
	}
}

// The document has to carry the text, not a summary of it. This asserts the
// clause Pebble's licence actually obliges us to reproduce is in the file
// verbatim, so a future rewrite of Render that drops the texts and keeps the
// table fails here.
func TestTheNoticesReproduceTheBindingClause(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), noticesFile))
	if err != nil {
		t.Fatalf("reading %s: %v", noticesFile, err)
	}
	notices := string(b)
	for _, want := range []string{
		"Copyright (c) 2011 The LevelDB-Go Authors. All rights reserved.",
		"Redistributions in binary form must reproduce the above",
		"THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS",
	} {
		if !strings.Contains(notices, want) {
			t.Errorf("%s does not contain %q", noticesFile, want)
		}
	}
}

func firstDifference(got, want string) string {
	g, w := strings.Split(got, "\n"), strings.Split(want, "\n")
	for i := 0; i < len(g) && i < len(w); i++ {
		if g[i] != w[i] {
			return "first difference at line " + strconv.Itoa(i+1) + ":\n  have: " + g[i] + "\n  want: " + w[i]
		}
	}
	return "the files agree for " + strconv.Itoa(min(len(g), len(w))) + " lines and then one ends: " +
		"have " + strconv.Itoa(len(g)) + " lines, want " + strconv.Itoa(len(w))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s is not the repository root: %v", root, err)
	}
	return root
}
