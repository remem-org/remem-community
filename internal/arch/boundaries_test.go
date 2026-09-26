// Package arch_test is the import-graph guard. It has no production code: its
// entire purpose is to fail the build when a dependency crosses a boundary the
// architecture depends on.
//
// It lands in Phase 1, before any subsystem exists, so the first Pebble import
// is rejected the moment it is written rather than discovered once a hundred
// files rely on it. It is the Go counterpart to the include_str! guards in
// Rust Remem's services/mod.rs, but exact rather than substring-matched,
// because go/build gives the real import graph instead of a text search.
package arch_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// modulePath is this module's import prefix; an import starting with it is one
// of ours, and the rest of the string is the repo-relative package path.
const modulePath = "github.com/remem-org/remem-go/"

// Each rule: a dependency prefix, and the ONLY package path allowed to import it.
var rules = []struct{ dep, allowed, why string }{
	{"github.com/cockroachdb/pebble", "internal/storage/pebble",
		"spec §40.1, Invariant 6: Pebble must not leak into the domain model"},
	{"go.etcd.io/raft", "internal/cluster/consensus/etcdraft",
		"spec §40.2, Invariant 7: etcd/raft must not leak into the database model"},
	{"github.com/yalue/onnxruntime_go", "internal/embedding/onnx",
		"the embedding runtime is replaceable; callers use embedding.Embedder"},
	{"github.com/daulet/tokenizers", "internal/embedding/onnx", "as above"},
	{modulePath + "internal/vector/hnsw", "internal/vector",
		"spec §40.3: no caller names HNSW concepts; callers use vector.Index"},
}

func TestInfrastructureDependenciesStayInTheirAdapters(t *testing.T) {
	for _, pkg := range allPackages(t) {
		imports := importsOf(t, pkg)
		for _, r := range rules {
			for _, imp := range imports {
				if !strings.HasPrefix(imp, r.dep) {
					continue
				}
				if pkg == r.allowed || strings.HasPrefix(pkg, r.allowed+"/") {
					continue
				}
				t.Errorf("%s imports %s.\n  Only %s may do that.\n  %s",
					pkg, imp, r.allowed, r.why)
			}
		}
	}
}

// The domain must not depend on the delivery mechanism.
func TestDomainDoesNotImportAPI(t *testing.T) {
	domain := []string{"internal/record", "internal/memory", "internal/graph",
		"internal/vector", "internal/query", "internal/lifecycle", "internal/attr",
		"internal/text", "internal/jobs", "internal/events"}
	for _, pkg := range domain {
		for _, imp := range importsOf(t, pkg) {
			if strings.HasPrefix(imp, modulePath+"internal/api") {
				t.Errorf("%s imports %s — dependencies point inward (spec §6)", pkg, imp)
			}
		}
	}
}

// The guard is worthless if it silently inspects nothing: a rename of internal/
// or a broken walk would turn every rule above into a no-op that passes.
func TestTheGuardActuallyReadsThePackages(t *testing.T) {
	pkgs := allPackages(t)
	if len(pkgs) < 4 {
		t.Fatalf("found only %d packages under internal/ and cmd/: %v", len(pkgs), pkgs)
	}
	want := modulePath + "internal/errs"
	for _, imp := range importsOf(t, "internal/version") {
		if imp == want {
			return
		}
	}
	t.Fatalf("internal/version is known to import %s; the import reader found none of it", want)
}

// repoRoot is two directories up from internal/arch, where the test runs.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("no go.mod at %s: the guard is looking in the wrong place", root)
	}
	return root
}

// allPackages returns every repo-relative package directory under internal/ and
// cmd/ — that is, every directory holding at least one .go file.
func allPackages(t *testing.T) []string {
	t.Helper()
	root := repoRoot(t)

	var pkgs []string
	for _, top := range []string{"internal", "cmd"} {
		base := filepath.Join(root, top)
		if _, err := os.Stat(base); os.IsNotExist(err) {
			continue // cmd/ arrives in Phase 3
		}
		err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			name := d.Name()
			if path != base && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata") {
				return filepath.SkipDir
			}
			if !hasGoFiles(t, path) {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			pkgs = append(pkgs, filepath.ToSlash(rel))
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", base, err)
		}
	}
	return pkgs
}

func hasGoFiles(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			return true
		}
	}
	return false
}

// importsOf returns every import of the repo-relative package dir, including
// the ones only its tests make: a boundary a test crosses is still crossed, and
// a test helper is the easiest place to smuggle an adapter into the domain.
//
// A package that does not exist yet contributes nothing. Most of the rules
// above name packages that later phases create, and a guard that failed until
// then would be a guard someone deleted.
func importsOf(t *testing.T, pkg string) []string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), filepath.FromSlash(pkg))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}
	p, err := build.ImportDir(dir, build.ImportComment)
	if err != nil {
		// A directory holding only _test.go files — internal/arch itself is one
		// — is reported as having no buildable Go files, with the test imports
		// already collected. Anything else is a real failure.
		if _, noGo := err.(*build.NoGoError); !noGo {
			t.Fatalf("reading imports of %s: %v", pkg, err)
		}
	}
	var out []string
	out = append(out, p.Imports...)
	out = append(out, p.TestImports...)
	out = append(out, p.XTestImports...)
	return out
}

// crossTenantCallers are the packages allowed to iterate every tenant.
//
// Rebuild, migration and export are the only work that is legitimately not
// about one tenant. Everything else that wants "all tenants" wants it because
// it has lost track of which one it is serving, which is the mistake Rust
// Remem made by shipping both vector_search and vector_search_partitioned:
// once an unscoped variant exists, something eventually calls it.
var crossTenantCallers = []string{
	"internal/jobs",
	"internal/snapshot",
	"internal/schema",
	"internal/tenant", // the interface and Ensure live here
	"internal/server", // the composition root provisions the default tenant
	"cmd/remem-admin", // export, import, rebuild, migrate, inspect
}

// The job framework runs work; it must not know what the work is.
//
// internal/jobs is infrastructure: a queue, a lease, a worker loop. Handlers are
// registered *into* it, and if it imported the packages whose work it runs, two
// things would follow. It would depend on every index in the system, which is
// the hub every later phase then has to add an edge to. And Phase 11's
// internal/discovery — which imports jobs in order to enqueue — would close a
// cycle the compiler would then have to refuse.
//
// The built-in rebuild handlers therefore live in internal/server, the
// composition root, where each is a closure over a Rebuild method that already
// exists. Phase 11's discovery handler is real domain logic and lives in
// internal/discovery, which is the direction this rule permits.
func TestTheJobFrameworkDoesNotImportTheWorkItRuns(t *testing.T) {
	forbidden := []string{
		"internal/vector", "internal/text", "internal/graph",
		"internal/record", "internal/query", "internal/memory", "internal/attr",
		// Phase 10's additions. internal/lifecycle imports jobs in order to be
		// a handler and to enqueue its own continuation, which is the direction
		// this rule permits; the reverse would make the framework depend on
		// retention policies, and internal/discovery importing jobs in Phase 11
		// would close the cycle.
		"internal/lifecycle", "internal/events",
		// Phase 11's addition, and the cycle the comment above predicted.
		// internal/discovery imports jobs to be a handler and to enqueue a
		// memory write's follow-up work; jobs importing discovery back would
		// close it.
		"internal/discovery",
	}
	for _, imp := range importsOf(t, "internal/jobs") {
		for _, f := range forbidden {
			if imp == modulePath+f || strings.HasPrefix(imp, modulePath+f+"/") {
				t.Errorf("internal/jobs imports %s.\n"+
					"  The job framework runs work and must not know what the work is: handlers\n"+
					"  are registered into it by the composition root. See docs/architecture/jobs.md.", imp)
			}
		}
	}
}

// TestForEachIsTheOnlyCrossTenantPath holds Invariant 1 at the level the import
// graph cannot: tenant.Directory.ForEach is a legal call, and what matters is
// who makes it.
//
// The detection is syntactic — a call to a method named ForEach, in a package
// that imports internal/tenant — because a full type-check of the module would
// need golang.org/x/tools and this guard has to be cheap enough to run on every
// commit. That trades a possible false positive for never missing a real one.
//
// The consequence is a naming rule, and it is worth stating because the guard
// has already enforced it once: **ForEach is reserved for the cross-tenant
// path**. A tenant-scoped walk is called Scan (see vector.Source). That is not
// a workaround for the guard's imprecision — it is a better name for the two
// different things, and it keeps the guard maximally strict instead of buying
// silence with an allowlist entry that would excuse a package from the real
// rule.
func TestForEachIsTheOnlyCrossTenantPath(t *testing.T) {
	for pkg, calls := range callsOf(t, "ForEach") {
		if allowedCrossTenant(pkg) {
			continue
		}
		t.Errorf("%s calls ForEach (%d times) — cross-tenant work is explicit and audited (Invariant 1).\n"+
			"  If this really is rebuild, migration or export work, add the package to crossTenantCallers with a reason.",
			pkg, calls)
	}
}

func allowedCrossTenant(pkg string) bool {
	for _, a := range crossTenantCallers {
		if pkg == a || strings.HasPrefix(pkg, a+"/") {
			return true
		}
	}
	return false
}

// callsOf counts calls to a method of the given name, per package, across every
// package that imports internal/tenant. Test files are included: a test that
// reaches across tenants is still a caller, and a helper in a _test.go file is
// the easiest place to hide one.
func callsOf(t *testing.T, method string) map[string]int {
	t.Helper()
	root := repoRoot(t)
	out := map[string]int{}

	for _, pkg := range allPackages(t) {
		if !importsTenant(t, pkg) {
			continue
		}
		dir := filepath.Join(root, filepath.FromSlash(pkg))
		fset := token.NewFileSet()
		// Every file in the package, parsed one by one: parser.ParseDir is
		// deprecated since Go 1.25 for ignoring build tags. Ignoring them is
		// what this guard wants, so a call behind a tag still counts.
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			f, err := parser.ParseFile(fset, name, nil, 0)
			if err != nil {
				t.Fatalf("parsing %s: %v", name, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == method {
					out[pkg]++
				}
				return true
			})
		}
	}
	return out
}

func importsTenant(t *testing.T, pkg string) bool {
	for _, imp := range importsOf(t, pkg) {
		if imp == modulePath+"internal/tenant" {
			return true
		}
	}
	return false
}

// The ForEach guard reads source with go/parser rather than the import graph,
// so it has its own way of silently inspecting nothing: a parse that fails
// open, or a walk that finds no package importing internal/tenant, turns the
// rule above into a no-op that passes.
func TestTheForEachGuardActuallyFindsCalls(t *testing.T) {
	calls := callsOf(t, "ForEach")
	if len(calls) == 0 {
		t.Fatal("the ForEach guard found no calls anywhere; internal/tenant's own tests make several, so the scan is broken")
	}
	if !importsTenant(t, "internal/tenant") {
		t.Fatal("internal/tenant is not seen as importing itself's package path; the import filter is broken")
	}
}
