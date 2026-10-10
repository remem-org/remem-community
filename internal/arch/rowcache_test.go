package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// attrRowKeyUsers is every shipped file allowed to build an attribute row's
// key, and why. The row cache (internal/attr/cache.go) is safe under snapshot
// isolation only because every write of an attribute row passes through
// attr.Indexer.Stage, which marks the row before its commit and moves the
// tenant's generation after it. A second writer that built the key itself would
// write a row the cache never hears about, and readers would be served the old
// one with nothing to say so. Reading the key is harmless; writing it anywhere
// but index.go is the defect this guards against.
var attrRowKeyUsers = map[string]string{
	"internal/attr/row.go":      "defines attr.RowKey over keys.AttrRow",
	"internal/attr/index.go":    "the one writer: Indexer.Stage, which marks the row for the cache",
	"internal/attr/select.go":   "reads a row through the reader's snapshot",
	"internal/inspect/check.go": "reads the space to report what is missing, and writes nothing",
	"internal/keys/keys.go":     "defines keys.AttrRow",
}

// TestOnlyTheIndexerWritesAttributeRows fails when a shipped file outside the
// allowlist builds an attribute row's key. It matches calls, not text, so a
// comment naming the function does not trip it.
func TestOnlyTheIndexerWritesAttributeRows(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if _, ok := attrRowKeyUsers[rel]; ok {
				return nil
			}
			if buildsAttrRowKey(t, path) {
				offenders = append(offenders, rel)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("these files build an attribute row key and are not the indexer: %v\n"+
			"Write attribute rows through attr.Indexer.Stage, which keeps the row cache "+
			"consistent; a read-only use belongs in attrRowKeyUsers with its reason.", offenders)
	}
}

// buildsAttrRowKey reports whether a file calls keys.AttrRow, attr.RowKey, or
// (inside package attr) RowKey.
func buildsAttrRowKey(t *testing.T, path string) bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := fn.X.(*ast.Ident); ok &&
				((pkg.Name == "keys" && fn.Sel.Name == "AttrRow") || (pkg.Name == "attr" && fn.Sel.Name == "RowKey")) {
				found = true
			}
		case *ast.Ident:
			if f.Name.Name == "attr" && fn.Name == "RowKey" {
				found = true
			}
		}
		return !found
	})
	return found
}
