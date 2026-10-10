package hnsw

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

// The running counts residentBytes reads are only as good as the sites that
// keep them, and the way they go wrong is quiet: a later edit adds a place
// that changes a list's length and forgets the count, the budget drifts, and
// the cache evicts too early or too late with nothing on a request path to
// notice. The property test catches a helper that adjusts wrongly; this
// catches a site that does not go through a helper at all.
//
// It reads every non-test file in the package and refuses any assignment to a
// node's links or back lists, or to the arena's chunks, outside the functions
// named here. Each of those is a place the count is kept by hand, and each is
// covered by TestResidentBytesMatchesARecountAfterEveryOperation or
// TestAMaterialisedOrRebuiltGraphIsChargedItsRecount.
func TestOnlyTheHelpersChangeAListsLength(t *testing.T) {
	allowed := map[string]map[string]bool{
		// The helpers that adjust linkEntries.
		"setList":    {"*list": true},
		"appendList": {"*list": true},
		// A new node's lists are empty, and count nothing.
		"alloc": {"links": true, "back": true},
		// Placed lists are counted in the loop that places them.
		"materialise": {"links": true, "back": true},
		// A decoded record is not in any graph yet.
		"decodeNode": {"links": true},
		// The one place the arena's capacity changes, and where arenaFloats
		// is kept.
		"grow": {"chunks": true},
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var bad []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, lhs := range as.Lhs {
					target := guarded(lhs)
					if target == "" || allowed[fn.Name.Name][target] {
						continue
					}
					bad = append(bad, fset.Position(as.Pos()).String()+": "+fn.Name.Name+" assigns to "+target)
				}
				return true
			})
		}
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("%s directly; route it through setList, appendList or dropFromList "+
			"so the running count residentBytes reads stays exact", b)
	}
}

// guarded names what an assignment's left-hand side writes when it is one of
// the things the running counts describe, and is empty otherwise.
func guarded(e ast.Expr) string {
	// links[l], back[l], chunks[c]: strip the indexes down to the field.
	for {
		ix, ok := e.(*ast.IndexExpr)
		if !ok {
			break
		}
		e = ix.X
	}
	switch v := e.(type) {
	case *ast.SelectorExpr:
		switch v.Sel.Name {
		case "links", "back", "chunks":
			return v.Sel.Name
		}
	case *ast.StarExpr:
		if id, ok := v.X.(*ast.Ident); ok && id.Name == "list" {
			return "*list"
		}
	}
	return ""
}
