package mcp_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/api/mcp"
)

// Every tool has a test that calls it. A tool nobody exercises is a schema
// nobody has checked against its handler, and the two drift silently -- the
// model sees a field the handler refuses, and the failure surfaces as a model
// that "cannot use the tool".
//
// The plan lists this test for Phase 13 and it did not exist.
func TestEveryMCPToolHasATest(t *testing.T) {
	fset := token.NewFileSet()
	// Every test file, parsed one by one: parser.ParseDir is deprecated since Go
	// 1.25 for ignoring build tags, which this test never relied on.
	names, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}

	var body strings.Builder
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				body.WriteString(lit.Value)
				body.WriteByte('\n')
			}
			return true
		})
	}

	for _, tool := range mcp.Tools() {
		if !strings.Contains(body.String(), `"`+tool.Name+`"`) {
			t.Errorf("no test names the tool %q; a tool without a test is a schema "+
				"nobody has checked against its handler", tool.Name)
		}
	}
}
