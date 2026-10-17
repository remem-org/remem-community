package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestEveryContentWriteStagesPassages holds design decision 5 of
// openspec/changes/passage-vectors. A record's passage rows are a function of
// its content, staged by the passage indexer from Record.Passages, where nil
// means "the content did not change". A write path that sets Content and leaves
// Passages nil therefore leaves the old content's passages in the index: the
// memory stays findable by what it no longer says, and nothing reports it.
//
// The rule is syntactic, as this package's guards are: in a package that
// imports internal/record, a function that writes a Content field — a
// record.Record literal naming Content, or an assignment to x.Content — must
// also name Passages. That is a proxy, and the allowlist below is where it is
// wrong, each entry with its reason. An entry that no longer matches anything
// fails too, so the list cannot rot into a set of exemptions for code that is
// gone.
func TestEveryContentWriteStagesPassages(t *testing.T) {
	allowed := map[string]string{
		"internal/record/repo_codec.go:decodeBody":                    "decodes a stored body: a read, not a write",
		"internal/record/repo_codec_projected.go:decodeBodyProjected": "decodes a stored body for a response: a read, not a write",
		"internal/memory/update.go:validate":                          "copies the content into a CreateReq to reuse create's validation; writes no record",
	}

	root := repoRoot(t)
	used := map[string]bool{}
	var problems []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "pb" || name == "pbext" || name == "arch" || name == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, site := range contentWritesWithoutPassages(t, path, rel) {
				if _, ok := allowed[site]; ok {
					used[site] = true
					continue
				}
				problems = append(problems, site)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range problems {
		t.Errorf("%s writes a record's Content and does not set Passages: the old content's passage rows "+
			"would stay in the index. Stage the new passages with the content (record.Record.Passages), or "+
			"add it to this test's allowlist with the reason it writes no record", p)
	}
	for site, why := range allowed {
		if !used[site] {
			t.Errorf("the allowlist names %s (%s), which no longer writes Content: remove the entry", site, why)
		}
	}
}

// contentWritesWithoutPassages returns "file:function" for each function in the
// file that writes a Content field and never names Passages, if the file's
// package works with records at all.
func contentWritesWithoutPassages(t *testing.T, path, rel string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", rel, err)
	}
	inRecord := f.Name.Name == "record" && strings.HasPrefix(rel, "internal/record/")
	importsRecord := inRecord
	for _, imp := range f.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p == modulePath+"internal/record" {
			importsRecord = true
		}
	}
	if !importsRecord {
		return nil
	}

	var out []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		writes, passages := false, false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				if isRecordType(n.Type, inRecord) {
					for _, el := range n.Elts {
						if kv, ok := el.(*ast.KeyValueExpr); ok {
							if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "Content" {
								writes = true
							}
						}
					}
				}
			case *ast.AssignStmt:
				if n.Tok == token.DEFINE {
					return true
				}
				for _, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Content" {
						writes = true
					}
				}
			case *ast.Ident:
				if n.Name == "Passages" {
					passages = true
				}
			}
			return true
		})
		if writes && !passages {
			out = append(out, rel+":"+fn.Name.Name)
		}
	}
	return out
}

func isRecordType(e ast.Expr, inRecord bool) bool {
	switch e := e.(type) {
	case *ast.SelectorExpr:
		x, ok := e.X.(*ast.Ident)
		return ok && x.Name == "record" && e.Sel.Name == "Record"
	case *ast.Ident:
		return inRecord && e.Name == "Record"
	}
	return false
}
