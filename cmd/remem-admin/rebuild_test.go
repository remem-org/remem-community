package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Every derived index has a rebuild path, which is Invariant 3 stated as a
// command. The two this phase adds — the attribute rows and the in-edge index —
// had none: one was rebuilt only by a migration and the other only by a job.
func TestRebuildRestoresEveryDerivedIndex(t *testing.T) {
	for _, tc := range []struct {
		index string
		space keys.Space
	}{
		{"attr", keys.SpaceAttrRow},
		{"text", keys.SpaceText},
		{"graph-in", keys.SpaceEdgeIn},
	} {
		t.Run(tc.index, func(t *testing.T) {
			dir, _ := seedCorpus(t)
			wipeSpace(t, dir, tc.space)
			if rowsIn(t, dir, tc.space) != 0 {
				t.Fatal("the space was not emptied, so the rebuild proves nothing")
			}

			if _, err := run([]string{"rebuild", "--data-dir", dir,
				"--index", tc.index}); err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if rowsIn(t, dir, tc.space) == 0 {
				t.Fatalf("rebuilding %s wrote no %s rows", tc.index, tc.space)
			}
		})
	}
}

func TestRebuildNamesTheIndexesItKnows(t *testing.T) {
	dir, _ := seedCorpus(t)
	_, err := run([]string{"rebuild", "--data-dir", dir, "--index", "hnsw"})
	if err == nil {
		t.Fatal("an index that does not exist was accepted")
	}
	if !strings.Contains(err.Error(), "graph-in") {
		t.Fatalf("the refusal does not list the indexes: %v", err)
	}

	if _, err := run([]string{"rebuild", "--data-dir", dir}); err == nil {
		t.Fatal("rebuild ran with no --index")
	}
}

// A mistyped --data-dir must be refused rather than turned into a new empty
// database that reports a clean rebuild over no data.
func TestRebuildRefusesADirectoryWithNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo")
	if _, err := run([]string{"rebuild", "--data-dir", missing, "--index", "attr"}); err == nil {
		t.Fatal("a path holding no database was accepted")
	}
	if _, err := pebble.Open(missing, pebble.Options{ReadOnly: true}); err == nil {
		t.Fatal("the refused path now holds a database")
	}
}

func wipeSpace(t *testing.T, dir string, space keys.Space) {
	t.Helper()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, space)
	it := kv.NewIterator(lower, upper)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, append([]byte(nil), it.Key()...))
	}
	_ = it.Close()
	for _, k := range doomed {
		if err := kv.Delete(context.Background(), k); err != nil {
			t.Fatal(err)
		}
	}
}

func rowsIn(t *testing.T, dir string, space keys.Space) int {
	t.Helper()
	kv, err := pebble.Open(dir, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, space)
	it := kv.NewIterator(lower, upper)
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	_ = it.Close()
	return n
}
