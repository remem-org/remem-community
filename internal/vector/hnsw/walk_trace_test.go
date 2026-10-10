package hnsw

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/remem-org/remem-go/internal/vector/distance"
)

// TestWriteWalkTrace writes the cosine walk's whole trace to the file named by
// REMEM_WALK_TRACE, and does nothing at all when that is unset.
//
// It exists because a digest can only say "something moved". When a change is
// *meant* to move it — a distance kernel that sums in a different order is the
// case the server speed plan's item 15 anticipated — the question becomes
// which answers changed and by how much, and that needs the answers themselves
// from two builds:
//
//	REMEM_WALK_TRACE=/tmp/scalar.txt go test -tags purego ./internal/vector/hnsw -run TestWriteWalkTrace
//	REMEM_WALK_TRACE=/tmp/neon.txt   go test            ./internal/vector/hnsw -run TestWriteWalkTrace
//
// The two files are then compared line by line: an `L` line per node pins the
// graph, a `Q` line per query pins the candidates and their distances in the
// order the walk returned them, with distances as their bits so that nothing is
// rounded away on the way out. The conclusion of that comparison is recorded in
// search_digest_test.go, which is where a reader of the digests will look.
func TestWriteWalkTrace(t *testing.T) {
	path := os.Getenv("REMEM_WALK_TRACE")
	if path == "" {
		t.Skip("REMEM_WALK_TRACE is unset; this test writes a trace for a cross-build comparison")
	}
	answers, g := walkTrace(t, distance.Cosine)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for num, n := range g.nodes {
		if n == nil {
			continue
		}
		fmt.Fprintf(w, "L %d", num)
		for _, links := range n.links {
			fmt.Fprint(w, " ")
			for i, m := range links {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, "%d", m)
			}
		}
		fmt.Fprintln(w)
	}
	for i, got := range answers {
		fmt.Fprintf(w, "Q %d %d", i, len(got))
		for _, cand := range got {
			fmt.Fprintf(w, " %d:%d", cand.node, math.Float32bits(cand.dist))
		}
		fmt.Fprintln(w)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d queries and %d live nodes to %s", len(answers), g.live, path)
}
