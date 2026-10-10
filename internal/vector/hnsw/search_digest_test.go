package hnsw

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"runtime"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// The recall tests say the walk is good enough. This one says it is *the same*.
//
// A digest over every candidate a fixed corpus returns, and over the whole
// graph's link structure, is the only guard strong enough for a change that is
// meant to be invisible: pooling the working set, replacing container/heap,
// reordering a loop. Recall is a mean over a hundred queries and hides a
// candidate swapped for its neighbour at the same distance; this does not.
//
// The digests change only when the algorithm is meant to change. A commit that
// moves one is claiming different answers, and must say so and say why. In
// particular the Tier 3 reworks in the server speed plan — contiguous
// vector storage, and a SIMD dot product in place of the scalar one — must
// reproduce these digests exactly or state in the commit message which results
// changed and why the new ones are right. A SIMD kernel that sums in a
// different order is such a case: it will move the digest even though it is
// correct, and the burden is then to show the distances moved by no more than
// their last bits and the orderings only where they tied. (The kernel that
// landed moved them by up to six of those bits, not one, which is what the
// section below reports rather than rounds off.)
//
// # The cosine digest and the NEON kernel
//
// Contiguous vector storage (plan item 15, first half) reproduced both digests
// exactly, as a change of storage layout must.
//
// The NEON dot product (second half) moved the cosine digest, and this is the
// burden discharged. distance.dot32 on arm64 sums into sixteen lanes and
// reduces them pairwise; the scalar loop sums into four accumulators in index
// order. Both are correct; neither is the "true" sum, and the tree is in fact
// the more accurate of the two (dot_internal_test.go measures 5.2e-08 worst
// relative error against 6.8e-08).
//
// Measured by running this harness under both kernels — `go test -tags purego`
// against the default build, through TestWriteWalkTrace below:
//
//   - The graphs are identical. All 3,000 live nodes have the same neighbour
//     list at every layer, so across 3,400 inserts and 400 removals the kernel
//     changed no insert decision at all.
//   - All 800 queries return the same candidates: the same node numbers, the
//     same count, as sets.
//   - Of 56,200 candidate positions, 22,734 have a distance that moved. The
//     largest move is 1.79e-07 and the mean is 2.5e-08. 1.79e-07 is three ULPs
//     of the dot product the distance is computed from, and six representable
//     float32 steps of the distance itself at that magnitude: the two kernels
//     disagree about the sum in its last two bits, and `1 - dot/(na*nb)`
//     magnifies that as it subtracts from one.
//   - 4 of those 56,200 positions are ordered differently, in 2 of the 800
//     queries. In every case the two candidates that swapped places are at
//     most *one representable float32 step* apart under either kernel: in one
//     query they are exactly equal under the scalar kernel and one step apart
//     under NEON, in the other exactly equal under NEON and one step apart
//     under the scalar kernel. Nothing further apart than one step is
//     reordered anywhere.
//
// The second half of that last point is worth stating plainly rather than
// calling it a tie: two of the four reordered positions are pairs the old
// kernel did *not* report as exactly equal. It separated them by one float32
// step, which is well below what the two kernels agree to — they disagree
// about a single distance by up to six steps — so neither kernel's order for
// that pair is the right one and nothing was lost by changing it. A reordering
// of two candidates further apart than the kernels' own disagreement would be a
// different claim, and this comment would have to withdraw it.
//
// So the arm64 cosine digest is the same answers to a last-bit tolerance, and
// is re-recorded on that basis: per kernel rather than loosened, because each
// kernel must still be exact against itself. The L2 metric never calls the dot
// product, and its digest is unchanged by both halves of item 15.
//
// # One pair per floating-point target
//
// The expected values live in digest_<target>_test.go, because the digest is
// over float bits and the rounding is a property of the code the compiler emits:
// arm64 fuses x*y+z into one FMA and rounds once, amd64 at GOAMD64=v1 does not,
// and amd64 at v3 fuses some kernels and not others. Each is correct, and none
// reproduces another's last bit. The targets are amd64 v1-v2, amd64 v3+, arm64
// with the NEON kernel, and arm64 under `purego`. On an architecture with
// nothing recorded the test skips, printing the digest it computed, so it is
// reported rather than passed without comparing.
//
// To re-record on one target, run there: the failure, or the skip, prints the
// digest it computed.

func TestSearchDigestIsUnchanged(t *testing.T) {
	for _, c := range []struct {
		name   string
		metric distance.Metric
		want   string
	}{
		{"l2", distance.L2, recordedDigests.l2},
		{"cosine", distance.Cosine, recordedDigests.cosine},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := walkDigest(t, c.metric)
			if c.want == "" {
				t.Skipf("no digest is recorded for %s (%s); it computed %s. "+
					"Record it in a digest_<target>_test.go once this target's results are trusted.",
					runtime.GOARCH, recordedDigests.covers, got)
			}
			if got != c.want {
				t.Fatalf("the walk's results changed on %s (%s).\n got %s\nwant %s\n"+
					"If this commit means to change the algorithm, say so and record the new digest "+
					"for every target; if it does not, it has a bug.",
					runtime.GOARCH, recordedDigests.covers, got, c.want)
			}
		})
	}
}

// walkDigests is one target's expected pair, and what it covers.
type walkDigests struct {
	l2, cosine string
	// covers names the code generation the pair was recorded under, for the
	// failure message: a digest is only meaningful against the rounding it was
	// taken with.
	covers string
}

// walkDigest digests every answer the walk gives together with the resulting
// link structure.
func walkDigest(t *testing.T, metric distance.Metric) string {
	t.Helper()
	answers, g := walkTrace(t, metric)

	h := sha256.New()
	var buf [8]byte
	for _, got := range answers {
		for _, cand := range got {
			binary.LittleEndian.PutUint32(buf[0:4], cand.node)
			// The distance's bits, not the float: a digest over %f would not
			// notice a change in the last place, which is exactly the change a
			// reordered sum makes.
			binary.LittleEndian.PutUint32(buf[4:8], math.Float32bits(cand.dist))
			h.Write(buf[:])
		}
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(got)))
		h.Write(buf[:4])
	}
	// The link structure pins the *insert* walk, which the queries only see
	// through its results.
	for num, n := range g.nodes {
		if n == nil {
			continue
		}
		binary.LittleEndian.PutUint32(buf[0:4], uint32(num))
		h.Write(buf[:4])
		for _, links := range n.links {
			for _, m := range links {
				binary.LittleEndian.PutUint32(buf[0:4], m)
				h.Write(buf[:4])
			}
			h.Write([]byte{'|'})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// walkTrace builds a fixed graph, churns it, queries it at four efs and returns
// every answer and the graph that gave them.
//
// It is separate from the digest so that two builds can be compared answer by
// answer rather than only by whether a hash moved — which is what a change of
// distance kernel needs, and what the long comment above this file's digests
// reports. See TestWriteWalkTrace.
func walkTrace(t *testing.T, metric distance.Metric) ([][]candidate, *graph) {
	t.Helper()

	g := newGraph(Defaults(), metric)
	rng := newRNG(99)

	// Deterministic ids, because a node's level is derived from its id: random
	// ids would build a different graph on every run and digest nothing.
	const corpus, churn = 3000, 400
	ids := make([]id.ID, corpus)
	for i := range ids {
		ids[i] = digestID(uint64(i) + 1)
		if _, err := g.insert(ids[i], rng.unit(24)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	// Removals and re-inserts, so the repair path and node-number reuse are in
	// the digest too and not just a freshly built graph.
	for i := range churn {
		g.remove(g.byID[ids[i]])
	}
	for i := range churn {
		if _, err := g.insert(digestID(uint64(1000000+i)), rng.unit(24)); err != nil {
			t.Fatalf("re-insert %d: %v", i, err)
		}
	}

	var answers [][]candidate
	for _, ef := range []int{1, 16, 64, 200} {
		for range 200 {
			got, err := g.nearest(rng.unit(24), ef)
			if err != nil {
				t.Fatalf("ef %d: %v", ef, err)
			}
			answers = append(answers, got)
		}
	}
	return answers, g
}

// digestID is a record id derived from a counter: unique, non-zero, and the same
// on every run, so levelOf gives the same heights and the corpus reproduces.
func digestID(n uint64) id.ID {
	var out id.ID
	binary.BigEndian.PutUint64(out[0:8], n*0x9e3779b97f4a7c15+1)
	binary.BigEndian.PutUint64(out[8:16], n*0xbf58476d1ce4e5b9+0x94d049bb133111eb)
	return out
}
