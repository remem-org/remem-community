package memory

import (
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/query"
)

func fingerprintVector(n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(math.Sin(float64(i))) * 0.1
	}
	return v
}

// A fingerprint is only ever compared with one computed in this process, inside
// the lifetime of a paging session, which lives in memory and never survives a
// restart. Within that process it has to be perfectly stable: a seed drawn per
// call or per Service would refuse every continuation of every search.
func TestSearchFingerprintIsStableWithinTheProcess(t *testing.T) {
	_, one := newSearchPagingService(t, nil, Config{})
	_, two := newSearchPagingService(t, nil, Config{})
	q := &query.Query{Tenant: "acme", Text: "raft leader election", Vector: fingerprintVector(384), Limit: 10}
	first := one.searchFingerprint(q, false)
	if one.searchFingerprint(q, false) != first {
		t.Fatal("one service fingerprinted one query differently twice")
	}
	if two.searchFingerprint(q, false) != first {
		t.Fatal("the seed is not process-wide")
	}
	other := *q
	other.Text = "paxos leader election"
	if one.searchFingerprint(&other, false) == first {
		t.Fatal("two questions share a fingerprint")
	}
}

// The vector is the whole of a semantic question. Hashing only its length, or
// any prefix of it, would resume a cursor against the ranking of a different
// embedding.
func TestSearchFingerprintBindsEveryVectorComponent(t *testing.T) {
	_, svc := newSearchPagingService(t, nil, Config{})
	base := query.Query{Tenant: "acme", Vector: fingerprintVector(384), Limit: 10}
	want := svc.searchFingerprint(&base, false)
	for _, i := range []int{0, 1, 191, 382, 383} {
		q := base
		q.Vector = append([]float32(nil), base.Vector...)
		q.Vector[i] += 1e-7
		if svc.searchFingerprint(&q, false) == want {
			t.Fatalf("component %d did not bind", i)
		}
	}
	// A component moving to its neighbour keeps the byte stream's length and
	// every other component, so only a position-sensitive hash separates them.
	q := base
	q.Vector = append([]float32(nil), base.Vector...)
	q.Vector[7], q.Vector[8] = q.Vector[8], q.Vector[7]
	if svc.searchFingerprint(&q, false) == want {
		t.Fatal("swapping two components did not bind")
	}
}

func BenchmarkSearchFingerprint384(b *testing.B) {
	q := &query.Query{Tenant: "acme", Text: "raft leader election", Vector: make([]float32, 384)}
	s := &Service{}
	b.ReportAllocs()
	for range b.N {
		_ = s.searchFingerprint(q, false)
	}
}

// A realistic search carries the archived predicate and a tag or two, where the
// predicate's rendering is the only handle the attribute interface offers.
func BenchmarkSearchFingerprint384Filtered(b *testing.B) {
	q := &query.Query{Tenant: "acme", Text: "raft leader election", Vector: make([]float32, 384),
		Tags:  []string{"infra", "consensus"},
		Preds: attr.Preds{attr.Eq(attr.SlotArchived, attr.Bool(false))}}
	s := &Service{}
	b.ReportAllocs()
	for range b.N {
		_ = s.searchFingerprint(q, false)
	}
}
