package memory

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
)

// legacyFingerprint is searchFingerprint as it was written before Phase 13
// made it cheaper: one binary.Write per vector component, which reflects on
// every call. At 384 dimensions that was 384 reflective writes a search. It is
// kept here as the reference the cheaper encoding must match byte for byte.
func legacyFingerprint(q *query.Query, includeArchived bool) [32]byte {
	h := sha256.New()
	fmt.Fprintf(h, "v1 tenant=%q namespace=%q text=%q keyword=%t explain=%t archived=%t order=%d desc=%t\n",
		q.Tenant, q.Namespace, q.Text, q.Keyword, q.Explain, includeArchived, q.OrderBy, q.Desc)
	for _, tag := range q.Tags {
		fmt.Fprintf(h, "tag=%q\n", tag)
	}
	for _, p := range q.Preds {
		fmt.Fprintf(h, "pred=%q\n", p.String())
	}
	fmt.Fprintf(h, "related=%v depth=%d direction=%q strength=%08x\n",
		q.RelatedTo, q.RelatedDepth, q.RelatedDirection, math.Float32bits(q.RelatedMinStrength))
	for _, typ := range q.RelatedTypes {
		fmt.Fprintf(h, "type=%q\n", typ)
	}
	fmt.Fprintf(h, "vector=%d\n", len(q.Vector))
	for _, v := range q.Vector {
		_ = binary.Write(h, binary.LittleEndian, v)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// The fingerprint binds a paging cursor to the exact query that made it, so a
// faster encoding must not change a single byte of what is hashed: a cursor
// minted before a change and presented after it would otherwise be refused as
// belonging to another query.
func TestSearchFingerprintIsUnchangedByItsEncoding(t *testing.T) {
	anchor := id.New()
	vec384 := make([]float32, 384)
	for i := range vec384 {
		vec384[i] = float32(math.Sin(float64(i))) * 0.1
	}
	for name, q := range map[string]*query.Query{
		"empty":    {Tenant: "acme"},
		"semantic": {Tenant: "acme", Text: "raft leader election", Vector: vec384, Limit: 10},
		"keyword":  {Tenant: "acme", Text: "INV-2024", Keyword: true, Tags: []string{"finance", "q3"}},
		"filtered": {Tenant: "acme", Vector: []float32{1, 0, -0.5, math.MaxFloat32},
			Preds: attr.Preds{attr.Eq(attr.SlotPolicy, attr.Str("long_term"))}},
		"related": {Tenant: "acme", Vector: vec384[:8], RelatedTo: &anchor, RelatedDepth: 2,
			RelatedDirection: graph.Out, RelatedMinStrength: 0.25,
			RelatedTypes: []graph.RelationshipType{graph.RelatedTo, graph.Supports}},
	} {
		for _, archived := range []bool{false, true} {
			if got, want := (&Service{}).searchFingerprint(q, archived), legacyFingerprint(q, archived); got != want {
				t.Errorf("%s (include_archived=%t): fingerprint changed", name, archived)
			}
		}
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
