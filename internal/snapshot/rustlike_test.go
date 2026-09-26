package snapshot_test

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// A snapshot shaped exactly like one Rust writes: source_impl "rust", only the
// byte-shared sections, records and vectors and edges interleaved a page at a
// time, and vectors that carry the true model id in a space this binary cannot
// use.
//
// It is built rather than read from a fixture because the fixture corpora are
// generated on demand (`make fixtures`) and most CI runs do not have them. What
// a committed Rust artifact holds instead is the framing —
// TestGoReadsWhatRustWrote — and what this holds is the *shape*: the section
// order, the interleaving, and above all the vectors-in-another-space problem
// that decides the whole import.
type rustOpt func(*rustCorpus)

type rustCorpus struct {
	records []*pb.Record
	vectors []*pb.Vector
	edges   []*pb.Edge
}

// rustRecordID is a deterministic id, so a test can look one up afterwards.
func rustRecordID(n byte) id.ID {
	var out id.ID
	out[0] = 0xA0
	out[15] = n
	return out
}

func withOrphanEdge() rustOpt {
	return func(c *rustCorpus) {
		c.edges = append(c.edges, &pb.Edge{
			Tenant: "default", From: rustRecordID(0).Bytes(), To: rustRecordID(99).Bytes(),
			RelationshipType: "related_to", Strength: 0.5,
		})
	}
}

func withSelfEdge() rustOpt {
	return func(c *rustCorpus) {
		c.edges = append(c.edges, &pb.Edge{
			Tenant: "default", From: rustRecordID(1).Bytes(), To: rustRecordID(1).Bytes(),
			RelationshipType: "related_to", Strength: 0.5,
		})
	}
}

// withVectorlessRecord adds a record with no vector, which is what an archived
// memory looks like in a Rust export: Rust retires an archived memory's vector
// from its similarity index, and the exporter writes no placeholder for one.
func withVectorlessRecord() rustOpt {
	return func(c *rustCorpus) {
		c.records = append(c.records, &pb.Record{
			Tenant: "default", Id: rustRecordID(2).Bytes(),
			Content:         "an archived memory, retired from the index",
			Policy:          "short_term",
			Archived:        true,
			CreatedAtUnixMs: 1700000000000, UpdatedAtUnixMs: 1700000000000,
			Importance: 0.5, Health: 100,
		})
	}
}

func rustLikeSnapshot(t *testing.T, opts ...rustOpt) []byte {
	t.Helper()

	c := &rustCorpus{}
	for n := range byte(2) {
		rid := rustRecordID(n)
		c.records = append(c.records, &pb.Record{
			Tenant: "default", Id: rid.Bytes(),
			Content:         "a memory written by the Rust implementation",
			Policy:          "short_term",
			CreatedAtUnixMs: 1700000000000, UpdatedAtUnixMs: 1700000000000,
			Importance: 0.5, Health: 100,
		})
		// The right model id, the right width, unit norm — and the wrong
		// space, because Rust CLS-pools where Go mean-pools (§II.10 row 16).
		//
		// Every one of those details matters to the test. A vector of the
		// wrong width, or one claiming a different model, would be rejected by
		// a check that has nothing to do with pooling, and the test would pass
		// while proving nothing: only source_impl distinguishes this vector
		// from one this binary produced, which is the whole point.
		values := make([]float32, embedding.Dim)
		for i := range values {
			values[i] = 1 / float32(math.Sqrt(embedding.Dim))
		}
		c.vectors = append(c.vectors, &pb.Vector{
			Tenant: "default", Id: rid.Bytes(),
			ModelId: embedding.Model, Dim: embedding.Dim,
			Values: values,
		})
	}
	c.edges = append(c.edges, &pb.Edge{
		Tenant: "default", From: rustRecordID(0).Bytes(), To: rustRecordID(1).Bytes(),
		RelationshipType: "related_to", Strength: 0.9, CreatedAtUnixMs: 1700000000000,
	})
	for _, o := range opts {
		o(c)
	}

	var buf bytes.Buffer
	w, err := snapshot.NewWriter(&buf, &pb.Header{
		CreatedAtUnixMs: 1700000000000,
		SourceImpl:      "rust", SourceVersion: "0.1.0", RecordFormatVersion: 1,
		Tenants:     []string{"default"},
		RecordCount: uint64(len(c.records)),
		VectorCount: uint64(len(c.vectors)),
		EdgeCount:   uint64(len(c.edges)),
	}, snapshot.WriterOpts{Compression: snapshot.CompressionZstd})
	must(t, err)

	must(t, w.WriteMessages(snapshot.SectionRecords, asMessages(c.records)))
	must(t, w.WriteMessages(snapshot.SectionVectors, asMessages(c.vectors)))
	must(t, w.WriteMessages(snapshot.SectionEdges, asMessages(c.edges)))
	must(t, w.WriteMessages(snapshot.SectionTenants, []proto.Message{
		&pb.Tenant{Id: "default", DisplayName: "default"},
	}))
	must(t, w.Close())
	return buf.Bytes()
}

func asMessages[T proto.Message](in []T) []proto.Message {
	out := make([]proto.Message, 0, len(in))
	for _, m := range in {
		out = append(out, m)
	}
	return out
}

// txnPut writes a record through the destination's own repository.
func txnPut(ctx context.Context, d *destination, rec *record.Record) error {
	return txn.Do(ctx, d.kv, func(tx txn.Tx) error {
		return d.repo.Put(tenant.NewContext(ctx, rec.Tenant), tx, rec)
	})
}
