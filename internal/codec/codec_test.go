package codec_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/version"
	"google.golang.org/protobuf/proto"
)

func sample() *pb.Record {
	rid := id.New()
	return &pb.Record{
		Id:              rid.Bytes(),
		SchemaVersion:   1,
		Type:            pb.RecordType_RECORD_TYPE_MEMORY,
		Content:         "the coffee machine needs descaling",
		CreatedAtUnixMs: 1_700_000_000_000,
		UpdatedAtUnixMs: 1_700_000_000_001,
		Tags:            []string{"office", "maintenance"},
		Source:          proto.String("mcp"),
	}
}

func TestRecordRoundTrips(t *testing.T) {
	want := sample()
	got, err := codec.UnmarshalRecord(marshalMust(t, want))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip changed the record:\n want %v\n  got %v", want, got)
	}
}

func TestEnvelopeRejectsUnknownMagic(t *testing.T) {
	_, err := codec.UnmarshalRecord([]byte("XXXX\x01\x01payload"))
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("a bad magic is corruption, not a parse miss: %v", err)
	}
}

func TestEnvelopeRejectsNewerVersion(t *testing.T) {
	b := marshalMust(t, sample())
	b[4] = 99
	if _, err := codec.UnmarshalRecord(b); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
}

func TestEnvelopeRejectsAnUnknownCodec(t *testing.T) {
	// A codec byte this binary does not know means a newer writer chose a body
	// encoding that did not exist here. That is a version problem, not damage.
	b := marshalMust(t, sample())
	b[5] = 99
	if _, err := codec.UnmarshalRecord(b); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
}

func TestEnvelopeRejectsATruncatedHeader(t *testing.T) {
	full := marshalMust(t, sample())
	for n := 0; n < codec.HeaderLen; n++ {
		if _, err := codec.UnmarshalRecord(full[:n]); !errs.Is(err, errs.Corruption) {
			t.Fatalf("a value truncated to %d bytes must be Corruption, got %v", n, err)
		}
	}
}

func TestEnvelopeRejectsACorruptBody(t *testing.T) {
	b := marshalMust(t, sample())
	// Damage inside the body, leaving the header intact.
	b[len(b)-1] ^= 0xFF
	b[len(b)-2] ^= 0xFF
	if _, err := codec.UnmarshalRecord(b); err != nil && !errs.Is(err, errs.Corruption) {
		t.Fatalf("a body that will not parse is Corruption, got %v", err)
	}
}

func TestEnvelopeVersionReadsTheHeaderWithoutDecoding(t *testing.T) {
	// A migration walks every record to decide what to do with it. Reading six
	// bytes must be enough; parsing a body it may be about to rewrite is not.
	env, cdc, err := codec.EnvelopeVersion(marshalMust(t, sample()))
	if err != nil {
		t.Fatal(err)
	}
	if uint32(env) != version.RecordEnvelope {
		t.Fatalf("envelope version = %d, want %d", env, version.RecordEnvelope)
	}
	if cdc != codec.CodecProtobuf {
		t.Fatalf("codec = %d, want %d", cdc, codec.CodecProtobuf)
	}
}

func TestUnknownProtoFieldsSurviveARewrite(t *testing.T) {
	// A future binary adds field 99. This binary must decode, re-encode, and
	// still carry field 99 — otherwise a rolling upgrade destroys data written
	// by the newer node (spec §21).
	withFuture := appendUnknownField(marshalMust(t, sample()), 99, []byte("future"))

	r, err := codec.UnmarshalRecord(withFuture)
	if err != nil {
		t.Fatal(err)
	}
	again, err := codec.MarshalRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(again, []byte("future")) {
		t.Fatal("unknown field dropped on rewrite")
	}
}

func TestUnknownFieldsSurviveAModifiedRewrite(t *testing.T) {
	// The realistic version: the older binary does not merely pass the record
	// through, it edits a field it does understand and writes it back.
	withFuture := appendUnknownField(marshalMust(t, sample()), 99, []byte("future"))
	r, err := codec.UnmarshalRecord(withFuture)
	if err != nil {
		t.Fatal(err)
	}

	r.Content = "edited by a binary that never heard of field 99"
	again, err := codec.MarshalRecord(r)
	if err != nil {
		t.Fatal(err)
	}

	reread, err := codec.UnmarshalRecord(again)
	if err != nil {
		t.Fatal(err)
	}
	if reread.Content != r.Content {
		t.Fatalf("the edit was lost: %q", reread.Content)
	}
	if !bytes.Contains(again, []byte("future")) {
		t.Fatal("the newer node's field was destroyed by an edit from an older node")
	}
}

func TestMarshalStampsTheCurrentVersions(t *testing.T) {
	b := marshalMust(t, sample())
	if !bytes.HasPrefix(b, []byte(codec.Magic)) {
		t.Fatalf("value does not begin with the magic: %x", b[:4])
	}
	if b[4] != byte(version.RecordEnvelope) {
		t.Fatalf("envelope version byte = %d, want %d", b[4], version.RecordEnvelope)
	}
	if b[5] != codec.CodecProtobuf {
		t.Fatalf("codec byte = %d, want %d", b[5], codec.CodecProtobuf)
	}
}

func TestMarshalRefusesNil(t *testing.T) {
	if _, err := codec.MarshalRecord(nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("want Invalid for a nil record, got %v", err)
	}
}

func marshalMust(t *testing.T, r *pb.Record) []byte {
	t.Helper()
	b, err := codec.MarshalRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// appendUnknownField appends a length-delimited protobuf field to the body,
// which is what a newer binary writing a field this one has never seen
// produces on the wire.
func appendUnknownField(value []byte, field int, payload []byte) []byte {
	out := append([]byte(nil), value...)
	out = binary.AppendUvarint(out, uint64(field)<<3|2) // wire type 2
	out = binary.AppendUvarint(out, uint64(len(payload)))
	return append(out, payload...)
}

// FuzzUnmarshalRecord holds the parser to one rule: whatever bytes it is given,
// it returns a record or a classified error, and never panics.
//
// A record value is the one place where bytes from disk are parsed on the read
// path of every request, so a panic here is a crash loop triggered by one
// damaged row (spec §42).
func FuzzUnmarshalRecord(f *testing.F) {
	f.Add(marshalMust(&testing.T{}, sample()))
	f.Add([]byte("RMM1\x01\x01"))
	f.Add([]byte("RMM1"))
	f.Add([]byte("XXXX\x01\x01payload"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := codec.UnmarshalRecord(b)
		if err != nil {
			if errs.KindOf(err) == errs.Unclassified {
				t.Fatalf("an error escaped without a kind: %v", err)
			}
			return
		}
		if r == nil {
			t.Fatal("nil record with a nil error")
		}
		// Anything that decoded must re-encode; a value the store accepted and
		// cannot write back is a record that can be read once and never saved.
		if _, err := codec.MarshalRecord(r); err != nil {
			t.Fatalf("a decoded record failed to re-encode: %v", err)
		}
	})
}

func TestVectorRoundTripsWithItsModelIdentity(t *testing.T) {
	v := &pb.Vector{ModelId: "all-MiniLM-L6-v2", Dim: 3, Values: []float32{0.5, -0.5, 0.7071}}
	b, err := codec.MarshalVector(v)
	if err != nil {
		t.Fatalf("MarshalVector: %v", err)
	}
	got, err := codec.UnmarshalVector(b)
	if err != nil {
		t.Fatalf("UnmarshalVector: %v", err)
	}
	if got.GetModelId() != v.GetModelId() || got.GetDim() != v.GetDim() {
		t.Fatalf("identity lost: %+v", got)
	}
	for i := range v.Values {
		if got.Values[i] != v.Values[i] {
			t.Fatalf("value %d: %v, want %v", i, got.Values[i], v.Values[i])
		}
	}
}

// A vector with no model id may not be stored at all. Once one is on disk,
// nothing can say later which model produced it.
func TestAVectorWithoutAModelIsRefused(t *testing.T) {
	_, err := codec.MarshalVector(&pb.Vector{Dim: 1, Values: []float32{1}})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestATruncatedVectorIsCorruption(t *testing.T) {
	b, err := codec.MarshalVector(&pb.Vector{ModelId: "m", Dim: 3, Values: []float32{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the body with fewer values than dim declares.
	var v pb.Vector
	if err := proto.Unmarshal(b[codec.HeaderLen:], &v); err != nil {
		t.Fatal(err)
	}
	v.Values = v.Values[:2]
	body, err := proto.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	damaged := append(append([]byte{}, b[:codec.HeaderLen]...), body...)
	if _, err := codec.UnmarshalVector(damaged); !errs.Is(err, errs.Corruption) {
		t.Fatalf("got %v, want Corruption", err)
	}
}

// The codec byte is what stops a value read from the wrong key space being
// decoded as a plausible one of the other kind.
func TestARecordValueIsNotAcceptedAsAVector(t *testing.T) {
	b, err := codec.MarshalRecord(&pb.Record{Id: id.New().Bytes(), Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.UnmarshalVector(b); !errs.Is(err, errs.Corruption) {
		t.Fatalf("a record value decoded as a vector: %v", err)
	}
	vb, err := codec.MarshalVector(&pb.Vector{ModelId: "m", Dim: 1, Values: []float32{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.UnmarshalRecord(vb); !errs.Is(err, errs.Corruption) {
		t.Fatalf("a vector value decoded as a record: %v", err)
	}
}

// --- edges -------------------------------------------------------------------

func TestEdgeRoundTrips(t *testing.T) {
	e := &pb.Edge{
		Strength: 0.75, CreatedAtUnixMs: 1_725_000_000_000, UpdatedAtUnixMs: 1_725_000_060_000,
		Meta: map[string]string{"by": "discovery"},
	}
	b, err := codec.MarshalEdge(e)
	if err != nil {
		t.Fatal(err)
	}
	back, err := codec.UnmarshalEdge(b)
	if err != nil {
		t.Fatal(err)
	}
	if back.GetStrength() != e.GetStrength() || back.GetCreatedAtUnixMs() != e.GetCreatedAtUnixMs() ||
		back.GetMeta()["by"] != "discovery" {
		t.Fatalf("round-tripped to %v, want %v", back, e)
	}
}

// An edge's stored bytes must be a function of the edge and of nothing else, so
// two nodes handed the same edge write the same row and a re-import produces the
// same database. A randomised map field order breaks that invisibly: both
// encodings decode to the same edge.
func TestEdgeMarshallingIsDeterministic(t *testing.T) {
	e := &pb.Edge{Strength: 0.5, Meta: map[string]string{
		"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6", "g": "7", "h": "8",
	}}
	first, err := codec.MarshalEdge(e)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		again, err := codec.MarshalEdge(e)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("attempt %d differs from the first: the edge marshaller is not deterministic", i)
		}
	}
}

func TestAnEdgeValueIsNotAcceptedAsARecordOrAVector(t *testing.T) {
	b, err := codec.MarshalEdge(&pb.Edge{Strength: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.UnmarshalRecord(b); !errs.Is(err, errs.Corruption) {
		t.Errorf("an edge value decoded as a record: %v", err)
	}
	if _, err := codec.UnmarshalVector(b); !errs.Is(err, errs.Corruption) {
		t.Errorf("an edge value decoded as a vector: %v", err)
	}
	rb, err := codec.MarshalRecord(&pb.Record{Id: id.New().Bytes(), Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.UnmarshalEdge(rb); !errs.Is(err, errs.Corruption) {
		t.Errorf("a record value decoded as an edge: %v", err)
	}
}

func TestMarshalEdgeRefusesNil(t *testing.T) {
	if _, err := codec.MarshalEdge(nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}
