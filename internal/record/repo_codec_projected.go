package record

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"google.golang.org/protobuf/encoding/protowire"
)

// The result page's decoder: the record body read down to the fields a
// Search, Related or List response is built from, and no further.
//
// # Why there are two decoders
//
// [decodeBody] is the full-fidelity one, and every path that may write the
// record back goes through it: Get, GetFrom, Scan and the lazy schema upgrade
// all need the schema version, the retained unknown fields and the original
// bytes that make Put's byte-identity check possible. Nothing here produces
// any of those, by design.
//
// A result page needs none of them. It reads the record, projects it into a
// memory.Memory and drops it, which made the full decode 20 percent of a
// first-page search for work nobody looked at: an intermediate pb.Record, one
// heap allocation per present optional scalar, an empty Vectors map, and a
// second copy of the whole body for an optimistic-concurrency token that no
// result page ever presents.
//
// # The cost of having two
//
// They can drift, and four tests exist to stop them:
// TestProjectedDecodeAgreesWithFullDecodeOnResultFields pins the two field by
// field over the shape a production body reaches;
// TestProjectedDecodeAgreesWithFullDecodeForAnyRecord does it over a drawn
// corpus; TestProjectedDecodeLeavesTheWriteOnlyFieldsUnset pins what the
// projection deliberately omits; and memory's
// TestResultsAreIdenticalWhicheverDecodeBuiltThem pins the memories the two
// build, which is what a caller actually sees. A field added to the proto that
// a response must show has to be added here as well, and those tests are what
// say so.
//
// What they do not and cannot catch is a field added to the proto that nothing
// reads yet: it will decode into neither, agree, and go missing from responses
// on the day something starts reading it. Adding a field to record.v1.Record
// means deciding whether a response shows it.

// decodeBodyProjected decodes a stored body into the subset of a [Record] that
// a result page reads, skipping every field it does not.
//
// What it leaves unset is the schema version, the record type, the sweep
// schedule (next_attention_at), the two decay clocks, the retained unknown
// fields and the original body bytes.
//
// So the returned record must never be written back. With no schema version
// and no original body, [Repo.Put] would stamp it with the version in force
// and fall back to a read-modify-write condition — losing the record's own
// version and every field this binary does not understand. Read paths that may
// write go through [decodeBody].
//
// That is enforced, not asked for: the record is marked projected, and Put
// refuses a marked record with [errs.Invalid] naming GetBodyFrom. The marker
// survives [Record.Clone], which is what Service.peek does to every result.
//
// The id inside the body is checked against the id in the key for the reason
// [decodeBody] gives: they are two copies of the same fact, and a disagreement
// means something wrote a record under the wrong key.
func decodeBodyProjected(t tenant.ID, ns tenant.Namespace, rid id.ID, value []byte, op string) (*Record, error) {
	b, err := codec.RecordBody(value)
	if err != nil {
		return nil, err
	}

	rec := &Record{ID: rid, Tenant: t, Namespace: ns, projected: true}
	f := &rec.Fields
	var bodyID []byte
	var ttlSecs uint64

	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, corruptBody(op)
		}
		b = b[n:]

		// A field whose wire type is not the one the schema declares is
		// treated as unknown and skipped, which is what the protobuf runtime
		// does with it too. Guessing at it would make this decoder accept
		// bodies the full one rejects.
		switch {
		case num == fieldID && typ == protowire.BytesType:
			bodyID, n = protowire.ConsumeBytes(b)
		case num == fieldContent && typ == protowire.BytesType:
			var v []byte
			v, n = protowire.ConsumeBytes(b)
			if n >= 0 {
				rec.Content = string(v)
			}
		case num == fieldCreatedAt && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			rec.CreatedAt = unixMilli(v)
		case num == fieldUpdatedAt && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			rec.UpdatedAt = unixMilli(v)
		case num == fieldTags && typ == protowire.BytesType:
			var v []byte
			v, n = protowire.ConsumeBytes(b)
			if n >= 0 {
				f.Tags = append(f.Tags, string(v))
			}
		case num == fieldSource && typ == protowire.BytesType:
			var v []byte
			v, n = protowire.ConsumeBytes(b)
			if n >= 0 {
				f.Source = string(v)
			}
		case num == fieldArchived && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.Archived = v != 0
		case num == fieldArchivedAt && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.ArchivedAt = unixMilli(v)
		case num == fieldPolicy && typ == protowire.BytesType:
			var v []byte
			v, n = protowire.ConsumeBytes(b)
			if n >= 0 {
				f.Policy = string(v)
			}
		case num == fieldImportance && typ == protowire.Fixed32Type:
			f.Importance, n = consumeFloat(b)
		case num == fieldHealth && typ == protowire.Fixed32Type:
			f.Health, n = consumeFloat(b)
		case num == fieldValence && typ == protowire.Fixed32Type:
			f.Valence, n = consumeFloat(b)
		case num == fieldArousal && typ == protowire.Fixed32Type:
			f.Arousal, n = consumeFloat(b)
		case num == fieldAccessedAt && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.AccessedAt = unixMilli(v)
		case num == fieldAccessCount && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.AccessCount = uint32(v)
		case num == fieldLastRecalledAt && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.LastRecalledAt = unixMilli(v)
		case num == fieldTTLSecs && typ == protowire.VarintType:
			ttlSecs, n = protowire.ConsumeVarint(b)
		case num == fieldProtectedUntil && typ == protowire.VarintType:
			var v uint64
			v, n = protowire.ConsumeVarint(b)
			f.ProtectedUntil = unixMilli(v)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return nil, corruptBody(op)
		}
		b = b[n:]
	}

	if !bytes.Equal(bodyID, rid[:]) {
		got, _ := id.FromBytes(bodyID)
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the record stored under %s says its id is %s", rid, got))
	}
	f.TTL = time.Duration(ttlSecs) * time.Second
	rec.Fields = f.WithDefaults()
	return rec, nil
}

// Field numbers of record.v1.Record. They are a durable format, so they are
// named here rather than written as literals in the walk above: a reader of
// that switch should be able to see which proto field each arm is.
const (
	fieldID             protowire.Number = 1
	fieldContent        protowire.Number = 4
	fieldCreatedAt      protowire.Number = 5
	fieldUpdatedAt      protowire.Number = 6
	fieldTags           protowire.Number = 7
	fieldSource         protowire.Number = 8
	fieldArchived       protowire.Number = 20
	fieldArchivedAt     protowire.Number = 21
	fieldPolicy         protowire.Number = 22
	fieldImportance     protowire.Number = 23
	fieldHealth         protowire.Number = 24
	fieldValence        protowire.Number = 25
	fieldArousal        protowire.Number = 26
	fieldAccessedAt     protowire.Number = 27
	fieldAccessCount    protowire.Number = 28
	fieldLastRecalledAt protowire.Number = 29
	fieldTTLSecs        protowire.Number = 31
	fieldProtectedUntil protowire.Number = 32
)

func consumeFloat(b []byte) (float32, int) {
	v, n := protowire.ConsumeFixed32(b)
	if n < 0 {
		return 0, n
	}
	return math.Float32frombits(v), n
}

// corruptBody reports a body that will not parse, wording it exactly as
// codec.UnmarshalRecord does: the envelope was valid, so this is damage rather
// than a value written by something else.
func corruptBody(op string) error {
	return errs.E(errs.Corruption, op, errors.New("the record body will not parse"))
}
