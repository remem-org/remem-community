package record

import (
	"bytes"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"google.golang.org/protobuf/proto"
)

// The record body's encoding: how a Record becomes the protobuf body under the
// record envelope, and back. Split from repo.go in Phase 13 to keep that file
// under the project's size limit; nothing here changed.

func encodeBody(rec *Record) ([]byte, error) {
	schemaVersion := rec.SchemaVersion
	if schemaVersion == 0 {
		// A record built by a caller that has no opinion is written at the
		// version in force. Zero is never stored: a record that does not say
		// which schema wrote it cannot be lazily upgraded, because nothing can
		// tell whether it is behind.
		schemaVersion = tenant.DefaultSchemaVersion
	}
	f := rec.Fields.WithDefaults()
	body := &pb.Record{
		Id:              rec.ID.Bytes(),
		SchemaVersion:   schemaVersion,
		Type:            pb.RecordType(rec.Type),
		Content:         rec.Content,
		CreatedAtUnixMs: uint64(rec.CreatedAt.UnixMilli()),
		UpdatedAtUnixMs: uint64(rec.UpdatedAt.UnixMilli()),
		Tags:            f.Tags,
		Archived:        f.Archived,

		Policy:                f.Policy,
		Importance:            f.Importance,
		Health:                f.Health,
		EmotionalValence:      f.Valence,
		Arousal:               f.Arousal,
		AccessedAtUnixMs:      unixMs(f.AccessedAt),
		AccessCount:           f.AccessCount,
		NextAttentionAtUnixMs: unixMs(f.NextAttentionAt),
	}
	if f.Source != "" {
		body.Source = proto.String(f.Source)
	}
	if !f.ArchivedAt.IsZero() {
		body.ArchivedAtUnixMs = proto.Uint64(uint64(f.ArchivedAt.UnixMilli()))
	}
	if !f.LastRecalledAt.IsZero() {
		body.LastRecalledAtUnixMs = proto.Uint64(uint64(f.LastRecalledAt.UnixMilli()))
	}
	// Seconds, matching Rust, so an import is a copy rather than a conversion
	// that can round. A sub-second TTL is rounded up rather than to zero: zero
	// means "never expires", and silently converting "expire in 300ms" into
	// "never expire" is the wrong direction to round in.
	if f.TTL > 0 {
		secs := uint64(f.TTL / time.Second)
		if f.TTL%time.Second != 0 {
			secs++
		}
		body.TtlSecs = proto.Uint64(secs)
	}
	if !f.ProtectedUntil.IsZero() {
		body.ProtectedUntilUnixMs = proto.Uint64(uint64(f.ProtectedUntil.UnixMilli()))
	}
	if !f.LastDecayAt.IsZero() {
		body.LastDecayAtUnixMs = proto.Uint64(uint64(f.LastDecayAt.UnixMilli()))
	}
	if !f.LastHealthCheckAt.IsZero() {
		body.LastHealthCheckAtUnixMs = proto.Uint64(uint64(f.LastHealthCheckAt.UnixMilli()))
	}
	return codec.MarshalRecord(body)
}

// decodeBody turns a stored value into a Record, checking that it is the row
// that was asked for.
//
// The id inside the body is compared with the id in the key. They are two
// copies of the same fact, and the reason the body carries one at all is that
// an export ships bodies without their keys — so when they disagree, something
// wrote a record under the wrong key, and answering with it would return one
// memory when another was requested.
func decodeBody(t tenant.ID, ns tenant.Namespace, rid id.ID, value []byte, op string) (*Record, error) {
	body, err := codec.UnmarshalRecord(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(body.GetId(), rid[:]) {
		got, _ := id.FromBytes(body.GetId())
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the record stored under %s says its id is %s", rid, got))
	}
	return &Record{
		originalBody:  bytes.Clone(value),
		ID:            rid,
		Tenant:        t,
		Namespace:     ns,
		Type:          Type(body.GetType()),
		Content:       body.GetContent(),
		SchemaVersion: body.GetSchemaVersion(),
		Fields: Fields{
			Tags:       body.GetTags(),
			Source:     body.GetSource(),
			Archived:   body.GetArchived(),
			ArchivedAt: unixMilli(body.GetArchivedAtUnixMs()),

			Policy:          body.GetPolicy(),
			Importance:      body.GetImportance(),
			Health:          body.GetHealth(),
			Valence:         body.GetEmotionalValence(),
			Arousal:         body.GetArousal(),
			AccessedAt:      unixMilli(body.GetAccessedAtUnixMs()),
			AccessCount:     body.GetAccessCount(),
			LastRecalledAt:  unixMilli(body.GetLastRecalledAtUnixMs()),
			NextAttentionAt: unixMilli(body.GetNextAttentionAtUnixMs()),

			TTL:               time.Duration(body.GetTtlSecs()) * time.Second,
			ProtectedUntil:    unixMilli(body.GetProtectedUntilUnixMs()),
			LastDecayAt:       unixMilli(body.GetLastDecayAtUnixMs()),
			LastHealthCheckAt: unixMilli(body.GetLastHealthCheckAtUnixMs()),
		}.WithDefaults(),
		Vectors:   map[string]*Vector{},
		CreatedAt: unixMilli(body.GetCreatedAtUnixMs()),
		UpdatedAt: unixMilli(body.GetUpdatedAtUnixMs()),
	}, nil
}

func decodeVector(rid id.ID, value []byte, op string) (*Vector, error) {
	v, err := vector.Decode(value)
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, fmt.Errorf("the vector of record %s: %w", rid, err))
	}
	return v, nil
}

// unixMs is the inverse of unixMilli: a zero time encodes as zero rather than
// as the epoch's negative millisecond count, so "unset" survives the round trip.
func unixMs(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixMilli())
}

func unixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
