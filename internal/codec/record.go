package codec

import (
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

// RecordBody validates a stored value's envelope and returns the encoded body
// inside it, without decoding that body.
//
// It exists for a reader that walks the wire format itself instead of going
// through the generated message — see record.decodeBodyProjected. Such a
// reader still has to be stopped by a value from the wrong key space or from a
// newer binary, and the envelope is the only thing that can stop it; this
// returns the body only once [checkHeader] has passed, so there is one place
// that decides what a record value looks like rather than two.
//
// The returned slice aliases b. It is a view into the caller's buffer, not a
// copy, which is the whole point: the alternative is the copy this avoids.
func RecordBody(b []byte) ([]byte, error) {
	const op = "codec.RecordBody"
	if err := checkHeader(b, op); err != nil {
		return nil, err
	}
	return b[HeaderLen:], nil
}

// MarshalRecord frames a record body for storage.
//
// The header and the body are written into one slice sized for both, so a
// record costs one allocation beyond what protobuf itself needs.
func MarshalRecord(r *pb.Record) ([]byte, error) {
	const op = "codec.MarshalRecord"
	if r == nil {
		return nil, errs.E(errs.Invalid, op, errors.New("record is nil"))
	}

	body, err := proto.Marshal(r)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the record body: %w", err))
	}

	out := make([]byte, 0, HeaderLen+len(body))
	out = header(out)
	return append(out, body...), nil
}

// UnmarshalRecord decodes a stored value.
//
// Unknown protobuf fields are retained on the returned message, which is the
// property that makes a rolling upgrade safe: an older binary that reads a
// record written by a newer one, edits a field it understands and writes it
// back must not destroy the fields it never heard of. That retention is the
// protobuf runtime's default behaviour, and TestUnknownProtoFieldsSurviveA-
// Rewrite exists to stop anyone turning it off — with a DiscardUnknown option,
// or by replacing this with a hand-rolled decoder.
func UnmarshalRecord(b []byte) (*pb.Record, error) {
	const op = "codec.UnmarshalRecord"
	if err := checkHeader(b, op); err != nil {
		return nil, err
	}

	var r pb.Record
	if err := proto.Unmarshal(b[HeaderLen:], &r); err != nil {
		// The header was valid, so this really is damage rather than a value
		// written by something else.
		return nil, errs.E(errs.Corruption, op, errors.New("the record body will not parse"))
	}
	return &r, nil
}
