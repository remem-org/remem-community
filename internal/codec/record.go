package codec

import (
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

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
