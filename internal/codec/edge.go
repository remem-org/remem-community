package codec

import (
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

// CodecEdgeProtobuf frames a record.v1.Edge.
//
// Edges get the same envelope as record bodies and vectors, and their own codec
// byte, so a value read out of the wrong key space is caught by its header
// rather than decoded into a plausible relationship.
const CodecEdgeProtobuf uint8 = 3

// deterministic is the marshaller every edge body goes through.
//
// Edge.meta is a protobuf map, and the Go runtime randomises map field order on
// marshal by default, so without this option encoding one edge twice produces
// two different byte strings that decode to the same thing.
//
// What that buys is that an edge's stored bytes are a function of the edge and
// of nothing else. Two nodes handed the same edge write the same row, which is
// what Invariants 8 and 9 require of anything inside a replicated apply path
// (Phase 14), and importing the same snapshot twice produces the same database
// rather than one that merely holds the same edges (Phase 12).
//
// It is deliberately *not* what makes graph.RebuildIn byte-identical: that
// rebuild copies stored values rather than re-encoding them, precisely so a
// field written by a newer binary survives being relayed by an older one.
var deterministic = proto.MarshalOptions{Deterministic: true}

// MarshalEdge frames an edge body for storage.
func MarshalEdge(e *pb.Edge) ([]byte, error) {
	const op = "codec.MarshalEdge"
	if e == nil {
		return nil, errs.E(errs.Invalid, op, errors.New("edge is nil"))
	}

	body, err := deterministic.Marshal(e)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the edge: %w", err))
	}
	out := make([]byte, 0, HeaderLen+len(body))
	out = headerFor(out, CodecEdgeProtobuf)
	return append(out, body...), nil
}

// UnmarshalEdge decodes a stored edge value.
func UnmarshalEdge(b []byte) (*pb.Edge, error) {
	const op = "codec.UnmarshalEdge"
	if err := checkHeaderCodec(b, op, CodecEdgeProtobuf); err != nil {
		return nil, err
	}

	var e pb.Edge
	if err := proto.Unmarshal(b[HeaderLen:], &e); err != nil {
		return nil, errs.E(errs.Corruption, op, errors.New("the edge body will not parse"))
	}
	return &e, nil
}
