package codec

import (
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

// CodecVectorProtobuf frames a record.v1.Vector.
//
// A vector value gets the same envelope as a record body rather than a raw
// float array, and the codec byte distinguishes them, so a value read from the
// wrong key space is caught by its header instead of decoded as 96 plausible
// floats.
const CodecVectorProtobuf uint8 = 2

// MarshalVector frames a canonical vector for storage.
func MarshalVector(v *pb.Vector) ([]byte, error) {
	const op = "codec.MarshalVector"
	if v == nil {
		return nil, errs.E(errs.Invalid, op, errors.New("vector is nil"))
	}
	if v.GetModelId() == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a vector without a model id may not be stored: a later model change would corrupt the space silently"))
	}
	if int(v.GetDim()) != len(v.GetValues()) {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"vector declares %d dimensions and carries %d values", v.GetDim(), len(v.GetValues())))
	}

	body, err := proto.Marshal(v)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the vector: %w", err))
	}
	out := make([]byte, 0, HeaderLen+len(body))
	out = headerFor(out, CodecVectorProtobuf)
	return append(out, body...), nil
}

// UnmarshalVector decodes a stored vector value.
func UnmarshalVector(b []byte) (*pb.Vector, error) {
	const op = "codec.UnmarshalVector"
	if err := checkHeaderCodec(b, op, CodecVectorProtobuf); err != nil {
		return nil, err
	}

	var v pb.Vector
	if err := proto.Unmarshal(b[HeaderLen:], &v); err != nil {
		return nil, errs.E(errs.Corruption, op, errors.New("the vector body will not parse"))
	}
	if int(v.GetDim()) != len(v.GetValues()) {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"a stored vector declares %d dimensions and carries %d values; it is truncated", v.GetDim(), len(v.GetValues())))
	}
	return &v, nil
}
