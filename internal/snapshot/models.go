package snapshot

import (
	"errors"
	"io"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"google.golang.org/protobuf/proto"
)

// VectorModel is one model and width a snapshot's vectors claim.
type VectorModel struct {
	ModelID string
	Dim     int
}

// VectorModels reads a whole snapshot and counts its vectors by the model and
// width each claims, writing nothing.
//
// The header does not say which model wrote a snapshot's vectors — every
// vector says so for itself — so a destination that can use only one model's
// vectors verbatim has to read the file to know whether an import can succeed.
// It is a separate pass rather than a question Import answers, because Import
// answers it only after the destination exists, and a refused import must leave
// no trace of having tried. The cost is one extra read of the file, paid only by
// a destination that cannot re-embed.
func VectorModels(r io.Reader) (map[VectorModel]uint64, error) {
	const op = "snapshot.VectorModels"
	rd, err := NewReader(r)
	if err != nil {
		return nil, err
	}
	out := make(map[VectorModel]uint64)
	for {
		blk, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if blk.Section != SectionVectors {
			continue
		}
		if err := blk.Each(func(raw []byte) error {
			var m pb.Vector
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			out[VectorModel{ModelID: m.GetModelId(), Dim: int(m.GetDim())}]++
			return nil
		}); err != nil {
			return nil, err
		}
	}
}
