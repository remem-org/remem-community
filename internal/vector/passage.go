package vector

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
)

// PassageRow is one stored passage of a memory's content: the byte range of the
// content it covers and its vector. Rows exist only for memories longer than
// one passage; a one-passage memory is indexed by its canonical vector.
//
// Every row is derived — from the content, the model and the passage format —
// and is never exported: losing them all is a rebuild, not data loss.
type PassageRow struct {
	Start, End uint32 // UTF-8 byte offsets into the content, [Start, End)
	Vector     *Vector
}

// passageFormat is the row's leading byte. A row of any other format is from a
// binary this one does not understand, and is refused as corruption rather than
// read as something it may not be; the passage_index manifest version is what
// refuses a whole newer directory at open.
const passageFormat = 1

// passageHeader is the format byte and the two u32 offsets.
const passageHeader = 1 + 4 + 4

// EncodePassage frames a passage row: [format][u32 start][u32 end], big-endian,
// then the vector exactly as Encode frames a canonical one, so a single decoder
// reads both and they cannot drift apart.
func EncodePassage(r PassageRow) ([]byte, error) {
	const op = "vector.EncodePassage"
	if r.Start > r.End {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("span [%d, %d) ends before it starts", r.Start, r.End))
	}
	v, err := Encode(r.Vector)
	if err != nil {
		return nil, err
	}
	b := make([]byte, passageHeader, passageHeader+len(v))
	b[0] = passageFormat
	binary.BigEndian.PutUint32(b[1:5], r.Start)
	binary.BigEndian.PutUint32(b[5:9], r.End)
	return append(b, v...), nil
}

// DecodePassage reads a stored passage row. Anything malformed is corruption:
// these bytes came from the store, so a row that does not decode is damage, not
// a bad request.
func DecodePassage(b []byte) (PassageRow, error) {
	const op = "vector.DecodePassage"
	if len(b) < passageHeader {
		return PassageRow{}, errs.E(errs.Corruption, op, fmt.Errorf("a passage row of %d bytes has no room for its span", len(b)))
	}
	if b[0] != passageFormat {
		return PassageRow{}, errs.E(errs.Corruption, op, fmt.Errorf("passage row format %d, this binary reads %d", b[0], passageFormat))
	}
	r := PassageRow{Start: binary.BigEndian.Uint32(b[1:5]), End: binary.BigEndian.Uint32(b[5:9])}
	if r.Start > r.End {
		return PassageRow{}, errs.E(errs.Corruption, op, fmt.Errorf("span [%d, %d) ends before it starts", r.Start, r.End))
	}
	v, err := Decode(b[passageHeader:])
	if err != nil {
		if errs.KindOf(err) == errs.Corruption {
			return PassageRow{}, err
		}
		return PassageRow{}, errs.E(errs.Corruption, op, errors.Join(errors.New("the passage's vector does not decode"), err))
	}
	// A canonical vector without a model id is tolerated on read, because rows
	// from before model ids were enforced exist and a rebuild inherits the
	// stored id (see Store.Replace). A passage row has no such past: this binary
	// wrote every one, always with the model's id, so one without is damage.
	if v.ModelID == "" {
		return PassageRow{}, errs.E(errs.Corruption, op, errors.New("a passage row's vector names no model"))
	}
	r.Vector = v
	return r, nil
}
