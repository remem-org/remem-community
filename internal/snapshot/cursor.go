package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"google.golang.org/protobuf/proto"
)

// The resume cursor: where an import stopped, and what it had done by then.
//
// # Why it is a value the caller holds rather than a row in the store
//
// Invariant 1 gives every key a tenant, and an import cursor spans them — a
// snapshot is one file covering every tenant it carries. Writing it into the
// key space would mean either an untenanted key or an arbitrary tenant owning
// everyone's progress. So the cursor is returned, and `remem-admin import`
// keeps it in a file beside the snapshot.
//
// # It is an optimisation, not a correctness mechanism
//
// Every row a snapshot carries is addressed by its own id, so re-applying a
// block writes the same bytes. An import with no cursor at all is correct; the
// cursor only saves the work.
const (
	cursorMagic   = "RMSC"
	cursorVersion = 1
)

// cursorState is a decoded resume point.
type cursorState struct {
	Blocks uint32
	Offset int64
	Stats  Stats
}

// fingerprint identifies the snapshot a cursor belongs to.
//
// It is taken over the header, which carries the creation time, the source
// version and the counts — enough that two different exports of the same corpus
// have different fingerprints. A cursor offered against the wrong file is
// refused rather than resumed at a byte offset that means something else there.
func fingerprint(h *pb.Header) [8]byte {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(h)
	if err != nil {
		// A header that will not re-encode cannot be fingerprinted, and a
		// zero fingerprint is a value a cursor can carry and compare, so this
		// degrades to "resume is refused" rather than to a panic.
		return [8]byte{}
	}
	sum := sha256.Sum256(raw)
	var out [8]byte
	copy(out[:], sum[:])
	return out
}

// makeCursorWithStats encodes a resume point and the work done to reach it.
func makeCursorWithStats(blocks uint32, offset int64, h *pb.Header, s Stats) []byte {
	fp := fingerprint(h)
	out := make([]byte, 0, 4+1+8+4+8+8*6)
	out = append(out, cursorMagic...)
	out = append(out, cursorVersion)
	out = append(out, fp[:]...)
	out = binary.LittleEndian.AppendUint32(out, blocks)
	out = binary.LittleEndian.AppendUint64(out, uint64(offset))
	for _, v := range []uint64{
		s.Tenants, s.Records, s.Vectors, s.MissingVectors, s.Edges, s.Events,
	} {
		out = binary.LittleEndian.AppendUint64(out, v)
	}
	return out
}

// parseCursor decodes a resume point and checks it belongs to this snapshot.
func parseCursor(b []byte, h *pb.Header) (cursorState, error) {
	const op = "snapshot.Import"

	if len(b) == 0 {
		return cursorState{}, nil
	}
	want := 4 + 1 + 8 + 4 + 8 + 8*6
	if len(b) != want {
		return cursorState{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"the resume cursor is %d bytes and this binary writes %d; it is not a cursor, or it "+
				"came from a different version", len(b), want))
	}
	if string(b[:4]) != cursorMagic || b[4] != cursorVersion {
		return cursorState{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"the resume cursor does not open like one (%q, version %d)", b[:4], b[4]))
	}
	fp := fingerprint(h)
	if string(b[5:13]) != string(fp[:]) {
		return cursorState{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"this resume cursor belongs to a different snapshot. Resuming at its byte offset "+
				"would land in the middle of unrelated data, so it is refused — delete the "+
				"cursor file and import from the beginning, which is safe because every row is "+
				"addressed by its own id"))
	}
	var st cursorState
	st.Blocks = binary.LittleEndian.Uint32(b[13:])
	st.Offset = int64(binary.LittleEndian.Uint64(b[17:]))
	at := 25
	for _, into := range []*uint64{
		&st.Stats.Tenants, &st.Stats.Records, &st.Stats.Vectors,
		&st.Stats.MissingVectors, &st.Stats.Edges, &st.Stats.Events,
	} {
		*into = binary.LittleEndian.Uint64(b[at:])
		at += 8
	}
	return st, nil
}
