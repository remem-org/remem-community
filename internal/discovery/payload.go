package discovery

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
)

// payloadVersion is the first byte of every discovery payload.
//
// It is a durable format: a row written today is read by a binary built next
// year, and an unknown version is refused by number rather than guessed at. One
// byte is the cheapest way to keep that option, and Invariant 4 requires it of
// every durable format in the system.
const payloadVersion = 1

// MaxSubjects bounds one job's subject list.
//
// It is well under what [jobs.MaxPayloadBytes] would allow, and the bound that
// matters is not the row's size: it is how much work one claim represents. A
// worker holding a lease over four thousand vector searches is a worker whose
// lease lapses halfway.
const MaxSubjects = 1000

// EncodePayload writes a discovery job's subjects.
//
//	byte 0        format version
//	uvarint       subject count
//	16 bytes × n  subject ids, in write order
//
// The order is the order the memories were written, and the handler consumes it
// front to back, so a checkpoint is simply this encoding of what is left. There
// is one decoder rather than two.
func EncodePayload(subjects []id.ID) ([]byte, error) {
	const op = "discovery.EncodePayload"

	switch {
	case len(subjects) == 0:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a discovery job with no subjects has nothing to do; the enqueue is what should have been skipped"))
	case len(subjects) > MaxSubjects:
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"a discovery job carries %d subjects, the limit is %d", len(subjects), MaxSubjects))
	}

	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(subjects)*len(id.ID{}))
	out = append(out, payloadVersion)
	out = binary.AppendUvarint(out, uint64(len(subjects)))
	for _, s := range subjects {
		out = append(out, s[:]...)
	}
	if len(out) > jobs.MaxPayloadBytes {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"a discovery job's payload is %d bytes, the limit is %d", len(out), jobs.MaxPayloadBytes))
	}
	return out, nil
}

// DecodePayload reads a discovery job's subjects.
//
// Every failure is [errs.Corruption] rather than Invalid: this parses a durable
// row, and a row that will not parse is damage, not a caller's mistake. Trailing
// bytes are refused for the same reason — they mean the writer and the reader
// disagree about the format, and silently ignoring the tail is how a newer
// field becomes invisible instead of loud.
func DecodePayload(b []byte) ([]id.ID, error) {
	const op = "discovery.DecodePayload"
	bad := func(format string, args ...any) error {
		return errs.E(errs.Corruption, op, fmt.Errorf(format, args...))
	}

	if len(b) == 0 {
		return nil, bad("a discovery job has an empty payload, so there is nothing to say what it was for")
	}
	if b[0] != payloadVersion {
		return nil, bad("a discovery job's payload is format version %d; this binary reads version %d",
			b[0], payloadVersion)
	}
	n, w := binary.Uvarint(b[1:])
	if w <= 0 {
		return nil, bad("a discovery job's payload has no readable subject count")
	}
	if n == 0 || n > MaxSubjects {
		return nil, bad("a discovery job's payload claims %d subjects; the range is 1 to %d", n, MaxSubjects)
	}

	rest := b[1+w:]
	width := len(id.ID{})
	if uint64(len(rest)) != n*uint64(width) {
		return nil, bad("a discovery job's payload claims %d subjects but carries %d bytes of them, not %d",
			n, len(rest), n*uint64(width))
	}

	out := make([]id.ID, 0, n)
	for off := 0; off < len(rest); off += width {
		rid, err := id.FromBytes(rest[off : off+width])
		if err != nil {
			return nil, bad("a discovery job's payload holds a malformed subject id at offset %d", off)
		}
		out = append(out, rid)
	}
	return out, nil
}
