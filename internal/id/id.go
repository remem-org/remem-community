// Package id generates and parses Remem's record identifiers.
//
// Spec §49 requires ids that are globally unique, generated without
// coordination between nodes, safe under concurrency, and sortable where that
// is useful. It also requires the plan to evaluate compatibility with the ids
// that already exist before choosing a format.
//
// The answer is both formats at once. Existing Rust Remem ids are UUIDv4 and
// must keep parsing and rendering unchanged — an id is a memory's identity,
// and an import that changed it would silently orphan every edge pointing at
// it. New ids are UUIDv7, whose leading 48 bits are the creation time in
// milliseconds, so a key built from an id sorts by creation time and a
// created_at listing is a forward scan rather than a sort.
//
// An ID is therefore any 16-byte UUID. Only [New] chooses the version.
package id

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/remem-org/remem-go/internal/errs"
)

// ID is a 128-bit identifier, stored and compared as its 16 raw bytes.
//
// It is a value type with no pointer inside it: comparable with ==, usable as
// a map key, and copied rather than shared. Its byte order is the UUID wire
// order, so bytes.Compare on two v7 ids orders them by creation time.
type ID [16]byte

// Zero is the nil UUID. It is never generated and never valid as a record id;
// it is what a missing id decodes to.
var Zero ID

// New returns a fresh UUIDv7: 48 bits of Unix milliseconds followed by random
// bits, generated locally with no coordination.
//
// It panics only if the system entropy source fails, which is not a condition
// a caller can handle — every id in the process would be unsafe.
func New() ID {
	return ID(uuid.Must(uuid.NewV7()))
}

// Parse reads the canonical 8-4-4-4-12 hexadecimal form, in either case.
//
// It accepts any UUID version, because it is the entry point for ids Rust
// Remem generated. A malformed string is [errs.Invalid]: it is caller input.
func Parse(s string) (ID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return Zero, errs.E(errs.Invalid, "id.Parse", err)
	}
	return ID(u), nil
}

// FromBytes copies a 16-byte identifier out of b.
//
// It copies deliberately: b is usually a slice into a decoded key or an
// iterator's buffer, which the storage layer reuses after the next step.
func FromBytes(b []byte) (ID, error) {
	if len(b) != 16 {
		return Zero, errs.E(errs.Invalid, "id.FromBytes",
			fmt.Errorf("an id is 16 bytes, got %d", len(b)))
	}
	var out ID
	copy(out[:], b)
	return out, nil
}

// Bytes returns the id's 16 raw bytes. The returned slice aliases a copy of
// the id, so writing to it cannot corrupt the original value.
func (i ID) Bytes() []byte {
	b := make([]byte, 16)
	copy(b, i[:])
	return b
}

// String renders the canonical lowercase 8-4-4-4-12 form.
func (i ID) String() string { return uuid.UUID(i).String() }

// IsZero reports whether the id is the nil UUID.
func (i ID) IsZero() bool { return i == Zero }

// Version reports the UUID version nibble: 7 for ids this binary generates, 4
// for those imported from Rust Remem.
func (i ID) Version() int { return int(i[6] >> 4) }

// Time returns the creation time encoded in a v7 id.
//
// For any other version it returns the zero Time: a v4 id carries no
// timestamp, and inventing a plausible-looking one would be worse than
// reporting none. Callers that need a creation time for every record read it
// from the record, not from the id.
func (i ID) Time() time.Time {
	if i.Version() != 7 {
		return time.Time{}
	}
	var ms [8]byte
	copy(ms[2:], i[:6]) // 48-bit big-endian milliseconds since the Unix epoch
	return time.UnixMilli(int64(binary.BigEndian.Uint64(ms[:]))).UTC()
}

// MarshalText renders the canonical string form, so an ID appears in JSON and
// in log output as a UUID rather than as an array of numbers.
func (i ID) MarshalText() ([]byte, error) { return []byte(i.String()), nil }

// UnmarshalText parses the canonical string form.
func (i *ID) UnmarshalText(b []byte) error {
	parsed, err := Parse(string(b))
	if err != nil {
		return err
	}
	*i = parsed
	return nil
}
