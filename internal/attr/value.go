// Package attr is Remem's indexed field layer: the small typed sidecar row
// that lets a filter be settled and a listing be ordered without reading a
// single record body.
//
// It is the mechanism plan §II.1 calls "the most valuable idea in the Rust
// codebase". A record body is protobuf and may be kilobytes; deciding "is this
// archived, and is its importance above 0.7" by decoding one is what turns a
// listing of ten into a scan of a hundred thousand. The row is tens of bytes,
// fixed-width per slot, and answers both questions.
//
// # Three things, in dependency order
//
//	slots.go      which fields are addressable, by number and by name
//	row.go        a record projected onto those slots, packed
//	index.go      the ordered index over one slot, maintained inside a write
//	select.go     the walk: an access path narrows, the row decides
//
// # Everything here is derived
//
// Rows and slot indexes are both derived (plan §II.4): rebuildable from record
// bodies alone, and never read by a recovery path. That is what makes a stale
// index entry harmless — every candidate an index produces is verified against
// its row before it is returned — and it is also the limit of that safety. A
// *missing* entry is not detectable here: a record whose entry was never
// written simply does not surface. Building the entries is therefore a
// migration that must finish before the server serves, not one that runs
// alongside it (see internal/schema's StrategyRebuild).
package attr

import (
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
)

// Value is one attribute value: a typed scalar addressed by a slot.
//
// It holds no slice, so it is comparable with ==, usable as a map key and
// copied rather than shared. Every numeric type lives in the same uint64 field
// — a float32 as its IEEE-754 bits — because a struct with one field per type
// would be twice the size and would still need the tag to be read correctly.
type Value struct {
	typ schema.SlotType
	num uint64
	str string
}

// Constructors, one per slot type. They are short names because a slot table
// is a long list of them and `attr.F32(0.5)` reads where `attr.Float32Value`
// does not.

// Bool returns a boolean value.
func Bool(v bool) Value {
	var n uint64
	if v {
		n = 1
	}
	return Value{typ: schema.SlotBool, num: n}
}

// U8 returns an 8-bit unsigned value.
func U8(v uint8) Value { return Value{typ: schema.SlotU8, num: uint64(v)} }

// U32 returns a 32-bit unsigned value.
func U32(v uint32) Value { return Value{typ: schema.SlotU32, num: uint64(v)} }

// U64 returns a 64-bit unsigned value. Every timestamp slot is one of these,
// in Unix milliseconds.
func U64(v uint64) Value { return Value{typ: schema.SlotU64, num: v} }

// F32 returns a 32-bit float value, normalised.
//
// Two values are folded here rather than at the call site, and both arrive from
// arithmetic rather than from a caller — so there is no input validation
// upstream that catches either.
//
// NaN becomes zero. It order-encodes above every finite value, so one of them
// poisons every range query over the slot. This is the backstop Rust's `finite`
// provides.
//
// Negative zero becomes positive zero. The order-preserving encoding puts -0.0
// below +0.0, faithfully — they are different bit patterns — but they are the
// same number, so a slot holding both has two index positions for one value.
// A record stored at -0.0 would then be invisible to a filter for 0, and would
// sort below every record at 0 for no reason a user could see. A property test
// found this; it is not the kind of thing a hand-written case reaches, because
// -0.0 is produced by multiplication rather than typed.
func F32(v float32) Value {
	// `v == 0` covers -0.0 without a signbit check: -0.0 == 0 is true, and
	// assigning the untyped constant yields +0.0.
	if math.IsNaN(float64(v)) || v == 0 {
		v = 0
	}
	return Value{typ: schema.SlotF32, num: uint64(math.Float32bits(v))}
}

// Str returns a string value.
func Str(v string) Value { return Value{typ: schema.SlotString, num: 0, str: v} }

// Type is the value's slot type.
func (v Value) Type() schema.SlotType { return v.typ }

// IsZero reports whether this is the zero Value — no type and no content,
// which is what a slot that was never set decodes to.
func (v Value) IsZero() bool { return v.typ == schema.SlotUnspecified }

// AsBool, AsUint, AsFloat32 and AsString read the value at its own type. Each
// reports false when the value is of another type, so a predicate written
// against the wrong slot returns "does not match" rather than a plausible
// number read out of the wrong bits.

// AsBool returns the boolean value.
func (v Value) AsBool() (bool, bool) {
	if v.typ != schema.SlotBool {
		return false, false
	}
	return v.num == 1, true
}

// AsUint returns any of the unsigned integer values, widened to uint64.
func (v Value) AsUint() (uint64, bool) {
	switch v.typ {
	case schema.SlotU8, schema.SlotU32, schema.SlotU64:
		return v.num, true
	default:
		return 0, false
	}
}

// AsFloat32 returns the float value.
func (v Value) AsFloat32() (float32, bool) {
	if v.typ != schema.SlotF32 {
		return 0, false
	}
	return math.Float32frombits(uint32(v.num)), true
}

// AsString returns the string value.
func (v Value) AsString() (string, bool) {
	if v.typ != schema.SlotString {
		return "", false
	}
	return v.str, true
}

// Order appends the order-preserving encoding of v to dst.
//
// These are the bytes an index key carries, so byte order is value order and
// "the ten most important memories" is a bounded scan rather than a sort over
// the corpus. They are also the bytes a fixed-width slot occupies in a row —
// one encoding, not two, because two encodings of the same value are two things
// that can disagree about which record an index entry points at.
func (v Value) Order(dst []byte) []byte {
	switch v.typ {
	case schema.SlotBool:
		return keys.PutBool(dst, v.num == 1)
	case schema.SlotU8:
		return keys.PutUint8(dst, uint8(v.num))
	case schema.SlotU32:
		return keys.PutUint32(dst, uint32(v.num))
	case schema.SlotU64:
		return keys.PutUint64(dst, v.num)
	case schema.SlotF32:
		return keys.PutFloat32(dst, math.Float32frombits(uint32(v.num)))
	case schema.SlotString:
		return keys.PutString(dst, v.str)
	default:
		// An untyped value has no width and no order. Appending nothing would
		// produce an index entry that addresses a different record than it
		// claims to, so nothing may reach here: the slot table refuses an
		// untyped slot at registration.
		return dst
	}
}

// OrderBytes is [Value.Order] into a fresh slice.
func (v Value) OrderBytes() []byte { return v.Order(nil) }

// Compare orders two values of the same type: -1, 0 or +1.
//
// Values of different types are ordered by their type number, which is
// arbitrary but total — a comparison that returned 0 for unlike types would
// make two different values look identical to a cursor.
func (v Value) Compare(o Value) int {
	if v.typ != o.typ {
		return cmpUint(uint64(v.typ), uint64(o.typ))
	}
	switch v.typ {
	case schema.SlotF32:
		a, _ := v.AsFloat32()
		b, _ := o.AsFloat32()
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	case schema.SlotString:
		switch {
		case v.str < o.str:
			return -1
		case v.str > o.str:
			return 1
		default:
			return 0
		}
	default:
		return cmpUint(v.num, o.num)
	}
}

// String renders the value for errors, logs and `remem-admin inspect`.
func (v Value) String() string {
	switch v.typ {
	case schema.SlotBool:
		return fmt.Sprintf("%t", v.num == 1)
	case schema.SlotU8, schema.SlotU32, schema.SlotU64:
		return fmt.Sprintf("%d", v.num)
	case schema.SlotF32:
		f, _ := v.AsFloat32()
		return fmt.Sprintf("%g", f)
	case schema.SlotString:
		return fmt.Sprintf("%q", v.str)
	default:
		return "unset"
	}
}

// decodeValue reads one value of type typ from the head of b, returning the
// bytes that follow it.
//
// The fixed-width types read exactly the width their type declares, which is
// what lets a reader skip past a slot it has no name for. A string is
// length-prefixed rather than terminated: a row is a payload, not a key, so
// nothing sorts on these bytes and a prefix is both shorter and cheaper than
// escaping every NUL.
func decodeValue(typ schema.SlotType, b []byte) (Value, []byte, error) {
	const op = "attr.decodeValue"

	switch typ {
	case schema.SlotBool:
		x, rest, err := keys.GetBool(b)
		return Bool(x), rest, err
	case schema.SlotU8:
		x, rest, err := keys.GetUint8(b)
		return U8(x), rest, err
	case schema.SlotU32:
		x, rest, err := keys.GetUint32(b)
		return U32(x), rest, err
	case schema.SlotU64:
		x, rest, err := keys.GetUint64(b)
		return U64(x), rest, err
	case schema.SlotF32:
		x, rest, err := keys.GetFloat32(b)
		return F32(x), rest, err
	case schema.SlotString:
		return decodeString(b, op)
	default:
		return Value{}, nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"slot type %s has no width, so nothing can be read at this offset", typ))
	}
}

// encodeValue appends v to dst in row form: the order encoding for a
// fixed-width type, a length-prefixed string otherwise.
func encodeValue(dst []byte, v Value) []byte {
	if v.typ == schema.SlotString {
		dst = appendUvarint(dst, uint64(len(v.str)))
		return append(dst, v.str...)
	}
	return v.Order(dst)
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
