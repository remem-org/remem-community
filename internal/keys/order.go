package keys

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
)

// Order-preserving value encoding.
//
// The attribute index (SpaceAttrIndex) sorts by value, and the store sorts by
// bytes. These functions are the bridge: for any two values a and b of the same
// type, bytes.Compare(Put(a), Put(b)) has the same sign as comparing a and b.
// That is what makes "the ten most important memories" a bounded scan instead
// of a sort over everything the tenant owns.
//
// Every function appends to dst and returns the extended slice, so a caller
// composing several fields pays one allocation. Every decoder returns the
// remaining bytes, because a key holds a value followed by more key.
//
// This is the piece of encoding most likely to be subtly wrong — an
// implementation that mangles order still produces plausible-looking bytes and
// still round-trips — which is why it gets property tests as well as these.

// PutUint64 appends v in big-endian order, which is already sort order for
// unsigned integers.
func PutUint64(dst []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, v)
}

// GetUint64 decodes a value written by [PutUint64].
func GetUint64(b []byte) (uint64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, short("keys.GetUint64", 8, len(b))
	}
	return binary.BigEndian.Uint64(b[:8]), b[8:], nil
}

// PutInt64 appends v with its sign bit flipped.
//
// Two's complement puts negative numbers above positive ones as unsigned
// bytes: -1 is 0xFFFF...FF and +1 is 0x0000...01. Flipping the sign bit maps
// the signed range onto the unsigned range in order, so -1 becomes 0x7FFF...FF
// and +1 becomes 0x8000...01.
func PutInt64(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v)^signBit64)
}

// GetInt64 decodes a value written by [PutInt64].
func GetInt64(b []byte) (int64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, short("keys.GetInt64", 8, len(b))
	}
	return int64(binary.BigEndian.Uint64(b[:8]) ^ signBit64), b[8:], nil
}

// PutFloat64 appends v so that byte order is numeric order.
//
// IEEE-754 floats compare correctly as unsigned integers only within one sign,
// and negatives run backwards. The transform fixes both at once: if the sign
// bit is set the value is negative, so invert every bit — which both moves it
// below the positives and reverses the backwards ordering; otherwise set the
// sign bit, lifting positives above every negative.
//
// The result orders -inf < -1.0 < -0.0 < +0.0 < 1.0 < +inf. NaN has no order
// and is encoded, not rejected: it lands above +inf and stays round-trippable,
// which is the least surprising place for a value that cannot be compared.
func PutFloat64(dst []byte, v float64) []byte {
	return binary.BigEndian.AppendUint64(dst, orderFloat64(math.Float64bits(v)))
}

// GetFloat64 decodes a value written by [PutFloat64].
func GetFloat64(b []byte) (float64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, short("keys.GetFloat64", 8, len(b))
	}
	return math.Float64frombits(unorderFloat64(binary.BigEndian.Uint64(b[:8]))), b[8:], nil
}

// PutFloat32 appends v with the same transform as [PutFloat64].
//
// Vector scores and importance values are float32, and widening them to
// float64 to reuse one encoder would double the width of every index entry
// that holds one.
func PutFloat32(dst []byte, v float32) []byte {
	return binary.BigEndian.AppendUint32(dst, orderFloat32(math.Float32bits(v)))
}

// GetFloat32 decodes a value written by [PutFloat32].
func GetFloat32(b []byte) (float32, []byte, error) {
	if len(b) < 4 {
		return 0, nil, short("keys.GetFloat32", 4, len(b))
	}
	return math.Float32frombits(unorderFloat32(binary.BigEndian.Uint32(b[:4]))), b[4:], nil
}

// PutUint32 appends v in big-endian order, which is already sort order for
// unsigned integers.
//
// It exists beside [PutUint64] rather than widening every u32 to eight bytes,
// because an attribute row carries one value per slot per record and the four
// bytes saved are paid on every record in the corpus.
func PutUint32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

// GetUint32 decodes a value written by [PutUint32].
func GetUint32(b []byte) (uint32, []byte, error) {
	if len(b) < 4 {
		return 0, nil, short("keys.GetUint32", 4, len(b))
	}
	return binary.BigEndian.Uint32(b[:4]), b[4:], nil
}

// PutUint8 appends one byte, which is already its own sort order.
func PutUint8(dst []byte, v uint8) []byte {
	return append(dst, v)
}

// GetUint8 decodes a value written by [PutUint8].
func GetUint8(b []byte) (uint8, []byte, error) {
	if len(b) < 1 {
		return 0, nil, short("keys.GetUint8", 1, len(b))
	}
	return b[0], b[1:], nil
}

// PutBool appends one byte. false sorts below true.
func PutBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 0x01)
	}
	return append(dst, 0x00)
}

// GetBool decodes a value written by [PutBool].
func GetBool(b []byte) (bool, []byte, error) {
	if len(b) < 1 {
		return false, nil, short("keys.GetBool", 1, len(b))
	}
	switch b[0] {
	case 0x00:
		return false, b[1:], nil
	case 0x01:
		return true, b[1:], nil
	default:
		return false, nil, errs.E(errs.Corruption, "keys.GetBool",
			fmt.Errorf("a bool is 0x00 or 0x01, found %#x", b[0]))
	}
}

// PutString appends v escaped and terminated.
//
// A value in a key is followed by more key, so the encoding has to say where
// the string ends. A length prefix would not do: it sorts by length before
// content, so "b" would sort below "aa". The terminator is 0x00 0x00, and any
// NUL inside the string is escaped to 0x00 0xFF so it cannot be mistaken for
// one. Since 0xFF sorts above 0x00, escaping also preserves order: "a" encodes
// to a-00-00 and "a\x00b" to a-00-FF-b-00-00, and the shorter correctly sorts
// first.
//
// Escaping is not optional. An unescaped NUL would end the field early, and
// two different values would produce the same bytes — one silently overwriting
// the other's index entry.
func PutString(dst []byte, v string) []byte {
	for i := 0; i < len(v); i++ {
		if v[i] == 0x00 {
			dst = append(dst, 0x00, 0xFF)
			continue
		}
		dst = append(dst, v[i])
	}
	return append(dst, 0x00, 0x00)
}

// GetString decodes a value written by [PutString], returning the bytes that
// follow the terminator.
func GetString(b []byte) (string, []byte, error) {
	const op = "keys.GetString"
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] != 0x00 {
			out = append(out, b[i])
			continue
		}
		if i+1 >= len(b) {
			return "", nil, errs.E(errs.Corruption, op,
				fmt.Errorf("string ends after an escape byte with nothing following it"))
		}
		switch b[i+1] {
		case 0x00:
			return string(out), b[i+2:], nil
		case 0xFF:
			out = append(out, 0x00)
			i++
		default:
			return "", nil, errs.E(errs.Corruption, op,
				fmt.Errorf("0x00 must be followed by 0x00 or 0xFF, found %#x", b[i+1]))
		}
	}
	return "", nil, errs.E(errs.Corruption, op, fmt.Errorf("string is not terminated"))
}

const (
	signBit64 uint64 = 1 << 63
	signBit32 uint32 = 1 << 31
)

func orderFloat64(bits uint64) uint64 {
	if bits&signBit64 != 0 {
		return ^bits
	}
	return bits | signBit64
}

func unorderFloat64(bits uint64) uint64 {
	if bits&signBit64 != 0 {
		return bits &^ signBit64
	}
	return ^bits
}

func orderFloat32(bits uint32) uint32 {
	if bits&signBit32 != 0 {
		return ^bits
	}
	return bits | signBit32
}

func unorderFloat32(bits uint32) uint32 {
	if bits&signBit32 != 0 {
		return bits &^ signBit32
	}
	return ^bits
}

func short(op string, want, got int) error {
	return errs.E(errs.Corruption, op,
		fmt.Errorf("need %d bytes, have %d", want, got))
}
