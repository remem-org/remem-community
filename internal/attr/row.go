package attr

import (
	"encoding/binary"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
)

// RowFormatVersion is the version of the packed row layout.
//
// It is a byte inside the row rather than one of the four durable formats in
// internal/version, and the difference is real: the four are directory-wide and
// gate whether a binary may open a database at all, where this one is per-row
// and lets a single unreadable row be reported as corruption without
// condemning the directory.
const RowFormatVersion uint8 = 1

// Row is a record projected onto the slot table.
//
// Layout:
//
//	[u8   row format version]
//	[u32  slot schema version]   which table the row was written against
//	[u8   bitmap length]         in bytes; bit i set means slot i is present
//	[     bitmap]
//	[     values, ascending slot order, present slots only]
//
// The bitmap length is stored rather than derived from the current table,
// because the table grows: a row written when the highest slot was 9 carries a
// 2-byte bitmap, and a reader sizing it from a 12-slot table would misparse
// every one of them. Storing it is exactly what makes "adding a slot rewrites
// nothing" true.
type Row struct {
	// SchemaVersion is the slot table revision this row was written against.
	SchemaVersion uint32

	// values is indexed by slot number, and a value with no type is a slot the
	// row does not carry. Slot numbers are small and dense, so a slice answers
	// what a map would without the map's buckets: every candidate a filtered
	// search examines decodes a row, and in Phase 13 a map per row was a large
	// share of a vector search's allocations.
	values []Value
	// present counts the typed entries in values, so Len need not scan.
	present int
}

// NewRow returns an empty row at the given slot table revision.
func NewRow(schemaVersion uint32) *Row {
	return &Row{SchemaVersion: schemaVersion}
}

// Set writes a slot's value. Every constructor gives a value its type, so a
// zero Value is not a value a row can carry; setting one clears the slot.
func (r *Row) Set(slot uint16, v Value) {
	if int(slot) >= len(r.values) {
		if v.typ == schema.SlotUnspecified {
			return
		}
		r.values = append(r.values, make([]Value, int(slot)+1-len(r.values))...)
	}
	had := r.values[slot].typ != schema.SlotUnspecified
	has := v.typ != schema.SlotUnspecified
	switch {
	case has && !had:
		r.present++
	case had && !has:
		r.present--
	}
	r.values[slot] = v
}

// Get returns a slot's value. The bool reports presence: a slot a row does not
// carry is different from one carrying a zero.
func (r *Row) Get(slot uint16) (Value, bool) {
	if int(slot) >= len(r.values) || r.values[slot].typ == schema.SlotUnspecified {
		return Value{}, false
	}
	return r.values[slot], true
}

// Len is how many slots the row carries.
func (r *Row) Len() int { return r.present }

// Slots returns the slot numbers the row carries, ascending.
func (r *Row) Slots() []uint16 {
	out := make([]uint16, 0, r.present)
	for slot, v := range r.values {
		if v.typ != schema.SlotUnspecified {
			out = append(out, uint16(slot))
		}
	}
	return out
}

// Encode packs the row against the table it was projected from.
func (r *Row) Encode(s *schema.Slots) ([]byte, error) {
	const op = "attr.Row.Encode"

	slots := r.Slots()
	bitmapLen := 0
	if len(slots) > 0 {
		highest := slots[len(slots)-1]
		if highest > schema.MaxEncodableSlot {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"row carries slot %d, above the highest encodable slot %d", highest, schema.MaxEncodableSlot))
		}
		bitmapLen = int(highest)/8 + 1
	}
	// The table's own bitmap is at least as wide as this row needs, and using
	// it keeps every row written by one binary the same shape — which is what
	// makes a corpus of them compress.
	if n := s.BitmapLen(); n > bitmapLen {
		bitmapLen = n
	}

	out := make([]byte, 0, 6+bitmapLen+8*len(slots))
	out = append(out, RowFormatVersion)
	out = binary.BigEndian.AppendUint32(out, r.SchemaVersion)
	out = append(out, uint8(bitmapLen))

	bitmap := make([]byte, bitmapLen)
	for _, slot := range slots {
		bitmap[slot/8] |= 1 << (slot % 8)
	}
	out = append(out, bitmap...)

	for _, slot := range slots {
		out = encodeValue(out, r.values[slot])
	}
	return out, nil
}

// DecodeRow unpacks a row against the current table.
//
// A slot the table has retired is read for its width and then dropped: the
// width is why the declaration survives retirement, and dropping the value is
// what "retired" means. A slot the table does not declare at all is corruption
// — without a type there is no width, so every value after it would be read at
// the wrong offset and would still look like a number.
func DecodeRow(b []byte, s *schema.Slots) (*Row, error) {
	const op = "attr.DecodeRow"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Corruption, op, fmt.Errorf(format, args...))
	}
	const headerLen = 1 + 4 + 1
	if len(b) < headerLen {
		return nil, bad("attribute row is %d bytes, need at least %d", len(b), headerLen)
	}
	if v := b[0]; v != RowFormatVersion {
		return nil, bad("attribute row format version %d is not one this binary reads (it writes %d)",
			v, RowFormatVersion)
	}
	bitmapLen := int(b[5])

	valuesAt := headerLen + bitmapLen
	if len(b) < valuesAt {
		return nil, bad("attribute row is truncated inside its bitmap")
	}
	// Sized once from the bitmap, which bounds every slot the row can carry,
	// so decoding never grows the slice.
	row := &Row{SchemaVersion: binary.BigEndian.Uint32(b[1:5]), values: make([]Value, bitmapLen*8)}
	bitmap := b[headerLen:valuesAt]
	rest := b[valuesAt:]

	for slot := 0; slot < bitmapLen*8; slot++ {
		if bitmap[slot/8]&(1<<(slot%8)) == 0 {
			continue
		}
		def, ok := s.Get(uint16(slot))
		if !ok {
			return nil, bad("attribute row carries slot %d, which the registered schema does not declare; "+
				"without a type it has no width, so nothing after it can be read", slot)
		}
		v, after, err := decodeValue(def.Type, rest)
		if err != nil {
			return nil, errs.E(errs.Corruption, op, fmt.Errorf(
				"slot %d (%q): %w", slot, def.Name, err))
		}
		rest = after
		if !def.Retired {
			row.Set(uint16(slot), v)
		}
	}
	return row, nil
}

// RowKey is the storage key of a record's attribute row.
func RowKey(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return keys.AttrRow(t, ns, rid)
}

// IndexKey is the storage key of one entry in a slot's ordered index.
func IndexKey(t tenant.ID, ns tenant.Namespace, slot uint16, v Value, rid id.ID) []byte {
	return keys.AttrIndex(t, ns, slot, v.OrderBytes(), rid)
}

// appendUvarint is binary.AppendUvarint, named here so row.go and value.go do
// not each import encoding/binary for one call.
func appendUvarint(dst []byte, v uint64) []byte { return binary.AppendUvarint(dst, v) }

// decodeString reads a length-prefixed string from the head of b.
func decodeString(b []byte, op string) (Value, []byte, error) {
	n, w := binary.Uvarint(b)
	if w <= 0 {
		return Value{}, nil, errs.E(errs.Corruption, op,
			fmt.Errorf("a string value ends before its length prefix"))
	}
	b = b[w:]
	if uint64(len(b)) < n {
		return Value{}, nil, errs.E(errs.Corruption, op,
			fmt.Errorf("a string value declares %d bytes and %d remain", n, len(b)))
	}
	return Str(string(b[:n])), b[n:], nil
}
