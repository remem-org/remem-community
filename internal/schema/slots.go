package schema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema/pb"
	"github.com/remem-org/remem-go/internal/storage"
	"google.golang.org/protobuf/proto"
)

// slotsPath is where the registered slot table lives, under the untenanted
// system space beside the format manifest. The table describes the whole
// directory and has to be readable before any tenant is known.
const slotsPath = "/slots"

// MaxEncodableSlot is the highest slot number an attribute row can address.
//
// The bound belongs to the row encoding, which is internal/attr's in Phase 5: a
// row header stores its presence bitmap's length in one byte, so 255 bytes of
// bitmap address slots 0 through 2039. It is restated here because registration
// is where a violation can still be a build failure — discovering it at encode
// time means discovering it once a row has already been truncated on a cast.
const MaxEncodableSlot uint16 = 2039

// SlotType is an attribute's storage type.
//
// The fixed-width types are Rust Remem's, so an imported corpus needs no
// remapping. A type is what gives a reader the width to skip past a slot it has
// no name for, which is the property that makes retirement work; [SlotString]
// is variable-width and is length-prefixed in the row for the same reason.
type SlotType uint8

const (
	// SlotUnspecified is the zero value: not a type, and refused at registration.
	SlotUnspecified SlotType = iota
	SlotBool
	SlotU8
	SlotU32
	SlotU64
	SlotF32
	SlotString
)

var slotTypeNames = [...]string{
	SlotUnspecified: "unspecified",
	SlotBool:        "bool",
	SlotU8:          "u8",
	SlotU32:         "u32",
	SlotU64:         "u64",
	SlotF32:         "f32",
	SlotString:      "string",
}

// String returns the stable name used in errors and in `remem-admin inspect`.
func (t SlotType) String() string {
	if int(t) < len(slotTypeNames) && slotTypeNames[t] != "" {
		return slotTypeNames[t]
	}
	return fmt.Sprintf("slot_type(%d)", uint8(t))
}

// Width reports the encoded width of a value of this type in bytes, and whether
// that width is fixed. A variable-width slot reports false, and a row carrying
// one length-prefixes it.
func (t SlotType) Width() (int, bool) {
	switch t {
	case SlotBool, SlotU8:
		return 1, true
	case SlotU32, SlotF32:
		return 4, true
	case SlotU64:
		return 8, true
	case SlotString:
		return 0, false
	default:
		return 0, false
	}
}

// SlotDef is one attribute's declaration.
type SlotDef struct {
	// Slot is the number. It is an address on disk: never reused, never
	// renumbered.
	Slot uint16
	// Name is what callers address the field by. Renaming is refused.
	Name string
	// Type gives the value its width.
	Type SlotType
	// Indexed is whether the slot carries an ordered index usable as an access
	// path. A retired slot has none whatever this says.
	Indexed bool
	// Retired means the slot is never written to new rows. Its declaration
	// survives so older rows stay decodable.
	Retired bool
}

// Slots is a registered attribute slot table.
//
// It is built at start-up by the package that owns the slot numbers —
// internal/attr, from Phase 5 — and then validated against the copy persisted
// in the directory. This package owns the discipline; it does not own the
// table's contents.
//
// A Slots is not safe for concurrent modification. It is registered once during
// construction and read thereafter.
type Slots struct {
	version uint32
	defs    map[uint16]SlotDef
	names   map[string]uint16
}

// NewSlots returns an empty table at the given schema revision.
func NewSlots(version uint32) *Slots {
	return &Slots{version: version, defs: map[uint16]SlotDef{}, names: map[string]uint16{}}
}

// Version is the table's own revision, bumped whenever a slot is added or
// retired. It is neither a storage format version nor a tenant's user-schema
// version.
func (s *Slots) Version() uint32 { return s.version }

// Register declares a slot.
//
// It refuses at registration rather than at encode time, because every one of
// these mistakes is silent afterwards: a reused number repoints existing rows,
// a duplicate name makes a query unresolvable, an untyped slot has no width, and
// a slot past the encodable bound truncates on a cast.
func (s *Slots) Register(d SlotDef) error {
	const op = "schema.Slots.Register"

	switch {
	case d.Name == "":
		return errs.E(errs.Invalid, op, fmt.Errorf("slot %d has no name", d.Slot))
	case d.Type == SlotUnspecified:
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"slot %d (%q) has no type; a slot with no type has no width, and nothing can skip past it", d.Slot, d.Name))
	case d.Slot > MaxEncodableSlot:
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"slot %d (%q) exceeds the highest encodable slot %d; an attribute row addresses slots through a "+
				"presence bitmap whose length is one byte", d.Slot, d.Name, MaxEncodableSlot))
	}
	if existing, ok := s.defs[d.Slot]; ok {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"slot %d is already %q and a slot number is never reused; retire it and add a new one instead",
			d.Slot, existing.Name))
	}
	if other, ok := s.names[d.Name]; ok {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"slot %d is named %q, which slot %d already answers to", d.Slot, d.Name, other))
	}

	s.defs[d.Slot] = d
	s.names[d.Name] = d.Slot
	return nil
}

// Retire withdraws a slot from new rows without forgetting how to read old ones.
//
// It is the only way a slot goes away. Deleting the declaration would lose the
// width a reader needs to skip the value, which turns every row written before
// the change into an unparseable one.
func (s *Slots) Retire(slot uint16) error {
	const op = "schema.Slots.Retire"

	d, ok := s.defs[slot]
	if !ok {
		return errs.E(errs.Invalid, op, fmt.Errorf("slot %d is not registered, so there is nothing to retire", slot))
	}
	d.Retired = true
	s.defs[slot] = d
	return nil
}

// Get returns a slot's declaration, retired or not.
func (s *Slots) Get(slot uint16) (SlotDef, bool) {
	d, ok := s.defs[slot]
	return d, ok
}

// Lookup returns the slot answering to a name.
func (s *Slots) Lookup(name string) (SlotDef, bool) {
	slot, ok := s.names[name]
	if !ok {
		return SlotDef{}, false
	}
	return s.defs[slot], true
}

// Defs returns every declaration, retired ones included, in slot order.
func (s *Slots) Defs() []SlotDef {
	out := make([]SlotDef, 0, len(s.defs))
	for _, d := range s.defs {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// IndexedSlots returns the live slots carrying an ordered index — the access
// paths a query planner may choose from. A retired slot is never among them.
func (s *Slots) IndexedSlots() []SlotDef {
	var out []SlotDef
	for _, d := range s.Defs() {
		if d.Indexed && !d.Retired {
			out = append(out, d)
		}
	}
	return out
}

// MaxSlot is the highest registered slot number, or zero on an empty table.
func (s *Slots) MaxSlot() uint16 {
	var max uint16
	for slot := range s.defs {
		if slot > max {
			max = slot
		}
	}
	return max
}

// BitmapLen is how many bytes of presence bitmap an attribute row needs to
// address every slot in this table.
func (s *Slots) BitmapLen() int {
	if len(s.defs) == 0 {
		return 0
	}
	return int(s.MaxSlot())/8 + 1
}

// Equal reports whether two tables declare exactly the same thing.
func (s *Slots) Equal(other *Slots) bool {
	if s == nil || other == nil {
		return s == other
	}
	if s.version != other.version || len(s.defs) != len(other.defs) {
		return false
	}
	for slot, d := range s.defs {
		if od, ok := other.defs[slot]; !ok || od != d {
			return false
		}
	}
	return true
}

// String renders the table in slot order, for logs and for `remem-admin inspect`.
func (s *Slots) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "v%d[", s.version)
	for i, d := range s.Defs() {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%d:%s/%s", d.Slot, d.Name, d.Type)
		if d.Indexed {
			b.WriteString("/indexed")
		}
		if d.Retired {
			b.WriteString("/retired")
		}
	}
	b.WriteString("]")
	return b.String()
}

// Validate refuses a registered table that cannot read what is already on disk.
//
// Adding slots is always safe. Everything else is not, and the reason is the
// same in each case: existing rows were encoded against the persisted
// declaration, so a mismatch does not fail — it misparses every one of them and
// returns values that look real.
func (s *Slots) Validate(persisted *Slots) error {
	const op = "schema.Slots.Validate"

	if persisted == nil {
		return nil
	}
	for _, old := range persisted.Defs() {
		now, ok := s.defs[old.Slot]
		if !ok {
			return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
				"slot %d (%q) exists on disk but not in the registered schema: retire slots, never drop them — "+
					"the declaration is the width a reader needs to skip past a value it no longer has a name for",
				old.Slot, old.Name))
		}
		if now.Type != old.Type || now.Name != old.Name {
			return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
				"slot %d changed from %q/%s to %q/%s: retire the slot and add a new one instead",
				old.Slot, old.Name, old.Type, now.Name, now.Type))
		}
		if old.Retired && !now.Retired {
			return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
				"slot %d (%q) is retired on disk and cannot be revived; rows written since the retirement do not carry it",
				old.Slot, old.Name))
		}
	}
	return nil
}

// SlotsKey is the storage key of the slot table.
//
// It is exported so a test can damage exactly that row and so `remem-admin`
// can name what it is inspecting. Nothing in the write path calls it from
// outside this package.
func SlotsKey() []byte { return keys.System(slotsPath) }

// ReadSlots reads the persisted table. The bool reports whether one exists: a
// directory that has never registered a slot is the ordinary case on a fresh
// install, not damage.
func ReadSlots(ctx context.Context, kv storage.KV) (*Slots, bool, error) {
	const op = "schema.ReadSlots"

	value, err := kv.Get(ctx, SlotsKey())
	if errs.Is(err, errs.NotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var row pb.SlotSchema
	if err := proto.Unmarshal(value, &row); err != nil {
		// Corruption, never an absent table: reading damage as "no schema yet"
		// would stamp a fresh table over rows encoded against the old one.
		return nil, false, errs.E(errs.Corruption, op, errors.New("the slot schema row will not parse"))
	}

	s := NewSlots(row.GetVersion())
	for _, d := range row.GetSlots() {
		if d.GetSlot() > uint32(MaxEncodableSlot) {
			return nil, false, errs.E(errs.Corruption, op, fmt.Errorf(
				"the slot schema declares slot %d, above the highest encodable slot %d", d.GetSlot(), MaxEncodableSlot))
		}
		def := SlotDef{
			Slot:    uint16(d.GetSlot()),
			Name:    d.GetName(),
			Type:    slotTypeFromProto(d.GetType()),
			Indexed: d.GetIndexed(),
			Retired: d.GetRetired(),
		}
		if def.Type == SlotUnspecified {
			return nil, false, errs.E(errs.Corruption, op, fmt.Errorf(
				"slot %d (%q) on disk has a type this binary does not know (%d); it cannot be given a width, "+
					"so no row carrying it can be decoded", def.Slot, def.Name, d.GetType()))
		}
		if err := s.Register(def); err != nil {
			return nil, false, errs.E(errs.Corruption, op, fmt.Errorf("the slot schema on disk is not a valid table: %w", err))
		}
	}
	return s, true, nil
}

// WriteSlots replaces the persisted table.
func WriteSlots(ctx context.Context, kv storage.KV, s *Slots) error {
	const op = "schema.WriteSlots"

	row := &pb.SlotSchema{Version: s.Version()}
	for _, d := range s.Defs() { // slot order, so the bytes are stable
		row.Slots = append(row.Slots, &pb.Slot{
			Slot:    uint32(d.Slot),
			Name:    d.Name,
			Type:    slotTypeToProto(d.Type),
			Indexed: d.Indexed,
			Retired: d.Retired,
		})
	}
	value, err := proto.Marshal(row)
	if err != nil {
		return errs.E(errs.Invalid, op, fmt.Errorf("encoding the slot schema: %w", err))
	}
	return kv.Set(ctx, SlotsKey(), value)
}

// OpenSlots is the gate the registered table passes through at start-up.
//
// A directory with no table is stamped with this one. A directory with a table
// is validated against it and refused if the registered table cannot read what
// is there. An additive change is persisted, so the next open validates against
// what is actually in force rather than against a table two upgrades old.
func OpenSlots(ctx context.Context, kv storage.KV, registered *Slots) error {
	persisted, ok, err := ReadSlots(ctx, kv)
	if err != nil {
		return err
	}
	if ok {
		if err := registered.Validate(persisted); err != nil {
			return err
		}
		if registered.Equal(persisted) {
			// Nothing moved. Do not rewrite the row a directory's decodability
			// depends on just because a process started.
			return nil
		}
	}
	return WriteSlots(ctx, kv, registered)
}

func slotTypeToProto(t SlotType) pb.SlotType {
	switch t {
	case SlotBool:
		return pb.SlotType_SLOT_TYPE_BOOL
	case SlotU8:
		return pb.SlotType_SLOT_TYPE_U8
	case SlotU32:
		return pb.SlotType_SLOT_TYPE_U32
	case SlotU64:
		return pb.SlotType_SLOT_TYPE_U64
	case SlotF32:
		return pb.SlotType_SLOT_TYPE_F32
	case SlotString:
		return pb.SlotType_SLOT_TYPE_STRING
	default:
		return pb.SlotType_SLOT_TYPE_UNSPECIFIED
	}
}

func slotTypeFromProto(t pb.SlotType) SlotType {
	switch t {
	case pb.SlotType_SLOT_TYPE_BOOL:
		return SlotBool
	case pb.SlotType_SLOT_TYPE_U8:
		return SlotU8
	case pb.SlotType_SLOT_TYPE_U32:
		return SlotU32
	case pb.SlotType_SLOT_TYPE_U64:
		return SlotU64
	case pb.SlotType_SLOT_TYPE_F32:
		return SlotF32
	case pb.SlotType_SLOT_TYPE_STRING:
		return SlotString
	default:
		return SlotUnspecified
	}
}
