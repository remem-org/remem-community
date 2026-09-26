package schema_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
)

// The slot registry's whole job is to refuse. Every test below is a refusal or
// the thing a refusal protects, because a registry that accepts a bad schema
// does not fail — it misparses every attribute row on disk and returns
// plausible wrong values.

// base is a two-slot table standing in for the real one, which arrives with
// internal/attr in Phase 5.
func base(t *testing.T) *schema.Slots {
	t.Helper()
	s := schema.NewSlots(1)
	mustRegister(t, s, schema.SlotDef{Slot: 0, Name: "archived", Type: schema.SlotBool})
	mustRegister(t, s, schema.SlotDef{Slot: 1, Name: "importance", Type: schema.SlotF32, Indexed: true})
	return s
}

func mustRegister(t *testing.T, s *schema.Slots, d schema.SlotDef) {
	t.Helper()
	if err := s.Register(d); err != nil {
		t.Fatalf("registering slot %d: %v", d.Slot, err)
	}
}

// A retired slot is not a deleted one: its declared type stays, and that type
// is the width a reader needs to skip past a value it no longer has a name for.
func TestRetiredSlotStaysDecodable(t *testing.T) {
	s := base(t)
	if err := s.Retire(1); err != nil {
		t.Fatal(err)
	}

	got, ok := s.Get(1)
	if !ok {
		t.Fatal("a retired slot must still be in the table; without it older rows cannot be decoded")
	}
	if got.Type != schema.SlotF32 {
		t.Fatalf("a retired slot keeps its type, got %v", got.Type)
	}
	if !got.Retired {
		t.Fatal("the slot is not marked retired")
	}
	// A retired slot is never an access path, whatever it was before.
	for _, d := range s.IndexedSlots() {
		if d.Slot == 1 {
			t.Fatal("a retired slot must not appear among the indexed slots")
		}
	}
}

func TestDroppingASlotIsRefused(t *testing.T) {
	persisted := base(t)

	current := schema.NewSlots(2)
	mustRegister(t, current, schema.SlotDef{Slot: 0, Name: "archived", Type: schema.SlotBool})

	err := current.Validate(persisted)
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "retire slots, never drop them") {
		t.Fatalf("the error must say what to do instead: %v", err)
	}
	if !strings.Contains(err.Error(), "importance") {
		t.Fatalf("the error must name the offending slot: %v", err)
	}
}

func TestSlotTypeChangeIsRefused(t *testing.T) {
	persisted := base(t)

	current := schema.NewSlots(2)
	mustRegister(t, current, schema.SlotDef{Slot: 0, Name: "archived", Type: schema.SlotBool})
	mustRegister(t, current, schema.SlotDef{Slot: 1, Name: "importance", Type: schema.SlotU32, Indexed: true})

	err := current.Validate(persisted)
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "importance") {
		t.Fatalf("the error must name the offending slot: %v", err)
	}
}

func TestSlotRenameIsRefused(t *testing.T) {
	persisted := base(t)

	current := schema.NewSlots(2)
	mustRegister(t, current, schema.SlotDef{Slot: 0, Name: "archived", Type: schema.SlotBool})
	mustRegister(t, current, schema.SlotDef{Slot: 1, Name: "weight", Type: schema.SlotF32, Indexed: true})

	if err := current.Validate(persisted); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("a rename repoints every query at a different column; got %v", err)
	}
}

func TestUnRetiringASlotIsRefused(t *testing.T) {
	persisted := base(t)
	if err := persisted.Retire(1); err != nil {
		t.Fatal(err)
	}

	current := base(t) // slot 1 live again
	if err := current.Validate(persisted); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
}

func TestAddingASlotValidates(t *testing.T) {
	persisted := base(t)

	current := base(t)
	mustRegister(t, current, schema.SlotDef{Slot: 2, Name: "health", Type: schema.SlotF32, Indexed: true})

	if err := current.Validate(persisted); err != nil {
		t.Fatalf("adding a slot is the one change that is always safe: %v", err)
	}
}

// A slot number is an address on disk. Reusing one points every row written
// under the old meaning at the new one.
func TestSlotNumberIsNeverReused(t *testing.T) {
	s := base(t)
	err := s.Register(schema.SlotDef{Slot: 1, Name: "health", Type: schema.SlotF32})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "never reused") {
		t.Fatalf("the error must say why: %v", err)
	}
}

func TestSlotNameIsUnique(t *testing.T) {
	s := base(t)
	if err := s.Register(schema.SlotDef{Slot: 2, Name: "importance", Type: schema.SlotF32}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("two slots answering to one name is a query that cannot be resolved; got %v", err)
	}
}

func TestRegisteringAnUntypedSlotIsRefused(t *testing.T) {
	s := schema.NewSlots(1)
	if err := s.Register(schema.SlotDef{Slot: 0, Name: "archived"}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a slot with no type has no width, so nothing can skip it; got %v", err)
	}
}

func TestASlotBeyondTheEncodableBoundIsRefused(t *testing.T) {
	s := schema.NewSlots(1)
	err := s.Register(schema.SlotDef{Slot: schema.MaxEncodableSlot + 1, Name: "too_high", Type: schema.SlotU32})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}

	ok := schema.NewSlots(1)
	if err := ok.Register(schema.SlotDef{Slot: schema.MaxEncodableSlot, Name: "at_the_limit", Type: schema.SlotU32}); err != nil {
		t.Fatalf("the bound itself must be usable: %v", err)
	}
}

func TestRetiringAnUnknownSlotIsRefused(t *testing.T) {
	s := base(t)
	if err := s.Retire(9); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
}

func TestSlotSchemaRoundTripsThroughDisk(t *testing.T) {
	kv := newKV(t)
	want := base(t)
	if err := want.Retire(1); err != nil {
		t.Fatal(err)
	}

	if err := schema.WriteSlots(ctx(), kv, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := schema.ReadSlots(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the row was written and did not read back")
	}
	if !got.Equal(want) {
		t.Fatalf("round trip changed the table:\n got %s\nwant %s", got, want)
	}
}

func TestAnAbsentSlotSchemaIsNotAnError(t *testing.T) {
	kv := newKV(t)
	// A directory that has never registered a slot is the ordinary case on a
	// fresh install, not damage.
	_, ok, err := schema.ReadSlots(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("an empty store reported a slot schema")
	}
}

// OpenSlots is the gate: it stamps a fresh directory, validates an existing
// one, and persists an additive change so the next open validates against it.
func TestOpenSlotsStampsThenValidatesThenPersists(t *testing.T) {
	kv := newKV(t)

	if err := schema.OpenSlots(ctx(), kv, base(t)); err != nil {
		t.Fatal(err)
	}
	stamped, ok, err := schema.ReadSlots(ctx(), kv)
	if err != nil || !ok {
		t.Fatalf("the first open must stamp the table: ok=%v err=%v", ok, err)
	}
	if !stamped.Equal(base(t)) {
		t.Fatalf("stamped %s", stamped)
	}

	grown := base(t)
	mustRegister(t, grown, schema.SlotDef{Slot: 2, Name: "health", Type: schema.SlotF32, Indexed: true})
	if err := schema.OpenSlots(ctx(), kv, grown); err != nil {
		t.Fatalf("an additive change must open: %v", err)
	}
	after, _, err := schema.ReadSlots(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := after.Get(2); !ok {
		t.Fatal("the added slot was not persisted, so the next open would validate against a stale table")
	}

	// And the refusal reaches through the gate, not only through Validate.
	shrunk := schema.NewSlots(3)
	mustRegister(t, shrunk, schema.SlotDef{Slot: 0, Name: "archived", Type: schema.SlotBool})
	if err := schema.OpenSlots(ctx(), kv, shrunk); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
}

// A slot row that will not parse is corruption, never an absent table: reading
// it as absent would stamp a fresh schema over rows encoded against the old one.
func TestADamagedSlotRowIsCorruption(t *testing.T) {
	kv := newKV(t)
	if err := kv.Set(ctx(), schema.SlotsKey(), []byte{0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := schema.ReadSlots(ctx(), kv); !errs.Is(err, errs.Corruption) {
		t.Fatalf("got %v", err)
	}
}
