package attr

import (
	"time"

	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
)

// The memory slot table.
//
// A slot number is an address on disk. It is never reused and never
// renumbered: retire a slot and add a new one, because a reused number
// repoints every row already written.
//
// Slots 0 to 9 keep the numbers Rust Remem gave them (services/attrs.rs), so a
// corpus exported from Rust and imported here lands on the same addresses.
// Slots 10 and 11 are new, and are what plan §II.10 row 3 buys: Rust orders
// listings by one field because its memtable holds a single version per key,
// and Pebble snapshots remove the constraint.
//
// Two slots changed meaning from Rust and both are recorded in
// docs/architecture/query.md: slot 1 was `memory_type`, a u8 enum, and is now
// `policy`, a named retention policy (plan §II.6); and health is indexed here
// where Rust left it unindexed, because ordering by health is one of the five
// orderings this phase promises. Neither change costs an import anything —
// attribute rows are derived, and an import re-projects them rather than
// carrying them.
const (
	SlotArchived        uint16 = 0
	SlotPolicy          uint16 = 1
	SlotImportance      uint16 = 2
	SlotCreatedAt       uint16 = 3
	SlotAccessedAt      uint16 = 4
	SlotAccessCount     uint16 = 5
	SlotHealth          uint16 = 6
	SlotValence         uint16 = 7
	SlotArousal         uint16 = 8
	SlotNextAttentionAt uint16 = 9
	SlotUpdatedAt       uint16 = 10
	SlotLastRecalledAt  uint16 = 11
)

// SchemaVersion is the slot table's own revision, stamped into every row.
//
// It is bumped whenever a slot is added or retired, and it is neither a durable
// storage format version nor a tenant's user-schema version. Version 1 is this
// table; Rust reached its own version 2 by a different route, and the two
// numbers are not comparable.
const SchemaVersion uint32 = 1

// slotDefs is the declaration, in slot order.
//
// `Indexed` is the expensive column. An indexed slot costs one extra key per
// record on every write and one deletion on every update, and buys an access
// path — a walk in value order that a listing or a due-time sweep can start
// from. A slot that nothing orders by and nothing filters selectively on is
// declared unindexed on purpose: it still settles predicates from the row, for
// free, because the row is read anyway.
var slotDefs = []schema.SlotDef{
	// Two values. A postings list over a boolean buys nothing a row read does
	// not already give, and costs a write per record.
	{Slot: SlotArchived, Name: "archived", Type: schema.SlotBool},
	// Low cardinality: three policies over a whole corpus. The index would be
	// three enormous runs, and walking one is walking the corpus.
	{Slot: SlotPolicy, Name: "policy", Type: schema.SlotString},

	{Slot: SlotImportance, Name: "importance", Type: schema.SlotF32, Indexed: true},
	{Slot: SlotCreatedAt, Name: "created_at", Type: schema.SlotU64, Indexed: true},

	// Recall telemetry. Projected so a filter can settle from the row, and
	// unindexed because nothing orders by them.
	{Slot: SlotAccessedAt, Name: "accessed_at", Type: schema.SlotU64},
	{Slot: SlotAccessCount, Name: "access_count", Type: schema.SlotU32},

	{Slot: SlotHealth, Name: "health", Type: schema.SlotF32, Indexed: true},

	{Slot: SlotValence, Name: "emotional_valence", Type: schema.SlotF32},
	{Slot: SlotArousal, Name: "arousal", Type: schema.SlotF32},

	// The due-time index the lifecycle sweeps walk from Phase 10. Indexed
	// because that walk is the whole point: it lets a sweep visit what is due
	// instead of everything that exists.
	{Slot: SlotNextAttentionAt, Name: "next_attention_at", Type: schema.SlotU64, Indexed: true},

	{Slot: SlotUpdatedAt, Name: "updated_at", Type: schema.SlotU64, Indexed: true},
	{Slot: SlotLastRecalledAt, Name: "last_recalled_at", Type: schema.SlotU64, Indexed: true},
}

// Table builds the registered slot table.
//
// It returns an error rather than panicking because [schema.Slots.Register] is
// where a duplicate number, a duplicate name or an untyped slot is caught, and
// a start-up failure naming the slot is more useful than a stack trace in a
// package initialiser.
func Table() (*schema.Slots, error) {
	s := schema.NewSlots(SchemaVersion)
	for _, d := range slotDefs {
		if err := s.Register(d); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// MustTable is [Table] for tests and for the composition root, both of which
// have no useful thing to do with the error.
func MustTable() *schema.Slots {
	s, err := Table()
	if err != nil {
		panic("attr: the memory slot table does not register: " + err.Error())
	}
	return s
}

// Project derives a record's row.
//
// Every non-retired slot is set. That is asserted by TestEverySlotIsPopulated
// and it is not a tidiness rule: a declared slot that projection skips has an
// index with no entries, so ordering a listing by it returns nothing at all —
// which reads as "you have no memories" rather than as a bug.
func Project(rec *record.Record) *Row {
	f := rec.Fields.WithDefaults()
	row := NewRow(SchemaVersion)

	row.Set(SlotArchived, Bool(f.Archived))
	row.Set(SlotPolicy, Str(f.Policy))
	row.Set(SlotImportance, F32(f.Importance))
	row.Set(SlotCreatedAt, U64(unixMs(rec.CreatedAt)))
	row.Set(SlotAccessedAt, U64(unixMs(f.AccessedAt)))
	row.Set(SlotAccessCount, U32(f.AccessCount))
	row.Set(SlotHealth, F32(f.Health))
	row.Set(SlotValence, F32(f.Valence))
	row.Set(SlotArousal, F32(f.Arousal))
	row.Set(SlotNextAttentionAt, U64(unixMs(f.NextAttentionAt)))
	row.Set(SlotUpdatedAt, U64(unixMs(rec.UpdatedAt)))
	row.Set(SlotLastRecalledAt, U64(unixMs(f.LastRecalledAt)))

	return row
}

// unixMs is a time as Unix milliseconds, with the zero time as zero.
//
// A zero time is "unset", and UnixMilli would render it as a large negative
// number that order-encodes below every real timestamp — so an unset
// last_recalled_at would sort ahead of a memory recalled a second ago.
func unixMs(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}
