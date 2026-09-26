package attr_test

import (
	"context"

	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const (
	testTenant = tenant.ID("acme")
	testNS     = tenant.DefaultNamespace
)

// TestEverySlotIsPopulated is the guard ported from Rust's
// `every_schema_slot_is_projected`.
//
// A slot that is declared and never written has an index with no entries, so a
// listing ordered by it returns nothing — which a user reads as "I have no
// memories", not as a bug. The failure is silent in every other test, which is
// why this one exists.
func TestEverySlotIsPopulated(t *testing.T) {
	table := attr.MustTable()
	row := attr.Project(sampleRecord(id.New()))

	for _, def := range table.Defs() {
		if def.Retired {
			continue
		}
		if _, ok := row.Get(def.Slot); !ok {
			t.Errorf("slot %d (%q) is declared and the projection does not set it: "+
				"its index would be empty and an ordering by it would return nothing",
				def.Slot, def.Name)
		}
	}
	if got, want := row.Len(), len(table.Defs()); got != want {
		t.Errorf("the projection sets %d slots and the table declares %d", got, want)
	}
}

func TestRowRoundTripsThroughItsEncoding(t *testing.T) {
	table := attr.MustTable()
	want := attr.Project(sampleRecord(id.New()))

	encoded, err := want.Encode(table)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	got, err := attr.DecodeRow(encoded, table)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if got.SchemaVersion != want.SchemaVersion {
		t.Errorf("schema version %d, want %d", got.SchemaVersion, want.SchemaVersion)
	}
	for _, slot := range want.Slots() {
		w, _ := want.Get(slot)
		g, ok := got.Get(slot)
		if !ok {
			t.Errorf("slot %d did not survive the round trip", slot)
			continue
		}
		if g != w {
			t.Errorf("slot %d decoded as %s, want %s", slot, g, w)
		}
	}
}

// A row written when the table was smaller must still decode. This is the
// property the stored bitmap length exists for, and losing it would make
// adding a slot a rewrite of every row in the corpus.
func TestARowWrittenAgainstAnOlderTableStillDecodes(t *testing.T) {
	old := schema.NewSlots(1)
	for _, d := range []schema.SlotDef{
		{Slot: attr.SlotArchived, Name: "archived", Type: schema.SlotBool},
		{Slot: attr.SlotImportance, Name: "importance", Type: schema.SlotF32, Indexed: true},
	} {
		if err := old.Register(d); err != nil {
			t.Fatalf("registering: %v", err)
		}
	}

	row := attr.NewRow(1)
	row.Set(attr.SlotArchived, attr.Bool(true))
	row.Set(attr.SlotImportance, attr.F32(0.75))
	encoded, err := row.Encode(old)
	if err != nil {
		t.Fatalf("encoding against the old table: %v", err)
	}

	got, err := attr.DecodeRow(encoded, attr.MustTable())
	if err != nil {
		t.Fatalf("decoding against the current table: %v", err)
	}
	if v, ok := got.Get(attr.SlotImportance); !ok || v != attr.F32(0.75) {
		t.Errorf("importance decoded as %s (present=%t), want 0.75", v, ok)
	}
	if _, ok := got.Get(attr.SlotUpdatedAt); ok {
		t.Error("a slot the old table did not have came back set")
	}
}

// TestUpdateReplacesTheOldIndexEntry is Task 5.2's failure mode, and it is the
// one that silently returns records at ranks they no longer hold.
func TestUpdateReplacesTheOldIndexEntry(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	rid := id.New()

	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.9
	stage(t, ctx, kv, ix, rid, rec)

	if !hasEntry(t, ctx, kv, attr.SlotImportance, attr.F32(0.9), rid) {
		t.Fatal("the entry at importance 0.9 was never written")
	}

	rec.Fields.Importance = 0.1
	stage(t, ctx, kv, ix, rid, rec)

	if hasEntry(t, ctx, kv, attr.SlotImportance, attr.F32(0.9), rid) {
		t.Error("the entry at importance 0.9 outlived the value: a listing of the most " +
			"important memories would still return this record at a rank it lost")
	}
	if !hasEntry(t, ctx, kv, attr.SlotImportance, attr.F32(0.1), rid) {
		t.Error("the entry at importance 0.1 was not written")
	}
}

func TestDeleteRemovesTheRowAndEveryEntry(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	rid := id.New()
	stage(t, ctx, kv, ix, rid, sampleRecord(rid))

	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.StageDrop(ctx, tx, testTenant, testNS, rid); err != nil {
		t.Fatalf("dropping: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if _, err := kv.Get(ctx, attr.RowKey(testTenant, testNS, rid)); !errs.Is(err, errs.NotFound) {
		t.Errorf("the row survived the delete: %v", err)
	}
	for _, def := range attr.MustTable().IndexedSlots() {
		if countEntries(t, ctx, kv, def.Slot) != 0 {
			t.Errorf("slot %d (%q) still holds an entry for a deleted record", def.Slot, def.Name)
		}
	}
}

// A stale entry must be harmless as well as rare: `select` verifies every
// candidate against its row, so an entry left behind by a write that did not
// clean up does not put a record at the wrong rank.
func TestAStaleIndexEntryIsVerifiedAway(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.1
	stage(t, ctx, kv, ix, rid, rec)

	// Forge the entry a buggy update would have left behind.
	if err := kv.Set(ctx, attr.IndexKey(testTenant, testNS, attr.SlotImportance, attr.F32(0.9), rid), nil); err != nil {
		t.Fatalf("forging a stale entry: %v", err)
	}

	page, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Desc: true, Limit: 10, Effort: 100,
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(page.IDs) != 1 {
		t.Fatalf("the walk returned %d records, want 1 — the stale entry was not verified away", len(page.IDs))
	}
	if page.Examined != 2 {
		t.Errorf("examined %d candidates, want 2: the stale entry still costs a row read", page.Examined)
	}
}

func TestSelectOrdersByValueInBothDirections(t *testing.T) {
	ctx, kv, ix := newIndexer(t)

	importances := []float32{0.2, 0.9, 0.5, 0.1, 0.7}
	ids := make([]id.ID, len(importances))
	for i, imp := range importances {
		ids[i] = id.New()
		rec := sampleRecord(ids[i])
		rec.Fields.Importance = imp
		stage(t, ctx, kv, ix, ids[i], rec)
	}

	asc := runAll(t, ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Limit: 10, Effort: 100,
	})
	desc := runAll(t, ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Desc: true, Limit: 10, Effort: 100,
	})

	if len(asc) != len(importances) || len(desc) != len(importances) {
		t.Fatalf("asc returned %d and desc %d, want %d each", len(asc), len(desc), len(importances))
	}
	for i := range asc {
		if asc[i] != desc[len(desc)-1-i] {
			t.Fatalf("the descending walk is not the reverse of the ascending one at position %d", i)
		}
	}
	// 0.1 is the lowest; ids[3] carries it.
	if asc[0] != ids[3] {
		t.Errorf("the ascending walk starts at %s, want the record with importance 0.1", asc[0])
	}
}

func TestPredicatesNarrowTheWalkAndAreReCheckedFromTheRow(t *testing.T) {
	ctx, kv, ix := newIndexer(t)

	for i := 0; i < 20; i++ {
		rid := id.New()
		rec := sampleRecord(rid)
		rec.Fields.Importance = float32(i) / 20
		rec.Fields.Archived = i%2 == 0
		stage(t, ctx, kv, ix, rid, rec)
	}

	lo, hi := attr.F32(0.5), attr.F32(0.8)
	page, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Limit: 50, Effort: 1000,
		Preds: []attr.Pred{
			attr.Range(attr.SlotImportance, &lo, &hi, true, false),
			// Unindexed, so it is settled from the row for free.
			attr.Eq(attr.SlotArchived, attr.Bool(false)),
		},
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	// i/20 in [0.5, 0.8) is i in 10..15; odd i are unarchived: 11, 13, 15.
	if len(page.IDs) != 3 {
		t.Errorf("returned %d records, want 3", len(page.IDs))
	}
	// The range bound must narrow the walk itself, not merely filter after it:
	// six candidates lie in the range, and examining twenty would mean the
	// bounds were never applied.
	if page.Examined != 6 {
		t.Errorf("examined %d candidates, want 6 — the range did not narrow the walk", page.Examined)
	}
}

// TestTruncatedIsSetAtTheEffortBound and its complement: a short page must say
// which kind of short it is.
func TestTruncatedAndExhaustedAreDistinct(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	for i := 0; i < 50; i++ {
		rid := id.New()
		rec := sampleRecord(rid)
		rec.Fields.Importance = float32(i) / 50
		// One record in fifty satisfies the filter.
		rec.Fields.Health = 1
		if i == 49 {
			rec.Fields.Health = 99
		}
		stage(t, ctx, kv, ix, rid, rec)
	}
	target := attr.F32(99)
	preds := []attr.Pred{attr.Eq(attr.SlotHealth, target)}

	tight, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Limit: 10, Effort: 5, Preds: preds,
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if !tight.Truncated || tight.Exhausted {
		t.Errorf("a walk stopped by its effort budget reported truncated=%t exhausted=%t",
			tight.Truncated, tight.Exhausted)
	}

	full, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Limit: 10, Effort: 1000, Preds: preds,
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if full.Truncated || !full.Exhausted {
		t.Errorf("a walk that reached the end of its range reported truncated=%t exhausted=%t",
			full.Truncated, full.Exhausted)
	}
	if len(full.IDs) != 1 {
		t.Errorf("the complete walk found %d matches, want 1", len(full.IDs))
	}
}

// TestSelectReadsOnlyThePageItReturns holds the completion criterion: listing
// ten from a large corpus reads ten payloads, at every page including the last.
func TestSelectReadsOnlyThePageItReturns(t *testing.T) {
	const corpus = 10000
	ctx, kv, ix := newIndexer(t)

	counting := &countingKV{KV: kv}
	for i := 0; i < corpus; i++ {
		rid := id.New()
		rec := sampleRecord(rid)
		rec.Fields.Importance = float32(i%1000) / 1000
		stage(t, ctx, kv, ix, rid, rec)
	}

	counting.reset()
	page, err := attr.Run(ctx, counting, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotCreatedAt, Desc: true, Limit: 10, Effort: 100,
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(page.IDs) != 10 {
		t.Fatalf("returned %d records, want 10", len(page.IDs))
	}
	if counting.bodies != 0 {
		t.Errorf("the walk read %d record bodies; it must read none — the payload is the caller's page", counting.bodies)
	}
	if counting.rows > 10 {
		t.Errorf("the walk read %d attribute rows for a page of 10; a candidate costs one row and no more", counting.rows)
	}
}

func TestSelectRefusesAnUnscopedWalk(t *testing.T) {
	ctx, kv, _ := newIndexer(t)
	_, err := attr.Run(ctx, kv, attr.Select{
		Slots: attr.MustTable(), Slot: attr.SlotCreatedAt, Limit: 1,
	})
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("an unscoped walk returned %v, want errs.Invalid (Invariant 1)", err)
	}
}

func TestOneTenantsWalkNeverSeesAnother(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	mine := id.New()
	stage(t, ctx, kv, ix, mine, sampleRecord(mine))

	other := id.New()
	rec := sampleRecord(other)
	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.Stage(ctx, tx, "acmecorp", testNS, other, rec); err != nil {
		t.Fatalf("staging into the neighbouring tenant: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	page, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotCreatedAt, Limit: 10, Effort: 100,
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(page.IDs) != 1 || page.IDs[0] != mine {
		t.Errorf("tenant %q's walk returned %d records; %q is a prefix of the neighbouring tenant's name",
			testTenant, len(page.IDs), testTenant)
	}
}

func TestContradictoryBoundsReturnAnEmptyCompleteAnswer(t *testing.T) {
	ctx, kv, ix := newIndexer(t)
	rid := id.New()
	stage(t, ctx, kv, ix, rid, sampleRecord(rid))

	lo, hi := attr.F32(0.9), attr.F32(0.1)
	page, err := attr.Run(ctx, kv, attr.Select{
		Tenant: testTenant, Namespace: testNS, Slots: attr.MustTable(),
		Slot: attr.SlotImportance, Limit: 10, Effort: 100,
		Preds: []attr.Pred{attr.Range(attr.SlotImportance, &lo, &hi, true, true)},
	})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(page.IDs) != 0 || !page.Exhausted || page.Truncated {
		t.Errorf("contradictory bounds gave %d results, exhausted=%t truncated=%t; want an empty, complete answer",
			len(page.IDs), page.Exhausted, page.Truncated)
	}
}

func TestNotReportsNoSlotSoItNeverNarrowsAWalk(t *testing.T) {
	lo, hi := attr.F32(0.4), attr.F32(0.6)
	inner := attr.Range(attr.SlotImportance, &lo, &hi, true, true)
	if _, ok := inner.Slot(); !ok {
		t.Fatal("a range predicate must report its slot")
	}
	if _, ok := attr.Not(inner).Slot(); ok {
		t.Error("a negated range reported a slot; narrowing a walk to it would drop half the matches")
	}
}

// --- helpers ----------------------------------------------------------------

func newIndexer(t *testing.T) (context.Context, storage.KV, *attr.Indexer) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := tenant.NewContext(context.Background(), testTenant)
	return ctx, kv, attr.NewIndexer(attr.MustTable())
}

func sampleRecord(rid id.ID) *record.Record {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	return &record.Record{
		ID:        rid,
		Tenant:    testTenant,
		Namespace: testNS,
		Type:      record.TypeMemory,
		Content:   "a memory",
		CreatedAt: now,
		UpdatedAt: now,
		Fields: record.Fields{
			// An explicit policy, so the lifecycle defaults do not fire and a
			// test that sets importance to zero gets a zero.
			Policy:          "long_term",
			Importance:      0.5,
			Health:          100,
			Valence:         0.2,
			Arousal:         0.3,
			AccessedAt:      now,
			AccessCount:     1,
			LastRecalledAt:  now,
			NextAttentionAt: now.Add(24 * time.Hour),
		},
	}
}

func stage(t *testing.T, ctx context.Context, kv storage.KV, ix *attr.Indexer, rid id.ID, rec *record.Record) {
	t.Helper()
	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.Stage(ctx, tx, testTenant, testNS, rid, rec); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func hasEntry(t *testing.T, ctx context.Context, kv storage.KV, slot uint16, v attr.Value, rid id.ID) bool {
	t.Helper()
	_, err := kv.Get(ctx, attr.IndexKey(testTenant, testNS, slot, v, rid))
	if err != nil && !errs.Is(err, errs.NotFound) {
		t.Fatalf("reading an index entry: %v", err)
	}
	return err == nil
}

func countEntries(t *testing.T, ctx context.Context, kv storage.KV, slot uint16) int {
	t.Helper()
	lower, upper := keys.AttrSlotRange(testTenant, testNS, slot)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}

func runAll(t *testing.T, ctx context.Context, kv storage.KV, sel attr.Select) []id.ID {
	t.Helper()
	page, err := attr.Run(ctx, kv, sel)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	return page.IDs
}

// countingKV counts what a walk actually reads, split by key space, so
// "reads only the page it returns" is measured rather than asserted.
type countingKV struct {
	storage.KV
	rows   int
	bodies int
}

func (c *countingKV) reset() { c.rows, c.bodies = 0, 0 }

func (c *countingKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	_, _, space, err := keys.ParseSpace(key)
	if err == nil {
		switch space {
		case keys.SpaceAttrRow:
			c.rows++
		case keys.SpaceRecord:
			c.bodies++
		}
	}
	return c.KV.Get(ctx, key)
}
