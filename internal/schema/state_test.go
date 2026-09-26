package schema_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/txn"
)

func sampleState() schema.MigrationState {
	return schema.MigrationState{
		ID:             "attr-slots-v2",
		Source:         map[string]uint32{"attr_schema": 1},
		Target:         map[string]uint32{"attr_schema": 2},
		State:          schema.StateRunning,
		Cursor:         []byte{0x00, 0xFF, 0x10},
		Processed:      4096,
		StartedAt:      time.UnixMilli(1_700_000_000_000).UTC(),
		LastProgressAt: time.UnixMilli(1_700_000_060_000).UTC(),
	}
}

func TestMigrationStateRoundTripsThroughDisk(t *testing.T) {
	kv := newKV(t)
	want := sampleState()

	if err := schema.WriteState(ctx(), kv, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := schema.ReadState(ctx(), kv, want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the row was written and did not read back")
	}
	if !got.Equal(want) {
		t.Fatalf("round trip changed the state:\n got %+v\nwant %+v", got, want)
	}
}

func TestAnAbsentMigrationStateIsNotAnError(t *testing.T) {
	kv := newKV(t)
	// A step that has never run has no row. That is the first run, not damage.
	_, ok, err := schema.ReadState(ctx(), kv, "never-ran")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("an empty store reported a migration state")
	}
}

// A state row that will not parse is corruption, never an absent row: reading
// damage as "never started" would restart a half-finished migration from the
// beginning, against a corpus it has already partly rewritten.
func TestADamagedMigrationStateIsCorruption(t *testing.T) {
	kv := newKV(t)
	if err := kv.Set(ctx(), schema.StateKey("x"), []byte{0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := schema.ReadState(ctx(), kv, "x"); !errs.Is(err, errs.Corruption) {
		t.Fatalf("got %v", err)
	}
}

// The id in the key and the id in the body are two copies of one fact. When
// they disagree, something wrote a state row under the wrong key, and resuming
// from it would resume the wrong migration.
func TestAMigrationStateUnderTheWrongKeyIsCorruption(t *testing.T) {
	kv := newKV(t)
	s := sampleState()
	if err := schema.WriteState(ctx(), kv, s); err != nil {
		t.Fatal(err)
	}
	value, err := kv.Get(ctx(), schema.StateKey(s.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx(), schema.StateKey("somebody-else"), value); err != nil {
		t.Fatal(err)
	}
	if _, _, err := schema.ReadState(ctx(), kv, "somebody-else"); !errs.Is(err, errs.Corruption) {
		t.Fatalf("got %v", err)
	}
}

func TestListStatesReturnsEveryRowInIDOrder(t *testing.T) {
	kv := newKV(t)
	for _, id := range []string{"c", "a", "b"} {
		s := sampleState()
		s.ID = id
		if err := schema.WriteState(ctx(), kv, s); err != nil {
			t.Fatal(err)
		}
	}

	got, err := schema.ListStates(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Fatalf("got %v", got)
	}
}

// State rows live in the untenanted system space beside the manifest, and they
// must not collide with the tenant directory or the format rows that share it.
func TestStateKeysDoNotCollideWithOtherSystemRows(t *testing.T) {
	k := schema.StateKey("attr-slots-v2")
	for name, other := range map[string][]byte{
		"the slot schema": schema.SlotsKey(),
	} {
		if bytes.Equal(k, other) {
			t.Fatalf("the migration state key collides with %s", name)
		}
	}
	lo, hi := schema.StateRange()
	if bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
		t.Fatal("a state key falls outside the range that scans for state rows")
	}
	if bytes.Compare(schema.SlotsKey(), lo) >= 0 && bytes.Compare(schema.SlotsKey(), hi) < 0 {
		t.Fatal("the slot schema row falls inside the migration state range and would be scanned as one")
	}
}

// StageState is what the checkpoint path uses: the cursor is staged into the
// caller's transaction so it commits with the work, never before or after it.
func TestStageStateIsInvisibleUntilTheTransactionCommits(t *testing.T) {
	kv := newKV(t)
	s := sampleState()

	tx := txn.New(kv)
	defer tx.Close()
	if err := schema.StageState(tx, s); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := schema.ReadState(ctx(), kv, s.ID); err != nil || ok {
		t.Fatalf("a staged state was visible before commit: ok=%v err=%v", ok, err)
	}
	if err := tx.Commit(ctx()); err != nil {
		t.Fatal(err)
	}
	got, ok, err := schema.ReadState(ctx(), kv, s.ID)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if !got.Equal(s) {
		t.Fatalf("got %+v", got)
	}
}
