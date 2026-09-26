package schema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// The attribute backfill is the step that decides whether an existing
// installation can be upgraded at all. Without it, a directory written before
// the attribute index existed serves every listing as an empty page — not an
// error, not a warning, an empty page from a corpus of thousands.

func TestTheBackfillBuildsRowsForACorpusWrittenWithoutThem(t *testing.T) {
	const corpus = 250
	h := newBackfillHarness(t, corpus)

	// The precondition: bodies exist, and the access path is empty.
	if got := h.indexedRecords(t); got != 0 {
		t.Fatalf("the corpus starts with %d indexed records, want 0 — the test is not "+
			"reproducing a directory written before the index existed", got)
	}

	h.run(t, nil, nil)

	if got := h.indexedRecords(t); got != corpus {
		t.Fatalf("after the backfill %d of %d records are listable", got, corpus)
	}
	for _, def := range attr.MustTable().IndexedSlots() {
		if got := h.entriesIn(t, def.Slot); got != corpus {
			t.Errorf("slot %d (%q) holds %d entries, want %d: an ordering by it would return "+
				"a partial corpus and say nothing about it", def.Slot, def.Name, got, corpus)
		}
	}
}

// A crash mid-backfill must resume where it stopped, not restart and not skip.
//
// The cursor and the rows commit in one transaction, which is what makes resume
// exact rather than approximate — there is no window in which the cursor says a
// record is indexed and it is not.
func TestTheBackfillResumesFromItsCheckpoint(t *testing.T) {
	// Larger than one checkpointed batch, so there is a second checkpoint to
	// fail at. A corpus that fits in one batch never checkpoints twice, and a
	// resume test over it would be asserting nothing.
	const corpus = 2500
	h := newBackfillHarness(t, corpus)

	// Fail the second checkpoint, standing in for a process that dies partway.
	var cursor []byte
	var done uint64
	commits := 0
	err := h.runRaw(t, nil, 0, func(ctx context.Context, tx txn.Tx, c []byte, p uint64) error {
		commits++
		if commits > 1 {
			return errs.E(errs.Storage, "test", fmt.Errorf("the disk went away"))
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		cursor, done = append([]byte(nil), c...), p
		return nil
	})
	if err == nil {
		t.Fatal("the interrupted run reported success")
	}
	partial := h.indexedRecords(t)
	if partial == 0 || partial >= corpus {
		t.Fatalf("the interrupted run indexed %d of %d records; the test needs it to stop partway",
			partial, corpus)
	}

	// Resume from the checkpoint the first run committed, and count the work.
	h.indexer.staged = 0
	h.run(t, cursor, &done)

	if want := corpus - partial; h.indexer.staged != want {
		t.Errorf("the resumed run staged %d records; want %d, the ones the committed cursor does "+
			"not cover. Staging is idempotent, so a run that ignored its cursor would leave the "+
			"same rows behind and redo %d records to get there", h.indexer.staged, want, corpus)
	}

	if got := h.indexedRecords(t); got != corpus {
		t.Fatalf("after resuming, %d of %d records are listable", got, corpus)
	}
	for _, def := range attr.MustTable().IndexedSlots() {
		if got := h.entriesIn(t, def.Slot); got != corpus {
			t.Errorf("slot %d (%q) holds %d entries after a resume, want %d — a record indexed "+
				"twice leaves a duplicate entry and one skipped leaves none",
				def.Slot, def.Name, got, corpus)
		}
	}
}

func TestTheBackfillCoversEveryTenant(t *testing.T) {
	h := newBackfillHarness(t, 0)
	for _, tn := range []tenant.ID{"acme", "acmecorp", "globex"} {
		h.ensureTenant(t, tn)
		h.seedFor(t, tn, 20)
	}

	h.run(t, nil, nil)

	for _, tn := range []tenant.ID{"acme", "acmecorp", "globex"} {
		if got := h.indexedFor(t, tn); got != 20 {
			t.Errorf("tenant %q has %d listable records, want 20", tn, got)
		}
	}
}

// The step must run before the server listens. A half-built access path does
// not return older answers, it returns fewer — and Truncated is reserved for
// the widening bound, so there is no honest way to report it in a response.
func TestTheBackfillRunsBeforeTheServerListens(t *testing.T) {
	reg, err := schema.Builtin(version.Current(), attr.NewIndexer(attr.MustTable()), nil)
	if err != nil {
		t.Fatalf("building the shipped registry: %v", err)
	}

	var step *schema.Migration
	for _, m := range reg.All() {
		if m.ID == schema.AttrBackfillID {
			step = &m
			break
		}
	}
	if step == nil {
		t.Fatalf("the shipped registry has no %q step", schema.AttrBackfillID)
	}
	if step.Strategy != schema.StrategyRebuild {
		t.Errorf("the backfill's strategy is %s; a background one would serve partial listings "+
			"while it worked", step.Strategy)
	}
	if step.RewritesData {
		t.Error("the backfill declares that it rewrites data; it only adds rows, and a " +
			"pre-migration backup would preserve nothing it touches")
	}

	startup, background := schema.Split([]schema.Migration{*step})
	if len(startup) != 1 || len(background) != 0 {
		t.Errorf("the plan split put %d steps at start-up and %d in the background; the backfill "+
			"belongs before the listener binds", len(startup), len(background))
	}
}

// A directory this binary wrote needs no backfill: the step's precondition is
// exact, so it does not re-run on every start.
func TestTheBackfillDoesNotReplanOnACurrentDirectory(t *testing.T) {
	reg, err := schema.Builtin(version.Current(), attr.NewIndexer(attr.MustTable()), nil)
	if err != nil {
		t.Fatal(err)
	}
	current := schema.Format{Subsystems: map[string]schema.Subsystem{}}
	for _, name := range version.FormatNames() {
		v, _ := version.Current().Get(name)
		current.Subsystems[name] = schema.Subsystem{Current: v, MinReader: v, MinWriter: v}
	}

	plan, err := reg.Plan(current, version.Current())
	if err != nil {
		t.Fatalf("planning against a current directory: %v", err)
	}
	if len(plan) != 0 {
		t.Errorf("a directory this binary wrote planned %d steps, want none", len(plan))
	}
}

// --- harness ----------------------------------------------------------------

type backfillHarness struct {
	kv      storage.KV
	dir     tenant.Directory
	indexer *countingIndexer
}

// countingIndexer counts records staged, which is the only way to tell a resume
// from a restart.
//
// Staging is idempotent — the same record produces the same row and the same
// entries — so a run that ignored its cursor entirely would leave exactly the
// state a correct resume leaves. Asserting on the final state alone therefore
// proves idempotence and nothing about resumption; the work done is what
// separates them.
type countingIndexer struct {
	*attr.Indexer
	staged int
}

func (c *countingIndexer) Stage(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	rid id.ID, rec *record.Record) error {
	c.staged++
	return c.Indexer.Stage(ctx, tx, t, ns, rid, rec)
}

func newBackfillHarness(t *testing.T, corpus int) *backfillHarness {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	h := &backfillHarness{
		kv:      kv,
		dir:     tenantkv.New(kv, clock.NewFake(clock.FakeStart)),
		indexer: &countingIndexer{Indexer: attr.NewIndexer(attr.MustTable())},
	}
	if corpus > 0 {
		h.ensureTenant(t, "acme")
		h.seedFor(t, "acme", corpus)
	}
	return h
}

func (h *backfillHarness) ensureTenant(t *testing.T, tn tenant.ID) {
	t.Helper()
	if _, err := tenant.Ensure(context.Background(), h.dir, tn); err != nil {
		t.Fatalf("provisioning tenant %q: %v", tn, err)
	}
}

// seedFor writes records the way a build with no attribute index would have:
// bodies only, through a repository with no indexer.
func (h *backfillHarness) seedFor(t *testing.T, tn tenant.ID, n int) {
	t.Helper()
	repo := record.NewRepo(h.kv)
	ctx := tenant.NewContext(context.Background(), tn)
	when := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	tx := txn.New(h.kv)
	defer tx.Close()
	for i := 0; i < n; i++ {
		rec := &record.Record{
			ID:        id.New(),
			Tenant:    tn,
			Namespace: tenant.DefaultNamespace,
			Type:      record.TypeMemory,
			Content:   fmt.Sprintf("memory %d", i),
			CreatedAt: when.Add(time.Duration(i) * time.Second),
			UpdatedAt: when.Add(time.Duration(i) * time.Second),
			Fields:    record.Fields{}.WithDefaults(),
		}
		if err := repo.Put(ctx, tx, rec); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("committing the seed corpus: %v", err)
	}
}

// run executes the shipped backfill step to completion.
func (h *backfillHarness) run(t *testing.T, resume []byte, done *uint64) {
	t.Helper()
	var from uint64
	if done != nil {
		from = *done
	}
	err := h.runRaw(t, resume, from, func(ctx context.Context, tx txn.Tx, _ []byte, _ uint64) error {
		return tx.Commit(ctx)
	})
	if err != nil {
		t.Fatalf("running the backfill: %v", err)
	}
}

// runRaw executes the step with a checkpoint the caller controls, so a test can
// interrupt it exactly where it chooses.
func (h *backfillHarness) runRaw(t *testing.T, resume []byte, done uint64,
	checkpoint func(context.Context, txn.Tx, []byte, uint64) error) error {
	t.Helper()

	reg, err := schema.Builtin(version.Current(), h.indexer, nil)
	if err != nil {
		t.Fatalf("building the shipped registry: %v", err)
	}
	for _, m := range reg.All() {
		if m.ID != schema.AttrBackfillID {
			continue
		}
		return m.Run(context.Background(), &schema.Context{
			KV:         h.kv,
			Tenants:    h.dir,
			Clock:      clock.NewFake(clock.FakeStart),
			Resume:     resume,
			Done:       done,
			Checkpoint: checkpoint,
		})
	}
	t.Fatalf("the shipped registry has no %q step", schema.AttrBackfillID)
	return nil
}

// indexedRecords counts what a listing over "acme" would actually find, which
// is the property that matters — not how many rows exist, but how many records
// the access path reaches.
func (h *backfillHarness) indexedRecords(t *testing.T) int { return h.indexedFor(t, "acme") }

func (h *backfillHarness) indexedFor(t *testing.T, tn tenant.ID) int {
	t.Helper()
	page, err := attr.Run(context.Background(), h.kv, attr.Select{
		Tenant: tn, Namespace: tenant.DefaultNamespace, Slots: attr.MustTable(),
		Slot: attr.SlotCreatedAt, Limit: 100000, Effort: 1000000,
	})
	if err != nil {
		t.Fatalf("walking the created_at index: %v", err)
	}
	return len(page.IDs)
}

func (h *backfillHarness) entriesIn(t *testing.T, slot uint16) int {
	t.Helper()
	lower, upper := keys.AttrSlotRange("acme", tenant.DefaultNamespace, slot)
	it := h.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}
