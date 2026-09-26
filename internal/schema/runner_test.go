package schema_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

// --- a corpus, and a migration that walks it --------------------------------

const corpusSize = 100

func recordKey(i int) []byte {
	var rid [16]byte
	binary.BigEndian.PutUint64(rid[8:], uint64(i))
	return keys.Record("acme", tenant.DefaultNamespace, keys.RecordMemory, rid)
}

func seedCorpus(t *testing.T, kv storage.KV) {
	t.Helper()
	for i := 0; i < corpusSize; i++ {
		if err := kv.Set(ctx(), recordKey(i), []byte(fmt.Sprintf("memory %d", i))); err != nil {
			t.Fatal(err)
		}
	}
}

// walker is a background migration that visits every record once, writing a
// durable counter beside each.
//
// The counter is durable rather than in memory on purpose. A crash-and-resume
// test that counted in memory would be asserting that the *process* touched
// each record once, which is not the property that matters and is not even
// true: a record whose transaction never committed is legitimately revisited.
// What must hold is that each record is *committed* exactly once, and only a
// counter written inside the same transaction as the work can show that.
type walker struct {
	// crashAfter panics once this many records have been committed, standing in
	// for a process that dies mid-migration.
	crashAfter int
	// seen counts in-memory visits, so a test can assert that a resumed run
	// really did skip the work the cursor covers.
	seen int
}

func (w *walker) migration() schema.Migration {
	return schema.Migration{
		ID:           "walk-the-corpus",
		Requires:     map[string]uint32{"attr_schema": 1},
		Advances:     map[string]uint32{"attr_schema": 2},
		RewritesData: true,
		Strategy:     schema.StrategyBackground,
		Run:          w.run,
	}
}

func (w *walker) run(c context.Context, mc *schema.Context) error {
	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceRecord)
	if mc.Resume != nil {
		lower = append(append([]byte(nil), mc.Resume...), 0x00) // strictly after
	}
	processed := mc.Done

	it := mc.KV.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		key := append([]byte(nil), it.Key()...)
		w.seen++

		tx := txn.New(mc.KV)
		mark := markFor(key)
		prev, err := tx.Get(mark)
		if err != nil && !errs.Is(err, errs.NotFound) {
			tx.Close()
			return err
		}
		tx.Set(mark, []byte{count(prev) + 1})

		processed++
		if w.crashAfter > 0 && int(processed) > w.crashAfter {
			tx.Close()
			panic("the process died mid-migration")
		}
		if err := mc.Checkpoint(c, tx, key, processed); err != nil {
			tx.Close()
			return err
		}
		tx.Close()
	}
	return it.Error()
}

// markFor turns a record key into its counter key by swapping the space byte.
func markFor(recordKey []byte) []byte {
	k := append([]byte(nil), recordKey...)
	k[2] = byte(keys.SpaceSession)
	return k
}

func count(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

func assertEveryRecordCommittedOnce(t *testing.T, kv storage.KV) {
	t.Helper()
	for i := 0; i < corpusSize; i++ {
		v, err := kv.Get(ctx(), markFor(recordKey(i)))
		if err != nil {
			t.Fatalf("record %d was never migrated: %v", i, err)
		}
		if len(v) != 1 || v[0] != 1 {
			t.Fatalf("record %d was committed %d times, not once", i, count(v))
		}
	}
}

func newRunner(t *testing.T, kv storage.KV, dataDir string) *schema.Runner {
	t.Helper()
	r, err := schema.NewRunner(schema.RunnerConfig{
		KV:      kv,
		Clock:   clock.NewFake(at2026()),
		DataDir: dataDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// migrated is the manifest a completed walk produces.
func migratedTo(v uint32) version.Versions {
	want := version.Current()
	want.AttrSchema = v
	return want
}

// --- the tests --------------------------------------------------------------

// The plan's completion criterion, at each of its three thresholds. A run that
// dies partway must resume and finish with every record committed exactly once
// — which is the difference between a migration that is resumable and one that
// merely restarts.
func TestBackgroundMigrationResumesAfterCrash(t *testing.T) {
	for _, crashAt := range []int{10, 40, 90} {
		t.Run(fmt.Sprintf("crash after %d%%", crashAt), func(t *testing.T) {
			kv := newKV(t)
			dir := t.TempDir()
			if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
				t.Fatal(err)
			}
			seedCorpus(t, kv)

			dying := &walker{crashAfter: crashAt}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("the migration was expected to die and did not")
					}
				}()
				_ = newRunner(t, kv, dir).Run(ctx(), []schema.Migration{dying.migration()})
			}()

			// The state row is what a crash leaves behind: running, with a
			// cursor that points at the last committed record.
			state, ok, err := schema.ReadState(ctx(), kv, "walk-the-corpus")
			if err != nil || !ok {
				t.Fatalf("no state was left to resume from: ok=%v err=%v", ok, err)
			}
			if state.State != schema.StateRunning {
				t.Fatalf("state is %s; a crashed migration is left running", state.State)
			}
			if state.Processed == 0 || int(state.Processed) >= corpusSize {
				t.Fatalf("the cursor recorded %d of %d records", state.Processed, corpusSize)
			}

			// Restart. A fresh walker, as a fresh process would have.
			resumed := &walker{}
			if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{resumed.migration()}); err != nil {
				t.Fatal(err)
			}

			assertEveryRecordCommittedOnce(t, kv)

			// And the resumed run really skipped the work the cursor covered,
			// rather than redoing it and overwriting the counters.
			if resumed.seen >= corpusSize {
				t.Fatalf("the resumed run visited %d records; the cursor covered %d of them",
					resumed.seen, state.Processed)
			}

			f, err := schema.ReadFormat(ctx(), kv)
			if err != nil {
				t.Fatal(err)
			}
			if got := f.Subsystems["attr_schema"].Current; got != 2 {
				t.Fatalf("the manifest is at attr_schema %d after a completed migration", got)
			}
			done, _, err := schema.ReadState(ctx(), kv, "walk-the-corpus")
			if err != nil {
				t.Fatal(err)
			}
			if done.State != schema.StateDone {
				t.Fatalf("state is %s after a completed migration", done.State)
			}
		})
	}
}

// The invariant the checkpoint exists for: the cursor is staged into the
// caller's transaction, so a commit that fails advances neither the work nor
// the cursor. A cursor that outran its work would skip records on resume, and
// nothing downstream would ever notice.
func TestProgressIsWrittenInTheWorkTransaction(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	broken := &failingCommits{KV: kv}
	r, err := schema.NewRunner(schema.RunnerConfig{KV: broken, Clock: clock.NewFake(at2026()), DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	w := &walker{}
	if err := r.Run(ctx(), []schema.Migration{w.migration()}); err == nil {
		t.Fatal("a migration whose commits all fail reported success")
	}

	// No work landed...
	if _, err := kv.Get(ctx(), markFor(recordKey(0))); !errs.Is(err, errs.NotFound) {
		t.Fatalf("work committed through a failing batch: %v", err)
	}
	// ...and neither did the cursor.
	state, ok, err := schema.ReadState(ctx(), kv, "walk-the-corpus")
	if err != nil {
		t.Fatal(err)
	}
	if ok && len(state.Cursor) > 0 {
		t.Fatalf("the cursor advanced past work that never committed: %x", state.Cursor)
	}
	if ok && state.Processed != 0 {
		t.Fatalf("the processed count advanced past work that never committed: %d", state.Processed)
	}
	if !ok || state.State != schema.StateFailed {
		t.Fatalf("a failed migration must leave a failed state row: ok=%v state=%v", ok, state.State)
	}
	if state.Error == "" {
		t.Fatal("the state row must carry why it failed; an operator should not need the log that scrolled past")
	}
}

// Idempotence: a plan that has already run is not re-run, and re-running it
// does not touch a byte. Anything else would mean an ordinary restart rewrote
// the corpus.
func TestMigrationIsIdempotent(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	plan := []schema.Migration{(&walker{}).migration()}
	if err := newRunner(t, kv, dir).Run(ctx(), plan); err != nil {
		t.Fatal(err)
	}

	var before bytes.Buffer
	if err := storage.Dump(ctx(), kv, &before); err != nil {
		t.Fatal(err)
	}

	second := &walker{}
	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{second.migration()}); err != nil {
		t.Fatal(err)
	}
	if second.seen != 0 {
		t.Fatalf("a completed migration ran again and visited %d records", second.seen)
	}

	var after bytes.Buffer
	if err := storage.Dump(ctx(), kv, &after); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.Bytes(), after.Bytes()) {
		t.Fatal("re-running a completed plan changed the store")
	}
}

// The manifest advances step by step, not once at the end. A crash after step
// one must not put step one back on the plan.
func TestTheManifestAdvancesAfterEachStep(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}

	failing := step("attr-2-to-3", "attr_schema", 2, 3)
	failing.Run = func(context.Context, *schema.Context) error { return errors.New("this step fails") }

	plan := []schema.Migration{step("attr-1-to-2", "attr_schema", 1, 2), failing}
	if err := newRunner(t, kv, dir).Run(ctx(), plan); err == nil {
		t.Fatal("a plan whose second step fails reported success")
	}

	f, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Subsystems["attr_schema"].Current; got != 2 {
		t.Fatalf("the manifest is at %d; the first step succeeded and the second did not", got)
	}
}

// --- the backup, from the runner's side -------------------------------------

func TestBackupPrecedesTheFirstRewrite(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	// The step asserts, from inside itself, that the copy already exists.
	rewriting := step("rewrites", "attr_schema", 1, 2)
	rewriting.RewritesData = true
	var sawBackup bool
	rewriting.Run = func(context.Context, *schema.Context) error {
		sawBackup = backupExists(t, dir)
		return nil
	}

	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{rewriting}); err != nil {
		t.Fatal(err)
	}
	if !sawBackup {
		t.Fatal("the step ran before its backup was taken")
	}
}

func TestNoBackupForANonRewritingStep(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{step("adds-rows", "attr_schema", 1, 2)}); err != nil {
		t.Fatal(err)
	}
	if backupExists(t, dir) {
		t.Fatal("a step that rewrites nothing leaves the originals intact and needs no copy")
	}
}

func TestBackupIsTakenOncePerPlan(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	first := step("first", "attr_schema", 1, 2)
	first.RewritesData = true
	second := step("second", "attr_schema", 2, 3)
	second.RewritesData = true

	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{first, second}); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(filepath.Join(dir, schema.BackupsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("a plan took %d backups; one copy of the pre-migration state is the point", len(entries))
	}
}

func TestBackupFailureAbortsWithDataUntouched(t *testing.T) {
	kv := newKV(t)
	dir := t.TempDir()
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}
	seedCorpus(t, kv)

	// A file where the backups directory has to go.
	if err := os.WriteFile(filepath.Join(dir, schema.BackupsDir), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}

	var ran bool
	rewriting := step("rewrites", "attr_schema", 1, 2)
	rewriting.RewritesData = true
	rewriting.Run = func(context.Context, *schema.Context) error { ran = true; return nil }

	if err := newRunner(t, kv, dir).Run(ctx(), []schema.Migration{rewriting}); err == nil {
		t.Fatal("a plan whose backup failed reported success")
	}
	if ran {
		t.Fatal("the step ran despite its backup failing")
	}
	f, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Subsystems["attr_schema"].Current; got != 1 {
		t.Fatalf("the manifest advanced to %d despite the step never running", got)
	}
	if _, err := kv.Get(ctx(), recordKey(0)); err != nil {
		t.Fatalf("the corpus did not survive a failed backup: %v", err)
	}
}

func backupExists(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, schema.BackupsDir))
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return len(entries) > 0
}

// --- construction -----------------------------------------------------------

func TestRunnerRefusesAnIncompleteConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  schema.RunnerConfig
	}{
		{"no store", schema.RunnerConfig{Clock: clock.NewFake(at2026()), DataDir: "/tmp"}},
		{"no clock", schema.RunnerConfig{KV: newKV(t), DataDir: "/tmp"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := schema.NewRunner(tc.cfg); !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestAnEmptyPlanIsNotAnError(t *testing.T) {
	kv := newKV(t)
	if err := newRunner(t, kv, t.TempDir()).Run(ctx(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedStepNamesItselfInTheError(t *testing.T) {
	kv := newKV(t)
	if _, err := schema.Open(ctx(), kv, migratedTo(1)); err != nil {
		t.Fatal(err)
	}

	failing := step("attr-1-to-2", "attr_schema", 1, 2)
	failing.Run = func(context.Context, *schema.Context) error { return errors.New("the disk went away") }

	err := newRunner(t, kv, t.TempDir()).Run(ctx(), []schema.Migration{failing})
	if err == nil {
		t.Fatal("no error")
	}
	if !strings.Contains(err.Error(), "attr-1-to-2") {
		t.Fatalf("the error must name the step an operator has to look at: %v", err)
	}
}

// --- a store whose batches never commit -------------------------------------

type failingCommits struct{ storage.KV }

func (f *failingCommits) NewBatch() storage.Batch { return brokenBatch{f.KV.NewBatch()} }

// Backup is forwarded so the wrapper reaches the step at all: a store that
// cannot copy itself is refused before any migration that rewrites data runs,
// and this test is about the commits, not about the backup.
func (f *failingCommits) Backup(ctx context.Context, dest string) error {
	return f.KV.(storage.Backupper).Backup(ctx, dest)
}

type brokenBatch struct{ storage.Batch }

func (brokenBatch) Commit(context.Context, bool) error {
	return errs.E(errs.Storage, "test.Commit", errors.New("the write did not land"))
}
