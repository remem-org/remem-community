//go:build crash

package crash

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/version"
)

const (
	migrationCorpus = 5500
	migrationStep   = 500
	migrationID     = "crash-walk-the-corpus"
	migrationTenant = tenant.ID("acme")
)

func init() { scenarios["migrate"] = childMigrate }

// migrationVersions is the manifest the corpus starts at: one attribute
// schema version behind what the test migration advances it to.
func migrationVersions() version.Versions {
	v := version.Current()
	v.AttrSchema = 1
	return v
}

func migrationRID(i int) id.ID {
	var rid id.ID
	binary.BigEndian.PutUint64(rid[8:], uint64(i))
	return rid
}

func migrationRecordKey(i int) []byte {
	return keys.Record(migrationTenant, tenant.DefaultNamespace, keys.RecordMemory, migrationRID(i))
}

// migrationMark is the durable per-record counter the walk writes. It lives in
// the session space only because that is a canonical space this test's corpus
// does not otherwise use.
func migrationMark(rid id.ID) []byte {
	return keys.Session(migrationTenant, tenant.DefaultNamespace, rid)
}

// childMigrate seeds the corpus on first run, then runs the real migration
// runner over it until the migration completes or the parent kills the process.
func childMigrate(t *testing.T, dir string) {
	ctx := context.Background()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatalf("opening %s: %v", dir, err)
	}
	defer func() { _ = kv.Close() }()

	if _, err := schema.Open(ctx, kv, migrationVersions()); err != nil {
		t.Fatalf("schema.Open: %v", err)
	}
	if _, err := kv.Get(ctx, migrationRecordKey(0)); errs.Is(err, errs.NotFound) {
		for start := 0; start < migrationCorpus; start += migrationStep {
			tx := txn.New(kv)
			for i := start; i < min(start+migrationStep, migrationCorpus); i++ {
				tx.Set(migrationRecordKey(i), []byte(fmt.Sprintf("memory %d", i)))
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("seeding: %v", err)
			}
			tx.Close()
		}
	} else if err != nil {
		t.Fatalf("probing the corpus: %v", err)
	}

	r, err := schema.NewRunner(schema.RunnerConfig{KV: kv, Clock: clock.System()})
	if err != nil {
		t.Fatal(err)
	}
	w := &crashWalker{}
	if err := r.Run(ctx, []schema.Migration{w.migration()}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	fmt.Printf("migrated visited=%d\n", w.seen)
}

// crashWalker is a background migration that visits every record once and
// writes a durable counter beside it, in the transaction that carries the
// cursor. It prints each 500th checkpoint *after* that transaction commits, so
// a parent that reads the line knows that much is on disk.
type crashWalker struct{ seen int }

func (w *crashWalker) migration() schema.Migration {
	return schema.Migration{
		ID:       migrationID,
		Requires: map[string]uint32{"attr_schema": 1},
		Advances: map[string]uint32{"attr_schema": 2},
		// No backup: what is under test is the resume, and a pre-migration
		// checkpoint of the directory on every restart would only slow it.
		RewritesData: false,
		Strategy:     schema.StrategyBackground,
		Run:          w.run,
	}
}

func (w *crashWalker) run(ctx context.Context, mc *schema.Context) error {
	mc.Total = migrationCorpus
	lower, upper := keys.SpaceRange(migrationTenant, tenant.DefaultNamespace, keys.SpaceRecord)
	if mc.Resume != nil {
		lower = append(append([]byte(nil), mc.Resume...), 0x00) // strictly after
	}
	processed := mc.Done

	it := mc.KV.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		key := append([]byte(nil), it.Key()...)
		_, _, _, rid, err := keys.ParseRecord(key)
		if err != nil {
			return err
		}
		w.seen++

		tx := txn.New(mc.KV)
		prev, err := tx.Get(migrationMark(rid))
		if err != nil && !errs.Is(err, errs.NotFound) {
			tx.Close()
			return err
		}
		count := byte(0)
		if len(prev) == 1 {
			count = prev[0]
		}
		tx.Set(migrationMark(rid), []byte{count + 1})

		processed++
		if err := mc.Checkpoint(ctx, tx, key, processed); err != nil {
			tx.Close()
			return err
		}
		tx.Close()
		if processed%migrationStep == 0 {
			fmt.Printf("checkpoint %d\n", processed)
		}
	}
	return it.Error()
}

// migrationMarks counts how many records carry no mark, one, and more than one.
type migrationMarks struct{ missing, once, more int }

func readMigration(t *testing.T, dir string) (schema.MigrationState, migrationMarks, schema.Format) {
	t.Helper()
	ctx := context.Background()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatalf("reopening after the kill: %v", err)
	}
	defer func() { _ = kv.Close() }()

	state, found, err := schema.ReadState(ctx, kv, migrationID)
	if err != nil || !found {
		t.Fatalf("no migration state row: found=%v err=%v", found, err)
	}
	var m migrationMarks
	for i := 0; i < migrationCorpus; i++ {
		v, err := kv.Get(ctx, migrationMark(migrationRID(i)))
		switch {
		case errs.Is(err, errs.NotFound):
			m.missing++
		case err != nil:
			t.Fatal(err)
		case len(v) == 1 && v[0] == 1:
			m.once++
		default:
			m.more++
		}
	}
	f, err := schema.ReadFormat(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	return state, m, f
}

// TestCrashDuringMigrationResumes: kill the migration runner at ten points,
// restarting after each, and it finishes with every record migrated exactly
// once.
//
// The strongest assertion is after each kill, not at the end: the number of
// records carrying a mark equals the count the durable cursor claims. That is
// the checkpoint's whole promise — work and cursor land together — checked
// against a process that died between one commit and the next.
func TestCrashDuringMigrationResumes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	for k := 1; k <= 10; k++ {
		target := uint64(k * migrationStep)
		c := spawn(t, "migrate", dir)
		var at uint64
		for at < target {
			n, err := strconv.ParseUint(c.waitFor("checkpoint ", 3*time.Minute), 10, 64)
			if err != nil {
				t.Fatalf("an unreadable checkpoint line: %v", err)
			}
			at = n
		}
		c.kill()

		state, marks, _ := readMigration(t, dir)
		switch {
		case state.State != schema.StateRunning:
			t.Fatalf("kill %d at checkpoint %d: the state row is %s, want running", k, at, state.State)
		case state.Processed < at:
			t.Fatalf("kill %d: the child printed checkpoint %d after committing it, and the state row says %d",
				k, at, state.Processed)
		case marks.more > 0:
			t.Fatalf("kill %d: %d records were committed more than once", k, marks.more)
		case uint64(marks.once) != state.Processed:
			t.Fatalf("kill %d: %d records carry a mark and the cursor claims %d: work and cursor did not commit together",
				k, marks.once, state.Processed)
		}
		t.Logf("kill %d: printed checkpoint %d, the directory holds %d processed", k, at, state.Processed)
	}

	c := spawn(t, "migrate", dir)
	visited, err := strconv.Atoi(c.waitFor("migrated visited=", 3*time.Minute))
	if err != nil {
		t.Fatalf("an unreadable completion line: %v", err)
	}
	c.wait(time.Minute)

	state, marks, format := readMigration(t, dir)
	switch {
	case state.State != schema.StateDone:
		t.Fatalf("after the final run the state row is %s, want done", state.State)
	case marks.once != migrationCorpus || marks.missing != 0 || marks.more != 0:
		t.Fatalf("after ten kills and a completion: %d migrated once, %d never, %d more than once",
			marks.once, marks.missing, marks.more)
	case format.Subsystems["attr_schema"].Current != 2:
		t.Fatalf("the manifest is at attr_schema %d after the migration completed",
			format.Subsystems["attr_schema"].Current)
	case visited > migrationCorpus-10*migrationStep:
		t.Fatalf("the final run visited %d records; ten resumes should have left at most %d",
			visited, migrationCorpus-10*migrationStep)
	}
	t.Logf("completed: the final run visited %d records", visited)
}
