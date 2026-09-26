package snapshot_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/snapshot"
)

// errAfter is a reader that dies partway, the way a process does.
type errAfter struct {
	src  *bytes.Reader
	left int
}

var errDied = errors.New("the import process died here")

func (e *errAfter) Read(p []byte) (int, error) {
	if e.left <= 0 {
		return 0, errDied
	}
	if len(p) > e.left {
		p = p[:e.left]
	}
	n, err := e.src.Read(p)
	e.left -= n
	return n, err
}

// A crashed import resumes where the cursor says, and the result is the corpus
// the file describes — no duplicates, nothing missing.
//
// The corpus is paged finely so that the file is many blocks rather than one:
// a resume test over a single block would resume at the beginning and prove
// nothing about the cursor.
func TestImportResumes(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	for i := range 40 {
		w.store(t, "default", "memory number "+string(rune('a'+i%26))+" of the corpus")
	}
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{PageSize: 4})

	d := newDestination(t, fakeEmbedder())

	// Run until the reader dies, keeping the last cursor a committed batch
	// reported. That is exactly what `remem-admin import` persists.
	var cursor []byte
	var atDeath snapshot.Stats
	_, err := snapshot.Import(context.Background(), d.dst,
		&errAfter{src: bytes.NewReader(raw), left: len(raw) / 2},
		snapshot.ImportOpts{Progress: func(p snapshot.Progress) {
			cursor, atDeath = p.Cursor, p.Stats
		}})
	if err == nil {
		t.Fatal("the truncated read did not fail")
	}
	if len(cursor) == 0 {
		t.Fatal("nothing was committed before the death, so there is no resume to test")
	}
	if atDeath.Records == 0 || atDeath.Records >= 40 {
		t.Fatalf("the run died after %d of 40 records, which is not partway", atDeath.Records)
	}

	rep, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Resume: cursor})
	must(t, err)

	if rep.Stats.Records != 40 {
		t.Fatalf("the resumed import accounts for %d records, want 40", rep.Stats.Records)
	}
	if got := count(t, d.kv, keys.SpaceRecord); got != 40 {
		t.Fatalf("the store holds %d records, want 40", got)
	}
	// Nothing was written twice: the resumed run skipped the blocks the first
	// one applied, and any it re-applied would have been identical anyway.
	if rep.Skipped != 0 {
		t.Fatalf("the resumed import treated %d rows as conflicts", rep.Skipped)
	}
}

// Resuming from the beginning is always correct, because every row is addressed
// by its own id. The cursor saves the work, not the correctness — so the same
// file imported twice from scratch gives the same corpus.
func TestResumeIsOnlyAnOptimisation(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	for range 10 {
		w.store(t, "default", "a memory")
	}
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{PageSize: 2})

	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst,
		&errAfter{src: bytes.NewReader(raw), left: len(raw) / 2}, snapshot.ImportOpts{})
	if err == nil {
		t.Fatal("the truncated read did not fail")
	}

	// No cursor at all: start again from the top.
	rep := d.importAll(t, raw, snapshot.ImportOpts{})
	if got := count(t, d.kv, keys.SpaceRecord); got != 10 {
		t.Fatalf("the store holds %d records, want 10", got)
	}
	if rep.Unchanged == 0 {
		t.Fatal("re-importing found nothing already present, so the first run wrote nothing")
	}
}

// A cursor from a different snapshot names a byte offset that means something
// else in this one, so it is refused rather than followed.
func TestACursorFromAnotherSnapshotIsRefused(t *testing.T) {
	w := populated(t)
	mine, _ := exportWorld(t, w, snapshot.ExportOpts{PageSize: 1})

	other := newWorld(t)
	other.tenant(t, "default")
	other.store(t, "default", "a different corpus entirely")
	theirs, _ := exportWorld(t, other, snapshot.ExportOpts{PageSize: 1})

	var cursor []byte
	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(theirs),
		snapshot.ImportOpts{Progress: func(p snapshot.Progress) { cursor = p.Cursor }})
	must(t, err)
	if len(cursor) == 0 {
		t.Fatal("the reference import produced no cursor")
	}

	other2 := newDestination(t, fakeEmbedder())
	_, err = snapshot.Import(context.Background(), other2.dst, bytes.NewReader(mine),
		snapshot.ImportOpts{Resume: cursor})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want Invalid, got %v", err)
	}
	if !strings.Contains(err.Error(), "belongs to a different snapshot") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
}

func TestAMalformedCursorIsRefused(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())

	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Resume: []byte("not a cursor")})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want Invalid, got %v", err)
	}
}

var _ io.Reader = (*errAfter)(nil)
