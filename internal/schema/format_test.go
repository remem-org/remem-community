package schema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/version"
)

func ctx() context.Context { return context.Background() }

func newKV(t *testing.T) storage.KV {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

func TestOpenRefusesANewerDirectory(t *testing.T) {
	kv := newKV(t)
	writeMust(t, kv, schema.Format{Subsystems: map[string]schema.Subsystem{
		"key_encoding": at(version.KeyEncoding + 1),
	}})

	_, err := schema.Open(ctx(), kv, version.Current())
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "key encoding") {
		t.Fatalf("the error must name the subsystem: %v", err)
	}
	// An operator reading this in a crash log needs to know which way to move.
	if !strings.Contains(err.Error(), "upgrade") && !strings.Contains(err.Error(), "Upgrade") {
		t.Fatalf("the error must say what to do about it: %v", err)
	}
}

func TestOpenAcceptsAnEmptyDirectory(t *testing.T) {
	// Brand-new and written-before-this-existed collapse into one path: both
	// read as version 0 and migrate forward.
	f, err := schema.Open(ctx(), newKV(t), version.Current())
	if err != nil {
		t.Fatal(err)
	}
	if f.Subsystems["key_encoding"].Current != version.KeyEncoding {
		t.Fatalf("a new directory is stamped at current: %v", f.Subsystems)
	}
	for _, name := range version.FormatNames() {
		if _, ok := f.Subsystems[name]; !ok {
			t.Errorf("a new directory must stamp every format, %q is missing", name)
		}
	}
}

func TestOpenIsIdempotent(t *testing.T) {
	// Restarting a server must not rewrite the manifest, or every restart is a
	// write to the one row that says whether the data is readable.
	kv := newKV(t)
	first, err := schema.Open(ctx(), kv, version.Current())
	if err != nil {
		t.Fatal(err)
	}
	second, err := schema.Open(ctx(), kv, version.Current())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range version.FormatNames() {
		if first.Subsystems[name] != second.Subsystems[name] {
			t.Fatalf("%s moved between opens: %v then %v", name, first.Subsystems[name], second.Subsystems[name])
		}
	}
}

func TestOpenRefusesAnUnknownSubsystem(t *testing.T) {
	kv := newKV(t)
	writeMust(t, kv, schema.Format{Subsystems: map[string]schema.Subsystem{"quantum": at(1)}})

	if _, err := schema.Open(ctx(), kv, version.Current()); err == nil {
		t.Fatal("a subsystem this binary has never heard of means unreadable files exist")
	} else if !strings.Contains(err.Error(), "quantum") {
		t.Fatalf("the error must name the subsystem: %v", err)
	}
}

func TestOpenAcceptsANewerDirectoryThatSaysOldReadersAreFine(t *testing.T) {
	// This is what min_reader and min_writer are for (spec §19.2). A format
	// bump that did not change how existing bytes are interpreted can lower
	// the bar deliberately, and then an older binary may still run.
	kv := newKV(t)
	writeMust(t, kv, schema.Format{Subsystems: map[string]schema.Subsystem{
		"key_encoding": {
			Current:   version.KeyEncoding + 1,
			MinReader: version.KeyEncoding,
			MinWriter: version.KeyEncoding,
		},
	}})

	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatalf("a directory that explicitly permits this reader was refused: %v", err)
	}
}

func TestOpenRefusesADirectoryThisBinaryMayReadButNotWrite(t *testing.T) {
	kv := newKV(t)
	writeMust(t, kv, schema.Format{Subsystems: map[string]schema.Subsystem{
		"key_encoding": {
			Current:   version.KeyEncoding + 1,
			MinReader: version.KeyEncoding,
			MinWriter: version.KeyEncoding + 1,
		},
	}})

	err := errFrom(schema.Open(ctx(), kv, version.Current()))
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("Remem opens for writing; a read-only-compatible directory must be refused: %v", err)
	}
	if !strings.Contains(err.Error(), "write") {
		t.Fatalf("the error must say it is the write side that is blocked: %v", err)
	}
}

func TestDefaultsMakeANewerDirectoryRefuseOldReaders(t *testing.T) {
	// The safe default: writing a manifest without thinking about
	// compatibility must lock older binaries out, not let them in.
	f := schema.CurrentFormat(version.Current())
	for name, s := range f.Subsystems {
		if s.MinReader != s.Current || s.MinWriter != s.Current {
			t.Errorf("%s defaults to min_reader=%d min_writer=%d, want both at current=%d",
				name, s.MinReader, s.MinWriter, s.Current)
		}
	}
}

func TestFormatRoundTripsThroughTheStore(t *testing.T) {
	kv := newKV(t)
	want := schema.Format{Subsystems: map[string]schema.Subsystem{
		"key_encoding":    {Current: 3, MinReader: 1, MinWriter: 2},
		"record_envelope": {Current: 7, MinReader: 7, MinWriter: 7},
	}}
	writeMust(t, kv, want)

	got, err := schema.ReadFormat(ctx(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Subsystems) != len(want.Subsystems) {
		t.Fatalf("read back %d subsystems, wrote %d", len(got.Subsystems), len(want.Subsystems))
	}
	for name, s := range want.Subsystems {
		if got.Subsystems[name] != s {
			t.Errorf("%s round tripped as %v, want %v", name, got.Subsystems[name], s)
		}
	}
}

func TestReadFormatOfAnEmptyDirectoryIsEmptyNotAnError(t *testing.T) {
	got, err := schema.ReadFormat(ctx(), newKV(t))
	if err != nil {
		t.Fatalf("a directory with no manifest is new, not broken: %v", err)
	}
	if len(got.Subsystems) != 0 {
		t.Fatalf("want no subsystems, got %v", got.Subsystems)
	}
}

func TestManifestLivesInTheSystemSpace(t *testing.T) {
	// It must be untenanted: it describes the whole directory, and a manifest
	// filed under a tenant would be invisible to a scan that has no tenant yet
	// — which is every scan at open time.
	kv := newKV(t)
	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatal(err)
	}

	lo, hi := keys.SystemRange()
	it := kv.NewIterator(lo, hi)
	defer it.Close()

	found := 0
	for ok := it.First(); ok; ok = it.Next() {
		found++
	}
	if found != len(version.FormatNames()) {
		t.Fatalf("found %d manifest rows in the system space, want %d", found, len(version.FormatNames()))
	}
}

func TestCorruptManifestRowIsCorruption(t *testing.T) {
	kv := newKV(t)
	if _, err := schema.Open(ctx(), kv, version.Current()); err != nil {
		t.Fatal(err)
	}
	// Truncate one row. This is canonical data: it must not read as absent.
	if err := kv.Set(ctx(), keys.System("/format/key_encoding"), []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if _, err := schema.ReadFormat(ctx(), kv); !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption for a truncated manifest row, got %v", err)
	}
}

func TestNeedsMigrationSeesAnOlderDirectory(t *testing.T) {
	f := schema.Format{Subsystems: map[string]schema.Subsystem{
		"key_encoding": at(version.KeyEncoding),
	}}
	// record_envelope is absent, which reads as 0 and is older than current.
	if !f.NeedsMigration(version.Current()) {
		t.Fatal("a manifest missing a format this binary writes needs migrating forward")
	}
	if schema.CurrentFormat(version.Current()).NeedsMigration(version.Current()) {
		t.Fatal("a directory stamped at current needs no migration")
	}
}

func at(v uint32) schema.Subsystem {
	return schema.Subsystem{Current: v, MinReader: v, MinWriter: v}
}

func writeMust(t *testing.T, kv storage.KV, f schema.Format) {
	t.Helper()
	if err := schema.WriteFormat(ctx(), kv, f); err != nil {
		t.Fatal(err)
	}
}

func errFrom(_ schema.Format, err error) error { return err }

func TestFormatRendersForOperators(t *testing.T) {
	// This string is what `remem-admin inspect` prints and what a startup log
	// carries; it must be stable and ordered, not map-random.
	f := schema.Format{Subsystems: map[string]schema.Subsystem{
		"record_envelope": {Current: 2, MinReader: 1, MinWriter: 2},
		"key_encoding":    {Current: 1, MinReader: 1, MinWriter: 1},
	}}
	got := f.String()
	want := "key_encoding{current=1 min_reader=1 min_writer=1} " +
		"record_envelope{current=2 min_reader=1 min_writer=2}"
	if got != want {
		t.Fatalf("Format.String() =\n %q\nwant\n %q", got, want)
	}
	for i := 0; i < 20; i++ {
		if f.String() != want {
			t.Fatal("Format.String() is not stable across calls")
		}
	}
}
