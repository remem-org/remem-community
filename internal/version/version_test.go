package version_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/version"
)

func TestCheckOpenRefusesNewerOnDisk(t *testing.T) {
	err := version.CheckOpen(version.Versions{KeyEncoding: version.KeyEncoding + 1})
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
	if !strings.Contains(err.Error(), "Upgrade the server") {
		t.Fatalf("the error must tell the operator what to do: %v", err)
	}
}

func TestCheckOpenAcceptsOlderOnDisk(t *testing.T) {
	// Older is what migrations are for; only newer is refused.
	if err := version.CheckOpen(version.Versions{KeyEncoding: 0}); err != nil {
		t.Fatalf("older on-disk must be acceptable: %v", err)
	}
}

func TestCheckOpenAcceptsCurrent(t *testing.T) {
	if err := version.CheckOpen(version.Current()); err != nil {
		t.Fatalf("the versions this binary writes must be openable: %v", err)
	}
}

// Every independently versioned durable format is gated, not just the first.
func TestEveryFormatIsChecked(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*version.Versions)
		want string
	}{
		{"key encoding", func(v *version.Versions) { v.KeyEncoding = version.KeyEncoding + 1 }, "key encoding"},
		{"record envelope", func(v *version.Versions) { v.RecordEnvelope = version.RecordEnvelope + 1 }, "record envelope"},
		{"snapshot format", func(v *version.Versions) { v.SnapshotFormat = version.SnapshotFormat + 1 }, "snapshot format"},
		{"attribute schema", func(v *version.Versions) { v.AttrSchema = version.AttrSchema + 1 }, "attribute slot schema"},
		{"cluster compatibility", func(v *version.Versions) { v.ClusterCompat = version.ClusterCompat + 1 }, "cluster compatibility"},
		{"text index", func(v *version.Versions) { v.TextIndex = version.TextIndex + 1 }, "text index"},
		{"passage index", func(v *version.Versions) { v.PassageIndex = version.PassageIndex + 1 }, "passage index"},
		{"user format", func(v *version.Versions) { v.UserFormat = version.UserFormat + 1 }, "user format"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := version.Current()
			tc.mut(&v)
			err := version.CheckOpen(v)
			if !errs.Is(err, errs.IncompatibleVersion) {
				t.Fatalf("want IncompatibleVersion, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error must name the offending format %q: %v", tc.want, err)
			}
		})
	}
}

func TestRefusalSaysDowngradeIsNeverAutomatic(t *testing.T) {
	// spec §59: downgrade behaviour must be explicit and must fail clearly.
	err := version.CheckOpen(version.Versions{SnapshotFormat: version.SnapshotFormat + 1})
	if !strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("the operator must be told a downgrade is not performed: %v", err)
	}
}

func TestNeedsMigrationDistinguishesOlderFromCurrent(t *testing.T) {
	if version.Current().NeedsMigration() {
		t.Fatal("a database this binary wrote needs no migration")
	}
	older := version.Current()
	older.RecordEnvelope--
	if !older.NeedsMigration() {
		t.Fatal("an older record envelope must be reported as migratable")
	}
}

// Invariant 11: the binary version is not the storage or cluster compatibility
// version, and nothing may conflate them.
func TestBinaryVersionIsSeparateFromFormatVersions(t *testing.T) {
	if version.Binary == "" {
		t.Fatal("version.Binary must always have a value, even in a dev build")
	}
	if strings.Contains(version.Binary, "\x00") {
		t.Fatal("version.Binary must be a printable string")
	}
	if version.ClusterCompat != 0 {
		t.Fatalf("no cluster format is active before Phase 14; got %d", version.ClusterCompat)
	}
}

func TestStringReportsEveryVersion(t *testing.T) {
	s := version.Current().String()
	for _, want := range []string{"key_encoding", "record_envelope", "snapshot_format", "attr_schema", "cluster_compat"} {
		if !strings.Contains(s, want) {
			t.Errorf("Versions.String() omits %s: %s", want, s)
		}
	}
}

// A directory written before passages existed records no passage_index. Absent
// reads as 0, which is older: the directory opens, and needs migrating rather
// than being refused — the re-embedding migration is what moves it forward.
func TestAnAbsentPassageIndexIsOlderAndOpens(t *testing.T) {
	v := version.Current()
	v.PassageIndex = 0
	if got, ok := v.Get("passage_index"); !ok || got != 0 {
		t.Fatalf("passage_index reads %d (known %v), want 0", got, ok)
	}
	if err := version.CheckOpen(v); err != nil {
		t.Fatalf("a directory without passage_index was refused: %v", err)
	}
	if !v.NeedsMigration() {
		t.Fatal("a directory without passage_index does not need migrating")
	}
}

// A directory written before users existed records no user_format. Absent reads
// as 0, which is older: the directory opens and is moved forward by a step that
// writes nothing, because such a directory holds no user rows to convert.
func TestAnAbsentUserFormatIsOlderAndOpens(t *testing.T) {
	v := version.Current()
	v.UserFormat = 0
	if got, ok := v.Get("user_format"); !ok || got != 0 {
		t.Fatalf("user_format reads %d (known %v), want 0", got, ok)
	}
	if err := version.CheckOpen(v); err != nil {
		t.Fatalf("a directory without user_format was refused: %v", err)
	}
	if !v.NeedsMigration() {
		t.Fatal("a directory without user_format does not need migrating")
	}
}
