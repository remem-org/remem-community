package keys_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

const (
	acme = tenant.ID("acme")
	ns   = tenant.DefaultNamespace
)

func TestTenantPrefixesCannotOverlap(t *testing.T) {
	// Without a length prefix, "acme" is a prefix of "acmecorp" and a scan of
	// one silently reads the other. That is a tenant-isolation defect.
	lo, hi := keys.TenantRange(acme)
	other := keys.Record("acmecorp", ns, keys.RecordMemory, id.New())
	if bytes.Compare(other, lo) >= 0 && bytes.Compare(other, hi) < 0 {
		t.Fatal("tenant acmecorp fell inside tenant acme's range")
	}
}

// The same defect one level down: a namespace prefixing another namespace.
func TestNamespacePrefixesCannotOverlap(t *testing.T) {
	lo, hi := keys.NamespaceRange(acme, "prod")
	other := keys.Record(acme, "production", keys.RecordMemory, id.New())
	if bytes.Compare(other, lo) >= 0 && bytes.Compare(other, hi) < 0 {
		t.Fatal("namespace production fell inside namespace prod's range")
	}
}

// The whole point of putting the tenant first: everything one tenant owns is
// one contiguous range, so erasing a departing customer, exporting them, or
// moving them to their own machine is a single operation rather than one per
// space that somebody must remember to keep in step.
func TestEverythingOneTenantOwnsIsOneRange(t *testing.T) {
	lo, hi := keys.TenantRange(acme)
	rid, other := id.New(), id.New()

	mine := [][]byte{
		keys.Record(acme, ns, keys.RecordMemory, rid),
		keys.Vector(acme, ns, rid),
		keys.EdgeOut(acme, ns, rid, 1, other),
		keys.EdgeIn(acme, ns, other, 1, rid),
		keys.AttrRow(acme, ns, rid),
		keys.AttrIndex(acme, ns, 3, []byte("value"), rid),
		keys.Text(acme, ns, "term", rid),
		keys.VectorIndex(acme, ns, 42),
		keys.Job(acme, ns, keys.JobPending, 1000, rid),
		keys.Event(acme, ns, rid, 1000, 1),
		keys.Session(acme, ns, rid),
		keys.Record(acme, "other-namespace", keys.RecordMemory, rid),
	}
	for _, k := range mine {
		if bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
			t.Errorf("a key the tenant owns fell outside its range: %x", k)
		}
	}

	theirs := [][]byte{
		// Same length as "acme", above and below it. These are the cases a
		// hand-written table forgets: an earlier TenantRange returned only the
		// length byte, which every different-length example still excluded by
		// accident. The property test in keys_prop_test.go found it; these
		// keep it found without needing a generator.
		keys.Record("acmf", ns, keys.RecordMemory, rid),
		keys.Record("acmd", ns, keys.RecordMemory, rid),
		keys.Record("zzzz", ns, keys.RecordMemory, rid),
		keys.Record("acme2", ns, keys.RecordMemory, rid),
		keys.Record("acm", ns, keys.RecordMemory, rid),
		keys.Vector("zzz", ns, rid),
		keys.System("/format/keys"),
	}
	for _, k := range theirs {
		if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
			t.Errorf("a key the tenant does not own fell inside its range: %x", k)
		}
	}
}

func TestRecordKeyRoundTrips(t *testing.T) {
	want := id.New()
	k := keys.Record(acme, ns, keys.RecordMemory, want)
	gotT, gotNS, gotType, gotID, err := keys.ParseRecord(k)
	if err != nil {
		t.Fatal(err)
	}
	if gotT != acme || gotNS != ns || gotType != keys.RecordMemory || gotID != want {
		t.Fatalf("round trip lost information: %v %v %v %v", gotT, gotNS, gotType, gotID)
	}
}

func TestParseRecordRejectsOtherSpaces(t *testing.T) {
	// A parser that accepts a key from another space would decode whatever
	// bytes happened to be there into a plausible record id.
	if _, _, _, _, err := keys.ParseRecord(keys.Vector(acme, ns, id.New())); !errs.Is(err, errs.Corruption) {
		t.Fatalf("parsing a vector key as a record must be Corruption, got %v", err)
	}
}

func TestParseRecordRejectsTruncatedKeys(t *testing.T) {
	full := keys.Record(acme, ns, keys.RecordMemory, id.New())
	for n := 0; n < len(full); n++ {
		if _, _, _, _, err := keys.ParseRecord(full[:n]); err == nil {
			t.Fatalf("a key truncated to %d bytes parsed successfully", n)
		}
	}
}

func TestSpacesDoNotCollide(t *testing.T) {
	seen := map[byte]string{}
	for _, s := range keys.AllSpaces() {
		if prev, dup := seen[byte(s)]; dup {
			t.Fatalf("space byte %#x used by both %s and %s", byte(s), prev, s)
		}
		seen[byte(s)] = s.String()
	}
	if len(seen) != len(keys.AllSpaces()) {
		t.Fatal("AllSpaces must list every space exactly once")
	}
}

// TestEverySpaceIsClassified: whether a space is canonical or derived is a fact
// about the space, and Invariant 3's guard in internal/server reads it. A space
// added without a class would be skipped by that guard silently.
func TestEverySpaceIsClassified(t *testing.T) {
	declared := map[keys.Space]bool{}
	for _, s := range keys.AllSpaces() {
		declared[s] = true
	}
	for _, s := range keys.AllSpaces() {
		switch s.Class() {
		case keys.Canonical:
			if len(s.DerivedFrom()) != 0 {
				t.Errorf("space %s is canonical and names sources %v: a canonical space is "+
					"derived from nothing, which is what makes it canonical", s, s.DerivedFrom())
			}
		case keys.Derived:
			if len(s.DerivedFrom()) == 0 {
				t.Errorf("space %s is derived and names no source: a rebuild has nothing to read", s)
			}
			for _, src := range s.DerivedFrom() {
				if !declared[src] || src == s {
					t.Errorf("space %s is derived from %#x, which is not another declared space",
						s, byte(src))
				}
			}
		default:
			t.Errorf("space %s has no class", s)
		}
	}
}

func TestEverySpaceHasAName(t *testing.T) {
	// The names reach logs, metric labels and `remem-admin inspect`.
	for _, s := range keys.AllSpaces() {
		if s.String() == "" || s.String() == "unknown" {
			t.Errorf("space %#x has no name", byte(s))
		}
	}
}

func TestUserKeysRequireATenant(t *testing.T) {
	if _, err := keys.RecordChecked("", ns, keys.RecordMemory, id.New()); err == nil {
		t.Fatal("a user-space key with no tenant must be refused (Invariant 1)")
	}
}

func TestCheckedBuilderRefusesEveryMalformedScope(t *testing.T) {
	cases := []struct {
		name string
		t    tenant.ID
		ns   tenant.Namespace
	}{
		{"no tenant", "", ns},
		{"no namespace", acme, ""},
		{"bad tenant", "Acme", ns},
		{"bad namespace", acme, "Prod"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := keys.RecordChecked(c.t, c.ns, keys.RecordMemory, id.New()); !errs.Is(err, errs.Invalid) {
				t.Fatalf("want Invalid, got %v", err)
			}
		})
	}
}

func TestCheckedBuilderAgreesWithTheUncheckedOne(t *testing.T) {
	rid := id.New()
	got, err := keys.RecordChecked(acme, ns, keys.RecordMemory, rid)
	if err != nil {
		t.Fatal(err)
	}
	if want := keys.Record(acme, ns, keys.RecordMemory, rid); !bytes.Equal(got, want) {
		t.Fatalf("checked and unchecked builders disagree:\n got %x\nwant %x", got, want)
	}
}

func TestSystemKeysAreUntenantedAndSortFirst(t *testing.T) {
	sys := keys.System("/format/keys")
	lo, hi := keys.SystemRange()
	if bytes.Compare(sys, lo) < 0 || bytes.Compare(sys, hi) >= 0 {
		t.Fatal("a system key fell outside the system range")
	}
	// Any user key must sort after every system key, so that a scan of the
	// system space cannot run into user data.
	user := keys.Record("a", "a", keys.RecordMemory, id.New())
	if bytes.Compare(user, hi) < 0 {
		t.Fatal("a user key sorted inside the system range")
	}
}

func TestSpaceRangeCoversOnlyItsSpace(t *testing.T) {
	rid := id.New()
	lo, hi := keys.SpaceRange(acme, ns, keys.SpaceRecord)

	if k := keys.Record(acme, ns, keys.RecordMemory, rid); bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
		t.Error("a record key fell outside the record space range")
	}
	for _, k := range [][]byte{
		keys.Vector(acme, ns, rid),
		keys.AttrRow(acme, ns, rid),
		keys.Record(acme, "elsewhere", keys.RecordMemory, rid),
	} {
		if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
			t.Errorf("a key outside the record space fell inside its range: %x", k)
		}
	}
}

func TestKeysSortByIDWithinASpace(t *testing.T) {
	// A UUIDv7's leading bits are its creation time, so a record scan is
	// already in creation order and a created_at listing needs no sort.
	var prev []byte
	for i := 0; i < 50; i++ {
		k := keys.Record(acme, ns, keys.RecordMemory, id.New())
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatal("record keys must ascend with v7 id creation time")
		}
		prev = k
	}
}

func TestJobKeysSortByDueTime(t *testing.T) {
	// The queue is scanned in due order, so the encoding must put due time
	// ahead of the job id and must be big-endian.
	early := keys.Job(acme, ns, keys.JobPending, 1_000, id.New())
	late := keys.Job(acme, ns, keys.JobPending, 2_000, id.New())
	if bytes.Compare(early, late) >= 0 {
		t.Fatal("an earlier due time must sort first")
	}
	other := keys.Job(acme, ns, keys.JobRunning, 1, id.New())
	if bytes.Compare(early, other) >= 0 {
		t.Fatal("job state must sort ahead of due time")
	}
}

func TestEventKeysSortBySubjectThenTime(t *testing.T) {
	subj := id.New()
	first := keys.Event(acme, ns, subj, 1_000, 1)
	second := keys.Event(acme, ns, subj, 1_000, 2)
	later := keys.Event(acme, ns, subj, 2_000, 0)
	if bytes.Compare(first, second) >= 0 {
		t.Fatal("the sequence number must break ties within a timestamp")
	}
	if bytes.Compare(second, later) >= 0 {
		t.Fatal("a later timestamp must sort after an earlier one")
	}
}

func TestTextKeysSeparateTerms(t *testing.T) {
	// Length-prefixing the term is what stops "cat" scanning "cats".
	lo, hi := keys.TextTermRange(acme, ns, "cat")
	if k := keys.Text(acme, ns, "cats", id.New()); bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
		t.Fatal("term \"cats\" fell inside term \"cat\"'s posting range")
	}
	if k := keys.Text(acme, ns, "cat", id.New()); bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
		t.Fatal("a posting for \"cat\" fell outside its own range")
	}
}

func TestEdgeDirectionsAreDistinct(t *testing.T) {
	from, to := id.New(), id.New()
	out := keys.EdgeOut(acme, ns, from, 7, to)
	in := keys.EdgeIn(acme, ns, to, 7, from)
	if bytes.Equal(out, in) {
		t.Fatal("the out-edge and its derived in-edge must be different keys")
	}
}

func TestPrefixRangeStopsAtThePrefix(t *testing.T) {
	lo, hi := keys.PrefixRange([]byte{0x01, 0x02})
	if !bytes.Equal(lo, []byte{0x01, 0x02}) || !bytes.Equal(hi, []byte{0x01, 0x03}) {
		t.Fatalf("PrefixRange = %x..%x", lo, hi)
	}
	// An all-0xFF prefix has no successor; the range must run to the end.
	if _, hi := keys.PrefixRange([]byte{0xFF, 0xFF}); hi != nil {
		t.Fatalf("a prefix with no successor must give an unbounded upper, got %x", hi)
	}
}

func TestBuildersAllocateOnce(t *testing.T) {
	// Spec §56: a key costs one allocation. Builders size the slice exactly
	// rather than letting append grow it.
	rid := id.New()
	cases := map[string]func(){
		"Record":    func() { _ = keys.Record(acme, ns, keys.RecordMemory, rid) },
		"Vector":    func() { _ = keys.Vector(acme, ns, rid) },
		"EdgeOut":   func() { _ = keys.EdgeOut(acme, ns, rid, 1, rid) },
		"AttrIndex": func() { _ = keys.AttrIndex(acme, ns, 1, []byte("v"), rid) },
		"Text":      func() { _ = keys.Text(acme, ns, "term", rid) },
		"Job":       func() { _ = keys.Job(acme, ns, keys.JobPending, 1, rid) },
	}
	for name, fn := range cases {
		if n := testing.AllocsPerRun(100, fn); n > 1 {
			t.Errorf("%s allocates %.0f times per key, want 1", name, n)
		}
	}
}

func TestScanHelpersConfineTheirScans(t *testing.T) {
	// Each of these is the range behind a real query, and each is the place a
	// scan could quietly wander into a neighbour's rows.
	subject, other := id.New(), id.New()

	cases := []struct {
		name    string
		lo, hi  []byte
		inside  [][]byte
		outside [][]byte
	}{
		{
			name: "attribute slot",
			lo:   first(keys.AttrSlotRange(acme, ns, 7)),
			hi:   second(keys.AttrSlotRange(acme, ns, 7)),
			inside: [][]byte{
				keys.AttrIndex(acme, ns, 7, keys.PutFloat64(nil, -1), subject),
				keys.AttrIndex(acme, ns, 7, keys.PutFloat64(nil, 99), other),
			},
			outside: [][]byte{
				keys.AttrIndex(acme, ns, 8, keys.PutFloat64(nil, 0), subject),
				keys.AttrIndex(acme, ns, 6, keys.PutFloat64(nil, 0), subject),
				keys.AttrRow(acme, ns, subject),
			},
		},
		{
			name:    "out-edges of one record",
			lo:      first(keys.EdgesFromRange(acme, ns, subject)),
			hi:      second(keys.EdgesFromRange(acme, ns, subject)),
			inside:  [][]byte{keys.EdgeOut(acme, ns, subject, 1, other), keys.EdgeOut(acme, ns, subject, 9, other)},
			outside: [][]byte{keys.EdgeOut(acme, ns, other, 1, subject), keys.EdgeIn(acme, ns, subject, 1, other)},
		},
		{
			name:    "in-edges of one record",
			lo:      first(keys.EdgesToRange(acme, ns, subject)),
			hi:      second(keys.EdgesToRange(acme, ns, subject)),
			inside:  [][]byte{keys.EdgeIn(acme, ns, subject, 1, other)},
			outside: [][]byte{keys.EdgeIn(acme, ns, other, 1, subject), keys.EdgeOut(acme, ns, subject, 1, other)},
		},
		{
			name:    "audit stream of one subject",
			lo:      first(keys.EventsForRange(acme, ns, subject)),
			hi:      second(keys.EventsForRange(acme, ns, subject)),
			inside:  [][]byte{keys.Event(acme, ns, subject, 1, 0), keys.Event(acme, ns, subject, 1<<40, 7)},
			outside: [][]byte{keys.Event(acme, ns, other, 1, 0)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, k := range c.inside {
				if bytes.Compare(k, c.lo) < 0 || bytes.Compare(k, c.hi) >= 0 {
					t.Errorf("a key the scan must reach fell outside it: %x", k)
				}
			}
			for _, k := range c.outside {
				if bytes.Compare(k, c.lo) >= 0 && bytes.Compare(k, c.hi) < 0 {
					t.Errorf("a key the scan must not reach fell inside it: %x", k)
				}
			}
		})
	}
}

func TestSpaceNamesAreStable(t *testing.T) {
	// These reach logs, metric labels and `remem-admin inspect`, so they are a
	// compatibility surface of their own.
	want := map[keys.Space]string{
		keys.SpaceSystem: "system", keys.SpaceRecord: "record", keys.SpaceVector: "vector",
		keys.SpaceEdgeOut: "edge_out", keys.SpaceEdgeIn: "edge_in", keys.SpaceAttrRow: "attr_row",
		keys.SpaceAttrIndex: "attr_index", keys.SpaceText: "text", keys.SpaceVectorIndex: "vector_index",
		keys.SpaceJob: "job", keys.SpaceEvent: "event", keys.SpaceSession: "session",
	}
	for s, name := range want {
		if got := s.String(); got != name {
			t.Errorf("space %#x is named %q, want %q", byte(s), got, name)
		}
	}
	if got := keys.Space(0xEE).String(); got != "unknown" {
		t.Errorf("an unassigned space byte is %q, want \"unknown\"", got)
	}
	if got := keys.RecordMemory.String(); got != "memory" {
		t.Errorf("RecordMemory is named %q", got)
	}
}

func first(lo, _ []byte) []byte  { return lo }
func second(_, hi []byte) []byte { return hi }
