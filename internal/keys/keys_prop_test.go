package keys_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"pgregory.net/rapid"
)

// Property tests for key ordering and tenant isolation.
//
// The deterministic cases use "acme" and "acmecorp", which is the overlap a
// person thinks of. These generate identifiers from the whole permitted
// alphabet at every permitted length, which is where a length-prefix mistake
// that only shows up at the 128-byte boundary would hide.

// idGen draws a well-formed identifier: the alphabet tenant.validate permits,
// at any length it permits.
func idGen() *rapid.Generator[string] {
	return rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789_-")), 1, tenant.MaxIDLen, -1)
}

func TestNoTenantsRangeContainsAnotherTenantsKeys(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := tenant.ID(idGen().Draw(rt, "a"))
		b := tenant.ID(idGen().Draw(rt, "b"))
		if a == b {
			rt.Skip("same tenant")
		}

		lo, hi := keys.TenantRange(a)
		for _, k := range everyKindOfKey(rt, b) {
			if bytes.Compare(k, lo) >= 0 && (hi == nil || bytes.Compare(k, hi) < 0) {
				rt.Fatalf("a key of tenant %q fell inside tenant %q's range: %x", b, a, k)
			}
		}
	})
}

func TestNoNamespacesRangeContainsAnotherNamespacesKeys(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		a := tenant.Namespace(idGen().Draw(rt, "a"))
		b := tenant.Namespace(idGen().Draw(rt, "b"))
		if a == b {
			rt.Skip("same namespace")
		}

		lo, hi := keys.NamespaceRange(tid, a)
		k := keys.Record(tid, b, keys.RecordMemory, id.New())
		if bytes.Compare(k, lo) >= 0 && (hi == nil || bytes.Compare(k, hi) < 0) {
			rt.Fatalf("namespace %q fell inside namespace %q's range", b, a)
		}
	})
}

func TestEveryKeyATenantOwnsIsInsideItsRange(t *testing.T) {
	// The other half of isolation: the range must not merely exclude other
	// tenants, it must include everything this one owns. A range that is too
	// narrow loses data on erasure and export.
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		lo, hi := keys.TenantRange(tid)
		for _, k := range everyKindOfKey(rt, tid) {
			if bytes.Compare(k, lo) < 0 || (hi != nil && bytes.Compare(k, hi) >= 0) {
				rt.Fatalf("a key tenant %q owns fell outside its own range: %x", tid, k)
			}
		}
	})
}

func TestRecordKeysAlwaysRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))
		rid := id.New()

		gotT, gotNS, gotType, gotID, err := keys.ParseRecord(keys.Record(tid, ns, keys.RecordMemory, rid))
		if err != nil {
			rt.Fatal(err)
		}
		if gotT != tid || gotNS != ns || gotType != keys.RecordMemory || gotID != rid {
			rt.Fatalf("round trip lost information: %q %q %v %v", gotT, gotNS, gotType, gotID)
		}
	})
}

func TestSpaceRangesNeverOverlap(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))

		spaces := keys.AllSpaces()
		type rng struct {
			lo, hi []byte
			name   string
		}
		var ranges []rng
		for _, s := range spaces {
			if s == keys.SpaceSystem {
				continue // untenanted; it has its own range
			}
			lo, hi := keys.SpaceRange(tid, ns, s)
			ranges = append(ranges, rng{lo, hi, s.String()})
		}
		for i := range ranges {
			for j := i + 1; j < len(ranges); j++ {
				a, b := ranges[i], ranges[j]
				if bytes.Compare(a.lo, b.hi) < 0 && bytes.Compare(b.lo, a.hi) < 0 {
					rt.Fatalf("space ranges %s and %s overlap", a.name, b.name)
				}
			}
		}
	})
}

func TestSystemKeysNeverCollideWithUserKeys(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		path := rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz/0123456789")), 1, 64, -1).Draw(rt, "path")
		sys := keys.System("/" + path)

		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		lo, hi := keys.TenantRange(tid)
		if bytes.Compare(sys, lo) >= 0 && (hi == nil || bytes.Compare(sys, hi) < 0) {
			rt.Fatalf("a system key fell inside tenant %q's range", tid)
		}
	})
}

// everyKindOfKey builds one key in every space, so an isolation property is
// checked against the whole layout rather than against records alone.
func everyKindOfKey(rt *rapid.T, tid tenant.ID) [][]byte {
	ns := tenant.Namespace(idGen().Draw(rt, "keyspace-namespace"))
	a, b := id.New(), id.New()
	return [][]byte{
		keys.Record(tid, ns, keys.RecordMemory, a),
		keys.Vector(tid, ns, a),
		keys.EdgeOut(tid, ns, a, 1, b),
		keys.EdgeIn(tid, ns, b, 1, a),
		keys.AttrRow(tid, ns, a),
		keys.AttrIndex(tid, ns, 1, keys.PutFloat64(nil, 0.5), a),
		keys.Text(tid, ns, "term", a),
		keys.VectorIndex(tid, ns, 1),
		keys.Job(tid, ns, keys.JobPending, 1, a),
		keys.Event(tid, ns, a, 1, 1),
		keys.Session(tid, ns, a),
		keys.JobPause(tid, ns, "vector.rebuild"),
		keys.JobRun(tid, ns, 1, a),
	}
}

// TestEveryJobPauseKeyRoundTrips: a pause is addressed by the type name it
// suppresses, so a name the builder can write and the parser cannot read would
// be a suppression nothing could list or lift.
func TestEveryJobPauseKeyRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))
		typ := rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789_.")), 1, 64, -1).
			Draw(rt, "type")

		gotT, gotNS, gotType, err := keys.ParseJobPause(keys.JobPause(tid, ns, typ))
		if err != nil {
			rt.Fatalf("ParseJobPause: %v", err)
		}
		if gotT != tid || gotNS != ns || gotType != typ {
			rt.Fatalf("round trip lost something: %q %q %q", gotT, gotNS, gotType)
		}
	})
}

// TestJobRunKeysSortByFinishThenID is what makes history readable and reapable
// as forward and backward scans: the reaper walks the oldest first and a page
// reads the newest backwards, and both are only correct if byte order is
// (finish time, run id) order at every value they can take.
func TestJobRunKeysSortByFinishThenID(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))
		fa := rapid.Uint64().Draw(rt, "finished_a")
		fb := rapid.Uint64().Draw(rt, "finished_b")
		ia := drawID(rt, "id_a")
		ib := drawID(rt, "id_b")

		ka := keys.JobRun(tid, ns, fa, ia)
		kb := keys.JobRun(tid, ns, fb, ib)

		want := compareTriple(0, fa, ia, 0, fb, ib)
		if got := sign(bytes.Compare(ka, kb)); got != want {
			rt.Fatalf("byte order %d, want %d for (%d,%x) vs (%d,%x)", got, want, fa, ia, fb, ib)
		}

		gotT, gotNS, gotFinished, gotID, err := keys.ParseJobRun(ka)
		if err != nil {
			rt.Fatalf("ParseJobRun: %v", err)
		}
		if gotT != tid || gotNS != ns || gotFinished != fa || gotID != ia {
			rt.Fatalf("round trip lost something: %q %q %d %x", gotT, gotNS, gotFinished, gotID)
		}
	})
}

// TestJobKeysSortByPartitionThenDueThenID is the ordering the whole queue rests
// on: claiming, lease reclamation and reaping are each a forward scan that
// stops at the first row past its bound, and that is only true if byte order is
// (partition, due time, id) order at every value those fields can take.
func TestJobKeysSortByPartitionThenDueThenID(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))

		states := keys.AllJobStates()
		sa := states[rapid.IntRange(0, len(states)-1).Draw(rt, "state_a")]
		sb := states[rapid.IntRange(0, len(states)-1).Draw(rt, "state_b")]
		da := rapid.Uint64().Draw(rt, "due_a")
		db := rapid.Uint64().Draw(rt, "due_b")
		ia := drawID(rt, "id_a")
		ib := drawID(rt, "id_b")

		ka := keys.Job(tid, ns, sa, da, ia)
		kb := keys.Job(tid, ns, sb, db, ib)

		want := compareTriple(byte(sa), da, ia, byte(sb), db, ib)
		if got := sign(bytes.Compare(ka, kb)); got != want {
			rt.Fatalf("byte order %d, want %d for (%#x,%d,%x) vs (%#x,%d,%x)",
				got, want, byte(sa), da, ia, byte(sb), db, ib)
		}
	})
}

// TestEveryJobKeyRoundTrips holds that nothing the builder can produce is
// unreadable. A key the store holds that the binary cannot parse is a job that
// can be neither claimed nor cancelled, and nothing would report it.
func TestEveryJobKeyRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		tid := tenant.ID(idGen().Draw(rt, "tenant"))
		ns := tenant.Namespace(idGen().Draw(rt, "namespace"))
		states := keys.AllJobStates()
		state := states[rapid.IntRange(0, len(states)-1).Draw(rt, "state")]
		due := rapid.Uint64().Draw(rt, "due")
		jid := drawID(rt, "id")

		gotT, gotNS, gotState, gotDue, gotID, err := keys.ParseJob(keys.Job(tid, ns, state, due, jid))
		if err != nil {
			rt.Fatalf("ParseJob: %v", err)
		}
		if gotT != tid || gotNS != ns || gotState != state || gotDue != due || gotID != jid {
			rt.Fatalf("round trip lost something: %q %q %#x %d %x", gotT, gotNS, byte(gotState), gotDue, gotID)
		}
	})
}

// drawID draws an identifier from the test's own bytes rather than calling
// id.New(). rapid replays draws and nothing else, so a subject that depends on
// a random source shrinks to nothing and reports "cannot reproduce a failure".
func drawID(rt *rapid.T, label string) id.ID {
	var out id.ID
	b := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(rt, label)
	copy(out[:], b)
	return out
}

func compareTriple(sa byte, da uint64, ia id.ID, sb byte, db uint64, ib id.ID) int {
	switch {
	case sa != sb:
		return sign(int(sa) - int(sb))
	case da != db:
		if da < db {
			return -1
		}
		return 1
	default:
		return sign(bytes.Compare(ia[:], ib[:]))
	}
}
