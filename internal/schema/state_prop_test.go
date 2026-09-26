package schema_test

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/version"
	"pgregory.net/rapid"
)

// A property test on the round trip, which CLAUDE.md makes mandatory for every
// serialisation in the repository (spec §46.4).
//
// The deterministic cases in state_test.go check the state a person thinks of.
// This checks the ones they do not: an empty cursor and a cursor of NULs, a
// zero timestamp beside a set one, an empty format map, a processed count at
// the top of its range. Migration state is the row a crashed run reads to
// decide where to resume, and a field that silently fails to round-trip there
// resumes the wrong work.
func TestMigrationStateRoundTripsForAnyValue(t *testing.T) {
	kv := newKV(t)

	rapid.Check(t, func(rt *rapid.T) {
		want := drawState(rt)

		if err := schema.WriteState(ctx(), kv, want); err != nil {
			rt.Fatalf("write: %v", err)
		}
		got, ok, err := schema.ReadState(ctx(), kv, want.ID)
		if err != nil {
			rt.Fatalf("read: %v", err)
		}
		if !ok {
			rt.Fatal("the row was written and did not read back")
		}
		if !got.Equal(want) {
			rt.Fatalf("round trip changed the state:\n got %+v\nwant %+v", got, want)
		}
	})
}

func drawState(rt *rapid.T) schema.MigrationState {
	// Ids are drawn from the shape a real migration id has — the registry
	// requires a non-empty one, and it becomes part of a key.
	id := rapid.StringMatching(`[a-z][a-z0-9-]{0,32}`).Draw(rt, "id")

	states := []schema.State{schema.StatePending, schema.StateRunning, schema.StateDone, schema.StateFailed}

	return schema.MigrationState{
		ID:             id,
		Source:         drawFormats(rt, "source"),
		Target:         drawFormats(rt, "target"),
		State:          rapid.SampledFrom(states).Draw(rt, "state"),
		Cursor:         rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "cursor"),
		Processed:      rapid.Uint64().Draw(rt, "processed"),
		StartedAt:      drawTime(rt, "started"),
		LastProgressAt: drawTime(rt, "progress"),
		Error:          rapid.String().Draw(rt, "error"),
	}
}

func drawFormats(rt *rapid.T, label string) map[string]uint32 {
	names := version.FormatNames()
	out := map[string]uint32{}
	for _, name := range names {
		if rapid.Bool().Draw(rt, label+":"+name) {
			out[name] = rapid.Uint32().Draw(rt, label+":"+name+":v")
		}
	}
	return out
}

// Times are stored as unix milliseconds, so the generator draws milliseconds:
// a property test that fed it nanoseconds would be asserting a precision the
// format never claimed. Zero is drawn deliberately — it is what "never started"
// looks like, and it must not come back as the epoch.
func drawTime(rt *rapid.T, label string) time.Time {
	ms := rapid.Uint64Range(0, 4_000_000_000_000).Draw(rt, label)
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
