package keys_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestJobStateRangeCoversOnlyItsPartition(t *testing.T) {
	// The three partitions exist so that the scan for due work never walks a
	// running or a finished row. A range that leaked into a neighbour would
	// make the claim scan's cost the size of the audit trail.
	lo, hi := keys.JobStateRange(acme, ns, keys.JobPending)

	in := keys.Job(acme, ns, keys.JobPending, 5, id.New())
	if bytes.Compare(in, lo) < 0 || bytes.Compare(in, hi) >= 0 {
		t.Fatal("a pending job fell outside the pending range")
	}
	for _, other := range []keys.JobState{keys.JobRunning, keys.JobDone} {
		k := keys.Job(acme, ns, other, 5, id.New())
		if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
			t.Fatalf("a %#x job fell inside the pending range", byte(other))
		}
	}
	// And a different tenant's pending job, which is the isolation half.
	if k := keys.Job("acmecorp", ns, keys.JobPending, 5, id.New()); bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
		t.Fatal("another tenant's pending job fell inside this tenant's range")
	}
}

func TestJobDueRangeStopsAtItsBound(t *testing.T) {
	// Claiming asks for "pending and due at or before now". The upper bound is
	// exclusive, so a job due one millisecond later must be outside it — that
	// is the difference between running work early and running it on time.
	rid := id.New()
	lo, hi := keys.JobDueRange(acme, ns, keys.JobPending, 100)

	for _, due := range []uint64{0, 1, 99, 100} {
		k := keys.Job(acme, ns, keys.JobPending, due, rid)
		if bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
			t.Fatalf("a job due at %d is not due at 100 according to the range", due)
		}
	}
	if k := keys.Job(acme, ns, keys.JobPending, 101, rid); bytes.Compare(k, hi) < 0 {
		t.Fatal("a job due after the bound fell inside the range")
	}
	// The largest id at the bound must still be included: the id trails the
	// due time, so an upper bound built without care excludes half of them.
	var maxID id.ID
	for i := range maxID {
		maxID[i] = 0xFF
	}
	if k := keys.Job(acme, ns, keys.JobPending, 100, maxID); bytes.Compare(k, hi) >= 0 {
		t.Fatal("the highest id due exactly at the bound was excluded")
	}
}

func TestJobKeyRoundTrips(t *testing.T) {
	jid := id.New()
	k := keys.Job(acme, ns, keys.JobRunning, 1_234_567, jid)

	gotT, gotNS, state, due, got, err := keys.ParseJob(k)
	if err != nil {
		t.Fatalf("ParseJob: %v", err)
	}
	if gotT != acme || gotNS != ns || state != keys.JobRunning || due != 1_234_567 || got != jid {
		t.Fatalf("ParseJob = %q %q %#x %d %v", gotT, gotNS, byte(state), due, got)
	}
}

func TestParseJobRefusesWhatIsNotAJob(t *testing.T) {
	// A key the store holds that this binary cannot read is corruption, not a
	// parse miss: returning a zero value would put a job under the wrong
	// tenant or in the wrong partition.
	cases := map[string][]byte{
		"another space":  keys.Record(acme, ns, keys.RecordMemory, id.New()),
		"truncated body": keys.Job(acme, ns, keys.JobPending, 1, id.New())[:8],
		"unknown state":  keys.Job(acme, ns, keys.JobState(0x7F), 1, id.New()),
	}
	for name, k := range cases {
		if _, _, _, _, _, err := keys.ParseJob(k); !errs.Is(err, errs.Corruption) {
			t.Errorf("%s: want a corruption error, got %v", name, err)
		}
	}
}

func TestJobStatesHaveStableNames(t *testing.T) {
	// The names appear in logs and in the admin API, so they are a surface.
	want := map[keys.JobState]string{
		keys.JobPending: "pending",
		keys.JobRunning: "running",
		keys.JobDone:    "done",
	}
	for state, name := range want {
		if got := state.String(); got != name {
			t.Errorf("JobState(%#x).String() = %q, want %q", byte(state), got, name)
		}
	}
	if got := keys.JobState(0x7F).String(); got != "unknown" {
		t.Errorf("an unknown state names itself %q", got)
	}
}

func TestJobRangesAreScopedToOneNamespace(t *testing.T) {
	other, err := tenant.ParseNamespace("staging")
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := keys.JobStateRange(acme, ns, keys.JobPending)
	if k := keys.Job(acme, other, keys.JobPending, 1, id.New()); bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
		t.Fatal("another namespace's job fell inside this one's range")
	}
}
