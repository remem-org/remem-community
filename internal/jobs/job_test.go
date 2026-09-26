package jobs_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

const acme = tenant.ID("acme")

func TestEveryStateMapsToExactlyOnePartition(t *testing.T) {
	// The key byte is a scan partition and the body's state is the finer state
	// within it. A state with no partition would be a job nothing scans; one
	// with two would be a job in two places.
	want := map[jobs.State]keys.JobState{
		jobs.Pending:   keys.JobPending,
		jobs.Retry:     keys.JobPending,
		jobs.Running:   keys.JobRunning,
		jobs.Completed: keys.JobDone,
		jobs.Failed:    keys.JobDone,
		jobs.Cancelled: keys.JobDone,
	}
	seen := map[keys.JobState]bool{}
	for _, s := range jobs.AllStates() {
		p, ok := want[s]
		if !ok {
			t.Fatalf("state %s has no expected partition; the mapping is a durable decision", s)
		}
		if got := s.Partition(); got != p {
			t.Errorf("%s maps to partition %s, want %s", s, got, p)
		}
		seen[p] = true
	}
	for _, p := range keys.AllJobStates() {
		if !seen[p] {
			t.Errorf("no state maps to partition %s, so nothing ever writes a row there", p)
		}
	}
}

func TestTerminalStatesAreTheOnesNothingRunsAgain(t *testing.T) {
	terminal := map[jobs.State]bool{jobs.Completed: true, jobs.Failed: true, jobs.Cancelled: true}
	for _, s := range jobs.AllStates() {
		if got := s.Terminal(); got != terminal[s] {
			t.Errorf("%s.Terminal() = %v", s, got)
		}
	}
}

func TestStateNamesAreStable(t *testing.T) {
	// They appear in the admin API and in metric labels.
	want := map[jobs.State]string{
		jobs.Pending: "pending", jobs.Running: "running", jobs.Retry: "retry",
		jobs.Completed: "completed", jobs.Failed: "failed", jobs.Cancelled: "cancelled",
	}
	for s, name := range want {
		if got := s.String(); got != name {
			t.Errorf("State(%d).String() = %q, want %q", uint8(s), got, name)
		}
	}
	if got := jobs.State(99).String(); got != "unknown" {
		t.Errorf("an unrecognised state names itself %q", got)
	}
	if _, err := jobs.ParseState("nonsense"); !errs.Is(err, errs.Invalid) {
		t.Error("parsing an unknown state name must be refused")
	}
	for s, name := range want {
		got, err := jobs.ParseState(name)
		if err != nil || got != s {
			t.Errorf("ParseState(%q) = %v, %v", name, got, err)
		}
	}
}

func TestTypeNamesAreConstrained(t *testing.T) {
	// A type name is written into every row of that type, so it is a durable
	// name. The character set is the one that survives a metric label, a log
	// field and a URL path segment without escaping.
	good := []string{"vector.rebuild", "text.rebuild", "jobs.reap", "a", "a_b.c9"}
	for _, s := range good {
		if _, err := jobs.ParseType(s); err != nil {
			t.Errorf("ParseType(%q): %v", s, err)
		}
	}
	bad := []string{"", "Vector.Rebuild", "vector rebuild", "vector/rebuild", strings.Repeat("a", 65)}
	for _, s := range bad {
		if _, err := jobs.ParseType(s); !errs.Is(err, errs.Invalid) {
			t.Errorf("ParseType(%q) was accepted", s)
		}
	}
}

func TestAJobKnowsWhichKeyItLivesAt(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	j := newJob(clk, jobs.Type("vector.rebuild"))

	j.State = jobs.Pending
	j.RunAt = clk.Now().Add(time.Minute)
	pending := mustKey(t, j)

	_, _, state, due, jid, err := keys.ParseJob(pending)
	if err != nil {
		t.Fatalf("ParseJob: %v", err)
	}
	if state != keys.JobPending || jid != j.ID {
		t.Fatalf("a pending job is at %s/%v", state, jid)
	}
	if want := uint64(j.RunAt.UnixMilli()); due != want {
		t.Fatalf("a pending job is due at %d, want its RunAt %d", due, want)
	}

	// Running: the encoded time is the lease expiry, because that is what the
	// reclamation scan orders by.
	j.State = jobs.Running
	j.Lease = &jobs.Lease{Owner: "node-1", ExpiresAt: clk.Now().Add(time.Minute), Token: id.New()}
	_, _, state, due, _, err = keys.ParseJob(mustKey(t, j))
	if err != nil {
		t.Fatalf("ParseJob: %v", err)
	}
	if state != keys.JobRunning || due != uint64(j.Lease.ExpiresAt.UnixMilli()) {
		t.Fatalf("a running job is at %s due %d", state, due)
	}

	// Terminal: the encoded time is when it finished, because that is what the
	// reaper orders by.
	j.State = jobs.Completed
	j.Lease = nil
	j.UpdatedAt = clk.Now().Add(2 * time.Minute)
	_, _, state, due, _, err = keys.ParseJob(mustKey(t, j))
	if err != nil {
		t.Fatalf("ParseJob: %v", err)
	}
	if state != keys.JobDone || due != uint64(j.UpdatedAt.UnixMilli()) {
		t.Fatalf("a finished job is at %s due %d", state, due)
	}
}

func TestARunningJobWithoutALeaseHasNoKey(t *testing.T) {
	// The running partition is ordered by lease expiry, so a running job with
	// no lease has nowhere to go. Writing it at zero would put it permanently
	// at the head of the reclamation scan.
	clk := clock.NewFake(clock.FakeStart)
	j := newJob(clk, "vector.rebuild")
	j.State = jobs.Running
	if _, err := j.Key(); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a running job with no lease produced a key: %v", err)
	}
}

func TestJobValidationRefusesWhatCannotBeStored(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	cases := map[string]func(*jobs.Job){
		"no tenant":         func(j *jobs.Job) { j.Tenant = "" },
		"malformed tenant":  func(j *jobs.Job) { j.Tenant = "Acme Corp" },
		"no type":           func(j *jobs.Job) { j.Type = "" },
		"malformed type":    func(j *jobs.Job) { j.Type = "Vector Rebuild" },
		"no id":             func(j *jobs.Job) { j.ID = id.Zero },
		"unknown state":     func(j *jobs.Job) { j.State = jobs.State(99) },
		"oversized payload": func(j *jobs.Job) { j.Payload = make([]byte, jobs.MaxPayloadBytes+1) },
	}
	for name, break_ := range cases {
		j := newJob(clk, "vector.rebuild")
		break_(j)
		if err := j.Validate(); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: want Invalid, got %v", name, err)
		}
	}
	if err := newJob(clk, "vector.rebuild").Validate(); err != nil {
		t.Errorf("a well-formed job was refused: %v", err)
	}
}

func TestJobRowRoundTripsEveryField(t *testing.T) {
	// A field that is written and not read back is a field that silently
	// resets — an attempt count that forgets is a job that retries forever.
	clk := clock.NewFake(clock.FakeStart)
	j := newJob(clk, "text.rebuild")
	j.Namespace = tenant.DefaultNamespace
	j.Payload = []byte("payload")
	j.State = jobs.Running
	j.Attempts = 3
	j.MaxAttempts = 6
	j.RunAt = clk.Now().Add(time.Minute).UTC()
	j.Lease = &jobs.Lease{Owner: "node-7", ExpiresAt: clk.Now().Add(time.Minute).UTC(), Token: id.New()}
	j.Checkpoint = []byte("cursor")
	j.LastError = "the last thing that went wrong"
	j.Priority = -3
	j.UpdatedAt = clk.Now().Add(time.Second).UTC()

	value, err := jobs.Marshal(j)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	key, err := j.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	got, err := jobs.Unmarshal(key, value)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.ID != j.ID || got.Tenant != j.Tenant || got.Namespace != j.Namespace {
		t.Fatalf("identity was lost: %v %q %q", got.ID, got.Tenant, got.Namespace)
	}
	if got.Type != j.Type || !bytes.Equal(got.Payload, j.Payload) || got.State != j.State {
		t.Fatalf("type, payload or state was lost: %+v", got)
	}
	if got.Attempts != j.Attempts || got.MaxAttempts != j.MaxAttempts || got.Priority != j.Priority {
		t.Fatalf("counters were lost: %+v", got)
	}
	if !got.RunAt.Equal(j.RunAt) || !got.CreatedAt.Equal(j.CreatedAt) || !got.UpdatedAt.Equal(j.UpdatedAt) {
		t.Fatalf("times were lost: %v %v %v", got.RunAt, got.CreatedAt, got.UpdatedAt)
	}
	if got.Lease == nil || got.Lease.Owner != j.Lease.Owner || got.Lease.Token != j.Lease.Token ||
		!got.Lease.ExpiresAt.Equal(j.Lease.ExpiresAt) {
		t.Fatalf("the lease was lost: %+v", got.Lease)
	}
	if !bytes.Equal(got.Checkpoint, j.Checkpoint) || got.LastError != j.LastError {
		t.Fatalf("checkpoint or error was lost: %+v", got)
	}
}

func TestUnmarshalRefusesARowThatDoesNotDecode(t *testing.T) {
	// Job state is canonical: a row this binary cannot read is corruption, and
	// falling back to a zero value would silently re-run somebody's work from
	// the start with no attempt count.
	clk := clock.NewFake(clock.FakeStart)
	key, err := newJob(clk, "vector.rebuild").Key()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.Unmarshal(key, []byte("not a protobuf message at all")); !errs.Is(err, errs.Corruption) {
		t.Errorf("a corrupt row decoded: %v", err)
	}
	if _, err := jobs.Unmarshal([]byte("not a key"), []byte{}); !errs.Is(err, errs.Corruption) {
		t.Errorf("a corrupt key decoded: %v", err)
	}
}

func TestUnmarshalRefusesARowThatDisagreesWithItsKey(t *testing.T) {
	// The key and the body say the same two things twice. A row stored at a due
	// time its body does not agree with is either in a scan that never reaches
	// it or in one that reaches it forever, and neither is discoverable from
	// the outside — so it is refused here rather than served.
	clk := clock.NewFake(clock.FakeStart)
	j := newJob(clk, "vector.rebuild")
	j.RunAt = clk.Now().Add(time.Hour)
	value, err := jobs.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}

	misplaced := keys.Job(j.Tenant, tenant.DefaultNamespace, keys.JobPending, 1, j.ID)
	if _, err := jobs.Unmarshal(misplaced, value); !errs.Is(err, errs.Corruption) {
		t.Errorf("a row stored at the wrong due time decoded: %v", err)
	}

	wrongPartition := keys.Job(j.Tenant, tenant.DefaultNamespace, keys.JobDone,
		uint64(j.RunAt.UnixMilli()), j.ID)
	if _, err := jobs.Unmarshal(wrongPartition, value); !errs.Is(err, errs.Corruption) {
		t.Errorf("a pending row stored in the done partition decoded: %v", err)
	}
}

func newJob(clk clock.Clock, typ jobs.Type) *jobs.Job {
	now := clk.Now().UTC()
	return &jobs.Job{
		ID: id.New(), Tenant: acme, Namespace: tenant.DefaultNamespace,
		Type: typ, State: jobs.Pending, MaxAttempts: 3,
		RunAt: now, CreatedAt: now, UpdatedAt: now,
	}
}

func mustKey(t *testing.T, j *jobs.Job) []byte {
	t.Helper()
	k, err := j.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	return k
}
