// Package jobs is Remem's background job framework: one durable queue that
// every background workload runs on.
//
// Spec §22 lists what would otherwise each invent its own worker infrastructure
// — relationship discovery, embedding generation, index rebuilding, TTL expiry,
// importance decay, promotion, archival, snapshot creation, cleanup — and asks
// for one framework instead. This is it.
//
// # Delivery is at-least-once, and handlers must be idempotent
//
// A job can run twice for three ordinary reasons: a lease expires while a slow
// handler is still working and another worker reclaims it; a process dies
// between the handler returning and the completion committing; a retry re-runs
// a handler that failed after doing half its work. Exactly-once would require a
// handler's writes and the queue's completion to be one transaction, and these
// handlers write far more than one transaction should hold.
//
// The obligation is therefore on the handler, it is written down here, and it
// is asserted rather than asked for: every registered type is run twice on one
// payload and the whole keyspace is compared.
//
// # Job state is canonical
//
// Nothing reconstructs it (plan §II.4). A lost job row is lost work, which is
// the specific defect this framework replaces — Rust Remem's discovery queue
// was a bounded channel that dropped work under load and counted the drops.
// There is no rebuild path for the job space and there must not be one.
//
// # Every operation takes a tenant explicitly
//
// Not from the context. A job framework legitimately works across tenants,
// through tenant.Directory.ForEach — this is one of the packages allowed to
// call it — and forging a context per tenant to satisfy a convention would make
// the scope less visible, not more. It is a parameter on every method, so it
// cannot be omitted.
package jobs

import (
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// State is a job's logical state.
//
// Six states over three durable key partitions (see [State.Partition]). The
// partition is what a scan selects on; the state is what an operator reads.
type State uint8

const (
	// Pending is queued and claimable once RunAt passes.
	Pending State = iota + 1
	// Running is leased by a worker.
	Running
	// Retry is pending again after a failure, with a later RunAt and a
	// non-zero attempt count. It is a distinct state because "waiting to run
	// for the first time" and "waiting to run again because it broke" are
	// different things to an operator looking at a queue.
	Retry
	// Completed is terminal: the handler returned nil.
	Completed
	// Failed is terminal: the handler exhausted its attempts.
	Failed
	// Cancelled is terminal: somebody stopped it.
	Cancelled
)

var stateNames = map[State]string{
	Pending: "pending", Running: "running", Retry: "retry",
	Completed: "completed", Failed: "failed", Cancelled: "cancelled",
}

// String returns the stable snake_case name. The names appear in the admin API
// and in metric labels, so they do not change once shipped.
func (s State) String() string {
	if n, ok := stateNames[s]; ok {
		return n
	}
	return "unknown"
}

// AllStates returns every state, in order. A new one must be added here as
// well as above, which is what TestEveryStateMapsToExactlyOnePartition checks.
func AllStates() []State {
	return []State{Pending, Running, Retry, Completed, Failed, Cancelled}
}

// ParseState reads a state name, for the admin surface's filters.
func ParseState(s string) (State, error) {
	for st, name := range stateNames {
		if name == s {
			return st, nil
		}
	}
	return 0, errs.E(errs.Invalid, "jobs.ParseState",
		fmt.Errorf("%q is not a job state; the states are pending, running, retry, completed, failed and cancelled", s))
}

// Terminal reports whether nothing will run this job again.
func (s State) Terminal() bool {
	return s == Completed || s == Failed || s == Cancelled
}

// Partition is the durable key byte a job in this state is stored under.
//
// Three partitions rather than six, and the mapping is a durable decision:
//
//	Pending, Retry                 → keys.JobPending, due at RunAt
//	Running                        → keys.JobRunning, due at the lease expiry
//	Completed, Failed, Cancelled   → keys.JobDone,    due when it finished
//
// Each of the three scans the queue performs — claim due work, reclaim lapsed
// leases, reap old rows — is then a forward scan that stops at the first row it
// does not want. Retry does not get its own partition because a retry *is* a
// pending job with a later due time, and splitting them would make the claim
// scan read two ranges and merge them by due time for no gain.
func (s State) Partition() keys.JobState {
	switch {
	case s == Running:
		return keys.JobRunning
	case s.Terminal():
		return keys.JobDone
	default:
		return keys.JobPending
	}
}

// Type names a registered handler.
//
// It is a durable name: it is written into every row of that type, read back by
// a registry lookup, and used as a metric label and a URL path segment. A
// retired type name stays retired.
type Type string

// MaxTypeLen bounds a type name, because it is stored on every row of that
// type.
const MaxTypeLen = 64

func (t Type) String() string { return string(t) }

// ParseType validates a type name.
//
// The character set is [a-z0-9_.] for the reason tenant ids are [a-z0-9_-]: the
// name travels through metric labels, log fields and URL paths, and every
// character needing escaping in one of those eventually gets escaped
// inconsistently in another. The dot is permitted because it is what separates
// a subsystem from its verb — "vector.rebuild" — and reads better than an
// underscore doing two jobs.
func ParseType(s string) (Type, error) {
	const op = "jobs.ParseType"
	if s == "" {
		return "", errs.E(errs.Invalid, op, errors.New("a job type must not be empty"))
	}
	if len(s) > MaxTypeLen {
		return "", errs.E(errs.Invalid, op,
			fmt.Errorf("job type %q is %d bytes, the limit is %d", s, len(s), MaxTypeLen))
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '.':
		default:
			return "", errs.E(errs.Invalid, op, fmt.Errorf(
				"job type %q contains %q; only [a-z0-9_.] is allowed", s, string(c)))
		}
	}
	return Type(s), nil
}

// MaxPayloadBytes bounds a payload.
//
// A payload is a reference to work, not the work: "rebuild this tenant's vector
// index", not the vectors. Anything larger belongs in the store the handler
// reads, and a job row that carries it is a row every claim scan pays to skip.
const MaxPayloadBytes = 64 << 10

// Lease is one worker's claim on a running job.
type Lease struct {
	// Owner is the node that holds the claim. One value today, many later.
	Owner string
	// ExpiresAt is when the claim lapses. It is also the due time encoded in
	// the running key, so a lapsed lease is at the head of a forward scan.
	ExpiresAt time.Time
	// Token fences a reclaimed job against its previous owner. Reclamation
	// draws a new one, so a worker that wakes up holding the old one is
	// refused rather than allowed to complete work somebody else has redone.
	Token id.ID
}

// Job is one unit of background work.
type Job struct {
	ID        id.ID
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Type      Type
	Payload   []byte

	State       State
	Attempts    uint32
	MaxAttempts uint32
	RunAt       time.Time
	Lease       *Lease
	Checkpoint  []byte
	LastError   string

	// Priority orders the jobs within one claim batch, and nothing wider. The
	// key is ordered by due time and priority is not in it, so it cannot order
	// the queue; saying so here is cheaper than an operator discovering it.
	Priority int8

	CreatedAt time.Time
	UpdatedAt time.Time

	// stored is the key this job was last read from or written at.
	//
	// Every transition deletes one key and writes another, and the key is a
	// function of fields the caller holds a pointer to. Recomputing the old key
	// from the current struct would be correct only as long as nothing mutated
	// it in between — and a handler is handed a *Job. Remembering the byte
	// slice makes the delete independent of that.
	stored []byte
}

// Key is where this job's row lives, given its current state.
func (j *Job) Key() ([]byte, error) {
	const op = "jobs.Job.Key"
	if j.Tenant == "" {
		return nil, errs.E(errs.Invalid, op, errors.New("a job key requires a tenant (Invariant 1)"))
	}
	ns := j.namespace()
	part := j.State.Partition()

	var due time.Time
	switch part {
	case keys.JobRunning:
		if j.Lease == nil {
			return nil, errs.E(errs.Invalid, op, errors.New(
				"a running job has no lease, so there is no expiry to order it by"))
		}
		due = j.Lease.ExpiresAt
	case keys.JobDone:
		due = j.UpdatedAt
	default:
		due = j.RunAt
	}
	return keys.Job(j.Tenant, ns, part, unixMilli(due), j.ID), nil
}

// StoredKey is the key this job was read from, which is what a transition must
// delete. It is empty for a job that has never been written.
func (j *Job) StoredKey() []byte { return j.stored }

func (j *Job) namespace() tenant.Namespace {
	if j.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return j.Namespace
}

// Validate refuses a job that cannot be stored or claimed.
func (j *Job) Validate() error {
	const op = "jobs.Job.Validate"
	if j.ID == id.Zero {
		return errs.E(errs.Invalid, op, errors.New("a job needs an id"))
	}
	if j.Tenant == "" || !j.Tenant.Valid() {
		return errs.E(errs.Invalid, op, fmt.Errorf("tenant %q is not a valid tenant id", j.Tenant))
	}
	if ns := j.namespace(); !ns.Valid() {
		return errs.E(errs.Invalid, op, fmt.Errorf("namespace %q is malformed", ns))
	}
	if _, err := ParseType(string(j.Type)); err != nil {
		return err
	}
	if _, known := stateNames[j.State]; !known {
		return errs.E(errs.Invalid, op, fmt.Errorf("state %d is not a job state", uint8(j.State)))
	}
	if len(j.Payload) > MaxPayloadBytes {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"payload is %d bytes, the limit is %d; a payload is a reference to work, not the work",
			len(j.Payload), MaxPayloadBytes))
	}
	return nil
}

// Clone returns a deep copy, so that a caller holding a job across a
// transition cannot observe a half-applied one.
func (j *Job) Clone() *Job {
	out := *j
	out.Payload = append([]byte(nil), j.Payload...)
	out.Checkpoint = append([]byte(nil), j.Checkpoint...)
	out.stored = append([]byte(nil), j.stored...)
	if j.Lease != nil {
		lease := *j.Lease
		out.Lease = &lease
	}
	return &out
}

func unixMilli(t time.Time) uint64 {
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}
