package jobs

import (
	"bytes"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs/pb"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"google.golang.org/protobuf/proto"
)

// The job row is protobuf, unframed and not independently versioned.
//
// Unframed because the key's space byte already says what the value is — the
// record envelope exists so a *body encoding* can be replaced under a value
// that several codecs share, and nothing else writes into the job space. Not
// independently versioned because protobuf's field-number evolution is what
// carries it forward, which is the footing the tenant directory and the session
// registry are already on; the four versioned formats are the ones whose
// meaning cannot be evolved that way.
//
// Deterministic marshalling, for the reason internal/codec gives for edges: a
// row's bytes should be a function of the job and nothing else, so that two
// nodes handed the same job write the same row (Invariants 8 and 9, Phase 14).
var deterministic = proto.MarshalOptions{Deterministic: true}

// Marshal encodes a job's row value.
//
// The tenant, the namespace-of-the-row and the id are not in it: they are in
// the key, and a second copy is a second thing that can disagree with the
// first. The namespace the job *acts on* is in the body, because that is the
// handler's input rather than the row's address.
func Marshal(j *Job) ([]byte, error) {
	const op = "jobs.Marshal"
	if err := j.Validate(); err != nil {
		return nil, err
	}

	row := &pb.Job{
		Type:            string(j.Type),
		Payload:         j.Payload,
		State:           uint32(j.State),
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		RunAtUnixMs:     unixMilli(j.RunAt),
		Checkpoint:      j.Checkpoint,
		LastError:       j.LastError,
		Priority:        int32(j.Priority),
		CreatedAtUnixMs: unixMilli(j.CreatedAt),
		UpdatedAtUnixMs: unixMilli(j.UpdatedAt),
		Namespace:       string(j.namespace()),
	}
	if j.Lease != nil {
		row.Lease = &pb.Lease{
			Owner:           j.Lease.Owner,
			ExpiresAtUnixMs: unixMilli(j.Lease.ExpiresAt),
			Token:           j.Lease.Token.Bytes(),
		}
	}

	out, err := deterministic.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the job: %w", err))
	}
	return out, nil
}

// Unmarshal decodes a row read from key.
//
// A row that does not decode is [errs.Corruption], never a zero value. Job
// state is canonical: a job silently reset to zero would be re-run from the
// start, with no attempt count, by a worker that had no way to know.
func Unmarshal(key, value []byte) (*Job, error) {
	const op = "jobs.Unmarshal"

	t, ns, part, due, jid, err := keys.ParseJob(key)
	if err != nil {
		return nil, err
	}

	var row pb.Job
	if err := proto.Unmarshal(value, &row); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the job row at %x does not decode: %w", key, err))
	}

	j := &Job{
		ID:          jid,
		Tenant:      t,
		Namespace:   ns,
		Type:        Type(row.GetType()),
		Payload:     row.GetPayload(),
		State:       State(row.GetState()),
		Attempts:    row.GetAttempts(),
		MaxAttempts: row.GetMaxAttempts(),
		RunAt:       fromUnixMilli(row.GetRunAtUnixMs()),
		Checkpoint:  row.GetCheckpoint(),
		LastError:   row.GetLastError(),
		Priority:    int8(row.GetPriority()),
		CreatedAt:   fromUnixMilli(row.GetCreatedAtUnixMs()),
		UpdatedAt:   fromUnixMilli(row.GetUpdatedAtUnixMs()),
		stored:      append([]byte(nil), key...),
	}
	if sub := row.GetNamespace(); sub != "" {
		j.Namespace = tenant.Namespace(sub)
	}
	if l := row.GetLease(); l != nil {
		token, err := id.FromBytes(l.GetToken())
		if err != nil {
			return nil, errs.E(errs.Corruption, op, fmt.Errorf(
				"the lease on job %s holds a malformed fencing token", jid))
		}
		j.Lease = &Lease{
			Owner:     l.GetOwner(),
			ExpiresAt: fromUnixMilli(l.GetExpiresAtUnixMs()),
			Token:     token,
		}
	}

	// The key and the body say the same two things twice — which partition the
	// job is in, and when it is due there — and both halves are written in one
	// transaction. A row where they disagree was not written by this code, and
	// trusting either half over the other would put a job in a scan that never
	// reaches it or one that reaches it forever.
	if j.State.Partition() != part {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"job %s is stored in the %s partition but its row says %s", jid, part, j.State))
	}
	if want, err := j.Key(); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf("job %s cannot say where it belongs: %w", jid, err))
	} else if !bytes.Equal(want, key) {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"job %s is stored due at %d but its row puts it elsewhere", jid, due))
	}
	return j, nil
}

func fromUnixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
