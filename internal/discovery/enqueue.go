package discovery

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Enqueuer stages discovery work into somebody else's transaction.
//
// It implements memory.Enqueuer, and that interface lives in internal/memory
// rather than here so that the write path holds a one-method contract instead
// of the queue: the durable type name is the composition root's, and a domain
// package naming its own would be the framework-imports-its-work cycle by
// another route.
//
// It is also what the backfill command uses, through [Enqueuer.Submit], so the
// two ways discovery is asked for produce the same row.
type Enqueuer struct {
	queue *jobs.Queue
	typ   jobs.Type
}

// NewEnqueuer builds one over the queue, for the given durable job type.
func NewEnqueuer(q *jobs.Queue, typ jobs.Type) *Enqueuer {
	return &Enqueuer{queue: q, typ: typ}
}

// EnqueueDiscovery stages one job for the memories tx is writing.
//
// A nil Enqueuer stages nothing and returns nil, so a composition root that
// wires discovery off does not have to branch at the call site.
func (e *Enqueuer) EnqueueDiscovery(ctx context.Context, tx txn.Tx,
	t tenant.ID, ns tenant.Namespace, subjects []id.ID,
) error {
	if e == nil || e.queue == nil || len(subjects) == 0 {
		return nil
	}
	j, err := e.job(t, ns, subjects)
	if err != nil {
		return err
	}
	return e.queue.Enqueue(ctx, tx, j)
}

// Submit enqueues one job in a transaction of its own.
//
// It is the backfill form: there is no memory write to ride along with, so the
// job stands alone. Everything else about the row is identical, which is what
// keeps a backfilled subject and a freshly written one the same work.
func (e *Enqueuer) Submit(ctx context.Context, t tenant.ID, ns tenant.Namespace, subjects []id.ID) error {
	if e == nil || e.queue == nil || len(subjects) == 0 {
		return nil
	}
	j, err := e.job(t, ns, subjects)
	if err != nil {
		return err
	}
	return e.queue.Submit(ctx, j)
}

func (e *Enqueuer) job(t tenant.ID, ns tenant.Namespace, subjects []id.ID) (*jobs.Job, error) {
	const op = "discovery.Enqueue"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a discovery job requires a tenant: there is no unscoped write path (Invariant 1)"))
	}
	payload, err := EncodePayload(subjects)
	if err != nil {
		return nil, err
	}
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	return &jobs.Job{Tenant: t, Namespace: ns, Type: e.typ, Payload: payload}, nil
}
