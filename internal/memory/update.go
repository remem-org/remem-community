package memory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// Update changes an existing memory.
//
// # What a content change costs
//
// Everything derived from the content is rebuilt: the embedding, the inverted
// index's postings, and the attribute row. [record.Repo.Put] stages all three
// in registration order inside one transaction, so there is no separate
// reindex step to forget and no window in which the record and its indexes
// disagree.
//
// # Why the read is inside the transaction body
//
// This is a read-modify-write, and Phase 11 established that reading outside
// the transaction is how a pair of concurrent writers both decide the same
// thing is safe to do. Put conditions its commit on the bytes the record was
// read as (tx.Expect over originalBody, record/repo.go), so a writer that lost
// the race cannot overwrite the winner — but detecting the loss is only half of
// it. Those bytes are fixed at read time, so a read placed outside this body
// would have every retry re-issue the same stale expectation: the loser would
// conflict identically until its attempt budget ran out rather than converging
// on the winner. That is not a thought experiment — it is what the first
// version of this function did, and sixteen concurrent writers on one memory
// failed every one of their writes.
//
// So the read is inside the retried region and the embedding is outside it: the
// embedding is a function of the request alone, it is the expensive thing here,
// and Invariant 9 keeps it out of an apply path in any case.
//
// A txn.Gate would be the wrong fix, for the reason Phase 11 names about
// discovery: it makes the race unlikely rather than impossible, and does
// nothing across nodes. The gate this write does take (Service.write) is there
// for the contention every write creates on a tenant's shared rows, and it is
// not what makes this correct.
func (s *Service) Update(ctx context.Context, req UpdateReq) (*Memory, error) {
	const op = "memory.Update"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.ID.IsZero() {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("an update needs a memory to change"))
	}
	if req.isEmpty() {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"an update must change something; every field was left unset"))
	}
	if err := req.validate(op); err != nil {
		return nil, err
	}

	// A first read, outside everything, and it is *not* the one the write is
	// built on. It decides two things that must not be decided inside a retried
	// body: whether the content actually moves — an embedding is the most
	// expensive thing in this path and an identical rewrite must not pay for
	// one — and the two refusals, which are worth answering without opening a
	// transaction at all. A point read costs nothing beside a model pass, so
	// paying for two of them to avoid one wasted embedding is the right trade.
	probe, err := s.repo.Get(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if probe.Fields.Archived {
		// Archiving is a retirement. An edit that silently resurrected one into
		// search would be the "why did my memory come back" counterpart of the
		// question §II.10 row 8 exists to answer. Un-archiving is a separate,
		// explicit verb and nothing has asked for it yet.
		return nil, errs.E(errs.Conflict, op, fmt.Errorf(
			"memory %s is archived and cannot be updated; archiving is a retirement", req.ID))
	}

	// Everything a txn.Do retry must not redraw is computed before the
	// transaction exists: the timestamp and the embedding (Invariant 9, and see
	// txn.Do). Nothing about the old content is needed to embed the new, so
	// there is no read-before-embed ordering to get wrong.
	now := s.clk.Now().UTC()

	var newVector *vector.Vector
	if req.Content != nil && *req.Content != probe.Content {
		vs, eerr := s.embedder.Embed(ctx, []string{*req.Content})
		if eerr != nil {
			return nil, eerr
		}
		if len(vs) != 1 {
			return nil, errs.E(errs.Storage, op, fmt.Errorf(
				"the embedder returned %d vectors for one memory", len(vs)))
		}
		newVector = &vector.Vector{ModelID: s.embedder.ModelID(), Dim: len(vs[0]), Values: vs[0]}
	}

	// What the attempt that commits decided, lifted out of the body so the
	// post-commit index call reads the committed attempt's answer and not an
	// abandoned one's.
	var (
		stored         *record.Record
		contentChanged bool
	)

	if err := s.write(ctx, t, func(tx txn.Tx) error {
		// The read is *inside* the retried region, and that is the whole of what
		// makes the retry mean anything. Put conditions its commit on the bytes
		// the record was read as (tx.Expect over originalBody, record/repo.go),
		// and those bytes are fixed at read time: a read outside this body would
		// have every attempt re-issue the same stale expectation, so a writer
		// that lost the race would conflict identically until its budget ran out
		// rather than converging. Measured before the read moved in: sixteen
		// concurrent writers on one memory, every one of them failing after
		// eight attempts.
		//
		// It reads committed state rather than through tx, which is enough: the
		// Expect still refuses a lost update, and a re-read that meets a newer
		// winner simply conflicts and tries again — this time from bytes that
		// can converge.
		rec, gerr := s.repo.Get(ctx, req.ID)
		if gerr != nil {
			return gerr
		}
		if rec.Fields.Archived {
			return errs.E(errs.Conflict, op, fmt.Errorf(
				"memory %s is archived and cannot be updated; archiving is a retirement", req.ID))
		}

		// Recomputed per attempt, against the record this attempt read. A winner
		// that already wrote this content leaves nothing for the vector restage
		// to do, and its vector is the one this attempt would have written.
		contentChanged = req.Content != nil && *req.Content != rec.Content
		v := newVector
		if !contentChanged {
			v = nil
		}
		changed := req.apply(rec, now, v)
		stored = rec

		if perr := s.repo.Put(ctx, tx, rec); perr != nil {
			return errs.E(errs.KindOf(perr), op, perr)
		}
		if err := s.enqueueRediscovery(ctx, tx, t, rec, contentChanged); err != nil {
			return err
		}
		if s.events == nil {
			return nil
		}
		// The names of the fields that moved, never their values. See
		// events.Updated for why.
		//
		// In the transaction rather than after it. An audit row saying a memory
		// changed, beside a memory that did not, is worse than no row at all —
		// and unlike a recall, whose failure costs one promotion, this event is
		// the only thing that will ever say why the content is different.
		_, aerr := s.events.Append(ctx, tx, events.Event{
			Tenant: t, Namespace: rec.Namespace, Subject: rec.ID,
			At: now, Kind: events.Updated, Actor: "api",
			Reason: fmt.Sprintf("a caller changed %s", strings.Join(changed, ", ")),
			After:  map[string]string{"fields": strings.Join(changed, ",")},
		})
		return aerr
	}); err != nil {
		return nil, err
	}
	rec := stored

	if contentChanged {
		// After the commit, never inside it. The vector index is the one index
		// maintained asynchronously (plan §II.4): the memory is changed the
		// moment its transaction lands, and being findable by its new meaning
		// follows a moment later.
		s.indexed(ctx, t, rec.ID, newVector.Values)
	}
	return fromRecord(rec), nil
}

// enqueueRediscovery stages the follow-up relationship work into the write's
// own transaction, the same way CreateBatch stages a create's: a failed write
// leaves no orphan job and a successful one cannot lose its discovery.
//
// Only a content change queues one. Discovery is a vector search over the
// memory's meaning, and a tags-only edit moved no meaning — there is nothing
// for a neighbour search to reconsider.
//
// What this deliberately does not do is retire an edge the *old* content
// justified. A discovered edge carries no marker distinguishing it from a
// user's, on purpose, and §II.10 row 8 forbids a heuristic deleting
// user-visible data. So similar_to becomes a claim about the corpus at each
// time the memory was written, which is the bound discovery.md already records
// for a corpus that grows — an update widens it rather than introducing it.
func (s *Service) enqueueRediscovery(ctx context.Context, tx txn.Tx, t tenant.ID,
	rec *record.Record, contentChanged bool) error {
	if s.discovery == nil || !contentChanged {
		return nil
	}
	return s.discovery.EnqueueDiscovery(ctx, tx, t, rec.Namespace, []id.ID{rec.ID})
}

// apply writes the set fields onto the record and reports which ones moved.
//
// It is the one place a request becomes a record, so the delivery surfaces
// cannot drift into applying the same field two different ways.
func (r UpdateReq) apply(rec *record.Record, now time.Time, v *vector.Vector) []string {
	var changed []string
	if r.Content != nil && *r.Content != rec.Content {
		rec.Content = *r.Content
		changed = append(changed, "content")
	}
	if v != nil {
		rec.Vectors[record.VectorContent] = v
	}
	if r.Tags != nil {
		rec.Fields.Tags = normaliseTags(*r.Tags)
		changed = append(changed, "tags")
	}
	if r.Source != nil {
		rec.Fields.Source = *r.Source
		changed = append(changed, "source")
	}
	if r.Policy != nil {
		rec.Fields.Policy = *r.Policy
		changed = append(changed, "policy")
	}
	if r.Importance != nil {
		rec.Fields.Importance = *r.Importance
		changed = append(changed, "importance")
	}
	if r.Valence != nil {
		rec.Fields.Valence = *r.Valence
		changed = append(changed, "emotional_valence")
	}
	if r.Arousal != nil {
		rec.Fields.Arousal = *r.Arousal
		changed = append(changed, "arousal")
	}
	if r.TTL != nil {
		rec.Fields.TTL = *r.TTL
		changed = append(changed, "ttl")
	}
	// One rule, both write paths. applyFlashbulb only ever sets, so lowering
	// arousal below the threshold never revokes a protection already granted.
	applyFlashbulb(&rec.Fields, now)
	rec.UpdatedAt = now
	return changed
}

func (r UpdateReq) isEmpty() bool {
	return r.Content == nil && r.Tags == nil && r.Source == nil && r.Policy == nil &&
		r.Importance == nil && r.Valence == nil && r.Arousal == nil && r.TTL == nil
}

// validate reuses create's range checks rather than restating them. Create and
// update must not drift into accepting different values for one field, and the
// way to guarantee that is one function rather than two that agree today.
func (r UpdateReq) validate(op string) error {
	c := CreateReq{Importance: r.Importance, Valence: r.Valence, Arousal: r.Arousal}
	if r.Content != nil {
		c.Content = *r.Content
	} else {
		// validateCreate refuses empty content, and an update that does not
		// touch the content must not be refused for it.
		c.Content = "unchanged"
	}
	if r.Tags != nil {
		c.Tags = *r.Tags
	}
	if r.TTL != nil {
		c.TTL = *r.TTL
	}
	return validateCreate(0, c, op)
}
