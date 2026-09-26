package memory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// Create stores one memory.
func (s *Service) Create(ctx context.Context, req CreateReq) (*Memory, error) {
	out, err := s.CreateBatch(ctx, []CreateReq{req})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// CreateBatch stores several memories in one transaction and one embedding
// call.
//
// It exists from the first version rather than being retrofitted, because
// batching is what makes agent ingestion cheap: one transaction, one embedding
// call for up to a batch's worth of texts, and later one discovery job. Create
// is defined in terms of it rather than the other way round, so there is one
// write path and it is the batched one — a separate single-item path is where
// the two eventually diverge.
//
// The whole batch commits or none of it does. A partial batch would leave an
// agent with no way to know which of its twenty facts were stored.
func (s *Service) CreateBatch(ctx context.Context, reqs []CreateReq) ([]*Memory, error) {
	const op = "memory.CreateBatch"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("no memories to store"))
	}
	if len(reqs) > MaxBatch {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"a batch of %d memories; the limit is %d, so send it in parts", len(reqs), MaxBatch))
	}

	texts := make([]string, len(reqs))
	for i, r := range reqs {
		if err := validateCreate(i, r, op); err != nil {
			return nil, err
		}
		texts[i] = r.Content
	}

	// One embedding call for the whole batch. The service below chunks to its
	// own batch size and coalesces with other callers; asking it once per
	// memory would defeat both.
	vectors, err := s.embedder.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vectors) != len(reqs) {
		return nil, errs.E(errs.Storage, op, fmt.Errorf(
			"the embedder returned %d vectors for %d memories", len(vectors), len(reqs)))
	}

	// Everything a retry must not redraw is drawn here, before the transaction
	// exists: the timestamp, and the id of every record. A commit that meets a
	// concurrent write is retried (see txn.Do), and a second attempt that
	// minted new ids would store different memories than the first attempted
	// to — which Invariant 9 forbids outright in the path Phase 14 replicates.
	now := s.clk.Now().UTC()
	modelID := s.embedder.ModelID()

	records := make([]*record.Record, len(reqs))
	out := make([]*Memory, len(reqs))
	for i, req := range reqs {
		// The lifecycle defaults are applied here, at the one place a memory is
		// born, rather than at encode time. The record in hand and the
		// attribute row projected from it then agree about the same memory; a
		// default applied later is invisible to both.
		//
		// They are written out rather than left to record.Fields.WithDefaults,
		// and that is a correctness point rather than a style one. WithDefaults
		// takes an empty policy as its signal that the whole group is unset,
		// which is right for a record decoded from disk and wrong here: a
		// caller who names a policy and nothing else would get importance 0 and
		// **health 0**, and health 0 archives on the first sweep. See
		// TestNamingAPolicyDoesNotArriveWithNoHealth.
		fields := record.Fields{
			Tags:       normaliseTags(req.Tags),
			Source:     req.Source,
			Policy:     req.Policy,
			TTL:        req.TTL,
			Importance: record.DefaultImportance,
			Health:     record.DefaultHealth,
		}
		if fields.Policy == "" {
			fields.Policy = record.DefaultPolicy
		}
		if req.Importance != nil {
			fields.Importance = *req.Importance
		}
		if req.Valence != nil {
			fields.Valence = *req.Valence
		}
		if req.Arousal != nil {
			fields.Arousal = *req.Arousal
		}
		applyFlashbulb(&fields, now)
		// A TTL is a short-term memory's and nothing else's: Rust drops it from
		// any memory that ends up long-term (memory_manager.rs:149-150), and a
		// flashbulb promotion is a promotion, which clears the TTL as every
		// other one does (internal/lifecycle/ttl.go). Checked after the
		// flashbulb rule, because that rule decides the policy.
		if fields.Policy != lifecycle.ShortTerm {
			fields.TTL = 0
		}

		rec := &record.Record{
			ID:        id.New(),
			Tenant:    t,
			Namespace: tenant.DefaultNamespace,
			Type:      record.TypeMemory,
			Content:   req.Content,
			Fields:    fields,
			Vectors: map[string]*vector.Vector{
				record.VectorContent: {
					ModelID: modelID,
					Dim:     len(vectors[i]),
					Values:  vectors[i],
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		records[i] = rec
		out[i] = fromRecord(rec)
	}

	subjects := make([]id.ID, len(records))
	for i, rec := range records {
		subjects[i] = rec.ID
	}

	if err := s.write(ctx, t, func(tx txn.Tx) error {
		for _, rec := range records {
			if err := s.repo.Put(ctx, tx, rec); err != nil {
				return err
			}
		}
		// In the transaction, not after it. Rust does a try_send once the write
		// has landed, so a full channel loses the follow-up work silently and
		// counts the loss (plan §II.10 row 5). Staged here, a failed write
		// leaves no orphan job and a successful one cannot lose its discovery.
		//
		// The body may run more than once under txn.Do, which gives each
		// attempt a fresh transaction, so a retry stages a fresh job row and
		// only the attempt that commits leaves one. The record ids above are
		// drawn outside the body because a retry must store the *same* memories;
		// a job id has no such obligation, since nothing outside the queue names
		// it before it exists.
		if s.discovery == nil {
			return nil
		}
		return s.discovery.EnqueueDiscovery(ctx, tx, t, tenant.DefaultNamespace, subjects)
	}); err != nil {
		return nil, err
	}

	// After the commit, never inside it. The vector index is the one index
	// maintained asynchronously (plan §II.4), so a memory is stored the moment
	// its transaction lands and being findable follows a moment later. An index
	// insert that fails leaves a canonical vector with no entry, which the next
	// materialisation repairs — see internal/vector/hnsw.
	for i, m := range out {
		s.indexed(ctx, t, m.ID, vectors[i])
	}
	return out, nil
}

// applyFlashbulb promotes a highly-arousing memory and protects it.
//
// Rust's rule, kept exactly (services/memory_manager.rs:114-125): arousal at or
// above 0.8 makes the memory long-term whatever the caller asked for, and
// immune to decay and forgetting for thirty days. The threshold is inclusive
// and there is no gradation around it — 0.8 protects, 0.79 does not — and the
// promotion *overrides* the requested policy rather than merging with it, so a
// request saying short_term with arousal 0.9 produces a long-term memory and
// the response reports the policy it got.
//
// It is a protection window on the record rather than a temporary policy that
// reverts. Plan §II.6 asks for the latter; saying it that way needs two more
// durable fields — what to revert to, and when — and makes a record's policy a
// value that changes by itself, which nothing else in the system does.
func applyFlashbulb(f *record.Fields, now time.Time) {
	if f.Arousal < lifecycle.FlashbulbArousal {
		return
	}
	f.Policy = lifecycle.LongTerm
	f.ProtectedUntil = now.Add(lifecycle.FlashbulbProtection)
}

func validateCreate(i int, r CreateReq, op string) error {
	where := ""
	if i > 0 {
		where = fmt.Sprintf(" (memory %d of the batch)", i)
	}
	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format+"%s", append(args, where)...))
	}

	if strings.TrimSpace(r.Content) == "" {
		return errs.E(errs.Invalid, op, fmt.Errorf("%w%s", errNoContent, where))
	}
	if len(r.Content) > MaxContentBytes {
		return bad("content is %d bytes, the limit is %d", len(r.Content), MaxContentBytes)
	}
	if len(r.Tags) > MaxTags {
		return bad("%d tags, the limit is %d", len(r.Tags), MaxTags)
	}
	for _, tag := range r.Tags {
		if strings.TrimSpace(tag) == "" {
			return bad("a tag must not be blank")
		}
	}

	// The affective and weighting values are refused out of range rather than
	// clamped. A caller who sent importance 5 meant something, and storing 1
	// silently makes every later ranking a lie they have no way to notice.
	if v := r.Importance; v != nil && (*v < 0 || *v > 1) {
		return bad("importance is %v; it is a weight from 0 to 1", *v)
	}
	if v := r.Valence; v != nil && (*v < -1 || *v > 1) {
		return bad("emotional valence is %v; it runs from -1 to 1", *v)
	}
	if v := r.Arousal; v != nil && (*v < 0 || *v > 1) {
		return bad("arousal is %v; it runs from 0 to 1", *v)
	}
	if r.TTL < 0 {
		return bad("a ttl of %v would expire the memory before it exists", r.TTL)
	}
	return nil
}

// normaliseTags lowercases, trims and de-duplicates, preserving order.
//
// Tags are ordinary terms in the text index rather than a special case (plan
// §II.10 row 2), so they are normalised the way a term is. Case-folding here
// rather than at query time means "Weather" and "weather" are one tag on disk,
// which is the only place the two can be reconciled without a scan.
func normaliseTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(tags))
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out
}
