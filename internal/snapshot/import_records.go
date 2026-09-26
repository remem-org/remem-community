package snapshot

// The records half of an import: reading a page out of the file, deciding what
// happens to its vectors, and writing it through the repository so that every
// derived row is built by the code that owns it.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"google.golang.org/protobuf/proto"
)

func (im *importer) readRecords(blk *Block) error {
	const op = "snapshot.Import"

	im.byID = make(map[id.ID]*pendingRecord)
	return blk.Each(func(raw []byte) error {
		var m pb.Record
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("decoding a record: %w", err))
		}
		rec, err := recordFrom(&m, im.now())
		if err != nil {
			return err
		}
		im.note(rec.Tenant)
		p := &pendingRecord{rec: rec}
		im.page = append(im.page, p)
		im.byID[rec.ID] = p
		return nil
	})
}

func (im *importer) readRecordExts(blk *Block) error {
	const op = "snapshot.Import"

	return blk.Each(func(raw []byte) error {
		var m pbext.RecordExt
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("decoding a record extension: %w", err))
		}
		rid, err := id.FromBytes(m.GetId())
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"a record extension carries a malformed id: %w", err))
		}
		p, ok := im.byID[rid]
		if !ok {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"a record extension names %s, which is not in the records block it annotates",
				rid))
		}
		p.rec.Fields.ArchivedAt = fromUnixMs(m.GetArchivedAtUnixMs())
		if v := m.GetSchemaVersion(); v != 0 {
			p.rec.SchemaVersion = v
		}
		return nil
	})
}

func (im *importer) readVectors(blk *Block) error {
	const op = "snapshot.Import"

	return blk.Each(func(raw []byte) error {
		var m pb.Vector
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("decoding a vector: %w", err))
		}
		rid, err := id.FromBytes(m.GetId())
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"a vector carries a malformed record id: %w", err))
		}
		p, ok := im.byID[rid]
		if !ok {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"the snapshot carries a vector for %s and no record for it in the same page. "+
					"A vector with no record is not importable data", rid))
		}
		p.vector = &vector.Vector{
			ModelID: m.GetModelId(), Dim: int(m.GetDim()), Values: m.GetValues(),
		}
		p.hasVector = true
		// Stats count what the snapshot *carried*, which is what the header's
		// claim is about. What the store ends up with is ReEmbedded plus
		// VectorsVerbatim, and the two are deliberately different numbers: a
		// Rust import reads two thousand vectors and keeps none of them.
		im.report.Stats.Vectors++
		return nil
	})
}

// flush writes whatever is buffered: the tenants block and then the records
// page, each of which waits only for the companion block that completes it.
func (im *importer) flush(ctx context.Context) error {
	if err := im.flushTenants(ctx); err != nil {
		return err
	}
	return im.flushPage(ctx)
}

// flushPage applies the vector policy to the buffered page, then writes it in
// batches through the record repository, so every derived row is built by the
// code that owns it.
func (im *importer) flushPage(ctx context.Context) error {
	if len(im.page) == 0 {
		return nil
	}
	page := im.page
	im.page, im.byID = nil, nil

	if err := im.settleVectors(ctx, page); err != nil {
		return err
	}

	batch := im.opts.batchSize()
	for start := 0; start < len(page); start += batch {
		end := min(start+batch, len(page))
		if err := im.writeRecords(ctx, page[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// settleVectors applies the vector policy to a page, re-embedding in one batch.
//
// One model run per page rather than per record: an embedder batches internally
// and a 250,000-record import that called it a record at a time would pay the
// per-run overhead a quarter of a million times.
//
// # A record the file carries no vector for
//
// It gets one only when this import is recomputing the page's vectors anyway.
// The reasoning is that absence means two different things depending on where
// the file came from. In a Rust snapshot it is bookkeeping — an archived memory
// has been retired from the similarity index — and Go keeps vectors for archived
// memories, so recomputing is the faithful import. In a Go snapshot it is a fact
// about the corpus, and inventing a vector would make a round trip lossy in the
// one direction nothing else can detect.
func (im *importer) settleVectors(ctx context.Context, page []*pendingRecord) error {
	const op = "snapshot.Import"

	all := im.reEmbedsEverything()
	var need []*pendingRecord
	var texts []string
	for _, p := range page {
		switch {
		case !all && im.keepVector(p):
			p.rec.Vectors[record.VectorContent] = p.vector
			im.report.VectorsVerbatim++
			continue
		case !all && !p.hasVector:
			im.report.Stats.MissingVectors++
			continue
		}
		if !p.hasVector {
			im.report.Stats.MissingVectors++
		} else {
			// The snapshot had one and it is not usable here. It is replaced,
			// never kept alongside — two vectors for one record in two spaces
			// is the failure this whole policy exists to avoid.
			p.vector, p.hasVector = nil, false
		}
		need = append(need, p)
		texts = append(texts, p.rec.Content)
	}
	if len(need) == 0 {
		return nil
	}
	if im.dst.Embedder == nil {
		return errs.E(errs.Unavailable, op, errors.New(
			"this import has to recompute embeddings and this binary has no embedder"))
	}
	vecs, err := im.dst.Embedder.Embed(ctx, texts)
	if err != nil {
		return err
	}
	if len(vecs) != len(need) {
		return errs.E(errs.Storage, op, fmt.Errorf(
			"the embedder returned %d vectors for %d texts", len(vecs), len(need)))
	}
	for i, p := range need {
		p.rec.Vectors[record.VectorContent] = &vector.Vector{
			ModelID: im.dst.Embedder.ModelID(),
			Dim:     len(vecs[i]),
			Values:  vecs[i],
		}
		im.report.ReEmbedded++
	}
	return nil
}

// reEmbedsEverything reports whether this import recomputes the whole corpus's
// vectors, rather than deciding record by record.
//
// It is a property of the snapshot and the policy, not of any one vector, and
// that is the point: a foreign snapshot's vectors are *all* in another space
// (§II.10 row 16), so there is nothing to decide per row.
func (im *importer) reEmbedsEverything() bool {
	if im.opts.Vectors == VectorsReEmbed {
		return true
	}
	return im.opts.Vectors == VectorsAuto && im.header.GetSourceImpl() != "go"
}

// keepVector decides whether one snapshot vector is usable as it stands.
//
// It is only consulted when the import is not recomputing everything, so the
// question here is narrower: this is a Go snapshot, and did *this* vector come
// from the model that is running now. The discriminator is never the model id
// alone — Rust stamps all-MiniLM-L6-v2 on its vectors and so does Go,
// truthfully, and they are different spaces — which is why the source
// implementation is settled first, in reEmbedsEverything.
func (im *importer) keepVector(p *pendingRecord) bool {
	if !p.hasVector || p.vector == nil || len(p.vector.Values) == 0 {
		return false
	}
	if im.opts.Vectors == VectorsVerbatim {
		return true
	}
	if im.dst.Embedder == nil {
		// A Go snapshot, auto, and no model to compare against or recompute
		// with. Keeping it is the only option that is not "lose the corpus",
		// and checkVectorPlan has already established the snapshot is Go's.
		return true
	}
	return p.vector.ModelID == im.dst.Embedder.ModelID() &&
		p.vector.Dim == im.dst.Embedder.Dim()
}

// writeRecords commits one batch of records through the repository.
func (im *importer) writeRecords(ctx context.Context, batch []*pendingRecord) error {
	const op = "snapshot.Import"

	// One transaction per tenant within the batch, because record.Repo.Put
	// takes the tenant from the context and a transaction spanning two would
	// need two contexts for one batch.
	for len(batch) > 0 {
		t := batch[0].rec.Tenant
		n := 0
		for n < len(batch) && batch[n].rec.Tenant == t {
			n++
		}
		part := batch[:n]
		batch = batch[n:]

		tctx := tenant.NewContext(ctx, t)
		// The conflict decision is taken *outside* the transaction, and that is
		// not tidiness. txn.Do retries a Conflict, on the reading that a
		// conflict is a lost race — so refusing from inside the body made an
		// import refuse eight times, with backoff between, over a disagreement
		// that was never going to resolve itself.
		write, err := im.decidePage(tctx, part)
		if err != nil {
			return err
		}
		if len(write) > 0 {
			err = txn.Do(tctx, im.dst.KV, func(tx txn.Tx) error {
				for _, p := range write {
					if err := im.dst.Records.Put(tctx, tx, p.rec); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return errs.E(errs.KindOf(err), op, err)
			}
		}
		im.report.Stats.Records += uint64(len(part))
	}
	return nil
}

// decidePage applies the conflict mode and returns what to write.
func (im *importer) decidePage(ctx context.Context, part []*pendingRecord) ([]*pendingRecord, error) {
	const op = "snapshot.Import"

	if im.opts.conflict() == ConflictOverwrite {
		return part, nil
	}
	write := make([]*pendingRecord, 0, len(part))
	for _, p := range part {
		existing, err := im.dst.Records.Get(ctx, p.rec.ID)
		if errs.Is(err, errs.NotFound) {
			write = append(write, p)
			continue
		}
		if err != nil {
			return nil, err
		}

		// Identical content is never a conflict. A resumed import re-applies
		// the batch that was in flight when it died, and running an import
		// twice is how an operator checks that it worked.
		differs := firstDifference(existing, p.rec)
		if differs == "" {
			im.report.Unchanged++
			continue
		}
		if im.opts.conflict() == ConflictSkip {
			im.report.Skipped++
			continue
		}
		return nil, errs.E(errs.Conflict, op, fmt.Errorf(
			"memory %s already exists in tenant %s and its %s differs from the snapshot's. "+
				"Import into an empty data directory, or choose --on-conflict=skip to keep what "+
				"is there or --on-conflict=overwrite to replace it",
			p.rec.ID, p.rec.Tenant, differs))
	}
	return write, nil
}

// firstDifference names the first field in which a stored record disagrees with
// what the snapshot says about it, or "" when they agree.
//
// It compares exactly the fields a snapshot carries, and it is a field list
// rather than a byte comparison of the encoded bodies for a specific reason:
// next_attention_at is *not* in the file and is recomputed on the way in from
// the retention policy in force here, so two records that agree about
// everything the snapshot knows would still encode differently. A byte
// comparison would report every re-import as a conflict.
//
// It returns the field name rather than a boolean because "already exists with
// different content" is not something an operator can act on, and because a
// disagreement here is more often a defect in this file than in the corpus.
func firstDifference(a, b *record.Record) string {
	if a == nil || b == nil {
		return "existence"
	}
	af, bf := a.Fields, b.Fields
	for _, c := range []struct {
		field string
		same  bool
	}{
		{"content", a.Content == b.Content},
		{"created_at", a.CreatedAt.Equal(b.CreatedAt)},
		{"updated_at", a.UpdatedAt.Equal(b.UpdatedAt)},
		{"schema_version", a.SchemaVersion == b.SchemaVersion},
		{"tags", slices.Equal(af.Tags, bf.Tags)},
		{"source", af.Source == bf.Source},
		{"archived", af.Archived == bf.Archived},
		{"archived_at", af.ArchivedAt.Equal(bf.ArchivedAt)},
		{"policy", af.Policy == bf.Policy},
		{"importance", af.Importance == bf.Importance},
		{"health", af.Health == bf.Health},
		{"emotional_valence", af.Valence == bf.Valence},
		{"arousal", af.Arousal == bf.Arousal},
		{"accessed_at", af.AccessedAt.Equal(bf.AccessedAt)},
		{"access_count", af.AccessCount == bf.AccessCount},
		{"last_recalled_at", af.LastRecalledAt.Equal(bf.LastRecalledAt)},
		{"ttl", af.TTL == bf.TTL},
		{"flashbulb_until", af.ProtectedUntil.Equal(bf.ProtectedUntil)},
		{"last_decay_at", af.LastDecayAt.Equal(bf.LastDecayAt)},
		{"last_health_check_at", af.LastHealthCheckAt.Equal(bf.LastHealthCheckAt)},
	} {
		if !c.same {
			return c.field
		}
	}
	return ""
}

func fromUnixMs(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}

// recordFrom rebuilds a record from the shared message.
//
// next_attention_at is deliberately not carried by the format and not set here:
// it is derived from the record and the retention policy in force *on this
// deployment*, and the record repository computes it through the lifecycle
// scheduler on the way in. Trusting a number written by a differently
// configured server would schedule an imported corpus by somebody else's rules.
func recordFrom(m *pb.Record, now time.Time) (*record.Record, error) {
	const op = "snapshot.Import"

	rid, err := id.FromBytes(m.GetId())
	if err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"a record carries a malformed id: %w", err))
	}
	t, err := tenant.Parse(m.GetTenant())
	if err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"record %s names tenant %q, which is not a tenant id: %w", rid, m.GetTenant(), err))
	}
	created := fromUnixMs(m.GetCreatedAtUnixMs())
	if created.IsZero() {
		created = now
	}
	updated := fromUnixMs(m.GetUpdatedAtUnixMs())
	if updated.IsZero() {
		updated = created
	}

	fields := record.Fields{
		Tags:              m.GetTags(),
		Source:            m.GetSource(),
		Archived:          m.GetArchived(),
		Policy:            m.GetPolicy(),
		Importance:        m.GetImportance(),
		Health:            m.GetHealth(),
		Valence:           m.GetEmotionalValence(),
		Arousal:           m.GetArousal(),
		AccessedAt:        fromUnixMs(m.GetAccessedAtUnixMs()),
		AccessCount:       m.GetAccessCount(),
		LastRecalledAt:    fromUnixMs(m.GetLastRecalledAtUnixMs()),
		TTL:               time.Duration(m.GetTtlSeconds()) * time.Second,
		ProtectedUntil:    fromUnixMs(m.GetFlashbulbUntilUnixMs()),
		LastDecayAt:       fromUnixMs(m.GetLastDecayAtUnixMs()),
		LastHealthCheckAt: fromUnixMs(m.GetLastHealthCheckAtUnixMs()),
	}
	// An archived record with no archive time — every Rust snapshot, which has
	// no field for one — is dated by its last update, which is the instant Rust
	// itself uses to age an archived memory towards cleanup.
	if fields.Archived {
		fields.ArchivedAt = updated
	}
	// WithDefaults keys off the policy, so a record from a source that names
	// none gets Remem's defaults rather than a health of zero, which always
	// archives (§II.10 row 8).
	fields = fields.WithDefaults()

	return &record.Record{
		ID:        rid,
		Tenant:    t,
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   m.GetContent(),
		Fields:    fields,
		Vectors:   map[string]*vector.Vector{},
		CreatedAt: created,
		UpdatedAt: updated,
		// The version in force here, not zero. A shared-schema snapshot carries
		// no user-schema version — Rust has no per-tenant schema at all — and
		// the record encoder writes the current one for a caller with no
		// opinion, so a record entering at zero would be *stored* at one and
		// then disagree with the file it came from on every re-import. The
		// companion section overrides this when the snapshot has one.
		SchemaVersion: tenant.DefaultSchemaVersion,
	}, nil
}
