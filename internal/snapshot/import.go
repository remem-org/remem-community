package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// ConflictMode decides what an import does when a row it is writing already
// exists with different content.
//
// Identical content is never a conflict under any mode. A resumed import
// re-applies the batch that was in flight when it died, and an import run twice
// over the same file is the ordinary way an operator checks that it worked —
// treating either as a collision would make the safe default unusable.
type ConflictMode uint8

const (
	// ConflictFail stops at the first row that exists and differs, naming it.
	// It is the default, because an import into a store that already holds
	// different data is almost always a mistake about which store.
	ConflictFail ConflictMode = iota + 1
	// ConflictSkip keeps what is already there and counts what it passed over.
	ConflictSkip
	// ConflictOverwrite replaces what is already there.
	ConflictOverwrite
)

var conflictNames = map[ConflictMode]string{
	ConflictFail: "fail", ConflictSkip: "skip", ConflictOverwrite: "overwrite",
}

func (c ConflictMode) String() string {
	if n, ok := conflictNames[c]; ok {
		return n
	}
	return fmt.Sprintf("conflict(%d)", uint8(c))
}

// ParseConflictMode turns a CLI word into a mode.
func ParseConflictMode(s string) (ConflictMode, error) {
	for m, n := range conflictNames {
		if n == s {
			return m, nil
		}
	}
	return 0, errs.E(errs.Invalid, "snapshot.ParseConflictMode", fmt.Errorf(
		"%q is not a conflict mode; the modes are fail, skip and overwrite", s))
}

// VectorPolicy decides what an import does with the vectors a snapshot carries.
//
// This is the phase's central decision and it is not the plan's. Plan
// §Phase 12's ImportOpts has ReEmbedMissing, which reads "present" as "usable" —
// and §II.10 row 16, discovered later, says a Rust vector is present and
// unusable. Rust CLS-pools where Go mean-pools, so a Rust vector is the right
// width, the right norm, and a point in a different space. Importing one
// verbatim produces a corpus that looks healthy and ranks wrongly, forever,
// with every vector carrying a model id that says all-MiniLM-L6-v2 — which is
// true, and is exactly why the model id cannot be the discriminator.
type VectorPolicy uint8

const (
	// VectorsAuto takes a vector verbatim only when the snapshot was written by
	// a Go binary and the vector's model id is the running model's, and
	// re-embeds otherwise. It is the default and the only setting an operator
	// should normally need.
	VectorsAuto VectorPolicy = iota
	// VectorsReEmbed re-embeds every record, whatever the snapshot claims. It
	// is what a change of embedding model needs.
	VectorsReEmbed
	// VectorsVerbatim trusts the file. It exists for a Go-to-Go restore on a
	// machine with no model, and it is unsafe on a Rust snapshot — which is why
	// [Import] refuses that combination by name rather than obeying it.
	VectorsVerbatim
)

// ImportOpts configures an import.
type ImportOpts struct {
	OnConflict ConflictMode
	Vectors    VectorPolicy
	// BatchSize is how many rows one transaction holds. It bounds memory and
	// the amount of work a crash costs.
	BatchSize int
	// Resume restarts from a cursor a previous run reported. An empty cursor
	// starts at the beginning, which is always safe: every row a snapshot
	// carries is keyed by its own id, so importing a block twice writes the
	// same bytes. Resume saves the work, not the correctness.
	Resume []byte
	// Progress is called after each committed batch, for a CLI that prints and
	// for the caller that persists a cursor.
	Progress func(Progress)
	// Now stamps rows that need an instant the snapshot does not carry.
	Now func() time.Time
}

// DefaultBatchSize is how many rows one import transaction holds.
const DefaultBatchSize = 500

func (o ImportOpts) batchSize() int {
	if o.BatchSize <= 0 {
		return DefaultBatchSize
	}
	return o.BatchSize
}

func (o ImportOpts) conflict() ConflictMode {
	if o.OnConflict == 0 {
		return ConflictFail
	}
	return o.OnConflict
}

// Progress reports how far an import has got.
type Progress struct {
	Blocks uint32
	Offset int64
	Stats  Stats
	// Cursor is what to hand back as [ImportOpts.Resume] to restart here.
	Cursor []byte
}

// Destination is what an import writes through.
//
// Every store is passed in rather than constructed here, for the reason
// [Sources] gives: each is owned by the package that owns its key space. The
// record repository in particular arrives already carrying its indexers and its
// lifecycle scheduler, so an imported memory gets the same attribute rows, text
// postings and next-attention time a stored one would — built by the code that
// owns them rather than by a second copy in this package.
type Destination struct {
	KV      storage.KV
	Tenants tenant.Directory
	Records record.Repo
	Edges   *graph.Store
	Events  *events.Store
	// Embedder re-embeds records whose vectors this binary cannot use. Nil is
	// allowed, and an import that then needs one refuses by name rather than
	// writing a corpus with no vectors.
	Embedder embedding.Embedder
}

// Report is what an import did, including everything it declined to write.
//
// [Report.Stats] counts what the *snapshot carried*, because that is what the
// header claims and what the cross-check at the end compares against. What the
// store ended up with is the fields below: a Rust import reads two thousand
// vectors, keeps none of them, and writes two thousand of its own.
type Report struct {
	Stats Stats

	// ReEmbedded counts records whose vectors were recomputed from content.
	ReEmbedded uint64
	// VectorsVerbatim counts records whose stored vector was taken as-is.
	VectorsVerbatim uint64

	// Skipped counts rows that already existed with different content, under
	// ConflictSkip.
	Skipped uint64
	// Unchanged counts rows that already existed with identical content. A
	// second import of the same file is all Unchanged and no Skipped, which is
	// what makes idempotence visible rather than merely claimed.
	Unchanged uint64

	// SelfEdges counts edges from a record to itself, which are rejected: Go's
	// write path cannot produce one, and traversal spends budget on them.
	SelfEdges []Rejected
	// OrphanEdges counts edges whose target record the snapshot does not carry.
	OrphanEdges []Rejected

	// Truncated says the lists above were capped. The counts are complete; the
	// examples are not.
	Truncated bool

	// Cursor is where the import finished, for a resume.
	Cursor []byte
}

// MaxRejected is how many rejected edges a report names individually.
//
// The counts are always exact. Naming every one of four million orphans is not
// a report, it is a second copy of the corpus — the bound internal/graph's
// Verify already settled on, for the same reason.
const MaxRejected = 1000

// Rejected is one edge an import declined to write, named so an operator can
// look it up rather than being told a number.
type Rejected struct {
	Tenant   tenant.ID
	From, To id.ID
	Type     string
	Reason   string
}

func (r Rejected) String() string {
	return fmt.Sprintf("%s: %s -%s-> %s (%s)", r.Tenant, r.From, r.Type, r.To, r.Reason)
}

// Import reads a snapshot into a store.
//
// # What it writes, and what it rebuilds
//
// Records, canonical vectors, out-edges, tenants and audit events come from the
// file. Everything derived from them — the attribute row and its slot entries,
// the text postings, the in-edge index — is built inside the same transaction
// as the row it derives from, by the indexers the record repository already
// carries. The HNSW graph is the one exception and is deliberately not built
// here: it is materialised lazily and rebuilt by a job, and until it is,
// vector.Index.Health reports the tenant as truncated rather than answering
// short (Phase 7).
//
// # Idempotence, and why resume is only an optimisation
//
// Every row a snapshot carries is addressed by its own id, so importing a block
// twice writes the same bytes. That is what makes a crashed import safe to
// restart from the beginning, and it is why [Report.Unchanged] exists: a second
// run over the same file writes nothing new and says so.
func Import(ctx context.Context, dst Destination, r io.Reader, opts ImportOpts) (Report, error) {
	const op = "snapshot.Import"

	if dst.KV == nil || dst.Tenants == nil || dst.Records == nil || dst.Edges == nil {
		return Report{}, errs.E(errs.Invalid, op, errors.New(
			"an import needs a store, a tenant directory, a record repository and an edge store"))
	}
	if _, err := ParseConflictMode(opts.conflict().String()); err != nil {
		return Report{}, err
	}

	rd, err := NewReader(r)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = rd.Close() }()

	im := &importer{dst: dst, opts: opts, header: rd.Header()}
	if err := im.checkVectorPlan(); err != nil {
		return Report{}, err
	}

	resume, err := parseCursor(opts.Resume, im.header)
	if err != nil {
		return Report{}, err
	}
	// A resumed import inherits the counts the earlier run reached, so the
	// header cross-check at the end is about the whole file rather than about
	// the tail of it.
	im.report.Stats = resume.Stats
	im.mark(resume.Blocks, resume.Offset)

	for {
		blk, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return im.report, err
		}
		if blk.Index < resume.Blocks {
			// Already applied by the run this one is resuming. Skipping is the
			// optimisation; re-applying would have been correct too.
			continue
		}
		if err := im.block(ctx, blk); err != nil {
			return im.report, err
		}
		// Everything up to and including this block is durable exactly when
		// nothing is buffered; otherwise the last legal restart is the page
		// boundary the importer marked.
		if im.idle() {
			im.mark(rd.Blocks(), rd.Offset())
		}
		im.report.Cursor = makeCursorWithStats(
			im.resumeBlocks, im.resumeOffset, im.header, im.resumeStats)
		if p := opts.Progress; p != nil {
			p(Progress{Blocks: rd.Blocks(), Offset: rd.Offset(),
				Stats: im.report.Stats, Cursor: im.report.Cursor})
		}
	}
	if err := im.flush(ctx); err != nil {
		return im.report, err
	}

	// The header's claim, checked against what was actually read. This is what
	// catches a damaged frame header — a section byte with a flipped bit
	// relabels a block and no checksum in the format notices, because the frame
	// header sits outside the CRC that follows it.
	if err := im.checkHeaderCounts(); err != nil {
		return im.report, err
	}
	if err := im.sweepOrphans(ctx); err != nil {
		return im.report, err
	}
	return im.report, nil
}

// checkVectorPlan refuses a combination that would write a corpus nothing can
// rank, before a single row is written.
func (im *importer) checkVectorPlan() error {
	const op = "snapshot.Import"

	foreign := im.header.GetSourceImpl() != "go"
	switch im.opts.Vectors {
	case VectorsVerbatim:
		if foreign {
			return errs.E(errs.Invalid, op, fmt.Errorf(
				"this snapshot was written by %q, and its vectors are not comparable with the "+
					"ones this binary produces: Rust pools the model's output differently "+
					"(implementation plan §II.10 row 16), so importing them verbatim would give "+
					"every memory a vector of the right width in the wrong space and every "+
					"search a plausible wrong answer. Import without --vectors=verbatim, on a "+
					"binary built with the embedder",
				im.header.GetSourceImpl()))
		}
		return nil
	case VectorsAuto, VectorsReEmbed:
		if im.dst.Embedder == nil && (foreign || im.opts.Vectors == VectorsReEmbed) {
			return errs.E(errs.Unavailable, op, errors.New(
				"this import has to recompute embeddings and this binary has no embedder. "+
					"Build with -tags onnx and CGO_ENABLED=1 after running scripts/fetch-model.sh, "+
					"or, for a Go snapshot written by this same model, pass --vectors=verbatim"))
		}
		return nil
	default:
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"%d is not a vector policy", uint8(im.opts.Vectors)))
	}
}

// checkHeaderCounts compares what the header claimed against what was read.
func (im *importer) checkHeaderCounts() error {
	const op = "snapshot.Import"

	type claim struct {
		what      string
		want, got uint64
	}
	claims := []claim{
		{"records", im.header.GetRecordCount(), im.report.Stats.Records},
		{"vectors", im.header.GetVectorCount(), im.report.Stats.Vectors},
		{"edges", im.header.GetEdgeCount(), im.report.Stats.Edges},
	}
	if im.header.GetSourceImpl() == "go" {
		claims = append(claims, claim{"events", im.header.GetEventCount(), im.report.Stats.Events})
	}
	for _, c := range claims {
		if c.want != c.got {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"the snapshot's header claims %d %s and the blocks hold %d. The header is not "+
					"checksummed and neither is a block's frame header, so this is what a "+
					"damaged or relabelled block looks like — the import is refused rather "+
					"than left half-applied",
				c.want, c.what, c.got))
		}
	}
	return nil
}
