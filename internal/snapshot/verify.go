package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/tenant"
	"google.golang.org/protobuf/proto"
)

// FindingKind names a way a snapshot and a live store can disagree.
type FindingKind uint8

const (
	// FindingMissing is a row the snapshot has and the store does not.
	FindingMissing FindingKind = iota + 1
	// FindingExtra is a row the store has and the snapshot does not.
	FindingExtra
	// FindingDiffers is a row both have, with different content.
	FindingDiffers
	// FindingCountMismatch is a header claim the snapshot's own blocks do not
	// support. It is about the file rather than about the store.
	FindingCountMismatch
)

var findingNames = map[FindingKind]string{
	FindingMissing:       "missing",
	FindingExtra:         "extra",
	FindingDiffers:       "differs",
	FindingCountMismatch: "count_mismatch",
}

func (k FindingKind) String() string {
	if n, ok := findingNames[k]; ok {
		return n
	}
	return fmt.Sprintf("finding(%d)", uint8(k))
}

// Finding is one disagreement, named so an operator can look it up.
type Finding struct {
	Kind   FindingKind
	What   string // "record", "vector", "edge", "event", "tenant", "header"
	Tenant tenant.ID
	Ref    string
	Detail string
}

func (f Finding) String() string {
	if f.Ref == "" {
		return fmt.Sprintf("%s %s: %s", f.Kind, f.What, f.Detail)
	}
	return fmt.Sprintf("%s %s %s/%s: %s", f.Kind, f.What, f.Tenant, f.Ref, f.Detail)
}

// MaxVerifyFindings bounds what a report names individually. The counts stay
// exact; the same bound and the same reason as internal/graph's Verify.
const MaxVerifyFindings = 1000

// VerifyReport is what a comparison found.
type VerifyReport struct {
	Snapshot Stats
	Store    Stats

	Findings []Finding
	// Broken is the total number of disagreements, which is exact even when
	// Findings has been capped.
	Broken int
	// Truncated says Findings was capped.
	Truncated bool
}

// Clean reports whether the snapshot and the store agree.
func (r VerifyReport) Clean() bool { return r.Broken == 0 }

func (r *VerifyReport) add(f Finding) {
	r.Broken++
	if len(r.Findings) < MaxVerifyFindings {
		r.Findings = append(r.Findings, f)
		return
	}
	r.Truncated = true
}

// Verify compares a snapshot against a live store, without writing anything.
//
// # What it is for
//
// It is the step between an import and decommissioning the system the snapshot
// came from. An import that reports success has proved that the file was
// readable and that no row refused to be written; it has not proved that what
// is now in the store is what was in the file. This reads both and says so.
//
// # What it compares, and what it deliberately does not
//
// Records are compared by a content hash over the fields the snapshot carries —
// the same field list [sameAsSnapshot] uses, and for the same reason:
// next_attention_at is derived on the way in from the retention policy in force
// here, so a stored record is *expected* to differ from the file in that one
// respect and comparing it would report every correct import as broken.
//
// Vectors are compared by norm rather than by value, and only when the snapshot
// is one this binary's model wrote. A Rust snapshot's vectors are in a different
// space (§II.10 row 16) and the store's are recomputed, so comparing them would
// be a measurement of the pooling difference. The norm is still worth checking:
// it is one, and a vector whose norm has drifted is one that will rank wrongly.
//
// Derived rows are not compared at all. They are not in the file, and they are
// rebuilt rather than restored.
func Verify(ctx context.Context, src Sources, r io.Reader) (VerifyReport, error) {
	const op = "snapshot.Verify"

	if src.Snap == nil || src.Records == nil {
		return VerifyReport{}, errs.E(errs.Invalid, op, errors.New(
			"a verification needs a snapshot of the store and a record repository"))
	}

	rd, err := NewReader(r)
	if err != nil {
		return VerifyReport{}, err
	}
	defer func() { _ = rd.Close() }()

	v := &verifier{
		src:       src,
		header:    rd.Header(),
		records:   map[scopedID][32]byte{},
		vectors:   map[scopedID]float64{},
		edges:     map[string]struct{}{},
		events:    map[string]struct{}{},
		hasVector: map[scopedID]bool{},
	}
	for {
		blk, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return v.report, err
		}
		if err := v.block(blk); err != nil {
			return v.report, err
		}
	}
	v.checkHeader()
	if err := v.compare(ctx); err != nil {
		return v.report, err
	}
	return v.report, nil
}

// scopedID addresses a row across tenants, which a verification legitimately
// spans: a snapshot is one file covering every tenant it carries.
type scopedID struct {
	tenant tenant.ID
	id     id.ID
}

type verifier struct {
	src    Sources
	header *pb.Header
	report VerifyReport

	records   map[scopedID][32]byte
	vectors   map[scopedID]float64
	hasVector map[scopedID]bool
	edges     map[string]struct{}
	events    map[string]struct{}
}

func (v *verifier) block(blk *Block) error {
	const op = "snapshot.Verify"

	switch blk.Section {
	case SectionRecords:
		return blk.Each(func(raw []byte) error {
			var m pb.Record
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			key, err := scopeOf(m.GetTenant(), m.GetId(), op)
			if err != nil {
				return err
			}
			v.records[key] = hashRecordMessage(&m)
			v.report.Snapshot.Records++
			return nil
		})
	case SectionVectors:
		return blk.Each(func(raw []byte) error {
			var m pb.Vector
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			key, err := scopeOf(m.GetTenant(), m.GetId(), op)
			if err != nil {
				return err
			}
			v.vectors[key] = norm(m.GetValues())
			v.hasVector[key] = true
			v.report.Snapshot.Vectors++
			return nil
		})
	case SectionEdges:
		return blk.Each(func(raw []byte) error {
			var m pb.Edge
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			v.edges[edgeRef(m.GetTenant(), m.GetFrom(), m.GetTo(), m.GetRelationshipType())] =
				struct{}{}
			v.report.Snapshot.Edges++
			return nil
		})
	case SectionTenants:
		return blk.Each(func([]byte) error {
			v.report.Snapshot.Tenants++
			return nil
		})
	case SectionGoEvents:
		return blk.Each(func(raw []byte) error {
			v.report.Snapshot.Events++
			return nil
		})
	case SectionGoRecordExt, SectionGoEdgeExt, SectionGoTenantExt:
		// The companions carry fields the shared messages have no room for.
		// They are restored and they are not what a verification is about: a
		// row that arrived with its metadata missing is a differing row, and
		// the record hash above already says so.
		return nil
	default:
		return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"this snapshot carries a %s block, which this binary cannot compare", blk.Section))
	}
}

// checkHeader compares the file's own claims against its own blocks. It is the
// one check that is about the snapshot rather than about the store, and it
// belongs here because a header nobody checks is a header nobody can trust.
func (v *verifier) checkHeader() {
	for _, c := range []struct {
		what      string
		want, got uint64
	}{
		{"records", v.header.GetRecordCount(), v.report.Snapshot.Records},
		{"vectors", v.header.GetVectorCount(), v.report.Snapshot.Vectors},
		{"edges", v.header.GetEdgeCount(), v.report.Snapshot.Edges},
	} {
		if c.want != c.got {
			v.report.add(Finding{
				Kind: FindingCountMismatch, What: "header",
				Detail: fmt.Sprintf("the header claims %d %s and the blocks hold %d",
					c.want, c.what, c.got),
			})
		}
	}
}

// compare walks the live store and reconciles it against what was read.
func (v *verifier) compare(ctx context.Context) error {
	const op = "snapshot.Verify"

	metas, err := exportTenants(ctx, v.src.Tenants, "", op)
	if err != nil {
		return err
	}
	sameModel := v.header.GetSourceImpl() == "go"

	for _, m := range metas {
		tctx := tenant.NewContext(ctx, m.ID)
		ns := tenant.DefaultNamespace
		var from *id.ID
		for {
			page, err := v.src.Records.Scan(tctx, v.src.Snap, from, DefaultPageSize)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, rec := range page {
				v.compareRecord(m.ID, rec, sameModel)
			}
			last := page[len(page)-1].ID
			from = &last
			if len(page) < DefaultPageSize {
				break
			}
		}

		sc := graph.Scope{Tenant: m.ID, Namespace: ns}
		if err := graph.ScanOut(ctx, v.src.Snap, sc, func(e graph.Edge) error {
			v.report.Store.Edges++
			ref := edgeRef(m.ID.String(), e.From[:], e.To[:], e.Type.String())
			if _, ok := v.edges[ref]; ok {
				delete(v.edges, ref)
				return nil
			}
			v.report.add(Finding{
				Kind: FindingExtra, What: "edge", Tenant: m.ID,
				Ref:    fmt.Sprintf("%s-%s->%s", e.From, e.Type, e.To),
				Detail: "the store holds a relationship the snapshot does not",
			})
			return nil
		}); err != nil {
			return err
		}
	}

	// Whatever is left in the maps is in the snapshot and not in the store.
	for key := range v.records {
		v.report.add(Finding{
			Kind: FindingMissing, What: "record", Tenant: key.tenant, Ref: key.id.String(),
			Detail: "the snapshot holds a memory the store does not",
		})
	}
	for ref := range v.edges {
		v.report.add(Finding{
			Kind: FindingMissing, What: "edge", Ref: ref,
			Detail: "the snapshot holds a relationship the store does not",
		})
	}
	sortFindings(v.report.Findings)
	return nil
}

func (v *verifier) compareRecord(t tenant.ID, rec *record.Record, sameModel bool) {
	v.report.Store.Records++
	key := scopedID{tenant: t, id: rec.ID}

	want, ok := v.records[key]
	if !ok {
		v.report.add(Finding{
			Kind: FindingExtra, What: "record", Tenant: t, Ref: rec.ID.String(),
			Detail: "the store holds a memory the snapshot does not",
		})
		return
	}
	delete(v.records, key)
	if got := hashRecord(t, rec); got != want {
		v.report.add(Finding{
			Kind: FindingDiffers, What: "record", Tenant: t, Ref: rec.ID.String(),
			Detail: "the stored memory differs from the snapshot in a field the snapshot carries",
		})
	}

	stored := rec.Vectors[record.VectorContent]
	if stored != nil {
		v.report.Store.Vectors++
	}
	hadVector := v.hasVector[key]
	switch {
	case hadVector && stored == nil:
		v.report.add(Finding{
			Kind: FindingMissing, What: "vector", Tenant: t, Ref: rec.ID.String(),
			Detail: "the snapshot carries an embedding for this memory and the store has none",
		})
	case !hadVector && stored != nil && sameModel:
		// Only worth reporting for a Go snapshot: a Rust import recomputes the
		// embeddings of records that had none, which is the faithful import
		// rather than a discrepancy.
		v.report.add(Finding{
			Kind: FindingExtra, What: "vector", Tenant: t, Ref: rec.ID.String(),
			Detail: "the store holds an embedding the snapshot does not",
		})
	case stored != nil:
		if n := norm(stored.Values); math.Abs(n-1) > 1e-3 {
			v.report.add(Finding{
				Kind: FindingDiffers, What: "vector", Tenant: t, Ref: rec.ID.String(),
				Detail: fmt.Sprintf("the stored embedding has norm %.6f, not one; it will rank "+
					"against a corpus it is not on the same scale as", n),
			})
		}
	}
	delete(v.vectors, key)
	delete(v.hasVector, key)
}

// hashRecordMessage hashes the fields a snapshot carries, from the file's own
// message.
func hashRecordMessage(m *pb.Record) [32]byte {
	h := sha256.New()
	writeHashed(h, m.GetTenant(), m.GetContent(), m.GetPolicy(), m.GetSource(),
		strings.Join(m.GetTags(), "\x00"))
	writeNums(h, boolByte(m.GetArchived()), uint64(m.GetAccessCount()),
		m.GetCreatedAtUnixMs(), m.GetUpdatedAtUnixMs(), m.GetAccessedAtUnixMs(),
		m.GetLastRecalledAtUnixMs(), m.GetFlashbulbUntilUnixMs(), m.GetTtlSeconds(),
		m.GetLastDecayAtUnixMs(), m.GetLastHealthCheckAtUnixMs())
	writeFloats(h, m.GetImportance(), m.GetEmotionalValence(), m.GetArousal(), m.GetHealth())
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// hashRecord hashes the same fields from a stored record.
//
// The two functions are deliberately adjacent: they are one definition of "the
// same memory" written twice, and a field added to one and not the other makes
// a verification quietly stop checking it.
func hashRecord(t tenant.ID, rec *record.Record) [32]byte {
	f := rec.Fields
	h := sha256.New()
	writeHashed(h, t.String(), rec.Content, f.Policy, f.Source, strings.Join(f.Tags, "\x00"))
	writeNums(h, boolByte(f.Archived), uint64(f.AccessCount),
		msOf(rec.CreatedAt), msOf(rec.UpdatedAt), msOf(f.AccessedAt),
		msOf(f.LastRecalledAt), msOf(f.ProtectedUntil), uint64(f.TTL.Seconds()),
		msOf(f.LastDecayAt), msOf(f.LastHealthCheckAt))
	writeFloats(h, f.Importance, f.Valence, f.Arousal, f.Health)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeHashed(h io.Writer, parts ...string) {
	for _, p := range parts {
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(p))))
		_, _ = io.WriteString(h, p)
	}
}

func writeNums(h io.Writer, nums ...uint64) {
	for _, n := range nums {
		_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, n))
	}
}

func writeFloats(h io.Writer, fs ...float32) {
	for _, f := range fs {
		_, _ = h.Write(binary.LittleEndian.AppendUint32(nil, math.Float32bits(f)))
	}
}

func boolByte(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func msOf(t interface{ UnixMilli() int64 }) uint64 {
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

func norm(v []float32) float64 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return math.Sqrt(sum)
}

func edgeRef(t string, from, to []byte, typ string) string {
	return fmt.Sprintf("%s/%x-%s->%x", t, from, typ, to)
}

func scopeOf(t string, rid []byte, op string) (scopedID, error) {
	parsed, err := id.FromBytes(rid)
	if err != nil {
		return scopedID{}, errs.E(errs.Corruption, op, err)
	}
	tid, err := tenant.Parse(t)
	if err != nil {
		return scopedID{}, errs.E(errs.Corruption, op, err)
	}
	return scopedID{tenant: tid, id: parsed}, nil
}

// sortFindings makes a report reproducible. A verification that listed the same
// corpus in a different order every run would be unusable in a diff, which is
// how an operator actually reads one.
func sortFindings(f []Finding) {
	sort.SliceStable(f, func(i, j int) bool {
		if f[i].Kind != f[j].Kind {
			return f[i].Kind < f[j].Kind
		}
		if f[i].What != f[j].What {
			return f[i].What < f[j].What
		}
		if f[i].Tenant != f[j].Tenant {
			return f[i].Tenant < f[j].Tenant
		}
		return f[i].Ref < f[j].Ref
	})
}
