package snapshot

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
	"google.golang.org/protobuf/proto"
)

// DefaultPageSize is how many records one block carries.
//
// It bounds peak memory: at most one page of decoded bodies, vectors and edges
// is held at once, whatever the corpus size. It matches the Rust exporter's
// page size so that a Go snapshot of the same corpus has the same block shape.
const DefaultPageSize = 1000

// ExportOpts configures an export.
type ExportOpts struct {
	// Compression defaults to zstd. A snapshot exists to be kept.
	Compression Compression
	// PageSize is records per block; zero takes [DefaultPageSize].
	PageSize int
	// Tenant limits the export to one tenant. Empty exports every tenant,
	// which is what a backup means.
	Tenant tenant.ID
	// SourceVersion stamps the header. Empty takes this binary's version.
	SourceVersion string
	// SharedOnly writes only the byte-shared sections 1 to 6, producing a file
	// any implementation compiling proto/snapshot/v1 can read — at the cost of
	// the five things that schema has no field for. It is off by default,
	// because a lossy backup is the thing this format exists not to be.
	SharedOnly bool
	// Now stamps the header's creation time. Nil takes the wall clock, which
	// is correct here: the header's timestamp is documentation of when a file
	// was made, not durable business logic (Invariant 8).
	Now func() time.Time
}

func (o ExportOpts) pageSize() int {
	if o.PageSize <= 0 {
		return DefaultPageSize
	}
	return o.PageSize
}

// Stats counts what an export wrote or an import read, so a caller can verify
// completeness rather than merely success.
type Stats struct {
	Tenants uint64
	Records uint64
	Vectors uint64
	// MissingVectors are records with no canonical vector. Archived memories
	// are the ordinary cause. An export never writes a placeholder for one.
	MissingVectors uint64
	Edges          uint64
	Events         uint64

	// EdgesWithMetaDropped counts edges whose caller-supplied metadata could
	// not be written, which happens only under [ExportOpts.SharedOnly]. It is
	// counted rather than ignored because losing user annotation silently is
	// exactly what this format exists not to do.
	EdgesWithMetaDropped uint64
	// EventsDropped counts audit events a shared-only export left behind.
	EventsDropped uint64
}

// EventSource yields a tenant's audit stream.
//
// It is declared here and implemented over internal/events so that this package
// depends on an interface rather than on the event store's paging, and so a
// caller with no audit stream — a test, or a build that never enabled the
// lifecycle — passes nil and exports none.
type EventSource interface {
	ScanTenant(ctx context.Context, t tenant.ID, ns tenant.Namespace,
		fn func(events.Event) error) error
}

// Sources are the stores an export reads.
//
// They are passed in rather than constructed here because every one of them is
// owned by the package that owns its key space, and a second reader of a
// durable layout is a second thing that can disagree about it.
type Sources struct {
	// Snap is the pinned view every canonical read goes through, so that a
	// snapshot of a running server is one point in time rather than a smear.
	Snap storage.Snapshot
	// Tenants is the tenant directory. Its read is outside Snap, because the
	// directory is not versioned with the corpus and a tenant registered
	// mid-export is a tenant with no records.
	Tenants tenant.Directory
	// Records reads canonical bodies and vectors through Snap.
	Records record.Repo
	// Events is optional; nil exports no audit stream.
	Events EventSource
}

// Export writes every canonical row reachable from src to w.
//
// # Two passes, and why
//
// The header opens the file and carries final counts, which are only known once
// the walk is done. The Rust exporter stages every block into a scratch file and
// assembles the snapshot around them afterwards. This counts first instead: a
// keys-only pass over the three canonical spaces, then the real walk, with a
// disagreement between the two aborting the export. Both readings come from one
// pinned snapshot, so a disagreement is not concurrency — it is the index and
// the walk disagreeing about what exists, which is precisely the silent
// short-export the count exists to catch.
//
// The first pass reads no values, so it costs iteration and not decoding.
func Export(ctx context.Context, src Sources, w io.Writer, opts ExportOpts) (Stats, error) {
	const op = "snapshot.Export"

	if src.Snap == nil || src.Tenants == nil || src.Records == nil {
		return Stats{}, errs.E(errs.Invalid, op,
			fmt.Errorf("an export needs a snapshot, a tenant directory and a record repository"))
	}
	if _, err := parseCompression(byte(opts.Compression), op); err != nil {
		return Stats{}, err
	}

	metas, err := exportTenants(ctx, src.Tenants, opts.Tenant, op)
	if err != nil {
		return Stats{}, err
	}

	counted, err := countRows(ctx, src.Snap, metas)
	if err != nil {
		return Stats{}, err
	}

	header := &pb.Header{
		CreatedAtUnixMs:     uint64(exportNow(opts).UnixMilli()),
		SourceImpl:          "go",
		SourceVersion:       exportVersion(opts),
		RecordFormatVersion: uint32(FormatVersion),
		Tenants:             tenantIDs(metas),
		RecordCount:         counted.Records,
		VectorCount:         counted.Vectors,
		EdgeCount:           counted.Edges,
		EventCount:          counted.Events,
	}
	if opts.SharedOnly {
		header.EventCount = 0
	}

	sw, err := NewWriter(w, header, WriterOpts{Compression: opts.Compression})
	if err != nil {
		return Stats{}, err
	}

	var got Stats
	if err := writeTenants(sw, metas, opts); err != nil {
		return Stats{}, err
	}
	got.Tenants = uint64(len(metas))

	for _, m := range metas {
		if err := exportTenant(ctx, src, sw, m.ID, opts, &got); err != nil {
			return Stats{}, err
		}
	}

	// The claim in the header, checked against what the walk actually wrote.
	// A header that overstates its corpus turns a partial export into a
	// restore that looks complete, which is the failure worth aborting for.
	if got.Records != counted.Records || got.Vectors != counted.Vectors || got.Edges != counted.Edges {
		return got, errs.E(errs.Corruption, op, fmt.Errorf(
			"the export walk disagrees with the key scan that sized it: %d/%d records, "+
				"%d/%d vectors, %d/%d edges. Both read one pinned snapshot, so this is a "+
				"canonical row that could be counted and not read — refusing to write a "+
				"snapshot that would restore short",
			got.Records, counted.Records, got.Vectors, counted.Vectors, got.Edges, counted.Edges))
	}
	if err := sw.Close(); err != nil {
		return got, err
	}
	return got, nil
}

func exportNow(opts ExportOpts) time.Time {
	if opts.Now != nil {
		return opts.Now()
	}
	return time.Now()
}

func exportVersion(opts ExportOpts) string {
	if opts.SourceVersion != "" {
		return opts.SourceVersion
	}
	return version.Binary
}

// exportTenants resolves which tenants the export covers.
func exportTenants(ctx context.Context, dir tenant.Directory, only tenant.ID,
	op string,
) ([]tenant.Meta, error) {
	if only != "" {
		m, err := dir.Get(ctx, only)
		if err != nil {
			return nil, err
		}
		return []tenant.Meta{m}, nil
	}
	metas, err := dir.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, errs.E(errs.NotFound, op, fmt.Errorf(
			"this data directory registers no tenants, so there is nothing to export. Check the "+
				"path: a mistyped one opens as an empty database"))
	}
	return metas, nil
}

func tenantIDs(metas []tenant.Meta) []string {
	out := make([]string, 0, len(metas))
	for _, m := range metas {
		out = append(out, m.ID.String())
	}
	return out
}

// writeTenants writes the tenants block and, unless the export is shared-only,
// its companion.
func writeTenants(sw *Writer, metas []tenant.Meta, opts ExportOpts) error {
	msgs := make([]proto.Message, 0, len(metas))
	exts := make([]proto.Message, 0, len(metas))
	for _, m := range metas {
		msgs = append(msgs, &pb.Tenant{
			Id:              m.ID.String(),
			DisplayName:     m.DisplayName,
			CreatedAtUnixMs: unixMs(m.CreatedAt),
		})
		exts = append(exts, &pbext.TenantExt{Id: m.ID.String(), SchemaVersion: m.SchemaVersion})
	}
	if err := sw.WriteMessages(SectionTenants, msgs); err != nil {
		return err
	}
	if opts.SharedOnly {
		return nil
	}
	return sw.WriteMessages(SectionGoTenantExt, exts)
}

// exportTenant walks one tenant a page at a time.
func exportTenant(ctx context.Context, src Sources, sw *Writer, t tenant.ID,
	opts ExportOpts, got *Stats,
) error {
	ctx = tenant.NewContext(ctx, t)
	ns := tenant.DefaultNamespace
	page := opts.pageSize()

	var from *id.ID
	for {
		recs, err := src.Records.Scan(ctx, src.Snap, from, page)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			break
		}
		if err := writePage(sw, t, recs, opts, got); err != nil {
			return err
		}
		last := recs[len(recs)-1].ID
		from = &last
		if len(recs) < page {
			break
		}
	}
	if err := exportEdges(ctx, src, sw, t, ns, opts, got); err != nil {
		return err
	}
	return exportEvents(ctx, src, sw, t, ns, opts, got)
}

// writePage turns one page of records into its blocks, in the order the reader
// requires: a base section, then the companion that annotates it.
func writePage(sw *Writer, t tenant.ID, recs []*record.Record, opts ExportOpts, got *Stats) error {
	bodies := make([]proto.Message, 0, len(recs))
	bodyExts := make([]proto.Message, 0, len(recs))
	vectors := make([]proto.Message, 0, len(recs))

	for _, rec := range recs {
		bodies = append(bodies, recordMessage(t, rec))
		bodyExts = append(bodyExts, &pbext.RecordExt{
			Tenant:           t.String(),
			Id:               rec.ID[:],
			ArchivedAtUnixMs: unixMs(rec.Fields.ArchivedAt),
			SchemaVersion:    rec.SchemaVersion,
		})
		got.Records++

		if v := rec.Vectors[record.VectorContent]; v != nil {
			vectors = append(vectors, &pb.Vector{
				Tenant:  t.String(),
				Id:      rec.ID[:],
				ModelId: v.ModelID,
				Dim:     uint32(v.Dim),
				Values:  v.Values,
			})
			got.Vectors++
		} else {
			got.MissingVectors++
		}
	}

	if err := sw.WriteMessages(SectionRecords, bodies); err != nil {
		return err
	}
	if !opts.SharedOnly {
		if err := sw.WriteMessages(SectionGoRecordExt, bodyExts); err != nil {
			return err
		}
	}
	return sw.WriteMessages(SectionVectors, vectors)
}

// exportEdges walks a tenant's canonical out-edges once, in key order, and
// writes them in blocks of one page.
//
// Out-edges only: in-edges are derived from them and are rebuilt on import
// (Invariant 3). Writing both would double the graph on disk and give a restore
// two sources for one fact, which is how they come to disagree.
//
// One scan of the whole space rather than a neighbour read per record: the
// records have already been walked, and asking the graph for each one's
// neighbours again would be a second seek per record for a fact one iterator
// already passes over in order.
func exportEdges(ctx context.Context, src Sources, sw *Writer, t tenant.ID, ns tenant.Namespace,
	opts ExportOpts, got *Stats,
) error {
	page := opts.pageSize()
	edges := make([]proto.Message, 0, page)
	exts := make([]proto.Message, 0, page)

	flush := func() error {
		if len(edges) == 0 {
			return nil
		}
		if err := sw.WriteMessages(SectionEdges, edges); err != nil {
			return err
		}
		edges = edges[:0]
		if opts.SharedOnly {
			return nil
		}
		if err := sw.WriteMessages(SectionGoEdgeExt, exts); err != nil {
			return err
		}
		exts = exts[:0]
		return nil
	}

	sc := graph.Scope{Tenant: t, Namespace: ns}
	err := graph.ScanOut(ctx, src.Snap, sc, func(e graph.Edge) error {
		edges = append(edges, &pb.Edge{
			Tenant:           t.String(),
			From:             e.From[:],
			To:               e.To[:],
			RelationshipType: e.Type.String(),
			Strength:         e.Strength,
			CreatedAtUnixMs:  unixMs(e.CreatedAt),
		})
		got.Edges++
		if opts.SharedOnly {
			if len(e.Meta) > 0 {
				got.EdgesWithMetaDropped++
			}
		} else {
			exts = append(exts, &pbext.EdgeExt{
				Tenant:           t.String(),
				From:             e.From[:],
				To:               e.To[:],
				RelationshipType: e.Type.String(),
				UpdatedAtUnixMs:  unixMs(e.UpdatedAt),
				Meta:             e.Meta,
			})
		}
		if len(edges) >= page {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// exportEvents writes the lifecycle audit stream.
//
// It is canonical data — the row that explains a hard-deleted memory outlives
// the memory — so a backup that omitted it would answer "why did this
// disappear" with nothing after a restore.
func exportEvents(ctx context.Context, src Sources, sw *Writer, t tenant.ID, ns tenant.Namespace,
	opts ExportOpts, got *Stats,
) error {
	if src.Events == nil {
		return nil
	}
	var batch []proto.Message
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := sw.WriteMessages(SectionGoEvents, batch)
		batch = batch[:0]
		return err
	}
	err := src.Events.ScanTenant(ctx, t, ns, func(ev events.Event) error {
		if opts.SharedOnly {
			got.EventsDropped++
			return nil
		}
		batch = append(batch, &pbext.Event{
			Tenant:   t.String(),
			Subject:  ev.Subject[:],
			AtUnixMs: unixMs(ev.At),
			Seq:      ev.Seq,
			Kind:     string(ev.Kind),
			Actor:    ev.Actor,
			Reason:   ev.Reason,
			Before:   ev.Before,
			After:    ev.After,
		})
		got.Events++
		if len(batch) >= opts.pageSize() {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// recordMessage projects a record onto the byte-shared message.
//
// Everything the shared schema has a field for goes here; what it has no field
// for goes into the companion. The split is deliberate and is checked by
// TestASharedOnlyExportIsExactlyWhatRustWouldWrite.
func recordMessage(t tenant.ID, rec *record.Record) *pb.Record {
	m := &pb.Record{
		Tenant:           t.String(),
		Id:               rec.ID[:],
		Content:          rec.Content,
		Policy:           rec.Fields.Policy,
		Archived:         rec.Fields.Archived,
		CreatedAtUnixMs:  unixMs(rec.CreatedAt),
		UpdatedAtUnixMs:  unixMs(rec.UpdatedAt),
		AccessedAtUnixMs: unixMs(rec.Fields.AccessedAt),
		AccessCount:      rec.Fields.AccessCount,
		Tags:             rec.Fields.Tags,
		Importance:       rec.Fields.Importance,
		EmotionalValence: rec.Fields.Valence,
		Arousal:          rec.Fields.Arousal,
		Health:           rec.Fields.Health,
	}
	if rec.Fields.Source != "" {
		m.Source = proto.String(rec.Fields.Source)
	}
	if !rec.Fields.LastRecalledAt.IsZero() {
		m.LastRecalledAtUnixMs = proto.Uint64(unixMs(rec.Fields.LastRecalledAt))
	}
	if !rec.Fields.ProtectedUntil.IsZero() {
		m.FlashbulbUntilUnixMs = proto.Uint64(unixMs(rec.Fields.ProtectedUntil))
	}
	if rec.Fields.TTL > 0 {
		m.TtlSeconds = proto.Uint64(uint64(rec.Fields.TTL / time.Second))
	}
	if !rec.Fields.LastDecayAt.IsZero() {
		m.LastDecayAtUnixMs = proto.Uint64(unixMs(rec.Fields.LastDecayAt))
	}
	if !rec.Fields.LastHealthCheckAt.IsZero() {
		m.LastHealthCheckAtUnixMs = proto.Uint64(unixMs(rec.Fields.LastHealthCheckAt))
	}
	return m
}

func unixMs(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

// countRows sizes the corpus by walking keys and reading no values.
//
// The header opens the file and must already carry final counts, so something
// has to know them before the first record is decoded. The Rust exporter stages
// every block into a scratch file and writes the header last; this counts
// first, which costs one ordered scan of three key spaces and no decoding, and
// buys the cross-check that makes the counts a check rather than a restatement:
// both passes read one pinned snapshot, so a disagreement is a canonical row
// that could be counted and not read.
func countRows(ctx context.Context, snap storage.Snapshot, metas []tenant.Meta) (Stats, error) {
	const op = "snapshot.countRows"

	var out Stats
	out.Tenants = uint64(len(metas))
	for _, m := range metas {
		ns := tenant.DefaultNamespace
		for _, c := range []struct {
			space keys.Space
			into  *uint64
		}{
			{keys.SpaceRecord, &out.Records},
			{keys.SpaceVector, &out.Vectors},
			{keys.SpaceEdgeOut, &out.Edges},
			{keys.SpaceEvent, &out.Events},
		} {
			n, err := countSpace(ctx, snap, m.ID, ns, c.space, op)
			if err != nil {
				return Stats{}, err
			}
			*c.into += n
		}
	}
	out.MissingVectors = out.Records - min(out.Vectors, out.Records)
	return out, nil
}

func countSpace(ctx context.Context, snap storage.Snapshot, t tenant.ID, ns tenant.Namespace,
	space keys.Space, op string,
) (uint64, error) {
	lower, upper := keys.SpaceRange(t, ns, space)
	it := snap.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var n uint64
	for ok := it.First(); ok; ok = it.Next() {
		n++
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, errs.E(errs.Unavailable, op, err)
			}
		}
	}
	return n, it.Error()
}
