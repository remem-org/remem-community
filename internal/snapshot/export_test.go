package snapshot_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/tenant"
	"google.golang.org/protobuf/proto"
)

// exported is a decoded snapshot, section by section, for assertions.
type exported struct {
	header   *pb.Header
	records  []*pb.Record
	vectors  []*pb.Vector
	edges    []*pb.Edge
	tenants  []*pb.Tenant
	recExts  []*pbext.RecordExt
	edgeExts []*pbext.EdgeExt
	tenExts  []*pbext.TenantExt
	events   []*pbext.Event
	sections []snapshot.Section
}

func decode(t *testing.T, raw []byte) *exported {
	t.Helper()
	r, err := snapshot.NewReader(bytes.NewReader(raw))
	must(t, err)
	defer func() { _ = r.Close() }()

	out := &exported{header: r.Header()}
	for {
		blk, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		must(t, err)
		out.sections = append(out.sections, blk.Section)
		must(t, blk.Each(func(b []byte) error {
			switch blk.Section {
			case snapshot.SectionRecords:
				m := &pb.Record{}
				out.records = append(out.records, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionVectors:
				m := &pb.Vector{}
				out.vectors = append(out.vectors, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionEdges:
				m := &pb.Edge{}
				out.edges = append(out.edges, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionTenants:
				m := &pb.Tenant{}
				out.tenants = append(out.tenants, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionGoRecordExt:
				m := &pbext.RecordExt{}
				out.recExts = append(out.recExts, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionGoEdgeExt:
				m := &pbext.EdgeExt{}
				out.edgeExts = append(out.edgeExts, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionGoTenantExt:
				m := &pbext.TenantExt{}
				out.tenExts = append(out.tenExts, m)
				return proto.Unmarshal(b, m)
			case snapshot.SectionGoEvents:
				m := &pbext.Event{}
				out.events = append(out.events, m)
				return proto.Unmarshal(b, m)
			default:
				return errors.New("unexpected section " + blk.Section.String())
			}
		}))
	}
}

func exportWorld(t *testing.T, w *world, opts snapshot.ExportOpts) ([]byte, snapshot.Stats) {
	t.Helper()
	src, done := w.sources()
	defer done()
	var buf bytes.Buffer
	stats, err := snapshot.Export(context.Background(), src, &buf, opts)
	must(t, err)
	return buf.Bytes(), stats
}

func populated(t *testing.T) *world {
	t.Helper()
	w := newWorld(t)
	w.tenant(t, "default")
	a := w.store(t, "default", "the first memory", tagged("alpha", "beta"))
	b := w.store(t, "default", "the second memory")
	c := w.store(t, "default", "an archived memory", archived(w.clk.Now().UTC()), noVector())
	w.connect(t, "default", a, b, graph.RelatedTo, 0.9, map[string]string{"why": "by hand"})
	w.connect(t, "default", b, c, graph.Supports, 0.4, nil)
	w.event(t, "default", a, events.Recalled, "a recall")
	w.event(t, "default", c, events.Archived, "health reached zero")
	return w
}

func TestExportWritesEveryCanonicalRowAndNoDerivedOne(t *testing.T) {
	w := populated(t)
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{Compression: snapshot.CompressionZstd})
	got := decode(t, raw)

	if stats.Records != 3 || stats.Vectors != 2 || stats.MissingVectors != 1 || stats.Edges != 2 {
		t.Fatalf("stats are %+v", stats)
	}
	if got.header.GetRecordCount() != 3 || got.header.GetVectorCount() != 2 ||
		got.header.GetEdgeCount() != 2 || got.header.GetEventCount() != 2 {
		t.Fatalf("header counts are %+v", got.header)
	}
	if got.header.GetSourceImpl() != "go" {
		t.Fatalf("source_impl is %q", got.header.GetSourceImpl())
	}
	if len(got.records) != 3 || len(got.vectors) != 2 || len(got.edges) != 2 ||
		len(got.tenants) != 1 || len(got.events) != 2 {
		t.Fatalf("sections hold %d records, %d vectors, %d edges, %d tenants, %d events",
			len(got.records), len(got.vectors), len(got.edges), len(got.tenants), len(got.events))
	}

	// An archived memory is exported with no vector rather than a placeholder,
	// and it is still a record: the corpus is not silently pruned by a backup.
	for _, r := range got.records {
		if r.GetArchived() && len(r.GetContent()) == 0 {
			t.Fatal("the archived record lost its content")
		}
	}

	// Derived rows are never written. Every section present is one of the six
	// this export produces, and none of them is an index.
	for _, s := range got.sections {
		switch s {
		case snapshot.SectionTenants, snapshot.SectionRecords, snapshot.SectionVectors,
			snapshot.SectionEdges, snapshot.SectionGoRecordExt, snapshot.SectionGoEdgeExt,
			snapshot.SectionGoTenantExt, snapshot.SectionGoEvents:
		default:
			t.Fatalf("the export wrote a %s block", s)
		}
	}
	// And the store really does hold the derived rows the export left out, so
	// the assertion above is about the exporter rather than about an empty store.
	for _, space := range []keys.Space{
		keys.SpaceEdgeIn, keys.SpaceAttrRow, keys.SpaceAttrIndex, keys.SpaceText,
	} {
		lower, upper := keys.SpaceRange("default", tenant.DefaultNamespace, space)
		it := w.kv.NewIterator(lower, upper)
		empty := !it.First()
		_ = it.Close()
		if empty {
			t.Fatalf("the store holds no %s rows, so this test proves nothing about them", space)
		}
	}
}

// The five things the shared schema has no field for survive, because Go writes
// its own companion sections beside the shared ones.
func TestTheCompanionSectionsCarryWhatTheSharedSchemaCannot(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	got := decode(t, raw)

	if len(got.recExts) != 3 || len(got.edgeExts) != 2 || len(got.tenExts) != 1 {
		t.Fatalf("companions hold %d record exts, %d edge exts, %d tenant exts",
			len(got.recExts), len(got.edgeExts), len(got.tenExts))
	}
	var archivedAt uint64
	for _, e := range got.recExts {
		if e.GetArchivedAtUnixMs() != 0 {
			archivedAt = e.GetArchivedAtUnixMs()
		}
		if e.GetSchemaVersion() != 1 {
			t.Fatalf("a record ext carries schema version %d", e.GetSchemaVersion())
		}
	}
	if archivedAt == 0 {
		t.Fatal("the archived record's archive time did not survive")
	}
	var withMeta int
	for _, e := range got.edgeExts {
		if e.GetMeta()["why"] == "by hand" {
			withMeta++
		}
	}
	if withMeta != 1 {
		t.Fatalf("%d edges carry their metadata, want 1", withMeta)
	}
	if got.events[0].GetActor() != "test" || got.events[0].GetBefore()["health"] != "100" {
		t.Fatalf("an event lost its structure: %+v", got.events[0])
	}
}

// A shared-only export is exactly what Rust would have written — and it says
// out loud what that costs, rather than dropping it quietly.
func TestASharedOnlyExportIsExactlyWhatRustWouldWrite(t *testing.T) {
	w := populated(t)
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{SharedOnly: true})
	got := decode(t, raw)

	for _, s := range got.sections {
		if !s.Shared() {
			t.Fatalf("a shared-only export wrote a %s block", s)
		}
	}
	if len(got.records) != 3 || len(got.edges) != 2 {
		t.Fatalf("the shared sections are incomplete: %d records, %d edges",
			len(got.records), len(got.edges))
	}
	if stats.EdgesWithMetaDropped != 1 {
		t.Fatalf("dropped-metadata count is %d, want 1", stats.EdgesWithMetaDropped)
	}
	if stats.EventsDropped != 2 {
		t.Fatalf("dropped-event count is %d, want 2", stats.EventsDropped)
	}
	if got.header.GetEventCount() != 0 {
		t.Fatalf("a shared-only header claims %d events", got.header.GetEventCount())
	}
}

// A companion block only means anything immediately after the block it
// annotates, and the reader holds that so an importer can buffer one page
// rather than a map of the corpus.
func TestACompanionBlockOutOfPlaceIsRefused(t *testing.T) {
	var buf bytes.Buffer
	wr, err := snapshot.NewWriter(&buf, &pb.Header{SourceImpl: "go"}, snapshot.WriterOpts{})
	must(t, err)
	must(t, wr.WriteMessages(snapshot.SectionVectors,
		[]proto.Message{&pb.Vector{Tenant: "default", Dim: 1, Values: []float32{1}}}))
	must(t, wr.WriteMessages(snapshot.SectionGoRecordExt,
		[]proto.Message{&pbext.RecordExt{Tenant: "default"}}))
	must(t, wr.Close())

	_, _, err = readAll(t, buf.Bytes())
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "annotates the records block") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

func TestExportOfOneTenantLeavesTheOtherOut(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.tenant(t, "second")
	w.store(t, "default", "mine")
	w.store(t, "second", "theirs")
	w.store(t, "second", "also theirs")

	raw, stats := exportWorld(t, w, snapshot.ExportOpts{Tenant: "second"})
	got := decode(t, raw)
	if stats.Records != 2 || len(got.records) != 2 {
		t.Fatalf("got %d records, want 2", len(got.records))
	}
	for _, r := range got.records {
		if r.GetTenant() != "second" {
			t.Fatalf("a record of tenant %q reached a second-tenant export", r.GetTenant())
		}
	}
	if len(got.header.GetTenants()) != 1 || got.header.GetTenants()[0] != "second" {
		t.Fatalf("the header names %v", got.header.GetTenants())
	}
}

// A tenant with no memories is still a tenant, and a restore that dropped it
// would lose its policies and its schema version.
func TestAnEmptyTenantIsStillExported(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.tenant(t, "empty")
	w.store(t, "default", "something")

	raw, stats := exportWorld(t, w, snapshot.ExportOpts{})
	got := decode(t, raw)
	if stats.Tenants != 2 || len(got.tenants) != 2 {
		t.Fatalf("got %d tenants, want 2", len(got.tenants))
	}
}

func TestExportingAnEmptyDirectoryIsRefusedByName(t *testing.T) {
	w := newWorld(t)
	src, done := w.sources()
	defer done()
	_, err := snapshot.Export(context.Background(), src, io.Discard, snapshot.ExportOpts{})
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("want NotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), "registers no tenants") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

// Paging is a memory bound, not a change of content: the same corpus exports to
// the same rows however finely it is paged.
func TestPageSizeChangesTheBlocksAndNotTheRows(t *testing.T) {
	w := populated(t)
	coarse, _ := exportWorld(t, w, snapshot.ExportOpts{PageSize: 1000})
	fine, _ := exportWorld(t, w, snapshot.ExportOpts{PageSize: 1})

	a, b := decode(t, coarse), decode(t, fine)
	if len(b.sections) <= len(a.sections) {
		t.Fatalf("fine paging produced %d blocks and coarse %d", len(b.sections), len(a.sections))
	}
	if len(a.records) != len(b.records) || len(a.edges) != len(b.edges) {
		t.Fatalf("paging changed the rows: %d/%d records, %d/%d edges",
			len(a.records), len(b.records), len(a.edges), len(b.edges))
	}
	for i := range a.records {
		if !proto.Equal(a.records[i], b.records[i]) {
			t.Fatalf("record %d differs between page sizes", i)
		}
	}
}
