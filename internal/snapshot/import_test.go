package snapshot_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"google.golang.org/protobuf/proto"
)

// destination is an empty store an import writes into, wired the way the
// composition root wires the server: the record repository already carries its
// indexers, so an imported memory gets the same derived rows a stored one does.
type destination struct {
	kv   *memkv.Store
	dst  snapshot.Destination
	repo record.Repo
}

func newDestination(t *testing.T, e embedding.Embedder) *destination {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)

	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()))
	return &destination{
		kv:   kv,
		repo: repo,
		dst: snapshot.Destination{
			KV:       kv,
			Tenants:  tenantkv.New(kv, clk),
			Records:  repo,
			Edges:    graph.NewStore(kv),
			Events:   events.NewStore(kv),
			Embedder: e,
		},
	}
}

func (d *destination) importAll(t *testing.T, raw []byte, opts snapshot.ImportOpts) snapshot.Report {
	t.Helper()
	rep, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw), opts)
	must(t, err)
	return rep
}

func fakeEmbedder() embedding.Embedder { return embeddingtest.New() }

func TestImportWritesTheCanonicalRowsAndRebuildsTheDerivedOnes(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})

	if rep.Stats.Records != 3 || rep.Stats.Edges != 2 || rep.Stats.Events != 2 ||
		rep.Stats.Tenants != 1 {
		t.Fatalf("import stats are %+v", rep.Stats)
	}
	// Every derived space the write path maintains was rebuilt on the way in,
	// by the indexers the repository carries rather than by this package.
	for _, space := range []keys.Space{
		keys.SpaceEdgeIn, keys.SpaceAttrRow, keys.SpaceAttrIndex, keys.SpaceText,
	} {
		lower, upper := keys.SpaceRange("default", tenant.DefaultNamespace, space)
		it := d.kv.NewIterator(lower, upper)
		empty := !it.First()
		_ = it.Close()
		if empty {
			t.Fatalf("the import built no %s rows", space)
		}
	}
}

// A round trip is lossless: everything the store held comes back, including the
// five things the shared schema has no field for.
func TestGoSnapshotsRoundTripLosslessly(t *testing.T) {
	w := populated(t)
	first, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, first, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})

	// Export the imported store and compare the two files section by section.
	snap := d.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	var buf bytes.Buffer
	_, err := snapshot.Export(context.Background(), snapshot.Sources{
		Snap: snap, Tenants: d.dst.Tenants, Records: d.repo, Events: d.dst.Events,
	}, &buf, snapshot.ExportOpts{})
	must(t, err)

	a, b := decode(t, first), decode(t, buf.Bytes())
	if len(a.records) != len(b.records) || len(a.vectors) != len(b.vectors) ||
		len(a.edges) != len(b.edges) || len(a.events) != len(b.events) ||
		len(a.edgeExts) != len(b.edgeExts) {
		t.Fatalf("the round trip changed the counts: %d/%d records, %d/%d vectors, "+
			"%d/%d edges, %d/%d events, %d/%d edge exts",
			len(a.records), len(b.records), len(a.vectors), len(b.vectors),
			len(a.edges), len(b.edges), len(a.events), len(b.events),
			len(a.edgeExts), len(b.edgeExts))
	}
	for i := range a.records {
		if !proto.Equal(a.records[i], b.records[i]) {
			t.Fatalf("record %d changed:\n before %v\n after  %v", i, a.records[i], b.records[i])
		}
	}
	for i := range a.vectors {
		if !proto.Equal(a.vectors[i], b.vectors[i]) {
			t.Fatalf("vector %d changed", i)
		}
	}
	for i := range a.edges {
		if !proto.Equal(a.edges[i], b.edges[i]) {
			t.Fatalf("edge %d changed:\n before %v\n after  %v", i, a.edges[i], b.edges[i])
		}
	}
	for i := range a.edgeExts {
		if !proto.Equal(a.edgeExts[i], b.edgeExts[i]) {
			t.Fatalf("edge extension %d changed — connection metadata did not survive:\n"+
				" before %v\n after  %v", i, a.edgeExts[i], b.edgeExts[i])
		}
	}
	for i := range a.events {
		if !proto.Equal(a.events[i], b.events[i]) {
			t.Fatalf("event %d changed:\n before %v\n after  %v", i, a.events[i], b.events[i])
		}
	}
	for i := range a.recExts {
		if !proto.Equal(a.recExts[i], b.recExts[i]) {
			t.Fatalf("record extension %d changed", i)
		}
	}
}

// The phase's central rule: a vector from an implementation that pools
// differently is present and unusable, so it is replaced rather than trusted.
func TestAForeignSnapshotsVectorsAreReEmbedded(t *testing.T) {
	raw := rustLikeSnapshot(t)

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{})

	if rep.ReEmbedded != 2 || rep.VectorsVerbatim != 0 {
		t.Fatalf("re-embedded %d and kept %d, want 2 and 0", rep.ReEmbedded, rep.VectorsVerbatim)
	}
	ctx := tenant.NewContext(context.Background(), "default")
	rec, err := d.repo.Get(ctx, rustRecordID(0))
	must(t, err)
	v := rec.Vectors[record.VectorContent]
	if v == nil {
		t.Fatal("the imported record has no vector")
	}
	if v.ModelID != fakeEmbedder().ModelID() {
		t.Fatalf("the vector claims model %q, want this binary's", v.ModelID)
	}
	// The stored vector is what this binary's embedder produces for that
	// content, not the noise the file carried.
	want, err := fakeEmbedder().Embed(ctx, []string{rec.Content})
	must(t, err)
	for i := range want[0] {
		if v.Values[i] != want[0][i] {
			t.Fatalf("component %d is %v, want %v", i, v.Values[i], want[0][i])
		}
	}
}

// A Go snapshot whose vectors this binary's model produced is taken verbatim,
// and one whose vectors came from a different model is not — even though both
// say "go" in the header. The model id is checked *after* the implementation,
// never instead of it.
func TestAGoSnapshotsVectorsAreKeptOnlyWhenTheModelMatches(t *testing.T) {
	t.Run("same model", func(t *testing.T) {
		w := newWorld(t)
		w.tenant(t, "default")
		w.store(t, "default", "a memory this model embedded", embeddedByThisModel())
		w.store(t, "default", "and another", embeddedByThisModel())
		raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

		d := newDestination(t, fakeEmbedder())
		rep := d.importAll(t, raw, snapshot.ImportOpts{})
		if rep.VectorsVerbatim != 2 || rep.ReEmbedded != 0 {
			t.Fatalf("kept %d and re-embedded %d, want 2 and 0",
				rep.VectorsVerbatim, rep.ReEmbedded)
		}
	})

	t.Run("different model", func(t *testing.T) {
		w := newWorld(t)
		w.tenant(t, "default")
		w.store(t, "default", "a memory some other model embedded")
		raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

		d := newDestination(t, fakeEmbedder())
		rep := d.importAll(t, raw, snapshot.ImportOpts{})
		if rep.VectorsVerbatim != 0 || rep.ReEmbedded != 1 {
			t.Fatalf("kept %d and re-embedded %d, want 0 and 1",
				rep.VectorsVerbatim, rep.ReEmbedded)
		}
	})
}

// A record a Go snapshot carries no vector for keeps none: absence is a fact
// about that corpus, and inventing a vector would make the round trip lossy in
// the one direction nothing else detects. A Rust snapshot is the other case —
// there, absence is bookkeeping about an archived memory, and every record is
// recomputed anyway.
func TestAMissingVectorStaysMissingInAGoSnapshot(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.store(t, "default", "embedded", embeddedByThisModel())
	w.store(t, "default", "never embedded", noVector())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{})
	if rep.ReEmbedded != 0 || rep.Stats.MissingVectors != 1 {
		t.Fatalf("re-embedded %d and counted %d missing, want 0 and 1",
			rep.ReEmbedded, rep.Stats.MissingVectors)
	}
	if count(t, d.kv, keys.SpaceVector) != 1 {
		t.Fatalf("the store holds %d vectors, want 1", count(t, d.kv, keys.SpaceVector))
	}
}

// A Rust snapshot's archived records carry no vector, and Go wants one: an
// archived memory here keeps its vector, so recomputing is the faithful import
// rather than an embellishment.
func TestARustSnapshotsMissingVectorsAreRecomputed(t *testing.T) {
	raw := rustLikeSnapshot(t, withVectorlessRecord())

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{})
	if rep.ReEmbedded != 3 {
		t.Fatalf("re-embedded %d, want 3 — the vectorless record included", rep.ReEmbedded)
	}
	if count(t, d.kv, keys.SpaceVector) != 3 {
		t.Fatalf("the store holds %d vectors, want 3", count(t, d.kv, keys.SpaceVector))
	}
}

// Refusing is the point: a corpus imported verbatim from a differently-pooled
// snapshot looks healthy and ranks wrongly, and nothing would ever report it.
func TestImportingForeignVectorsVerbatimIsRefusedByName(t *testing.T) {
	raw := rustLikeSnapshot(t)
	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want Invalid, got %v", err)
	}
	for _, want := range []string{"§II.10 row 16", "wrong space"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q: %v", want, err)
		}
	}
}

func TestImportingWithoutAnEmbedderIsRefusedByName(t *testing.T) {
	raw := rustLikeSnapshot(t)
	d := newDestination(t, nil)
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{})
	if !errs.Is(err, errs.Unavailable) {
		t.Fatalf("want Unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "-tags onnx") {
		t.Fatalf("the refusal does not say what to do: %v", err)
	}
}

func TestOrphanEdgesAreReportedNotSilentlyDropped(t *testing.T) {
	raw := rustLikeSnapshot(t, withOrphanEdge(), withSelfEdge())

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{})

	if len(rep.OrphanEdges) != 1 {
		t.Fatalf("reported %d orphan edges, want 1: %v", len(rep.OrphanEdges), rep.OrphanEdges)
	}
	if len(rep.SelfEdges) != 1 {
		t.Fatalf("reported %d self edges, want 1: %v", len(rep.SelfEdges), rep.SelfEdges)
	}
	if !strings.Contains(rep.OrphanEdges[0].String(), "no record for the target") {
		t.Fatalf("the orphan is not explained: %v", rep.OrphanEdges[0])
	}

	// And neither reached the graph: an edge to a record that does not exist
	// makes traversal spend its budget reaching nothing.
	ctx := tenant.NewContext(context.Background(), "default")
	out, err := graph.NewService(d.kv, clock.NewFake(clock.FakeStart)).
		Out(ctx, rustRecordID(0), graph.NeighbourOpts{})
	must(t, err)
	for _, e := range out {
		if e.To == rustRecordID(99) || e.To == e.From {
			t.Fatalf("a rejected edge was written: %v -> %v", e.From, e.To)
		}
	}
}

func TestImportIsIdempotent(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	first := d.importAll(t, raw, snapshot.ImportOpts{})
	second := d.importAll(t, raw, snapshot.ImportOpts{})

	if first.Unchanged != 0 {
		t.Fatalf("the first import found %d records already present", first.Unchanged)
	}
	if second.Unchanged != 3 {
		t.Fatalf("the second import found %d unchanged records, want 3", second.Unchanged)
	}
	if second.Skipped != 0 {
		t.Fatalf("the second import skipped %d records as conflicting", second.Skipped)
	}
	// A second run over the same file changes nothing, which is what makes
	// restarting a crashed import safe from the beginning.
	if count(t, d.kv, keys.SpaceRecord) != 3 {
		t.Fatalf("the second import wrote extra records")
	}
	if count(t, d.kv, keys.SpaceEdgeOut) != 2 {
		t.Fatalf("the second import wrote extra edges")
	}
	if count(t, d.kv, keys.SpaceEvent) != 2 {
		t.Fatalf("the second import wrote extra events")
	}
}

func TestConflictModes(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	rid := decode(t, raw).records[0].GetId()

	// A store that already holds one of the ids with different content.
	seed := func(t *testing.T) *destination {
		t.Helper()
		d := newDestination(t, fakeEmbedder())
		d.importAll(t, raw, snapshot.ImportOpts{})
		ctx := tenant.NewContext(context.Background(), "default")
		got, err := id.FromBytes(rid)
		must(t, err)
		rec, err := d.repo.Get(ctx, got)
		must(t, err)
		rec.Content = "something else entirely"
		must(t, txnPut(ctx, d, rec))
		return d
	}

	t.Run("fail", func(t *testing.T) {
		d := seed(t)
		_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
			snapshot.ImportOpts{OnConflict: snapshot.ConflictFail})
		if !errs.Is(err, errs.Conflict) {
			t.Fatalf("want Conflict, got %v", err)
		}
		if !strings.Contains(err.Error(), "--on-conflict=skip") {
			t.Fatalf("the refusal does not say what the choices are: %v", err)
		}
	})

	t.Run("skip", func(t *testing.T) {
		d := seed(t)
		rep := d.importAll(t, raw, snapshot.ImportOpts{OnConflict: snapshot.ConflictSkip})
		if rep.Skipped != 1 || rep.Unchanged != 2 {
			t.Fatalf("skipped %d and found %d unchanged, want 1 and 2", rep.Skipped, rep.Unchanged)
		}
		ctx := tenant.NewContext(context.Background(), "default")
		got, err := id.FromBytes(rid)
		must(t, err)
		rec, err := d.repo.Get(ctx, got)
		must(t, err)
		if rec.Content != "something else entirely" {
			t.Fatalf("skip overwrote the record: %q", rec.Content)
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		d := seed(t)
		rep := d.importAll(t, raw, snapshot.ImportOpts{OnConflict: snapshot.ConflictOverwrite})
		if rep.Skipped != 0 {
			t.Fatalf("overwrite skipped %d records", rep.Skipped)
		}
		ctx := tenant.NewContext(context.Background(), "default")
		got, err := id.FromBytes(rid)
		must(t, err)
		rec, err := d.repo.Get(ctx, got)
		must(t, err)
		if rec.Content == "something else entirely" {
			t.Fatal("overwrite left the local record in place")
		}
	})
}

// The header carries no checksum — Rust's does not either, and the file is
// byte-shared, so Go cannot add one. Nothing trusts it: an import counts what
// it actually read and refuses when the two disagree.
//
// This is the other half of the property TestASingleFlippedByteInAPayloadOrTrailerIsAlwaysCaught
// deliberately excludes.
func TestADamagedHeaderCountIsCaught(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{Compression: snapshot.CompressionNone})
	damaged := lieAboutRecordCount(t, raw, 99)

	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(damaged),
		snapshot.ImportOpts{})
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "claims 99 records and the blocks hold 3") {
		t.Fatalf("the refusal does not name the disagreement: %v", err)
	}
}

// lieAboutRecordCount rewrites the header's record_count, leaving every block
// untouched. The header carries no checksum, so nothing in the framing notices.
func lieAboutRecordCount(t *testing.T, raw []byte, count uint64) []byte {
	t.Helper()
	hlen := int(binary.LittleEndian.Uint32(raw[12:]))
	var h pb.Header
	must(t, proto.Unmarshal(raw[16:16+hlen], &h))
	h.RecordCount = count
	lie, err := proto.Marshal(&h)
	must(t, err)

	out := append([]byte(nil), raw[:12]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(lie)))
	out = append(out, lie...)
	return append(out, raw[16+hlen:]...)
}

// A block whose section byte was flipped is caught too, though usually one step
// earlier: the messages of one section rarely decode as another's, so protobuf
// refuses before the counts are compared. Either way it does not pass.
func TestARelabelledBlockDoesNotPass(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{SharedOnly: true,
		Compression: snapshot.CompressionNone})
	raw[frameOf(t, raw, snapshot.SectionRecords)] = byte(snapshot.SectionTenants)

	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{})
	if err == nil {
		t.Fatal("a relabelled block imported cleanly")
	}
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
}

// Sections declared in the shared schema and written by no released binary are
// refused rather than skipped (spec §59).
func TestADeclaredButUnwrittenSectionIsRefused(t *testing.T) {
	var buf bytes.Buffer
	wr, err := snapshot.NewWriter(&buf, &pb.Header{SourceImpl: "go"}, snapshot.WriterOpts{})
	must(t, err)
	must(t, wr.WriteMessages(snapshot.SectionEvents,
		[]proto.Message{&pb.Event{Tenant: "default", Kind: "recalled"}}))
	must(t, wr.Close())

	d := newDestination(t, fakeEmbedder())
	_, err = snapshot.Import(context.Background(), d.dst, bytes.NewReader(buf.Bytes()),
		snapshot.ImportOpts{})
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
	if !strings.Contains(err.Error(), "no released Remem writes") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
}

func TestCrossTenantRowsLandInTheirOwnTenants(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.tenant(t, "second")
	a := w.store(t, "default", "mine")
	b := w.store(t, "second", "theirs")
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	first := tenant.NewContext(context.Background(), "default")
	second := tenant.NewContext(context.Background(), "second")
	if _, err := d.repo.Get(first, a); err != nil {
		t.Fatalf("the first tenant's memory is missing: %v", err)
	}
	if _, err := d.repo.Get(first, b); !errs.Is(err, errs.NotFound) {
		t.Fatalf("the second tenant's memory is readable as the first: %v", err)
	}
	if _, err := d.repo.Get(second, b); err != nil {
		t.Fatalf("the second tenant's memory is missing: %v", err)
	}
}

// frameOf returns the byte offset of the first frame carrying section, by
// walking the frames rather than assuming their order — which changed once
// already when the exporter started writing tenants first.
func frameOf(t *testing.T, raw []byte, section snapshot.Section) int {
	t.Helper()
	at := 16 + int(binary.LittleEndian.Uint32(raw[12:]))
	for at < len(raw)-24 {
		if snapshot.Section(raw[at]) == section {
			return at
		}
		at += 13 + int(binary.LittleEndian.Uint32(raw[at+5:]))
	}
	t.Fatalf("no %s block in the snapshot", section)
	return 0
}

func count(t *testing.T, kv *memkv.Store, space keys.Space) int {
	t.Helper()
	n := 0
	for _, tid := range []tenant.ID{"default", "second"} {
		lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, space)
		it := kv.NewIterator(lower, upper)
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		_ = it.Close()
	}
	return n
}
