package text_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// document is one memory as a rebuild's source yields it.
type document struct {
	id      id.ID
	content string
	tags    []string
}

// corpusSource replays a fixed list of memories, which is what a rebuild reads
// from the canonical record bodies in production.
type corpusSource []document

func (c corpusSource) Scan(ctx context.Context, t tenant.ID, ns tenant.Namespace,
	fn func(id.ID, string, []string) error) error {
	for _, d := range c {
		if err := fn(d.id, d.content, d.tags); err != nil {
			return err
		}
	}
	return nil
}

// snapshotSpace copies every row of a tenant's text space, so two states of the
// index can be compared byte for byte.
func snapshotSpace(t *testing.T, kv storage.KV, tn tenant.ID) map[string][]byte {
	t.Helper()
	lower, upper := keys.SpaceRange(tn, ns, keys.SpaceText)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	out := map[string][]byte{}
	for ok := it.First(); ok; ok = it.Next() {
		out[string(it.Key())] = bytes.Clone(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	return out
}

// TestRebuildReproducesPostings is the plan's test: rebuilt postings are
// byte-identical to incrementally maintained ones.
//
// Byte-identical, not merely equivalent. Two indexes holding the same terms
// with different bytes cannot be compared by an audit, cannot be compared
// between two replicas in Phase 14, and hide exactly the class of difference —
// a document length off by one, a frequency counted twice — that changes
// ranking without changing which memories are found.
func TestRebuildReproducesPostings(t *testing.T) {
	incremental := memkv.New()
	t.Cleanup(func() { _ = incremental.Close() })
	rebuilt := memkv.New()
	t.Cleanup(func() { _ = rebuilt.Close() })

	ix := text.New()
	ctx := context.Background()

	corpus := make(corpusSource, 0, 40)
	for i := range 40 {
		corpus = append(corpus, document{
			id:      id.New(),
			content: fmt.Sprintf("memory %d about kubernetes clusters and 東京 and 🎉 repeated kubernetes", i),
			tags:    []string{"infra", fmt.Sprintf("batch-%d", i%3)},
		})
	}

	for _, d := range corpus {
		err := txn.Do(ctx, incremental, func(tx txn.Tx) error {
			return ix.Add(ctx, tx, acme, ns, d.id, d.content, d.tags)
		})
		if err != nil {
			t.Fatalf("indexing: %v", err)
		}
	}

	if err := ix.Rebuild(ctx, rebuilt, acme, ns, corpus); err != nil {
		t.Fatalf("rebuilding: %v", err)
	}

	want := snapshotSpace(t, incremental, acme)
	got := snapshotSpace(t, rebuilt, acme)

	if len(want) != len(got) {
		t.Fatalf("the rebuilt index holds %d rows and the incremental one %d", len(got), len(want))
	}
	for key, value := range want {
		other, ok := got[key]
		if !ok {
			_, _, term, rid, _ := keys.ParseText([]byte(key))
			t.Fatalf("the rebuilt index is missing the row for term %q, record %s", term, rid)
		}
		if !bytes.Equal(value, other) {
			_, _, term, rid, _ := keys.ParseText([]byte(key))
			t.Fatalf("term %q, record %s: incremental %x, rebuilt %x", term, rid, value, other)
		}
	}
}

// A rebuild replaces, so what was in the index and is not in the source goes.
// A rebuild that only added would leave a corpus permanently holding the terms
// of memories deleted while the index was damaged.
func TestRebuildDropsWhatTheSourceNoLongerHas(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()
	ctx := context.Background()

	stale := write(t, ix, kv, acme, "a memory that has since been deleted", "obsolete")
	kept := document{id: id.New(), content: "the memory that survives", tags: []string{"current"}}
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		return ix.Add(ctx, tx, acme, ns, kept.id, kept.content, kept.tags)
	}); err != nil {
		t.Fatalf("indexing: %v", err)
	}

	if err := ix.Rebuild(ctx, kv, acme, ns, corpusSource{kept}); err != nil {
		t.Fatalf("rebuilding: %v", err)
	}

	for term, found := range postingsFor(t, kv, acme) {
		for _, rid := range found {
			if rid == stale {
				t.Fatalf("the term %q still lists a record the source no longer has", term)
			}
		}
	}
	stats, err := text.ReadStats(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading statistics: %v", err)
	}
	if stats.Documents != 1 {
		t.Fatalf("the rebuilt corpus reports %d documents, want 1", stats.Documents)
	}
}

// An index being rebuilt is incomplete, and it has to say so. This is the
// lesson Phase 7 paid for with vector.Index.Health: a damaged index that serves
// a short page silently is indistinguishable from a small corpus.
func TestARebuildingIndexReportsItselfDegraded(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()
	ctx := context.Background()

	write(t, ix, kv, acme, "kubernetes cluster notes")

	healthy, err := ix.Health(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading health: %v", err)
	}
	if healthy.Degraded {
		t.Fatalf("an intact index reports itself degraded: %q", healthy.Reason)
	}

	// A rebuild interrupted after it marked itself and before it finished.
	// Restartable, not resumable (the disposition Phase 7 settled on): the
	// marker survives, so a search says its answer may be incomplete until
	// somebody runs the rebuild again.
	failed := errNotFinished
	err = ix.Rebuild(ctx, kv, acme, ns, sourceFunc(func(context.Context, tenant.ID, tenant.Namespace,
		func(id.ID, string, []string) error) error {
		return failed
	}))
	if err == nil {
		t.Fatal("a rebuild whose source failed reported success")
	}

	degraded, err := ix.Health(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading health: %v", err)
	}
	if !degraded.Degraded {
		t.Fatal("an interrupted rebuild left the index reporting itself intact")
	}
	if degraded.Reason == "" {
		t.Fatal("a degraded index gave no reason an operator could act on")
	}

	// Running it again clears the marker.
	if err := ix.Rebuild(ctx, kv, acme, ns, corpusSource{}); err != nil {
		t.Fatalf("rebuilding again: %v", err)
	}
	after, err := ix.Health(ctx, kv, acme, ns)
	if err != nil {
		t.Fatalf("reading health: %v", err)
	}
	if after.Degraded {
		t.Fatalf("a completed rebuild left the index degraded: %q", after.Reason)
	}
}

// A tenant nobody has written to is not degraded. "Empty" and "broken" must not
// be the same answer, or every new tenant's first search warns.
func TestAnEmptyTenantIsHealthy(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	got, err := text.New().Health(context.Background(), kv, "never-written", ns)
	if err != nil {
		t.Fatalf("reading health: %v", err)
	}
	if got.Degraded {
		t.Fatalf("an untouched tenant reports itself degraded: %q", got.Reason)
	}
}

// A rebuild of one tenant must not touch another's rows.
func TestRebuildIsTenantScoped(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()
	ctx := context.Background()

	write(t, ix, kv, "globex", "another tenant's memory about pangolins")
	before := snapshotSpace(t, kv, "globex")

	if err := ix.Rebuild(ctx, kv, acme, ns, corpusSource{}); err != nil {
		t.Fatalf("rebuilding: %v", err)
	}

	after := snapshotSpace(t, kv, "globex")
	if len(before) != len(after) {
		t.Fatalf("rebuilding one tenant changed another's index: %d rows became %d", len(before), len(after))
	}
	for key, value := range before {
		if !bytes.Equal(value, after[key]) {
			t.Fatal("rebuilding one tenant changed another's rows")
		}
	}
}

type sourceFunc func(context.Context, tenant.ID, tenant.Namespace, func(id.ID, string, []string) error) error

func (f sourceFunc) Scan(ctx context.Context, t tenant.ID, nsp tenant.Namespace,
	fn func(id.ID, string, []string) error) error {
	return f(ctx, t, nsp, fn)
}

var errNotFinished = fmt.Errorf("the source gave up half way")
