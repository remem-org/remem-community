package record_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// passageCorpus stores n memories through a repository with the passage
// indexer, each three passages long under the fake embedder, and returns their
// ids in id order.
func passageCorpus(t *testing.T, kv storage.KV, n int) []id.ID {
	t.Helper()
	fake := embeddingtest.New()
	repo := record.NewRepo(kv, record.WithIndexer(record.NewPassageIndexer()))
	var ids []id.ID
	for i := range n {
		content := strings.Repeat(fmt.Sprintf("memory%d word ", i), 3*embeddingtest.PassageWords/2)
		docs, err := fake.EmbedDocuments(context.Background(), []string{content})
		must(t, err)
		rec := &record.Record{
			ID: id.New(), Tenant: "acme", Type: record.TypeMemory, Content: content,
			Fields:    record.Fields{Policy: "short_term", Health: 100, Importance: 0.5},
			Vectors:   map[string]*record.Vector{record.VectorContent: {ModelID: embedding.Model, Dim: embedding.Dim, Values: docs[0].Vector}},
			Passages:  record.PassageSetOf(embedding.Model, docs[0]),
			CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(1, 0),
		}
		must(t, txn.Do(ctx(), kv, func(tx txn.Tx) error { return repo.Put(ctx(), tx, rec) }))
		ids = append(ids, rec.ID)
	}
	return ids
}

// passageSpace is every passage row in the store, key and value.
func passageSpace(t *testing.T, kv storage.KV) [][]byte {
	t.Helper()
	lo, hi := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpacePassage)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	var out [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, append(bytes.Clone(it.Key()), it.Value()...))
	}
	return out
}

func wipePassages(t *testing.T, kv storage.KV) {
	t.Helper()
	lo, hi := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpacePassage)
	it := kv.NewIterator(lo, hi)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, bytes.Clone(it.Key()))
	}
	_ = it.Close()
	for _, k := range doomed {
		must(t, kv.Delete(context.Background(), k))
	}
}

func sameSpace(t *testing.T, what string, a, b [][]byte) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: %d passage rows, want %d", what, len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("%s: passage row %d differs", what, i)
		}
	}
}

// A rebuild writes exactly what the write path staged: the same rows from the
// same content through the same PassageSetOf.
func TestRebuildPassagesRestoresWhatTheWritesStaged(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	passageCorpus(t, kv, 5)
	want := passageSpace(t, kv)
	if len(want) != 15 {
		t.Fatalf("%d rows to begin with, want 15", len(want))
	}
	wipePassages(t, kv)

	n, err := record.RebuildPassages(context.Background(), kv, "acme", tenant.DefaultNamespace,
		embeddingtest.New(), nil, nil)
	must(t, err)
	if n != 5 {
		t.Fatalf("restaged %d records, want 5", n)
	}
	sameSpace(t, "after the rebuild", passageSpace(t, kv), want)
}

// An interrupted rebuild continues from its checkpoint: what the first run
// finished is not embedded again.
func TestRebuildPassagesResumesFromItsCheckpoint(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	passageCorpus(t, kv, 40)
	want := passageSpace(t, kv)
	wipePassages(t, kv)

	stop := errors.New("interrupted")
	var saved []byte
	first := embeddingtest.New()
	_, err := record.RebuildPassages(context.Background(), kv, "acme", tenant.DefaultNamespace,
		first, nil, func(cp []byte) error { saved = cp; return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("the first run ended with %v", err)
	}
	if saved == nil {
		t.Fatal("no checkpoint was saved before the interruption")
	}

	second := embeddingtest.New()
	_, err = record.RebuildPassages(context.Background(), kv, "acme", tenant.DefaultNamespace,
		second, saved, nil)
	must(t, err)
	sameSpace(t, "after resuming", passageSpace(t, kv), want)
	if first.Texts()+second.Texts() != 40 {
		t.Fatalf("embedded %d + %d texts for 40 memories: the resumed run redid finished work",
			first.Texts(), second.Texts())
	}
}

// rewritingEmbedder rewrites one memory's content — with its own passages, as
// any write does — while the rebuild is embedding the page that holds it.
type rewritingEmbedder struct {
	*embeddingtest.Fake
	once func()
}

func (r *rewritingEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	if r.once != nil {
		r.once()
		r.once = nil
	}
	return r.Fake.EmbedDocuments(ctx, texts)
}

// The rebuild embeds outside its transaction, so a memory can be rewritten in
// between. The write staged the new content's passages; the rebuild, holding
// the old content's, must leave them alone rather than restore a stale index.
func TestRebuildPassagesLeavesAConcurrentRewriteAlone(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ids := passageCorpus(t, kv, 3)
	repo := record.NewRepo(kv, record.WithIndexer(record.NewPassageIndexer()))
	fake := embeddingtest.New()
	newContent := strings.Repeat("rewritten ", 2*embeddingtest.PassageWords)

	e := &rewritingEmbedder{Fake: fake, once: func() {
		must(t, txn.Do(ctx(), kv, func(tx txn.Tx) error {
			rec, err := repo.Get(ctx(), ids[1])
			if err != nil {
				return err
			}
			docs, err := fake.EmbedDocuments(ctx(), []string{newContent})
			if err != nil {
				return err
			}
			rec.Content = newContent
			rec.Passages = record.PassageSetOf(embedding.Model, docs[0])
			return repo.Put(ctx(), tx, rec)
		}))
	}}
	_, err := record.RebuildPassages(context.Background(), kv, "acme", tenant.DefaultNamespace, e, nil, nil)
	must(t, err)

	lo, hi := keys.PassagesOfRange("acme", tenant.DefaultNamespace, ids[1])
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		row, err := vector.DecodePassage(it.Value())
		must(t, err)
		if int(row.End) > len(newContent) || !strings.HasPrefix(newContent[row.Start:row.End], "rewritten") {
			t.Fatalf("passage %d describes the old content: the rebuild overwrote the rewrite's rows", n)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("%d passage rows for the rewritten two-passage memory", n)
	}
}

// External mode has no passages and no model to compute them with, so a
// rebuild embeds nothing and leaves no passage row behind.
func TestRebuildPassagesInExternalModeEmbedsNothingAndClearsTheSpace(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	passageCorpus(t, kv, 3)
	ext := embedding.NewExternal("cohere-embed-english-v3.0", embedding.Dim)
	n, err := record.RebuildPassages(context.Background(), kv, "acme", tenant.DefaultNamespace, ext, nil, nil)
	must(t, err)
	if n != 3 {
		t.Fatalf("restaged %d records, want 3", n)
	}
	if rows := passageSpace(t, kv); len(rows) != 0 {
		t.Fatalf("%d passage rows remain in external mode", len(rows))
	}
}
