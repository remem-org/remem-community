package server

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// The passage indexer is a repository option, so a server that forgot to
// register it would still compute passages on every write and then drop them —
// and nothing else would notice, because search falls back to the document
// vector. The first version of the wiring did forget it, in the composition
// root, while every test fixture had it.
func TestTheServerStagesPassageRows(t *testing.T) {
	d := mustDeps(t)
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)
	content := strings.Repeat("word ", 3*embeddingtest.PassageWords)
	m, err := d.memories.Create(ctx, memory.CreateReq{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := keys.PassagesOfRange(tid, tenant.DefaultNamespace, m.ID)
	it := d.kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if n != 3 {
		t.Fatalf("a three-passage memory stored through the server has %d passage rows", n)
	}
}

// An import of a Go snapshot of this model, run without the model, leaves its
// memories indexed by their canonical vectors alone and their tenant marked.
// The next start computes the passages before it binds, and clears the mark.
func TestAStartComputesThePassagesAnImportOwed(t *testing.T) {
	dir := t.TempDir()
	onDisk := func(c *config.Config) {
		c.Storage.Engine = "pebble"
		c.Storage.Path = dir
	}
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)

	first := mustDepsWith(t, onDisk)
	content := strings.Repeat("word ", 3*embeddingtest.PassageWords)
	m, err := first.memories.Create(ctx, memory.CreateReq{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	// What an import without the model writes: the memory with its canonical
	// vector, no passage rows, and the tenant marked.
	tx := txn.New(first.kv)
	if err := vector.StageDeletePassages(tx, tid, tenant.DefaultNamespace, m.ID); err != nil {
		t.Fatal(err)
	}
	record.MarkPassagesPending(tx, tid)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.close(); err != nil {
		t.Fatal(err)
	}

	second := mustDepsWith(t, onDisk)
	lo, hi := keys.PassagesOfRange(tid, tenant.DefaultNamespace, m.ID)
	it := second.kv.NewIterator(lo, hi)
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	_ = it.Close()
	if n != 3 {
		t.Fatalf("%d passage rows after the start; it was owed three", n)
	}
	pending, err := record.PassagesPending(context.Background(), second.kv)
	if err != nil || len(pending) != 0 {
		t.Fatalf("still pending after the start: %v %v", pending, err)
	}
}

// The running server's repair path clears the mark too: a passages.rebuild job
// is the same function, through the job framework.
func TestThePassagesRebuildJobClearsThePendingMark(t *testing.T) {
	d := mustDeps(t)
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)
	seedMemories(t, d, tid)
	tx := txn.New(d.kv)
	record.MarkPassagesPending(tx, tid)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	handler, err := d.registry.Handler(TypePassagesRebuild)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(),
		&jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace, Type: TypePassagesRebuild}, noCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if pending, err := record.PassagesPending(context.Background(), d.kv); err != nil || len(pending) != 0 {
		t.Fatalf("still pending after the job: %v %v", pending, err)
	}
}
