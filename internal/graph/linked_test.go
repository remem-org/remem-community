package graph_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/txn"
)

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Linked answers about the pair, not about a direction or a type: a
// relationship is a fact about two records, and a caller asking "are these two
// connected" must not get a different answer depending on which of them it
// named first.
func TestLinkedSeesEveryTypeInBothDirections(t *testing.T) {
	for _, typ := range graph.RelationshipTypes() {
		t.Run(typ.String(), func(t *testing.T) {
			for _, reversed := range []bool{false, true} {
				svc, kv, _, ctx := serviceUnderTest(t)
				a, b := id.New(), id.New()

				from, to := a, b
				if reversed {
					from, to = b, a
				}
				mustDo(t, txn.Do(ctx, kv, func(tx txn.Tx) error {
					return svc.Add(ctx, tx, graph.Edge{
						From: from, To: to, Type: typ, Strength: 0.5,
					})
				}))

				tx := txn.New(kv)
				linked, err := svc.Linked(ctx, tx, a, b)
				tx.Close()
				mustDo(t, err)
				if !linked {
					t.Fatalf("a %s edge from %s did not make the pair linked (reversed=%v)",
						typ, from, reversed)
				}
			}
		})
	}
}

func TestAnUnconnectedPairIsNotLinked(t *testing.T) {
	svc, kv, _, ctx := serviceUnderTest(t)

	tx := txn.New(kv)
	defer tx.Close()
	linked, err := svc.Linked(ctx, tx, id.New(), id.New())
	mustDo(t, err)
	if linked {
		t.Fatal("two records with no edges between them reported as linked")
	}
}

// The point of the method rather than a plain read: the answer is held to the
// commit, so a writer that connects the pair in between makes this transaction
// conflict instead of proceeding on a stale "no".
//
// The concurrent writer here connects them in the *opposite* direction, which is
// the case a forward-only check misses and the one Phase 11's end-to-end run
// actually hit.
func TestAnUnlinkedAnswerIsHeldToTheCommit(t *testing.T) {
	svc, kv, _, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()

	tx := txn.New(kv)
	defer tx.Close()
	linked, err := svc.Linked(ctx, tx, a, b)
	mustDo(t, err)
	if linked {
		t.Fatal("the pair started out linked")
	}
	mustDo(t, svc.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.SimilarTo, Strength: 0.9}))

	// Somebody else connects them the other way round, and commits first.
	mustDo(t, txn.Do(ctx, kv, func(other txn.Tx) error {
		return svc.Add(ctx, other, graph.Edge{
			From: b, To: a, Type: graph.SimilarTo, Strength: 0.9,
		})
	}))

	if err := tx.Commit(ctx); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the commit returned %v, want a conflict: the pair was connected after the check", err)
	}
}

// An already-linked pair gets no expectations, so an unrelated update of the
// edge that linked them does not fail somebody else's transaction.
func TestALinkedAnswerHoldsNothing(t *testing.T) {
	svc, kv, _, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()

	mustDo(t, txn.Do(ctx, kv, func(tx txn.Tx) error {
		return svc.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.RelatedTo, Strength: 0.5})
	}))

	tx := txn.New(kv)
	defer tx.Close()
	linked, err := svc.Linked(ctx, tx, a, b)
	mustDo(t, err)
	if !linked {
		t.Fatal("the pair is connected and did not report as linked")
	}
	// Something the caller is entitled to write while holding that answer.
	c := id.New()
	mustDo(t, svc.Add(ctx, tx, graph.Edge{From: a, To: c, Type: graph.SimilarTo, Strength: 0.8}))

	// Meanwhile the edge that linked a and b is restrengthened by someone else.
	mustDo(t, txn.Do(ctx, kv, func(other txn.Tx) error {
		return svc.Update(ctx, other, a, b, graph.RelatedTo, 0.9)
	}))

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the commit failed with %v; a linked answer holds no keys, so an update "+
			"of the edge that produced it must not conflict", err)
	}
}

func TestLinkedRefusesARecordAgainstItself(t *testing.T) {
	svc, kv, _, ctx := serviceUnderTest(t)
	rid := id.New()

	tx := txn.New(kv)
	defer tx.Close()
	if _, err := svc.Linked(ctx, tx, rid, rid); errs.KindOf(err) != errs.Invalid {
		t.Fatalf("asking whether a record is linked to itself returned %v", err)
	}
}
