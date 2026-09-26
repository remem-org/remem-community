package graph

import (
	"bytes"
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
)

// MaxFindings bounds how many individual problems one report names.
//
// A corpus whose reverse index was never built has one finding per edge, and a
// report that listed every one of them would be a diagnostic tool that runs out
// of memory on the corpus it was called to diagnose. The *counts* stay complete
// past the cap, because a bounded report must not become a misleading one:
// "here are the first thousand of 4.2 million" is useful, and "here are a
// thousand" is not.
const MaxFindings = 1000

// FindingKind names a way the graph can be inconsistent.
type FindingKind uint8

const (
	// FindingMissingReverse is a canonical out-edge with no entry in the
	// derived index. Reads in the `In` direction under-report.
	FindingMissingReverse FindingKind = iota + 1
	// FindingOrphanReverse is a derived entry with no canonical edge behind
	// it. Reads in the `In` direction return a relationship that does not
	// exist, which is the worse of the two.
	FindingOrphanReverse
	// FindingReverseDisagrees is a derived entry whose bytes differ from the
	// edge it is derived from, so the same relationship has two strengths
	// depending on which way it is read.
	FindingReverseDisagrees
	// FindingMissingRecord is an edge pointing at a record that does not
	// exist. Traversal reads no record bodies by design, so this is where an
	// orphan surfaces.
	FindingMissingRecord
	// FindingSelfEdge is an edge from a record to itself. Nothing this binary
	// writes can produce one; an import or an older writer can.
	FindingSelfEdge
)

var findingNames = [...]string{
	FindingMissingReverse:   "missing_reverse",
	FindingOrphanReverse:    "orphan_reverse",
	FindingReverseDisagrees: "reverse_disagrees",
	FindingMissingRecord:    "missing_record",
	FindingSelfEdge:         "self_edge",
}

// String returns the stable name a report prints and an operator greps for.
func (k FindingKind) String() string {
	if int(k) < len(findingNames) && findingNames[k] != "" {
		return findingNames[k]
	}
	return fmt.Sprintf("finding(%d)", uint8(k))
}

// Repairable reports whether rebuilding the reverse index fixes this finding.
//
// It is the question an operator actually has, and the answer splits the kinds
// cleanly: everything about the derived index is a rebuild away, and everything
// about the canonical edges is not. A missing record is a decision — remove the
// edge, or restore the memory — and no automatic repair should be making it.
func (k FindingKind) Repairable() bool {
	switch k {
	case FindingMissingReverse, FindingOrphanReverse, FindingReverseDisagrees:
		return true
	default:
		return false
	}
}

// Finding is one inconsistency.
type Finding struct {
	Kind     FindingKind
	From, To id.ID
	Type     RelationshipType
	// Detail is a human sentence naming what is wrong, for the operator
	// reading the report rather than for a program.
	Detail string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s: %s -%s-> %s: %s", f.Kind, f.From, f.Type, f.To, f.Detail)
}

// Report is what one verification found.
type Report struct {
	Tenant string

	// Edges is how many canonical out-edges were examined.
	Edges int
	// Reverse is how many derived entries were examined.
	Reverse int
	// Broken is how many problems were found, which is not len(Findings) once
	// the cap has bitten.
	Broken int

	// Findings are the first [MaxFindings] problems, in key order.
	Findings []Finding
	// Truncated reports that more problems were found than are listed.
	Truncated bool
}

// Clean reports whether the graph is consistent.
func (r Report) Clean() bool { return r.Broken == 0 }

// Verify checks one tenant's graph for inconsistency and reports what it finds.
//
// It is `remem-admin graph verify`, and it exists because the two edge key
// spaces are written together and can therefore only fall apart in ways nothing
// on the read path is looking for. An out-edge with no reverse entry makes "what
// points at this" under-report; a reverse entry with no edge makes it
// over-report; and an edge to a deleted record is reached by traversal, costs a
// node out of its budget, and is then dropped where records are materialised —
// so the query returns fewer memories than it was asked for and says nothing.
//
// # What it costs
//
// Two full scans of the tenant's edge spaces and one existence check per
// endpoint. It is a diagnostic, not a health check: it is proportional to the
// corpus and belongs in an operator's hands rather than on a timer.
//
// # What it does not do
//
// It repairs nothing. [FindingKind.Repairable] says which findings a rebuild of
// the reverse index would clear; the rest — an edge to a record that is gone, a
// self-edge — are decisions about user data, and a checker that quietly made
// them would be destroying relationships an operator never saw.
func Verify(ctx context.Context, kv storage.KV, sc Scope) (Report, error) {
	const op = "graph.Verify"

	if err := sc.validate(op); err != nil {
		return Report{}, err
	}
	rep := Report{Tenant: string(sc.Tenant)}

	exists := recordChecker(ctx, kv, sc)

	// Pass one: every canonical edge must have a reverse entry holding the same
	// bytes, and both its endpoints must exist.
	seen := make(map[string]struct{})
	err := scanSpace(ctx, kv, sc, keys.SpaceEdgeOut, op,
		func(from id.ID, typ RelationshipType, to id.ID, value []byte) error {
			rep.Edges++

			if from == to {
				rep.add(Finding{Kind: FindingSelfEdge, From: from, To: to, Type: typ,
					Detail: "a memory is connected to itself"})
			}
			for _, endpoint := range [2]id.ID{from, to} {
				ok, err := exists(endpoint)
				if err != nil {
					return err
				}
				if !ok {
					rep.add(Finding{Kind: FindingMissingRecord, From: from, To: to, Type: typ,
						Detail: fmt.Sprintf("memory %s does not exist", endpoint)})
				}
			}

			reverseKey := InKey(sc, from, typ, to)
			seen[string(reverseKey)] = struct{}{}
			reverse, err := kv.Get(ctx, reverseKey)
			switch {
			case errs.Is(err, errs.NotFound):
				rep.add(Finding{Kind: FindingMissingReverse, From: from, To: to, Type: typ,
					Detail: "the edge has no entry in the reverse index; rebuild it"})
			case err != nil:
				return err
			case !bytes.Equal(reverse, value):
				rep.add(Finding{Kind: FindingReverseDisagrees, From: from, To: to, Type: typ,
					Detail: "the reverse entry holds different bytes from the edge it is derived from; rebuild it"})
			}
			return nil
		})
	if err != nil {
		return Report{}, err
	}

	// Pass two: nothing may be in the reverse index that pass one did not put
	// there. This is the direction a reconciliation would miss, and the one
	// that makes a relationship appear out of nothing.
	err = scanSpace(ctx, kv, sc, keys.SpaceEdgeIn, op,
		func(from id.ID, typ RelationshipType, to id.ID, _ []byte) error {
			rep.Reverse++
			if _, ok := seen[string(InKey(sc, from, typ, to))]; !ok {
				rep.add(Finding{Kind: FindingOrphanReverse, From: from, To: to, Type: typ,
					Detail: "the reverse index holds an entry with no edge behind it; rebuild it"})
			}
			return nil
		})
	if err != nil {
		return Report{}, err
	}
	return rep, nil
}

// add records a finding, keeping the count complete past the listing cap.
func (r *Report) add(f Finding) {
	r.Broken++
	if len(r.Findings) < MaxFindings {
		r.Findings = append(r.Findings, f)
		return
	}
	r.Truncated = true
}

// recordChecker answers "does this memory exist", memoised.
//
// The memo matters: a hub with a thousand edges would otherwise cost a thousand
// reads of the same record body key. It is bounded by the number of distinct
// endpoints, which is what the tenant's corpus already is.
func recordChecker(ctx context.Context, kv storage.KV, sc Scope) func(id.ID) (bool, error) {
	known := map[id.ID]bool{}
	return func(rid id.ID) (bool, error) {
		if ok, seen := known[rid]; seen {
			return ok, nil
		}
		_, err := kv.Get(ctx, keys.Record(sc.Tenant, sc.namespace(), keys.RecordMemory, rid))
		switch {
		case err == nil:
			known[rid] = true
			return true, nil
		case errs.Is(err, errs.NotFound):
			known[rid] = false
			return false, nil
		default:
			return false, err
		}
	}
}

// scanSpace walks one edge space, decoding each key's identity.
//
// Both spaces share the layout — anchor, type, far endpoint — and differ only in
// which endpoint is the anchor, so one walker serves both and the caller is
// handed (from, to) already the right way round.
func scanSpace(ctx context.Context, kv storage.KV, sc Scope, space keys.Space, op string,
	fn func(from id.ID, typ RelationshipType, to id.ID, value []byte) error) error {

	lower, upper := keys.SpaceRange(sc.Tenant, sc.namespace(), space)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		anchor, typ, far, err := parseOutKey(it.Key(), op)
		if err != nil {
			return err
		}
		from, to := anchor, far
		if space == keys.SpaceEdgeIn {
			// The in-edge key is (to, type, from).
			from, to = far, anchor
		}
		if err := fn(from, typ, to, it.Value()); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}
