package graph

import (
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/txn"
)

// rebuildBatch is how many staged operations one rebuild transaction holds.
//
// A rebuild of a large tenant in a single transaction would be one batch the
// size of the whole reverse index, which is a memory cost proportional to the
// corpus and a commit nothing can make progress against. Batching gives up
// atomicity of the *whole* rebuild, which is the right trade for a derived
// index: a half-rebuilt reverse index is repaired by running the rebuild again,
// where an out-of-memory commit repairs nothing.
const rebuildBatch = 1000

// RebuildIn rebuilds one tenant's in-edge index from its canonical out-edges,
// and reports how many entries it wrote.
//
// It is the reverse index's recovery path, and Invariant 3 is what shapes it:
// a derived index is rebuilt, never repaired. The space is therefore *cleared*
// first rather than reconciled — an entry nothing derives is removed by
// construction, including the orphan a crash between the two key spaces would
// leave, which no reconciliation that only looked at real edges would ever see.
//
// The rebuild reproduces the index byte for byte, and it does so by *copying*
// each canonical value rather than decoding and re-encoding it. That is not an
// optimisation. Decoding would drop any protobuf field a newer binary wrote and
// this one has never heard of, which is exactly what a rolling upgrade must not
// do to data it is only relaying — and the reverse index would then differ from
// the edges it is derived from in a way nothing but a byte comparison could see.
//
// # Scope
//
// One tenant and one namespace. This is deliberately not a cross-tenant
// function: rebuilding every tenant is `tenant.ForEach` at the call site, which
// is the single audited cross-tenant path (Invariant 1), and `internal/graph`
// is not on its allowlist.
//
// # While it runs
//
// The reverse index is incomplete between the clear and the last batch, so
// reads in the `In` direction under-report. It is registered as the
// `graph.rebuild_in` job type (Phase 9) and Phase 12 gives it a CLI verb;
// neither takes the tenant out of service for the duration, and that is a debt
// this comment records rather than settles. For the reason internal/schema's
// StrategyRebuild exists — a half-built access path does not return older
// answers, it returns fewer — a tenant being rebuilt should be refusing
// `direction=in` reads rather than under-reporting them. Nothing in the product
// triggers this rebuild automatically, so the exposure today is an operator who
// ran it deliberately.
func RebuildIn(ctx context.Context, kv storage.KV, sc Scope) (int, error) {
	const op = "graph.RebuildIn"

	if err := sc.validate(op); err != nil {
		return 0, err
	}

	inLower, inUpper := keys.SpaceRange(sc.Tenant, sc.namespace(), keys.SpaceEdgeIn)
	if err := clearRange(ctx, kv, inLower, inUpper); err != nil {
		return 0, errs.E(errs.KindOf(err), op, err)
	}

	written := 0
	tx := txn.New(kv)
	defer func() { tx.Close() }()

	lower, upper := keys.SpaceRange(sc.Tenant, sc.namespace(), keys.SpaceEdgeOut)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		from, typ, to, err := parseOutKey(it.Key(), op)
		if err != nil {
			return 0, err
		}
		// Verbatim, for the reason stated on RebuildIn.
		tx.Set(InKey(sc, from, typ, to), append([]byte(nil), it.Value()...))
		written++

		if tx.Len() >= rebuildBatch {
			if err := tx.Commit(ctx); err != nil {
				return 0, errs.E(errs.KindOf(err), op, err)
			}
			tx.Close()
			tx = txn.New(kv)
		}
		if err := ctx.Err(); err != nil {
			return 0, errs.E(errs.Unavailable, op, err)
		}
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	if tx.Len() > 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, errs.E(errs.KindOf(err), op, err)
		}
	}
	return written, nil
}

// clearRange deletes every key in a half-open range, in bounded batches.
//
// The keys are collected before they are deleted rather than deleted during the
// walk: an iterator observes the state it was created in, and mutating the range
// underneath one is a contract this codebase does not make of its adapters.
func clearRange(ctx context.Context, kv storage.KV, lower, upper []byte) error {
	for {
		it := kv.NewIterator(lower, upper)
		batch := make([][]byte, 0, rebuildBatch)
		for ok := it.First(); ok && len(batch) < rebuildBatch; ok = it.Next() {
			batch = append(batch, append([]byte(nil), it.Key()...))
		}
		err := it.Error()
		_ = it.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		tx := txn.New(kv)
		for _, k := range batch {
			tx.Delete(k)
		}
		err = tx.Commit(ctx)
		tx.Close()
		if err != nil {
			return err
		}
	}
}

// parseOutKey recovers all three parts of an out-edge key's identity.
//
// scanNeighbours needs only the relationship type and the far endpoint, because
// it already knows the anchor. A rebuild walks the whole space and knows
// neither, so it reads both endpoints out of the key.
func parseOutKey(k []byte, op string) (from id.ID, typ RelationshipType, to id.ID, err error) {
	const tail = idLen + relLen + idLen
	if len(k) < tail {
		return id.Zero, 0, id.Zero, errs.E(errs.Corruption, op,
			errors.New("a key in the out-edge space is too short to hold an edge"))
	}
	body := k[len(k)-tail:]
	from, err = id.FromBytes(body[:idLen])
	if err != nil {
		return id.Zero, 0, id.Zero, errs.E(errs.Corruption, op,
			errors.New("an out-edge key holds a malformed source id"))
	}
	typ = RelationshipType(uint16(body[idLen])<<8 | uint16(body[idLen+1]))
	to, err = id.FromBytes(body[idLen+relLen:])
	if err != nil {
		return id.Zero, 0, id.Zero, errs.E(errs.Corruption, op,
			errors.New("an out-edge key holds a malformed target id"))
	}
	return from, typ, to, nil
}
