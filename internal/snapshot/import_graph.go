package snapshot

// The graph half of an import: canonical out-edges, the companion section that
// restores what the shared schema cannot carry, and the sweep that removes the
// edges pointing at records the corpus does not hold.

import (
	"context"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// applyEdges writes canonical out-edges, and with them the derived in-edge rows.
func (im *importer) applyEdges(ctx context.Context, blk *Block) error {
	const op = "snapshot.Import"

	msgs, err := SplitMessages(blk)
	if err != nil {
		return err
	}
	batch := im.opts.batchSize()
	for start := 0; start < len(msgs); start += batch {
		end := min(start+batch, len(msgs))
		if err := im.writeEdges(ctx, msgs[start:end], op); err != nil {
			return err
		}
	}
	return nil
}

func (im *importer) writeEdges(ctx context.Context, msgs [][]byte, op string) error {
	return txn.Do(ctx, im.dst.KV, func(tx txn.Tx) error {
		for _, raw := range msgs {
			var m pb.Edge
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, fmt.Errorf("decoding an edge: %w", err))
			}
			e, sc, err := edgeFrom(&m, im.now())
			if err != nil {
				return err
			}
			im.note(sc.Tenant)
			im.report.Stats.Edges++
			if e.From == e.To {
				// Nothing Go's write path can produce. Traversal would spend
				// budget returning the anchor to itself.
				im.reject(&im.report.SelfEdges, Rejected{
					Tenant: sc.Tenant, From: e.From, To: e.To, Type: e.Type.String(),
					Reason: "an edge from a record to itself",
				})
				continue
			}
			if err := im.dst.Edges.Stage(ctx, tx, sc, e); err != nil {
				return err
			}
		}
		return nil
	})
}

func edgeFrom(m *pb.Edge, now time.Time) (graph.Edge, graph.Scope, error) {
	const op = "snapshot.Import"

	from, err := id.FromBytes(m.GetFrom())
	if err != nil {
		return graph.Edge{}, graph.Scope{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"an edge carries a malformed source id: %w", err))
	}
	to, err := id.FromBytes(m.GetTo())
	if err != nil {
		return graph.Edge{}, graph.Scope{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"an edge carries a malformed target id: %w", err))
	}
	t, err := tenant.Parse(m.GetTenant())
	if err != nil {
		return graph.Edge{}, graph.Scope{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"an edge names tenant %q: %w", m.GetTenant(), err))
	}
	typ, err := graph.ParseRelationshipType(m.GetRelationshipType())
	if err != nil {
		return graph.Edge{}, graph.Scope{}, errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"the edge %s -> %s is a %q, which is not one of the eight relationship types this "+
				"binary knows (%v). A relationship type is two bytes of every edge key and is "+
				"never renumbered, so an unknown one means the snapshot came from a newer Remem",
			from, to, m.GetRelationshipType(), graph.RelationshipNames()))
	}
	created := fromUnixMs(m.GetCreatedAtUnixMs())
	if created.IsZero() {
		created = now
	}
	return graph.Edge{
			From: from, To: to, Type: typ, Strength: m.GetStrength(),
			CreatedAt: created, UpdatedAt: created,
		},
		graph.Scope{Tenant: t, Namespace: tenant.DefaultNamespace}, nil
}

// applyEdgeExts restores connection metadata and the edge's update time.
func (im *importer) applyEdgeExts(ctx context.Context, blk *Block) error {
	const op = "snapshot.Import"

	msgs, err := SplitMessages(blk)
	if err != nil {
		return err
	}
	batch := im.opts.batchSize()
	for start := 0; start < len(msgs); start += batch {
		end := min(start+batch, len(msgs))
		if err := im.writeEdgeExts(ctx, msgs[start:end], op); err != nil {
			return err
		}
	}
	return nil
}

func (im *importer) writeEdgeExts(ctx context.Context, msgs [][]byte, op string) error {
	return txn.Do(ctx, im.dst.KV, func(tx txn.Tx) error {
		for _, raw := range msgs {
			var m pbext.EdgeExt
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, fmt.Errorf("decoding an edge extension: %w", err))
			}
			from, err := id.FromBytes(m.GetFrom())
			if err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			to, err := id.FromBytes(m.GetTo())
			if err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			if from == to {
				continue // the self-edge it annotates was rejected
			}
			t, err := tenant.Parse(m.GetTenant())
			if err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			typ, err := graph.ParseRelationshipType(m.GetRelationshipType())
			if err != nil {
				return errs.E(errs.IncompatibleVersion, op, err)
			}
			sc := graph.Scope{Tenant: t, Namespace: tenant.DefaultNamespace}
			e, err := im.dst.Edges.Get(ctx, im.dst.KV, sc, from, to, typ)
			if errs.Is(err, errs.NotFound) {
				continue
			}
			if err != nil {
				return err
			}
			e.Meta = m.GetMeta()
			if u := fromUnixMs(m.GetUpdatedAtUnixMs()); !u.IsZero() {
				e.UpdatedAt = u
			}
			if err := im.dst.Edges.Stage(ctx, tx, sc, e); err != nil {
				return err
			}
		}
		return nil
	})
}

// sweepOrphans removes edges whose target record the import did not write, and
// names them.
//
// It runs at the end rather than per edge, because the Rust exporter interleaves
// records and edges a page at a time: an edge to a record in a later page would
// look like an orphan at the moment it is read and is not one. So the whole file
// is applied first, and what is left pointing at nothing is what is actually
// pointing at nothing.
//
// The edges are removed rather than kept. An edge to a record that does not
// exist is not user data a restore can recover — the record it describes is
// already gone — and leaving it makes traversal spend its node budget reaching
// records that are not there, which is how "memories related to this one"
// quietly returns fewer than it was asked for.
func (im *importer) sweepOrphans(ctx context.Context) error {
	const op = "snapshot.Import"

	for t := range im.tenants {
		sc := graph.Scope{Tenant: t, Namespace: tenant.DefaultNamespace}
		var dead []graph.Edge
		err := graph.ScanOut(ctx, im.dst.KV, sc, func(e graph.Edge) error {
			_, err := im.dst.KV.Get(ctx, record.BodyKey(t, sc.Namespace, e.To))
			if errs.Is(err, errs.NotFound) {
				dead = append(dead, e)
				return nil
			}
			return err
		})
		if err != nil {
			return errs.E(errs.KindOf(err), op, err)
		}
		if len(dead) == 0 {
			continue
		}
		for start := 0; start < len(dead); start += im.opts.batchSize() {
			end := min(start+im.opts.batchSize(), len(dead))
			part := dead[start:end]
			if err := txn.Do(ctx, im.dst.KV, func(tx txn.Tx) error {
				for _, e := range part {
					if err := im.dst.Edges.StageDelete(ctx, tx, sc, e.From, e.To, e.Type); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return errs.E(errs.KindOf(err), op, err)
			}
			for _, e := range part {
				im.reject(&im.report.OrphanEdges, Rejected{
					Tenant: t, From: e.From, To: e.To, Type: e.Type.String(),
					Reason: "the snapshot carries no record for the target",
				})
			}
		}
	}
	return nil
}
