package hnsw

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// NodeFormatVersion is the version of the packed node record.
//
// It is a byte inside the row, not one of the four directory-wide formats in
// internal/version, for the same reason attr.RowFormatVersion is: a node record
// this binary cannot read is one unreadable row of a *derived* index. That is a
// rebuild, not a refusal to open the database.
const NodeFormatVersion uint8 = 1

// A node record is:
//
//	[u8      format version]
//	[16      record id]
//	[u8      top layer]
//	[per layer 0..top:]
//	  [uvarint neighbour count]
//	  [uvarint × count neighbour node numbers]
//
// There is no vector in it and there is no deleted flag in it, and both
// absences are the design. The vector lives once, in the canonical row, which
// is also what decides whether this node exists at all — see the package
// comment. The top layer is stored even though levelOf recomputes it from the
// record id, because it is the one thing that changes when M is reconfigured:
// a record whose stored layer disagrees with the one this binary would give it
// is read as a graph built under different parameters, which is a rebuild.
func encodeNode(n *node) []byte {
	size := 1 + 16 + 1
	for _, l := range n.links {
		size += 8 + len(l)*4
	}
	b := make([]byte, 0, size)
	b = append(b, NodeFormatVersion)
	b = append(b, n.id[:]...)
	b = append(b, uint8(n.level))
	for l := 0; l <= n.level; l++ {
		var links []uint32
		if l < len(n.links) {
			links = n.links[l]
		}
		b = binary.AppendUvarint(b, uint64(len(links)))
		for _, m := range links {
			b = binary.AppendUvarint(b, uint64(m))
		}
	}
	return b
}

// storedNode is a decoded node record, before it is known whether the record it
// names still has a canonical vector.
type storedNode struct {
	num   uint32
	id    id.ID
	level int
	links [][]uint32
}

func decodeNode(num uint32, b []byte) (*storedNode, error) {
	const op = "hnsw.decodeNode"
	bad := func(msg string) error {
		return errs.E(errs.Corruption, op, fmt.Errorf("node record %d: %s", num, msg))
	}

	if len(b) < 1+16+1 {
		return nil, bad("shorter than a header")
	}
	if b[0] != NodeFormatVersion {
		return nil, errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"node record %d is format %d and this binary reads %d", num, b[0], NodeFormatVersion))
	}
	rid, err := id.FromBytes(b[1:17])
	if err != nil {
		return nil, bad("holds a malformed record id")
	}
	level := int(b[17])
	if level > maxLevel {
		return nil, bad(fmt.Sprintf("claims layer %d, above the %d bound", level, maxLevel))
	}

	out := &storedNode{num: num, id: rid, level: level, links: make([][]uint32, level+1)}
	rest := b[18:]
	for l := 0; l <= level; l++ {
		count, w := binary.Uvarint(rest)
		if w <= 0 {
			return nil, bad(fmt.Sprintf("ends before the neighbour count of layer %d", l))
		}
		rest = rest[w:]
		links := make([]uint32, 0, count)
		for range count {
			m, w := binary.Uvarint(rest)
			if w <= 0 {
				return nil, bad(fmt.Sprintf("ends inside the neighbours of layer %d", l))
			}
			rest = rest[w:]
			links = append(links, uint32(m))
		}
		out.links[l] = links
	}
	return out, nil
}

// nodeKey addresses one node record. Node number zero is never written: it is
// the in-memory value for "no node", and keeping it out of the key space means
// a decode can never mistake the two.
func nodeKey(t tenant.ID, num uint32) []byte {
	return keys.VectorIndex(t, tenant.DefaultNamespace, uint64(num))
}

func numFromKey(k []byte) (uint32, error) {
	if len(k) < 8 {
		return 0, errs.E(errs.Corruption, "hnsw.numFromKey",
			errors.New("a key in the vector-index range is too short to hold a node number"))
	}
	n := binary.BigEndian.Uint64(k[len(k)-8:])
	if n == 0 || n > 0xFFFFFFFF {
		return 0, errs.E(errs.Corruption, "hnsw.numFromKey",
			fmt.Errorf("node number %d is outside the range this index allocates", n))
	}
	return uint32(n), nil
}

// loadResult is what one materialisation learned, beyond the graph itself.
type loadResult struct {
	// Corrupt counts node records that could not be decoded. Any at all means
	// the tenant is served degraded and a rebuild is wanted.
	Corrupt int
	// Reindexed counts canonical vectors that had no node record and were
	// inserted during the load. That is the ordinary repair after a crash
	// between a commit and its asynchronous index write, and it is not a
	// degradation.
	Reindexed int
	// Dropped counts node records naming a record with no canonical vector.
	// This is how a deletion that never reached the index is undone.
	Dropped int
}

// pendingWork is what [Index.materialise] leaves undone: the canonical vectors
// no node record covered, in key order, and the node records it could not read.
// [Index.load] finishes it in one transaction; [Index.Rebuild] finishes it in
// committed batches, which is what makes a rebuild resumable.
type pendingWork struct {
	pending    []id.ID
	vectors    map[id.ID]*vector.Vector
	unreadable [][]byte
}

// load materialises one tenant's graph and inserts every canonical vector no
// node record covered, committing the repair in one transaction.
func (x *Index) load(ctx context.Context, t tenant.ID) (*graph, loadResult, error) {
	var work pendingWork
	g, res, err := x.materialise(ctx, t, &work)
	if err != nil {
		return nil, res, err
	}

	// Anything canonical that no node record covered is inserted now, in key
	// order, so two servers materialising the same corpus build the same graph.
	tx := txn.New(x.kv)
	defer tx.Close()
	staged := 0

	// A node record that could not be read is deleted rather than left in
	// place. It is derived data and nothing is lost by dropping it — the record
	// it named is re-inserted below from its canonical vector — and leaving it
	// would make the tenant report itself degraded at every materialisation
	// for ever, since the same row would fail to decode every time.
	for _, k := range work.unreadable {
		tx.Delete(k)
		staged++
	}

	for _, rid := range work.pending {
		dirty, err := g.insert(rid, work.vectors[rid].Values)
		if err != nil {
			return nil, res, err
		}
		res.Reindexed++
		staged += x.stage(tx, t, g, dirty)
	}
	if staged > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, res, err
		}
	}
	return g, res, nil
}

// materialise builds one tenant's graph from the canonical vectors and the node
// records, reconciling the two, and reports into work what it did not insert.
//
// The reconciliation runs in both directions and that is the whole reason this
// index needs no tombstone and no repair sweep:
//
//   - a node record naming a record with no canonical vector is dropped, so a
//     hard delete is complete the moment its transaction commits, whether or
//     not the process lived long enough to rewrite the graph;
//   - a canonical vector with no node record is inserted, so an index write
//     lost to a crash costs a little work at the next materialisation rather
//     than leaving a memory that exists and cannot be found.
//
// Corruption is survivable in the same way: an unreadable node record is
// skipped, its record then looks like one with no node, and it is re-inserted.
// The tenant is still reported degraded, because the links that pointed *into*
// the unreadable node are gone and the graph's quality is no longer the one the
// parameters describe.
func (x *Index) materialise(ctx context.Context, t tenant.ID, work *pendingWork) (*graph, loadResult, error) {
	const op = "hnsw.load"
	var res loadResult

	vectors := make(map[id.ID]*vector.Vector)
	order := make([]id.ID, 0, 64)
	modelID := ""
	if err := x.store.ScanVectors(ctx, t, func(rid id.ID, v *vector.Vector) error {
		if v.ModelID != "" {
			switch {
			case modelID == "":
				modelID = v.ModelID
			case modelID != v.ModelID:
				return errs.E(errs.Invalid, op, fmt.Errorf(
					"tenant %s holds vectors from two models, %q and %q: they are not one space and cannot be indexed together",
					t, modelID, v.ModelID))
			}
		}
		vectors[rid] = v.Clone()
		order = append(order, rid)
		return nil
	}); err != nil {
		return nil, res, err
	}
	if x.modelID != "" && modelID != "" && modelID != x.modelID {
		return nil, res, errs.E(errs.Invalid, op, fmt.Errorf(
			"tenant %s was embedded with %q and this server runs %q: searching one space with the other's queries returns confident nonsense, so the corpus must be re-embedded",
			t, modelID, x.modelID))
	}

	g := newGraph(x.params, x.metric)

	stored, maxNum, unreadable, err := x.readNodes(ctx, t, &res)
	if err != nil {
		return nil, res, err
	}

	// Place every node whose record still has a canonical vector. Node numbers
	// are preserved across a restart so the stored neighbour lists keep meaning
	// what they said.
	g.nodes = make([]*node, maxNum+1)
	for _, sn := range stored {
		v, ok := vectors[sn.id]
		if !ok {
			res.Dropped++
			continue
		}
		if sn.level != levelOf(sn.id, g.params.M) {
			// The graph was built under a different M. Its links are still
			// readable but they are not the structure these parameters
			// describe, so the tenant is degraded and wants a rebuild.
			res.Corrupt++
		}
		n := &node{id: sn.id, vec: v.Values, norm: distance.Norm(v.Values), level: sn.level}
		n.links = sn.links
		n.back = make([][]uint32, sn.level+1)
		g.nodes[sn.num] = n
		g.byID[sn.id] = sn.num
		g.live++
		if g.dim == 0 {
			g.dim = len(v.Values)
		}
	}

	// Drop links to node numbers that are not present, then derive the reverse
	// adjacency from what survived.
	for _, n := range g.nodes {
		if n == nil {
			continue
		}
		for l := range n.links {
			out := n.links[l][:0]
			for _, m := range n.links[l] {
				if int(m) < len(g.nodes) && g.nodes[m] != nil {
					out = append(out, m)
				}
			}
			n.links[l] = out
		}
	}
	for num, n := range g.nodes {
		if n == nil {
			continue
		}
		for l := range n.links {
			for _, m := range n.links[l] {
				g.addBack(m, uint32(num), l)
			}
		}
	}
	g.entry, g.entryLevel = g.findEntry()
	for num := uint32(1); num < uint32(len(g.nodes)); num++ {
		if g.nodes[num] == nil {
			g.free = append(g.free, num)
		}
	}

	// What no node record covered is left for the caller, in key order, so
	// that two servers finishing the same corpus build the same graph.
	work.vectors = vectors
	work.unreadable = unreadable
	for _, rid := range order {
		if _, ok := g.byID[rid]; !ok {
			work.pending = append(work.pending, rid)
		}
	}
	return g, res, nil
}

// readNodes decodes every node record of a tenant, counting the ones it cannot.
func (x *Index) readNodes(ctx context.Context, t tenant.ID, res *loadResult) ([]*storedNode, uint32, [][]byte, error) {
	lower, upper := keys.SpaceRange(t, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := x.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var out []*storedNode
	var unreadable [][]byte
	var maxNum uint32
	for ok := it.First(); ok; ok = it.Next() {
		num, err := numFromKey(it.Key())
		if err != nil {
			res.Corrupt++
			unreadable = append(unreadable, append([]byte(nil), it.Key()...))
			continue
		}
		sn, err := decodeNode(num, it.Value())
		if err != nil {
			res.Corrupt++
			unreadable = append(unreadable, append([]byte(nil), it.Key()...))
			continue
		}
		if num > maxNum {
			maxNum = num
		}
		out = append(out, sn)
		if err := ctx.Err(); err != nil {
			return nil, 0, nil, errs.E(errs.Unavailable, "hnsw.readNodes", err)
		}
	}
	return out, maxNum, unreadable, it.Error()
}

// stage writes the node records for the given node numbers into tx, deleting
// the rows of numbers that are no longer live. It returns how many operations
// it staged.
func (x *Index) stage(tx txn.Tx, t tenant.ID, g *graph, nums []uint32) int {
	for _, num := range nums {
		if int(num) < len(g.nodes) && g.nodes[num] != nil {
			tx.Set(nodeKey(t, num), encodeNode(g.nodes[num]))
			continue
		}
		tx.Delete(nodeKey(t, num))
	}
	return len(nums)
}

// clearNodes removes every node record a tenant has. It is the first half of a
// rebuild and of nothing else.
func (x *Index) clearNodes(ctx context.Context, t tenant.ID) error {
	lower, upper := keys.SpaceRange(t, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := x.kv.NewIterator(lower, upper)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, append([]byte(nil), it.Key()...))
	}
	err := it.Error()
	_ = it.Close()
	if err != nil {
		return err
	}
	if len(doomed) == 0 {
		return nil
	}
	tx := txn.New(x.kv)
	defer tx.Close()
	for _, k := range doomed {
		tx.Delete(k)
	}
	return tx.Commit(ctx)
}
