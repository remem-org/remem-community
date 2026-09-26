// Package inspect diagnoses a data directory: whether the rows Remem derives
// from its canonical data still agree with it.
//
// It exists as a package rather than inside `remem-admin` because two callers
// need the same answer: the command an operator runs against a stopped server,
// and the crash tests in test/crash, which kill a real process mid-write and
// then ask exactly this question of what it left behind. Neither can import the
// other, and the checker reads five subsystems' rows, so it belongs to none of
// them.
//
// # What it asserts, and what it deliberately does not
//
// Every finding is something no correct sequence of writes can leave behind:
//
//   - a key in a tenant's range that does not parse under its space;
//   - a record body that does not decode;
//   - a derived row — an attribute row, an attribute index entry, a text row, a
//     canonical vector — naming a record that does not exist;
//   - a record with no attribute row, which listings and filters cannot see;
//   - anything graph.Verify reports about the two edge spaces.
//
// It does not assert the other direction for the indexes whose presence depends
// on a record's state — that every live record has postings, or a vector —
// because what archiving withdraws is each index's own business, and a checker
// that flags legitimate state teaches an operator to ignore it. Nor does it
// check HNSW node records: a node naming a deleted record is reconciled when the
// tenant next materialises, by design, and is not damage.
package inspect

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// MaxFindings bounds one tenant's report. A directory damaged badly enough to
// exceed it has told an operator what they need to know, and a report
// proportional to the corpus is not one anybody reads.
const MaxFindings = 1000

// Finding is one inconsistency.
type Finding struct {
	Tenant tenant.ID
	// Space is the key space the offending row lives in, by its stable name.
	Space string
	// Key is the offending row's key, for `remem-admin inspect key`.
	Key []byte
	// Problem is a sentence an operator can act on. It never quotes content.
	Problem string
}

func (f Finding) String() string {
	return fmt.Sprintf("tenant %s, %s %x: %s", f.Tenant, f.Space, f.Key, f.Problem)
}

// Report is what checking one tenant found.
type Report struct {
	Tenant tenant.ID
	// Records is how many record keys were examined.
	Records int
	// Findings are the first MaxFindings problems.
	Findings []Finding
	// Truncated reports that more problems were found than are listed.
	Truncated bool
}

// Clean reports whether nothing was found.
func (r Report) Clean() bool { return len(r.Findings) == 0 }

// CheckTenant examines one tenant.
//
// There is deliberately no function here that walks every tenant. Cross-tenant
// work goes through tenant.Directory.ForEach in a package that is audited for
// it (Invariant 1), and `remem-admin` already is one; a diagnostic library that
// quietly fanned out would be a second, unaudited path — which is what the
// boundary guard refused when this package first had one.
//
// It is one ordered pass over the tenant's namespace. Key order does the
// bookkeeping: the record space's byte sorts ahead of every space derived from
// it, so by the time the first derived row is read, every record id is known.
func CheckTenant(ctx context.Context, kv storage.KV, t tenant.ID) (Report, error) {
	const op = "inspect.CheckTenant"
	ns := tenant.DefaultNamespace
	rep := Report{Tenant: t}
	add := func(space string, key []byte, format string, args ...any) {
		if len(rep.Findings) >= MaxFindings {
			rep.Truncated = true
			return
		}
		rep.Findings = append(rep.Findings, Finding{
			Tenant: t, Space: space, Key: append([]byte(nil), key...),
			Problem: fmt.Sprintf(format, args...),
		})
	}

	records := map[id.ID]bool{}
	withRow := map[id.ID]bool{}

	lower, upper := keys.NamespaceRange(t, ns)
	it := kv.NewIterator(lower, upper)
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		_, _, space, err := keys.ParseSpace(k)
		if err != nil {
			add("unknown", k, "a key in this tenant's range does not parse: %v", err)
			continue
		}
		switch space {
		case keys.SpaceRecord:
			_, _, _, rid, err := keys.ParseRecord(k)
			if err != nil {
				add(space.String(), k, "a record key does not parse: %v", err)
				continue
			}
			records[rid] = true
		case keys.SpaceAttrRow:
			rid, ok := tailID(k)
			switch {
			case !ok:
				add(space.String(), k, "an attribute row key holds no record id")
			case !records[rid]:
				add(space.String(), k, "an attribute row for record %s, which does not exist", rid)
			default:
				withRow[rid] = true
			}
		case keys.SpaceAttrIndex, keys.SpaceVector:
			rid, ok := tailID(k)
			switch {
			case !ok:
				add(space.String(), k, "a key holds no record id")
			case !records[rid]:
				add(space.String(), k, "a %s row for record %s, which does not exist", space, rid)
			}
		case keys.SpaceText:
			_, _, _, rid, err := keys.ParseText(k)
			switch {
			case err != nil:
				add(space.String(), k, "a text key does not parse: %v", err)
			case rid == id.Zero:
				// The tenant's corpus statistics row, which names no record.
			case !records[rid]:
				add(space.String(), k, "a text row for record %s, which does not exist", rid)
			}
		}
		if err := ctx.Err(); err != nil {
			_ = it.Close()
			return rep, errs.E(errs.Unavailable, op, err)
		}
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return rep, err
	}
	_ = it.Close()
	rep.Records = len(records)

	for rid := range records {
		if !withRow[rid] {
			add(keys.SpaceAttrRow.String(), keys.AttrRow(t, ns, rid),
				"record %s has no attribute row, so no listing or filter can return it", rid)
		}
	}

	if err := checkBodies(ctx, kv, t, add); err != nil {
		return rep, err
	}

	g, err := graph.Verify(ctx, kv, graph.Scope{Tenant: t, Namespace: ns})
	if err != nil {
		return rep, err
	}
	for _, f := range g.Findings {
		add("edge", nil, "%s", f.String())
	}
	if g.Truncated {
		add("edge", nil, "the graph has %d problems, more than are listed", g.Broken)
	}
	return rep, nil
}

// checkBodies decodes every record body through one snapshot.
//
// The first body that does not decode ends the walk with a finding saying so,
// because the repository cannot page past a row it cannot read. That is enough
// to tell an operator the directory needs attention; the key is in the finding.
func checkBodies(ctx context.Context, kv storage.KV, t tenant.ID,
	add func(space string, key []byte, format string, args ...any),
) error {
	const page = 500
	repo := record.NewRepo(kv)
	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	tctx := tenant.NewContext(ctx, t)
	var from *id.ID
	for {
		recs, err := repo.Scan(tctx, snap, from, page)
		if err != nil {
			switch errs.KindOf(err) {
			case errs.Corruption, errs.IncompatibleVersion:
				var at []byte
				if from != nil {
					at = keys.Record(t, tenant.DefaultNamespace, keys.RecordMemory, *from)
				}
				add(keys.SpaceRecord.String(), at,
					"a record body after this key does not decode, and no later body was checked: %v", err)
				return nil
			}
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		last := recs[len(recs)-1].ID
		from = &last
		if len(recs) < page {
			return nil
		}
	}
}

// tailID reads the record id every derived key in these spaces ends with.
func tailID(k []byte) (id.ID, bool) {
	const idLen = 16
	if len(k) < idLen {
		return id.Zero, false
	}
	rid, err := id.FromBytes(k[len(k)-idLen:])
	return rid, err == nil
}
