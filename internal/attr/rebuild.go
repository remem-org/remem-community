package attr

import (
	"context"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// RebuildRows reprojects every record's attribute row and slot entries, and
// reports how many records it restaged.
//
// It walks the records and re-stages each through the indexer that owns the
// projection, which is the same code the write path runs — so a rebuilt row is
// the row a fresh write would have produced, by construction rather than by
// review.
//
// Unlike the vector and text rebuilds it does not clear first. An attribute row
// is addressed by its record's id and a slot entry by its value, so re-staging
// replaces both: the indexer reads the row it is replacing and removes the
// entries that row pointed at. An entry left over from a record that no longer
// exists is removed by the record's own deletion, in the transaction that
// deletes it.
//
// It lives here rather than in `remem-admin`, where it began, because two
// callers need it: the command, for a stopped server, and the `attr.rebuild`
// job, for a running one. Until Phase 13 only the first existed, which left the
// attribute rows the one derived space a running server could not repair.
func RebuildRows(ctx context.Context, kv storage.KV, t tenant.ID, ns tenant.Namespace) (int, error) {
	ix := NewIndexer(MustTable())
	repo := record.NewRepo(kv, record.WithIndexer(ix))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	tctx := tenant.NewContext(ctx, t)
	written := 0
	var from *id.ID
	for {
		page, err := repo.Scan(tctx, snap, from, rebuildPage)
		if err != nil {
			return written, err
		}
		if len(page) == 0 {
			return written, nil
		}
		if err := txn.Do(tctx, kv, func(tx txn.Tx) error {
			for _, rec := range page {
				if err := ix.Stage(tctx, tx, t, ns, rec.ID, rec); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return written, err
		}
		written += len(page)
		last := page[len(page)-1].ID
		from = &last
		if len(page) < rebuildPage {
			return written, nil
		}
	}
}

// rebuildPage bounds the rebuild's memory and the size of one transaction.
// A rebuild of a large tenant in a single transaction would be a commit
// proportional to the corpus; batching gives up atomicity of the whole rebuild,
// which is the right trade for a derived index — the repair for a half-finished
// one is to run it again.
const rebuildPage = 500
