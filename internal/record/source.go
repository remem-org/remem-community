package record

import (
	"context"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// scanPage is how many records one page of a walk holds.
//
// Bounded, because a rebuild of a large tenant must not materialise it. The
// number is a memory decision rather than a correctness one: every page is read
// from the same snapshot, so the walk sees one state however many pages it
// takes.
const scanPage = 500

// ContentSource yields a tenant's memories as content and tags.
//
// It is the input to the keyword index's rebuild, and it lives here rather than
// in internal/text because that package indexes content and tags and has no
// business knowing what a record is. It satisfies text.Source structurally,
// which is the direction that keeps the dependency pointing inward.
//
// Both the `remem-admin text rebuild` command and the server's rebuild job read
// through this, so there is one definition of "every memory this tenant has"
// rather than one per caller.
type ContentSource struct{ kv storage.KV }

// NewContentSource returns a source over kv.
func NewContentSource(kv storage.KV) ContentSource { return ContentSource{kv: kv} }

// Scan calls fn for every memory in the tenant, in id order.
//
// The scope is set on the context here rather than carried in, because a
// rebuild legitimately runs across tenants and there is no request to inherit a
// scope from. One snapshot covers the whole walk, so a record written while it
// runs is either wholly in the rebuilt index or wholly absent from it, never
// half of each.
func (s ContentSource) Scan(ctx context.Context, t tenant.ID, ns tenant.Namespace,
	fn func(rid id.ID, content string, tags []string) error,
) error {
	ctx = tenant.NewContext(ctx, t)

	snap := s.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()

	repo := NewRepo(s.kv)
	var from *id.ID
	for {
		page, err := repo.Scan(ctx, snap, from, scanPage)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		for _, rec := range page {
			if err := fn(rec.ID, rec.Content, rec.Fields.Tags); err != nil {
				return err
			}
		}
		last := page[len(page)-1].ID
		from = &last
		if len(page) < scanPage {
			return nil
		}
	}
}
