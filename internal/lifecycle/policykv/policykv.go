// Package policykv is the durable store for a tenant's retention policy
// overrides, in the untenanted system key space beside the tenant directory.
//
// # Why this is not in internal/lifecycle
//
// The shape internal/tenant/tenantkv already uses: the package that owns the
// meaning owns the type, and the adapter that knows the key encoding is a
// subpackage beside it. internal/lifecycle is imported by internal/memory and
// by the composition root, and neither needs to know where an override is
// stored.
//
// # Why this is not a field on the tenant directory row
//
// Plan §Phase 10 Task 1 says "per-tenant overrides stored in tenant metadata",
// and this is that — one row per tenant, in the same space, read the same way.
// It is not a field *on* the directory row because internal/keys imports
// internal/tenant, so internal/tenant cannot import internal/lifecycle: putting
// the overrides there would mean either a second declaration of the Policy
// struct, or moving retention semantics into the package whose job is isolation
// identity. A row of its own costs one system prefix.
package policykv

import (
	"context"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv/pb"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// prefix is where override rows live. Untenanted, like the directory row it
// sits beside: it has to be readable to decide how a tenant behaves, which is
// before any of that tenant's data is touched.
const prefix = "/policies/"

// Store reads and writes policy overrides.
type Store struct {
	kv  storage.KV
	clk clock.Clock
}

// New returns the store over kv.
func New(kv storage.KV, clk clock.Clock) *Store { return &Store{kv: kv, clk: clk} }

// Key returns the storage key of a tenant's override row.
//
// Exported so a test can damage exactly one and so remem-admin can name a row
// it is inspecting. Nothing in the read path calls it from outside.
func Key(t tenant.ID) []byte { return keys.System(prefix + string(t)) }

// Get returns a tenant's overrides. A tenant with none returns an empty map and
// no error: having no overrides is the ordinary state, not a missing row.
func (s *Store) Get(ctx context.Context, t tenant.ID) (map[string]lifecycle.Policy, error) {
	const op = "policykv.Get"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"reading retention policies requires a tenant (Invariant 1)"))
	}
	b, err := s.kv.Get(ctx, Key(t))
	if errs.Is(err, errs.NotFound) {
		return map[string]lifecycle.Policy{}, nil
	}
	if err != nil {
		return nil, err
	}
	return decode(t, b, op)
}

// Put replaces a tenant's whole override table.
//
// It is a replacement rather than a merge, and it validates before it writes: an
// override that names a policy to promote into that does not exist is refused
// here, not discovered by a sweep three weeks later when nothing promotes and
// nobody can say why.
//
// The write is conditional on the row it read, so two operators editing at once
// do not silently lose one edit. That matters more here than for most rows: an
// override is set once and forgotten, so a lost write is a setting somebody
// believes is in force and is not.
func (s *Store) Put(ctx context.Context, t tenant.ID, policies map[string]lifecycle.Policy) error {
	const op = "policykv.Put"

	if t == "" {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"writing retention policies requires a tenant (Invariant 1)"))
	}
	if _, err := tenant.Parse(string(t)); err != nil {
		return err
	}
	// The whole resolved table has to be valid, not just the overrides: an
	// override promoting into a built-in is legal, and one promoting into a
	// name nothing defines is not.
	if _, err := lifecycle.NewPolicies(policies); err != nil {
		return err
	}

	row := &pb.Overrides{
		Policies:        make(map[string]*pb.RetentionPolicy, len(policies)),
		UpdatedAtUnixMs: uint64(s.clk.Now().UnixMilli()),
	}
	for name, p := range policies {
		row.Policies[name] = encodePolicy(p)
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(row)
	if err != nil {
		return errs.E(errs.Invalid, op, fmt.Errorf("encoding the policy row for %q: %w", t, err))
	}

	key := Key(t)
	tx := txn.New(s.kv)
	defer tx.Close()

	old, err := tx.Get(key)
	if err != nil && !errs.Is(err, errs.NotFound) {
		return err
	}
	tx.Expect(key, old, err == nil)
	tx.Set(key, value)
	return tx.Commit(ctx)
}

// Loader is [Get] in the shape lifecycle.NewRegistry takes, so the composition
// root does not have to write the adapter.
func (s *Store) Loader() func(context.Context, tenant.ID) (map[string]lifecycle.Policy, error) {
	return s.Get
}

func encodePolicy(p lifecycle.Policy) *pb.RetentionPolicy {
	out := &pb.RetentionPolicy{
		PromoteTo:       p.PromoteTo,
		ImportanceDecay: p.ImportanceDecay,
		HealthDecay:     p.HealthDecay,
	}
	if p.TTL != nil {
		out.TtlSecs = proto.Uint64(uint64(*p.TTL / time.Second))
	}
	if p.PromoteAtRecalls != nil {
		out.PromoteAtRecalls = proto.Uint32(*p.PromoteAtRecalls)
	}
	if p.ArchiveAtHealth != nil {
		out.ArchiveAtHealth = proto.Float32(*p.ArchiveAtHealth)
	}
	if p.CleanupAfter != nil {
		out.CleanupAfterSecs = proto.Uint64(uint64(*p.CleanupAfter / time.Second))
	}
	return out
}

// decode reads an override row.
//
// A row that will not parse is [errs.Corruption], never an empty table. Falling
// back to the built-ins would apply the wrong retention to a tenant that had
// deliberately chosen different rules — and the visible outcome would be
// memories archived on a schedule nobody set.
func decode(t tenant.ID, b []byte, op string) (map[string]lifecycle.Policy, error) {
	var row pb.Overrides
	if err := proto.Unmarshal(b, &row); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the retention policy row for tenant %q will not parse", t))
	}
	out := make(map[string]lifecycle.Policy, len(row.GetPolicies()))
	for name, p := range row.GetPolicies() {
		out[name] = lifecycle.Policy{
			Name:             name,
			TTL:              secs(p.TtlSecs),
			PromoteAtRecalls: p.PromoteAtRecalls,
			PromoteTo:        p.GetPromoteTo(),
			ImportanceDecay:  p.GetImportanceDecay(),
			HealthDecay:      p.GetHealthDecay(),
			ArchiveAtHealth:  p.ArchiveAtHealth,
			CleanupAfter:     secs(p.CleanupAfterSecs),
		}
	}
	return out, nil
}

func secs(v *uint64) *time.Duration {
	if v == nil {
		return nil
	}
	d := time.Duration(*v) * time.Second
	return &d
}
