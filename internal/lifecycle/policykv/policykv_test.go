package policykv_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/storage/memkv"
)

func newStore(t *testing.T) (*policykv.Store, *memkv.Store) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return policykv.New(kv, clock.NewFake(clock.FakeStart)), kv
}

func TestATenantWithNoOverridesIsNotAMissingRow(t *testing.T) {
	s, _ := newStore(t)
	got, err := s.Get(context.Background(), "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d overrides, want none", len(got))
	}
}

// Every field of a Policy survives the round trip. A field that encoded and did
// not decode would look exactly like a tenant that had never set it — and the
// visible outcome is memories retained on rules nobody chose.
func TestEveryPolicyFieldSurvivesTheRoundTrip(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	ttl := 90 * time.Minute
	promoteAt := uint32(7)
	archiveAt := float32(12.5)
	cleanup := 14 * 24 * time.Hour

	want := lifecycle.Policy{
		Name:             "custom",
		TTL:              &ttl,
		PromoteAtRecalls: &promoteAt,
		PromoteTo:        lifecycle.LongTerm,
		ImportanceDecay:  0.987,
		HealthDecay:      3.5,
		ArchiveAtHealth:  &archiveAt,
		CleanupAfter:     &cleanup,
	}
	if err := s.Put(ctx, "acme", map[string]lifecycle.Policy{"custom": want}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get(ctx, "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(deref(got["custom"]), deref(want)) {
		t.Fatalf("round trip changed the policy:\n got %+v\nwant %+v", deref(got["custom"]), deref(want))
	}

	// The struct is compared field by field above through deref; this catches a
	// field added to Policy and forgotten in the codec, which the comparison
	// alone would not.
	if n := reflect.TypeOf(lifecycle.Policy{}).NumField(); n != 8 {
		t.Fatalf("lifecycle.Policy has %d fields; the codec in policykv handles 8. "+
			"A new field that is not encoded reads back as unset, which looks exactly "+
			"like a tenant that never configured it", n)
	}
}

// Nil is "never" and must not come back as zero: a cleanup_after of zero would
// delete a memory the moment it was archived.
func TestNilMeansNeverAndSurvivesAsNil(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	pinned, _ := lifecycle.Builtin(lifecycle.Pinned)
	if err := s.Put(ctx, "acme", map[string]lifecycle.Policy{lifecycle.Pinned: pinned}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p := got[lifecycle.Pinned]
	switch {
	case p.TTL != nil:
		t.Errorf("a nil ttl came back as %v", *p.TTL)
	case p.CleanupAfter != nil:
		t.Errorf("a nil cleanup came back as %v — this policy would delete on archive", *p.CleanupAfter)
	case p.ArchiveAtHealth != nil:
		t.Errorf("a nil archive threshold came back as %v", *p.ArchiveAtHealth)
	case p.PromoteAtRecalls != nil:
		t.Errorf("a nil promotion threshold came back as %v", *p.PromoteAtRecalls)
	}
}

// An override is refused where it is written, not where it is used. A typo that
// surfaces as "nothing promotes" three weeks later is a typo nobody finds.
func TestAnInvalidOverrideIsRefusedAtWriteTime(t *testing.T) {
	s, kv := newStore(t)
	ctx := context.Background()

	broken, _ := lifecycle.Builtin(lifecycle.ShortTerm)
	broken.PromoteTo = "medium_term"

	err := s.Put(ctx, "acme", map[string]lifecycle.Policy{lifecycle.ShortTerm: broken})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	if _, err := kv.Get(ctx, policykv.Key("acme")); !errs.Is(err, errs.NotFound) {
		t.Fatal("the refused override was written anyway")
	}
}

func TestAnUnscopedReadOrWriteIsRefused(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Get(ctx, ""); !errs.Is(err, errs.Invalid) {
		t.Errorf("an unscoped read is %v, want Invalid (Invariant 1)", err)
	}
	if err := s.Put(ctx, "", nil); !errs.Is(err, errs.Invalid) {
		t.Errorf("an unscoped write is %v, want Invalid (Invariant 1)", err)
	}
}

// A row that will not parse is corruption, never an empty table. Falling back
// to the built-ins would apply the wrong retention to a tenant that had
// deliberately chosen different rules.
func TestAnUnreadableRowIsCorruptionNotAnEmptyTable(t *testing.T) {
	s, kv := newStore(t)
	ctx := context.Background()
	if err := kv.Set(ctx, policykv.Key("acme"), []byte("not a protobuf at all")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := s.Get(ctx, "acme"); !errs.Is(err, errs.Corruption) {
		t.Fatalf("got %v, want Corruption", err)
	}
}

func deref(p lifecycle.Policy) map[string]any {
	out := map[string]any{
		"name": p.Name, "promote_to": p.PromoteTo,
		"importance_decay": p.ImportanceDecay, "health_decay": p.HealthDecay,
		"ttl": nil, "promote_at": nil, "archive_at": nil, "cleanup_after": nil,
	}
	if p.TTL != nil {
		out["ttl"] = *p.TTL
	}
	if p.PromoteAtRecalls != nil {
		out["promote_at"] = *p.PromoteAtRecalls
	}
	if p.ArchiveAtHealth != nil {
		out["archive_at"] = *p.ArchiveAtHealth
	}
	if p.CleanupAfter != nil {
		out["cleanup_after"] = *p.CleanupAfter
	}
	return out
}
