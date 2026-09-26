package record_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// countingScheduler answers a fixed time and remembers it was asked.
type countingScheduler struct {
	at    time.Time
	calls int
}

func (s *countingScheduler) NextAttention(context.Context, tenant.ID, *record.Record) time.Time {
	s.calls++
	return s.at
}

// The repository is where the schedule is written, so that no write path can
// produce a record the lifecycle sweep will never visit.
//
// The failure this prevents is silent and permanent: a memory with no schedule
// that something read as "never" would decay, expire and archive never, and
// nothing would report it — it would simply be a memory that outlived its
// policy.
func TestPutComputesTheScheduleOnEveryWrite(t *testing.T) {
	ctx := tenant.NewContext(context.Background(), "acme")
	kv := memkv.New()
	want := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	sched := &countingScheduler{at: want}
	repo := record.NewRepo(kv, record.WithScheduler(sched))

	rec := &record.Record{
		ID:        id.New(),
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   "a memory",
		Fields:    record.Fields{}.WithDefaults(),
		CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}

	tx := txn.New(kv)
	if err := repo.Put(ctx, tx, rec); err != nil {
		tx.Close()
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		tx.Close()
		t.Fatalf("commit: %v", err)
	}
	tx.Close()

	if sched.calls != 1 {
		t.Fatalf("the scheduler was asked %d times, want once per write", sched.calls)
	}
	// The caller's record reflects what was stored, the way it already does for
	// the concurrency token.
	if !rec.Fields.NextAttentionAt.Equal(want) {
		t.Errorf("the caller's record says %v, want %v", rec.Fields.NextAttentionAt, want)
	}

	got, err := repo.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Fields.NextAttentionAt.Equal(want) {
		t.Errorf("the stored record says %v, want %v", got.Fields.NextAttentionAt, want)
	}
}

// A caller's stale value is overwritten rather than trusted. A record read a
// week ago and written back must not reinstate the schedule it had then.
func TestPutOverwritesAStaleScheduleRatherThanTrustingIt(t *testing.T) {
	ctx := tenant.NewContext(context.Background(), "acme")
	kv := memkv.New()
	want := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	repo := record.NewRepo(kv, record.WithScheduler(&countingScheduler{at: want}))

	rec := &record.Record{
		ID:        id.New(),
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   "a memory",
		Fields: record.Fields{
			Policy:          "long_term",
			NextAttentionAt: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}

	tx := txn.New(kv)
	if err := repo.Put(ctx, tx, rec); err != nil {
		tx.Close()
		t.Fatalf("Put: %v", err)
	}
	_ = tx.Commit(ctx)
	tx.Close()

	if !rec.Fields.NextAttentionAt.Equal(want) {
		t.Fatalf("the stale schedule survived: %v", rec.Fields.NextAttentionAt)
	}
}

// The four lifecycle fields Phase 10 adds survive a round trip. Each is a
// durable field number, and one that encoded and did not decode would look
// exactly like a memory that had never decayed.
func TestTheLifecycleFieldsRoundTrip(t *testing.T) {
	ctx := tenant.NewContext(context.Background(), "acme")
	kv := memkv.New()
	repo := record.NewRepo(kv)

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rec := &record.Record{
		ID:        id.New(),
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   "a memory",
		Fields: record.Fields{
			Policy:            "long_term",
			Importance:        0.5,
			Health:            90,
			TTL:               90 * time.Minute,
			ProtectedUntil:    base.Add(30 * 24 * time.Hour),
			LastDecayAt:       base.Add(time.Hour),
			LastHealthCheckAt: base.Add(2 * time.Hour),
		},
		CreatedAt: base,
		UpdatedAt: base,
	}

	tx := txn.New(kv)
	if err := repo.Put(ctx, tx, rec); err != nil {
		tx.Close()
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		tx.Close()
		t.Fatalf("commit: %v", err)
	}
	tx.Close()

	got, err := repo.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	switch {
	case got.Fields.TTL != 90*time.Minute:
		t.Errorf("ttl = %v, want 90m", got.Fields.TTL)
	case !got.Fields.ProtectedUntil.Equal(rec.Fields.ProtectedUntil):
		t.Errorf("protected until = %v, want %v", got.Fields.ProtectedUntil, rec.Fields.ProtectedUntil)
	case !got.Fields.LastDecayAt.Equal(rec.Fields.LastDecayAt):
		t.Errorf("last decay = %v, want %v", got.Fields.LastDecayAt, rec.Fields.LastDecayAt)
	case !got.Fields.LastHealthCheckAt.Equal(rec.Fields.LastHealthCheckAt):
		t.Errorf("last health check = %v, want %v", got.Fields.LastHealthCheckAt, rec.Fields.LastHealthCheckAt)
	}
}

// A sub-second TTL rounds up rather than to zero. Zero means "never expires",
// so rounding down would turn "expire in 300ms" into "expire never" — the
// wrong direction, and invisible.
func TestASubSecondTTLRoundsUpNotAway(t *testing.T) {
	ctx := tenant.NewContext(context.Background(), "acme")
	kv := memkv.New()
	repo := record.NewRepo(kv)

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "a memory",
		Fields:    record.Fields{Policy: "short_term", TTL: 300 * time.Millisecond},
		CreatedAt: base, UpdatedAt: base,
	}
	tx := txn.New(kv)
	if err := repo.Put(ctx, tx, rec); err != nil {
		tx.Close()
		t.Fatalf("Put: %v", err)
	}
	_ = tx.Commit(ctx)
	tx.Close()

	got, err := repo.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Fields.TTL != time.Second {
		t.Fatalf("a 300ms ttl stored as %v; zero would mean never expires", got.Fields.TTL)
	}
}
