package events_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const (
	acme  = tenant.ID("acme")
	other = tenant.ID("globex")
	ns    = tenant.DefaultNamespace
)

func store(t *testing.T) (*events.Store, *memkv.Store) {
	t.Helper()
	kv := memkv.New()
	return events.NewStore(kv), kv
}

// append writes one event through its own transaction, which is how every
// caller outside a lifecycle pass uses the store.
func appendOne(t *testing.T, s *events.Store, kv *memkv.Store, e events.Event) events.Event {
	t.Helper()
	ctx := context.Background()
	tx := txn.New(kv)
	defer tx.Close()
	out, err := s.Append(ctx, tx, e)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return out
}

func at(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func TestHistoryIsOrderedAndTenantScoped(t *testing.T) {
	s, kv := store(t)
	subject := id.New()

	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(1_000), Kind: events.Recalled, Actor: "api", Reason: "fetched by id"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(3_000), Kind: events.Archived, Actor: "active_forgetting", Reason: "health reached 0"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(2_000), Kind: events.Promoted, Actor: "ttl", Reason: "3 recalls"})

	// Another tenant's stream, at the same subject id, must be invisible.
	appendOne(t, s, kv, events.Event{Tenant: other, Namespace: ns, Subject: subject,
		At: at(2_500), Kind: events.Recalled, Actor: "api", Reason: "not acme's"})

	got, err := s.History(context.Background(), kv, events.Query{
		Tenant: acme, Namespace: ns, Subject: subject, Limit: 10,
	})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	want := []events.Kind{events.Archived, events.Promoted, events.Recalled}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(want), got)
	}
	for i, k := range want {
		if got[i].Kind != k {
			t.Errorf("event %d is %s, want %s — history is newest-first", i, got[i].Kind, k)
		}
		if got[i].Tenant != acme {
			t.Errorf("event %d belongs to %s", i, got[i].Tenant)
		}
	}
}

func TestHistoryRefusesAnUnscopedRead(t *testing.T) {
	s, kv := store(t)
	_, err := s.History(context.Background(), kv, events.Query{
		Namespace: ns, Subject: id.New(), Limit: 10,
	})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an unscoped history read is %v, want Invalid (Invariant 1)", err)
	}
}

// Two events in one millisecond keep the order they happened in. The sequence
// number is the only thing that can hold them apart, and without it the second
// silently overwrites the first.
func TestTwoEventsInOneMillisecondBothSurvive(t *testing.T) {
	s, kv := store(t)
	subject := id.New()

	first := appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(5_000), Kind: events.Recalled, Actor: "api"})
	second := appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(5_000), Kind: events.Archived, Actor: "cleanup"})

	if first.Seq == second.Seq {
		t.Fatalf("both events took seq %d", first.Seq)
	}
	got, err := s.History(context.Background(), kv, events.Query{
		Tenant: acme, Namespace: ns, Subject: subject, Limit: 10,
	})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 — one overwrote the other", len(got))
	}
	if got[0].Kind != events.Archived || got[1].Kind != events.Recalled {
		t.Errorf("order is %s then %s, want archived then recalled", got[0].Kind, got[1].Kind)
	}
}

// Two events staged into one transaction collide the same way, and the store
// cannot see them through an iterator — so the probe has to go through the
// transaction itself.
func TestTwoEventsInOneTransactionBothSurvive(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	subject := id.New()

	tx := txn.New(kv)
	defer tx.Close()
	for _, k := range []events.Kind{events.Recalled, events.Archived, events.HardDeleted} {
		if _, err := s.Append(ctx, tx, events.Event{Tenant: acme, Namespace: ns,
			Subject: subject, At: at(9_000), Kind: k, Actor: "test"}); err != nil {
			t.Fatalf("Append %s: %v", k, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.History(ctx, kv, events.Query{Tenant: acme, Namespace: ns, Subject: subject, Limit: 10})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 — staged events overwrote each other", len(got))
	}
}

func TestAppendRefusesAnUnknownKind(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	tx := txn.New(kv)
	defer tx.Close()

	_, err := s.Append(ctx, tx, events.Event{Tenant: acme, Namespace: ns,
		Subject: id.New(), At: at(1), Kind: events.Kind("consolidated"), Actor: "test"})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an unknown kind is %v, want Invalid", err)
	}
	if err == nil || !strings.Contains(err.Error(), "consolidated") {
		t.Errorf("the refusal does not name the kind: %v", err)
	}
}

func TestTrimRemovesOnlyWhatIsPastTheCutoff(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	subject := id.New()

	for _, ms := range []int64{1_000, 2_000, 3_000, 4_000} {
		appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
			At: at(ms), Kind: events.Recalled, Actor: "api"})
	}

	tx := txn.New(kv)
	removed, err := s.Trim(ctx, tx, events.Scope{Tenant: acme, Namespace: ns, Subject: subject}, at(3_000), 100)
	if err != nil {
		tx.Close()
		t.Fatalf("Trim: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		tx.Close()
		t.Fatalf("commit: %v", err)
	}
	tx.Close()

	if removed != 2 {
		t.Fatalf("trimmed %d events, want 2 (strictly before the cutoff)", removed)
	}
	got, err := s.History(ctx, kv, events.Query{Tenant: acme, Namespace: ns, Subject: subject, Limit: 10})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(got) != 2 || got[1].At != at(3_000) {
		t.Fatalf("after trimming, history is %+v", got)
	}
}

// A hard delete removes the subject's whole stream and then writes the one row
// that says why. Ordering matters: the deletion staged first, the event after.
func TestDeleteSubjectThenAppendLeavesExactlyTheDeletionEvent(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	subject := id.New()

	for _, ms := range []int64{1_000, 2_000, 3_000} {
		appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
			At: at(ms), Kind: events.Recalled, Actor: "api"})
	}

	tx := txn.New(kv)
	defer tx.Close()
	scope := events.Scope{Tenant: acme, Namespace: ns, Subject: subject}
	if _, err := s.DeleteSubject(ctx, tx, scope); err != nil {
		t.Fatalf("DeleteSubject: %v", err)
	}
	if _, err := s.Append(ctx, tx, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(4_000), Kind: events.HardDeleted, Actor: "cleanup",
		Reason: "archived longer than the retention"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.History(ctx, kv, events.Query{Tenant: acme, Namespace: ns, Subject: subject, Limit: 10})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(got) != 1 || got[0].Kind != events.HardDeleted {
		t.Fatalf("history after a hard delete is %+v, want exactly the deletion event", got)
	}
}

func TestNewestRecallIsFoundWithoutReadingTheStream(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	subject := id.New()

	if _, ok, err := s.NewestRecall(ctx, kv, events.Scope{Tenant: acme, Namespace: ns, Subject: subject}); err != nil || ok {
		t.Fatalf("NewestRecall on an empty stream = (%v, %v), want (false, nil)", ok, err)
	}

	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(1_000), Kind: events.Recalled, Actor: "api"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(2_000), Kind: events.Archived, Actor: "cleanup"})

	// The newest event is the archive; the newest *recall* is the older row,
	// which is the whole point of asking for one kind.
	got, ok, err := s.NewestRecall(ctx, kv, events.Scope{Tenant: acme, Namespace: ns, Subject: subject})
	if err != nil || !ok {
		t.Fatalf("NewestRecall = (%v, %v)", ok, err)
	}
	if !got.At.Equal(at(1_000)) {
		t.Errorf("newest recall is at %v, want %v", got.At, at(1_000))
	}
}

// The fold reads recalls strictly after the record's watermark, and nothing
// else. Counting an archive as a recall would inflate access_count.
func TestRecallsSinceCountsOnlyRecallsAndOnlyNewerOnes(t *testing.T) {
	s, kv := store(t)
	ctx := context.Background()
	subject := id.New()

	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(1_000), Kind: events.Recalled, Actor: "api"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(2_000), Kind: events.Archived, Actor: "cleanup"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(3_000), Kind: events.Recalled, Actor: "api"})
	appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: subject,
		At: at(4_000), Kind: events.Recalled, Actor: "api"})

	scope := events.Scope{Tenant: acme, Namespace: ns, Subject: subject}
	n, newest, err := s.RecallsSince(ctx, kv, scope, at(1_000))
	if err != nil {
		t.Fatalf("RecallsSince: %v", err)
	}
	if n != 2 {
		t.Errorf("counted %d recalls after 1000ms, want 2", n)
	}
	if !newest.Equal(at(4_000)) {
		t.Errorf("newest recall is %v, want %v", newest, at(4_000))
	}

	// A watermark at the very beginning folds everything.
	n, _, err = s.RecallsSince(ctx, kv, scope, time.Time{})
	if err != nil {
		t.Fatalf("RecallsSince from zero: %v", err)
	}
	if n != 3 {
		t.Errorf("counted %d recalls from the start, want 3", n)
	}
}

func TestNoEventCarriesMemoryContent(t *testing.T) {
	// Every field an event can carry is written by this package's callers, and
	// the guard is that the encoder refuses a value long enough to be prose.
	// It is a blunt rule on purpose: a precise one would need to know what
	// content looks like, and the point is that content never gets close.
	s, kv := store(t)
	ctx := context.Background()
	tx := txn.New(kv)
	defer tx.Close()

	long := strings.Repeat("a plausible sentence of remembered prose. ", 20)
	_, err := s.Append(ctx, tx, events.Event{Tenant: acme, Namespace: ns, Subject: id.New(),
		At: at(1), Kind: events.Archived, Actor: "cleanup",
		After: map[string]string{"content": long}})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an over-long event value is %v, want Invalid", err)
	}
	if err == nil || !strings.Contains(err.Error(), "content") {
		t.Errorf("the refusal does not name the offending field: %v", err)
	}
}

func TestTheStoreTakesItsTimeFromTheCaller(t *testing.T) {
	// Invariant 8: nothing here reads a wall clock. The event's timestamp is
	// the caller's, so a sweep that stamped one instant stamps every row of
	// that pass with it.
	s, kv := store(t)
	frozen := clock.NewFake(at(1_700_000_000_000))
	e := appendOne(t, s, kv, events.Event{Tenant: acme, Namespace: ns, Subject: id.New(),
		At: frozen.Now(), Kind: events.Recalled, Actor: "api"})
	if !e.At.Equal(frozen.Now()) {
		t.Errorf("the stored event is at %v, want the caller's %v", e.At, frozen.Now())
	}
}

// A kind that is declared but missing from AllKinds is invisible to the
// table-driven transition test and to anything that enumerates the stream, so
// the two lists have to agree.
func TestUpdatedIsADeclaredKind(t *testing.T) {
	if !events.Updated.Valid() {
		t.Fatal("events.Updated is not a kind this binary writes")
	}
	var found bool
	for _, k := range events.AllKinds() {
		if k == events.Updated {
			found = true
		}
	}
	if !found {
		t.Fatal("events.Updated is valid but missing from AllKinds()")
	}
}
