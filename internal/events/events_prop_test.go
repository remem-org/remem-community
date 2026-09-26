package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"pgregory.net/rapid"
)

// An event survives the round trip through its own key and value.
//
// The seq is deliberately not compared: it is assigned by the store, not by the
// caller, and comparing it would test the generator rather than the encoding.
func TestAnEventRoundTripsThroughTheStore(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Every generator is seeded from a drawn value, never from id.New() or
		// a clock: rapid replays draws and nothing else, so a subject drawn
		// from a fresh uuid would make a failure unreproducible.
		raw := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(rt, "subject")
		subject, err := id.FromBytes(raw)
		if err != nil || subject.IsZero() {
			rt.Skip("not a usable id")
		}

		want := events.Event{
			Tenant:    acme,
			Namespace: ns,
			Subject:   subject,
			At:        time.UnixMilli(rapid.Int64Range(1, 1<<45).Draw(rt, "at")).UTC(),
			Kind:      rapid.SampledFrom(events.AllKinds()).Draw(rt, "kind"),
			Actor:     rapid.StringMatching(`[a-z_]{0,24}`).Draw(rt, "actor"),
			Reason:    rapid.StringN(0, 64, 128).Draw(rt, "reason"),
			Before:    drawDelta(rt, "before"),
			After:     drawDelta(rt, "after"),
		}

		kv := memkv.New()
		s := events.NewStore(kv)
		ctx := context.Background()

		tx := txn.New(kv)
		stored, err := s.Append(ctx, tx, want)
		if err != nil {
			tx.Close()
			rt.Fatalf("Append: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			tx.Close()
			rt.Fatalf("commit: %v", err)
		}
		tx.Close()

		got, err := s.History(ctx, kv, events.Query{
			Tenant: acme, Namespace: ns, Subject: subject, Limit: 2,
		})
		if err != nil {
			rt.Fatalf("History: %v", err)
		}
		if len(got) != 1 {
			rt.Fatalf("stored one event, read back %d", len(got))
		}
		assertSame(rt, stored, got[0])
	})
}

// Two events about one subject come back in the order they happened, whatever
// order they were written in. That is the whole reason the key carries an
// instant and a sequence rather than only an instant.
func TestHistoryIsAlwaysNewestFirst(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(rt, "subject")
		subject, err := id.FromBytes(raw)
		if err != nil || subject.IsZero() {
			rt.Skip("not a usable id")
		}

		stamps := rapid.SliceOfN(rapid.Int64Range(1, 5_000), 2, 12).Draw(rt, "stamps")
		kinds := rapid.SampledFrom(events.AllKinds())

		kv := memkv.New()
		s := events.NewStore(kv)
		ctx := context.Background()

		for i, ms := range stamps {
			tx := txn.New(kv)
			_, err := s.Append(ctx, tx, events.Event{
				Tenant: acme, Namespace: ns, Subject: subject,
				At: time.UnixMilli(ms).UTC(), Kind: kinds.Draw(rt, "kind"), Actor: "test",
			})
			if err != nil {
				tx.Close()
				rt.Fatalf("Append %d: %v", i, err)
			}
			if err := tx.Commit(ctx); err != nil {
				tx.Close()
				rt.Fatalf("commit %d: %v", i, err)
			}
			tx.Close()
		}

		got, err := s.History(ctx, kv, events.Query{
			Tenant: acme, Namespace: ns, Subject: subject, Limit: len(stamps) + 1,
		})
		if err != nil {
			rt.Fatalf("History: %v", err)
		}
		if len(got) != len(stamps) {
			rt.Fatalf("wrote %d events, read back %d — some shared a key", len(stamps), len(got))
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].At.Before(got[i].At) {
				rt.Fatalf("event %d is older than event %d: %v then %v",
					i-1, i, got[i-1].At, got[i].At)
			}
		}
	})
}

// Paging by Position visits every event exactly once, whatever the page size.
func TestPagingHistoryVisitsEveryEventOnce(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(rt, "subject")
		subject, err := id.FromBytes(raw)
		if err != nil || subject.IsZero() {
			rt.Skip("not a usable id")
		}

		n := rapid.IntRange(1, 20).Draw(rt, "events")
		page := rapid.IntRange(1, 5).Draw(rt, "page")

		kv := memkv.New()
		s := events.NewStore(kv)
		ctx := context.Background()

		// Every event at the same millisecond, which is the hard case: only
		// the sequence number separates them, so a resume that compared
		// instants alone would loop or skip.
		for i := 0; i < n; i++ {
			tx := txn.New(kv)
			if _, err := s.Append(ctx, tx, events.Event{
				Tenant: acme, Namespace: ns, Subject: subject,
				At: time.UnixMilli(7_000).UTC(), Kind: events.Recalled, Actor: "test",
			}); err != nil {
				tx.Close()
				rt.Fatalf("Append %d: %v", i, err)
			}
			if err := tx.Commit(ctx); err != nil {
				tx.Close()
				rt.Fatalf("commit %d: %v", i, err)
			}
			tx.Close()
		}

		seen := map[uint32]int{}
		var after *events.Position
		for {
			got, err := s.History(ctx, kv, events.Query{
				Tenant: acme, Namespace: ns, Subject: subject, Limit: page, Before: after,
			})
			if err != nil {
				rt.Fatalf("History: %v", err)
			}
			if len(got) == 0 {
				break
			}
			for _, e := range got {
				seen[e.Seq]++
			}
			last := got[len(got)-1]
			after = &events.Position{At: last.At, Seq: last.Seq}
		}
		if len(seen) != n {
			rt.Fatalf("paged over %d distinct events, wrote %d", len(seen), n)
		}
		for seq, times := range seen {
			if times != 1 {
				rt.Fatalf("event seq %d came back %d times", seq, times)
			}
		}
	})
}

func drawDelta(rt *rapid.T, label string) map[string]string {
	keys := rapid.SliceOfNDistinct(
		rapid.StringMatching(`[a-z_]{1,12}`), 0, 4,
		func(s string) string { return s },
	).Draw(rt, label+" fields")
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = rapid.StringN(0, 32, 64).Draw(rt, label+" value")
	}
	return out
}

func assertSame(rt *rapid.T, want, got events.Event) {
	rt.Helper()
	switch {
	case got.Tenant != want.Tenant:
		rt.Fatalf("tenant %q, want %q", got.Tenant, want.Tenant)
	case got.Namespace != tenant.Namespace(ns):
		rt.Fatalf("namespace %q", got.Namespace)
	case got.Subject != want.Subject:
		rt.Fatalf("subject %s, want %s", got.Subject, want.Subject)
	case !got.At.Equal(want.At):
		rt.Fatalf("at %v, want %v", got.At, want.At)
	case got.Kind != want.Kind:
		rt.Fatalf("kind %s, want %s", got.Kind, want.Kind)
	case got.Actor != want.Actor:
		rt.Fatalf("actor %q, want %q", got.Actor, want.Actor)
	case got.Reason != want.Reason:
		rt.Fatalf("reason %q, want %q", got.Reason, want.Reason)
	case len(got.Before) != len(want.Before) || len(got.After) != len(want.After):
		rt.Fatalf("deltas are %v/%v, want %v/%v", got.Before, got.After, want.Before, want.After)
	}
	for k, v := range want.After {
		if got.After[k] != v {
			rt.Fatalf("after[%q] = %q, want %q", k, got.After[k], v)
		}
	}
	for k, v := range want.Before {
		if got.Before[k] != v {
			rt.Fatalf("before[%q] = %q, want %q", k, got.Before[k], v)
		}
	}
}
