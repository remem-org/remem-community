package lifecycle

import (
	"fmt"
	"strconv"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/record"
)

// expire retires or promotes a memory whose time is up, and reports whether it
// did either.
//
// # Two outcomes, two events, and no sequence
//
// A memory that ran out of time and had been used is promoted; one that had not
// is archived. Rust makes the same choice at lifecycle_manager.rs:207-214, with
// the threshold at three recalls and the comparison inclusive.
//
// The archive is recorded as `expired` rather than as `expired` followed by
// `archived`. Two rows for one transition would double the stream for no
// information: what an operator needs is why the memory was retired, and
// "expired" and "archived" are two *causes* of retirement rather than two steps
// of one. Both carry `archived: true` in their After, so a reader asking "when
// was this retired" reads one field and not two kinds.
//
// # Promotion clears the TTL
//
// Rust sets `ttl = None` on promotion (lifecycle_manager.rs:160). Leaving it
// would make the memory expire again the moment it was next visited, under a
// policy that has no promotion left to give — so a memory promoted for being
// useful would be archived for having been useful.
func expire(pol Policy, rec *record.Record, now time.Time, out *Result) bool {
	ttl := effectiveTTL(pol, rec)
	if ttl <= 0 || now.Before(rec.CreatedAt.Add(ttl)) {
		return false
	}

	if pol.PromoteAtRecalls != nil && rec.Fields.AccessCount >= *pol.PromoteAtRecalls {
		before := rec.Fields.Policy
		rec.Fields.Policy = pol.PromoteTo
		rec.Fields.TTL = 0
		out.note(Note{
			Kind:  events.Promoted,
			Actor: "ttl",
			Reason: fmt.Sprintf("ttl elapsed with %d recalls, at or above the %d this policy promotes at",
				rec.Fields.AccessCount, *pol.PromoteAtRecalls),
			Before: map[string]string{"policy": before},
			After:  map[string]string{"policy": rec.Fields.Policy},
		})
		return true
	}

	retire(rec, now)
	out.note(Note{
		Kind:  events.Expired,
		Actor: "ttl",
		Reason: fmt.Sprintf("ttl of %s elapsed with %s",
			ttl, recallPhrase(rec.Fields.AccessCount, pol.PromoteAtRecalls)),
		Before: map[string]string{"archived": "false"},
		After:  map[string]string{"archived": "true"},
	})
	return true
}

// retire marks a record archived without touching updated_at.
//
// Rust moves updated_at here because cleanup_archived selects on it. Go
// schedules cleanup from archived_at, so the lifecycle can leave updated_at
// alone — which keeps it meaning "recently edited" for the listing that orders
// by it, rather than "recently swept".
func retire(rec *record.Record, now time.Time) {
	rec.Fields.Archived = true
	rec.Fields.ArchivedAt = now
}

// recallPhrase says why a memory was not promoted, in words rather than in
// numbers a reader has to interpret.
func recallPhrase(count uint32, threshold *uint32) string {
	if threshold == nil {
		return "no promotion available under this policy"
	}
	if count == 0 {
		return "no recalls"
	}
	return strconv.FormatUint(uint64(count), 10) + " recalls, below the " +
		strconv.FormatUint(uint64(*threshold), 10) + " this policy promotes at"
}
