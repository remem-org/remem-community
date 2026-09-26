package lifecycle

import (
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/record"
)

// archive retires a memory whose health has run out.
//
// It archives. It never deletes, under any configuration — plan §II.10 row 8
// turns Rust's opt-in `hard_delete_on_forgetting` into a rule, because a
// heuristic that destroys user data has no honest failure mode: the memory is
// gone, the reason was a number nobody saw, and there is nothing to restore.
// Rust ships the switch and defaults it off (lifecycle_manager.rs:428-431); Go
// does not ship it, and TestNoConfigurationMakesAHeuristicHardDelete holds
// every built-in policy at every health to that.
//
// The comparison is `<=` rather than `<`, matching Rust, so a policy archiving
// at health 0 archives a memory that has reached exactly 0.
func archive(pol Policy, rec *record.Record, now time.Time, out *Result) {
	if pol.ArchiveAtHealth == nil || rec.Fields.Archived {
		return
	}
	if rec.Fields.Health > *pol.ArchiveAtHealth {
		return
	}

	before := f32s(rec.Fields.Health)
	retire(rec, now)
	out.note(Note{
		Kind:  events.Archived,
		Actor: "active_forgetting",
		Reason: fmt.Sprintf("health reached %s, at or below the %s this policy archives at",
			before, f32s(*pol.ArchiveAtHealth)),
		Before: map[string]string{"archived": "false", "health": before},
		After:  map[string]string{"archived": "true", "health": before},
	})
}

// cleanup removes a record that has been archived longer than its retention.
//
// # This is the one destructive path, and it is not a heuristic
//
// The distinction the completion criterion turns on: a heuristic decided to
// *archive*, days or weeks ago, and that decision was recoverable the whole
// time — the record survived, it stayed fetchable by id, and it stayed in every
// export. What deletes it is the elapsing of a stated retention over a record
// that is already retired, which is a retention policy and not a judgement
// about the memory's worth. `pinned` never reaches it at all, because its
// CleanupAfter is nil.
//
// # What goes with it
//
// The caller removes the record, its canonical vector, its edges and its whole
// event stream in one transaction, and then writes the `hard_deleted` event —
// in that order, so the row saying what happened survives the deletion of
// everything else about the memory. That single row is the answer to "why did
// my memory disappear", and it is why the stream is trimmed rather than kept.
func cleanup(pol Policy, rec *record.Record, now time.Time, out *Result) {
	if pol.CleanupAfter == nil {
		return
	}
	archivedFor := now.Sub(archivedAt(rec))
	if archivedFor < *pol.CleanupAfter {
		return
	}

	out.Delete = true
	out.note(Note{
		Kind:  events.HardDeleted,
		Actor: "cleanup",
		// Rounded to the second, not the hour. The default retention is thirty
		// days and an hour is invisible against it — but a tenant may set a
		// retention of seconds, and "archived for 0s, past this policy's 5s
		// retention" reads as a memory deleted the instant it was retired. A
		// reason that looks like a bug is worse than no reason at all.
		Reason: fmt.Sprintf("archived for %s, past this policy's %s retention",
			archivedFor.Round(time.Second), *pol.CleanupAfter),
		Before: map[string]string{"archived": "true"},
		After:  map[string]string{"exists": "false"},
	})
}
