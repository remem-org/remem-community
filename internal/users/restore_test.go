package users_test

import (
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/users"
)

func snapshotUser(name string, st users.State) users.User {
	at := time.UnixMilli(1_700_000_000_000).UTC()
	return users.User{ID: id.New(), Name: name, State: st, CreatedAt: at, UpdatedAt: at.Add(time.Minute)}
}

// A restored user is the snapshot's user exactly, id and times included, and
// its name is held: the name row is rebuilt from the user row.
func TestARestoredUserIsTheSnapshotsUser(t *testing.T) {
	s, _ := newService(t)
	u := snapshotUser("Planner", users.Disabled)
	got, err := s.Restore(in("acme"), u, users.RestoreFail)
	if err != nil || got != users.Restored {
		t.Fatalf("restore: %v, %v", got, err)
	}
	back, err := s.Get(in("acme"), u.ID)
	if err != nil || back != u {
		t.Fatalf("read back %+v, want %+v (%v)", back, u, err)
	}
	if _, err := s.Create(in("acme"), "planner"); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the restored name is not held: %v", err)
	}
}

// A second import of the same file changes nothing and is not a conflict.
func TestRestoringTheSameUserAgainIsUnchanged(t *testing.T) {
	s, _ := newService(t)
	u := snapshotUser("planner", users.Active)
	_, _ = s.Restore(in("acme"), u, users.RestoreFail)
	if got, err := s.Restore(in("acme"), u, users.RestoreFail); err != nil || got != users.Unchanged {
		t.Fatalf("a second restore: %v, %v", got, err)
	}
}

func TestADifferingUserFollowsTheConflictMode(t *testing.T) {
	s, _ := newService(t)
	stored := snapshotUser("planner", users.Active)
	_, _ = s.Restore(in("acme"), stored, users.RestoreFail)
	incoming := stored
	incoming.Name = "researcher"

	if _, err := s.Restore(in("acme"), incoming, users.RestoreFail); !errs.Is(err, errs.Conflict) || !strings.Contains(err.Error(), "name") {
		t.Fatalf("fail mode: %v, want Conflict naming the field", err)
	}
	if got, err := s.Restore(in("acme"), incoming, users.RestoreSkip); err != nil || got != users.Skipped {
		t.Fatalf("skip mode: %v, %v", got, err)
	}
	if back, _ := s.Get(in("acme"), stored.ID); back.Name != "planner" {
		t.Fatalf("skip changed the user to %+v", back)
	}
	if got, err := s.Restore(in("acme"), incoming, users.RestoreOverwrite); err != nil || got != users.Restored {
		t.Fatalf("overwrite mode: %v, %v", got, err)
	}
	if back, _ := s.Get(in("acme"), stored.ID); back.Name != "researcher" {
		t.Fatalf("overwrite left %+v", back)
	}
	if _, err := s.Create(in("acme"), "planner"); err != nil {
		t.Fatalf("the overwritten name was not released: %v", err)
	}
}

// A different user already holding the name is never resolved by renaming or
// removing it, whatever the mode says: a user is never deleted, and a name is
// unique. Skip passes over it; fail and overwrite refuse, naming the holder.
func TestANameHeldByAnotherUserIsNeverTakenFromThem(t *testing.T) {
	s, _ := newService(t)
	holder, _ := s.Create(in("acme"), "planner")
	incoming := snapshotUser("Planner", users.Active)

	for _, mode := range []users.RestoreMode{users.RestoreFail, users.RestoreOverwrite} {
		_, err := s.Restore(in("acme"), incoming, mode)
		if !errs.Is(err, errs.Conflict) || !strings.Contains(err.Error(), holder.ID.String()) {
			t.Fatalf("mode %v: %v, want Conflict naming %s", mode, err, holder.ID)
		}
	}
	if got, err := s.Restore(in("acme"), incoming, users.RestoreSkip); err != nil || got != users.Skipped {
		t.Fatalf("skip: %v, %v", got, err)
	}
	if _, err := s.Get(in("acme"), incoming.ID); !errs.Is(err, errs.NotFound) {
		t.Fatalf("the incoming user was written: %v", err)
	}
}

func TestAMalformedRestoredUserIsRefused(t *testing.T) {
	s, _ := newService(t)
	bad := snapshotUser("plan​ner", users.Active)
	if _, err := s.Restore(in("acme"), bad, users.RestoreFail); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a name with an invisible character: %v", err)
	}
	noState := snapshotUser("planner", 0)
	if _, err := s.Restore(in("acme"), noState, users.RestoreFail); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a user with no state: %v", err)
	}
}
