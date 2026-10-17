package users

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// RestoreMode is what Restore does with a user that exists and differs from the
// snapshot's. It mirrors the snapshot import's conflict modes.
type RestoreMode uint8

const (
	// RestoreFail refuses, naming the field that differs.
	RestoreFail RestoreMode = iota + 1
	// RestoreSkip keeps what is there.
	RestoreSkip
	// RestoreOverwrite replaces what is there with the snapshot's user.
	RestoreOverwrite
)

func (m RestoreMode) String() string {
	switch m {
	case RestoreFail:
		return "fail"
	case RestoreSkip:
		return "skip"
	case RestoreOverwrite:
		return "overwrite"
	}
	return "unknown"
}

// RestoreOutcome is what Restore did.
type RestoreOutcome uint8

const (
	// Restored wrote the snapshot's user.
	Restored RestoreOutcome = iota + 1
	// Unchanged found the user already exactly as the snapshot has it.
	Unchanged
	// Skipped found a differing user, or the name held by another, and kept
	// what was there.
	Skipped
)

func (o RestoreOutcome) String() string {
	switch o {
	case Restored:
		return "restored"
	case Unchanged:
		return "unchanged"
	case Skipped:
		return "skipped"
	}
	return "unknown"
}

// restoreConflict is what a restore body returns when the stored state
// disagrees with the snapshot. Like nameTaken, it is a plain value rather than
// an errs.Conflict, so that txn.Do does not retry a disagreement about data as
// if it were a lost race; Restore decides what it means outside the retry.
type restoreConflict struct {
	field  string // the first field that differs, for a user that exists
	holder id.ID  // set when the name is held by a different user
}

func (c restoreConflict) Error() string { return "restore conflict" }

// Restore writes a user exactly as a snapshot carries it: its id, name, state
// and times, which a create would assign rather than take. It rebuilds the
// user's name row, which a snapshot does not carry because it is derived.
//
// A name held by a different user is never resolved by renaming or removing
// that user, whatever mode says: a user is never deleted and a name is unique.
// Skip passes over it, and fail and overwrite refuse, naming the holder.
func (s *Service) Restore(ctx context.Context, u User, mode RestoreMode) (RestoreOutcome, error) {
	const op = "users.Restore"
	t, err := tenant.Require(ctx)
	if err != nil {
		return 0, err
	}
	if u.ID.IsZero() {
		return 0, errs.E(errs.Invalid, op, errors.New("the user has no id"))
	}
	if u.State != Active && u.State != Disabled {
		return 0, errs.E(errs.Invalid, op, fmt.Errorf("user %s has no valid state", u.ID))
	}
	folded, err := foldName(u.Name)
	if err != nil {
		return 0, err
	}
	u.CreatedAt, u.UpdatedAt = u.CreatedAt.UTC(), u.UpdatedAt.UTC()

	var outcome RestoreOutcome
	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		ukey := keys.User(t, ns, u.ID)
		raw, err := tx.Get(ukey)
		switch {
		case errs.Is(err, errs.NotFound):
			if err := claimName(tx, t, folded, u.ID); err != nil {
				var taken nameTaken
				if errors.As(err, &taken) {
					return restoreConflict{holder: taken.holder}
				}
				return err
			}
			tx.Expect(ukey, nil, false)
			tx.Set(ukey, encodeUser(u))
			outcome = Restored
			return nil
		case err != nil:
			return err
		}

		stored, err := decodeUser(u.ID, raw, op)
		if err != nil {
			return err
		}
		field := firstDifference(stored, u)
		if field == "" {
			outcome = Unchanged
			return nil
		}
		if mode != RestoreOverwrite {
			return restoreConflict{field: field}
		}
		old, err := foldName(stored.Name)
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("user %s holds a name that does not validate: %w", u.ID, err))
		}
		if old != folded {
			if err := claimName(tx, t, folded, u.ID); err != nil {
				var taken nameTaken
				if errors.As(err, &taken) {
					return restoreConflict{holder: taken.holder}
				}
				return err
			}
			oldKey := keys.UserName(t, ns, old)
			tx.Expect(oldKey, encodeName(u.ID), true)
			tx.Delete(oldKey)
		}
		tx.Expect(ukey, raw, true)
		tx.Set(ukey, encodeUser(u))
		outcome = Restored
		return nil
	})

	var c restoreConflict
	if !errors.As(err, &c) {
		return outcome, err
	}
	if mode == RestoreSkip {
		return Skipped, nil
	}
	if !c.holder.IsZero() {
		return 0, errs.E(errs.Conflict, op, fmt.Errorf(
			"user %s cannot be restored as %q in tenant %s: user %s holds that name, and a name is "+
				"never taken from the user holding it", u.ID, u.Name, t, c.holder))
	}
	return 0, errs.E(errs.Conflict, op, fmt.Errorf(
		"user %s already exists in tenant %s and its %s differs from the snapshot's", u.ID, t, c.field))
}

// firstDifference names the first field in which two users disagree, compared
// at the millisecond precision rows store.
func firstDifference(a, b User) string {
	switch {
	case a.Name != b.Name:
		return "name"
	case a.State != b.State:
		return "state"
	case a.CreatedAt.UnixMilli() != b.CreatedAt.UnixMilli():
		return "created_at"
	case a.UpdatedAt.UnixMilli() != b.UpdatedAt.UnixMilli():
		return "updated_at"
	}
	return ""
}
