package schema

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/record"
)

// RecordUpgrade is one step in the lazy migration chain (spec §20.2): how to
// turn a record written against one user-schema version into the next.
//
// It is a *user*-schema step, not a storage-format one. The two are different
// version scales that must never be confused (spec §19.3): a tenant moving from
// schema 14 to 15 does not touch the record envelope's version, and bumping the
// record envelope does not move any tenant.
type RecordUpgrade struct {
	From uint32
	To   uint32

	// Apply rewrites the record in place. It sees a record already decoded, so
	// it works in terms of fields rather than bytes — which is what makes a
	// lazy upgrade writable at all, and what limits it to changes an old and a
	// new record can coexist through (spec §20.2's "only when mixed-format
	// operation is safe").
	Apply func(*record.Record) error
}

// NewRecordUpgrader builds the chain behind [record.WithUpgrader].
//
// An empty chain returns a nil Upgrader, and that is the intended result rather
// than an oversight: no registered upgrades means the read path should carry no
// hook at all, not a hook that decides to do nothing on every read.
//
// The chain must be contiguous and strictly increasing. A gap is refused here,
// because a record at a version nothing can leave is a record that will be
// re-examined on every single read, forever, silently.
func NewRecordUpgrader(ups ...RecordUpgrade) (record.Upgrader, error) {
	const op = "schema.NewRecordUpgrader"

	if len(ups) == 0 {
		return nil, nil
	}

	steps := make([]RecordUpgrade, len(ups))
	copy(steps, ups)
	sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })

	byFrom := make(map[uint32]RecordUpgrade, len(steps))
	for i, u := range steps {
		switch {
		case u.Apply == nil:
			return nil, errs.E(errs.Invalid, op, fmt.Errorf("the upgrade from schema %d has no Apply", u.From))
		case u.To <= u.From:
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"an upgrade goes from schema %d to %d; a step must move a version forward", u.From, u.To))
		}
		if _, dup := byFrom[u.From]; dup {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"two upgrades leave schema %d; the chain would branch and nothing decides which way", u.From))
		}
		if i > 0 && steps[i-1].To != u.From {
			return nil, errs.E(errs.Invalid, op, fmt.Errorf(
				"the upgrade chain jumps from schema %d to %d: a record at %d could never leave it, "+
					"and would be re-examined on every read forever", steps[i-1].To, u.From, steps[i-1].To))
		}
		byFrom[u.From] = u
	}

	return &recordUpgrader{byFrom: byFrom, target: steps[len(steps)-1].To}, nil
}

type recordUpgrader struct {
	byFrom map[uint32]RecordUpgrade
	target uint32
}

// Upgrade walks the chain from the record's version to the current one.
//
// A record at a version the chain does not know is left alone rather than
// refused. That is deliberate and it is the rolling-upgrade case: a record
// written by a newer binary, at a schema this one has never heard of, is
// readable — protobuf keeps the fields this binary does not understand — and
// refusing it would take a mixed-version cluster down on the read path.
func (u *recordUpgrader) Upgrade(_ context.Context, rec *record.Record, from uint32) (uint32, bool, error) {
	const op = "schema.RecordUpgrader.Upgrade"

	if rec == nil {
		return from, false, errs.E(errs.Invalid, op, errors.New("record is nil"))
	}

	at := from
	changed := false
	for at < u.target {
		step, ok := u.byFrom[at]
		if !ok {
			break
		}
		if err := step.Apply(rec); err != nil {
			// Not best-effort. A half-applied upgrade returned as if it were
			// whole is a record the caller reads as current and is not.
			return from, false, errs.E(errs.KindOf(err), op, fmt.Errorf(
				"upgrading record %s from user schema %d to %d: %w", rec.ID, step.From, step.To, err))
		}
		at = step.To
		changed = true
	}
	return at, changed, nil
}
