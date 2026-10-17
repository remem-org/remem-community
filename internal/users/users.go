// Package users holds a tenant's users and the tokens that authenticate them.
//
// # What a user is
//
// A user is who a request is, inside its tenant: an agent, a person, a service.
// It exists so that two callers of one tenant are two callers. Before it, a
// tenant-bound key's identity was its tenant name, so every agent of a tenant
// arrived as the same caller, and nothing durable recorded who did anything.
//
// A user belongs to exactly one tenant, and its id never changes and is never
// reused: audit events and, later, evidence groups refer to it by id. That is
// why a user is disabled and never deleted, and why a rename changes nothing
// but the name. Memories are not owned by users: they belong to the tenant, and
// a user is who acted on them.
//
// # Where rows live
//
// Everything is in the tenant's user space (keys.SpaceUser): user rows, token
// rows, and the name index that makes a name unique within the tenant. The one
// untenanted row is the token index at /tokens/<token id>, which names a
// token's tenant so that authentication, which runs before any tenant is
// known, can find it. See docs/architecture/tenancy.md.
package users

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// ns is the namespace user rows are written under. A user belongs to a tenant,
// not to a namespace, and namespaces are fixed at the default.
const ns = tenant.DefaultNamespace

// State is whether a user's tokens authenticate.
type State uint8

const (
	// Active users' tokens authenticate.
	Active State = iota + 1
	// Disabled users' tokens are refused. The user is kept and can be enabled.
	Disabled
)

func (s State) String() string {
	switch s {
	case Active:
		return "active"
	case Disabled:
		return "disabled"
	}
	return "unknown"
}

// ParseState reads the stable name of a state.
func ParseState(s string) (State, error) {
	switch s {
	case "active":
		return Active, nil
	case "disabled":
		return Disabled, nil
	}
	return 0, errs.E(errs.Invalid, "users.ParseState", fmt.Errorf(
		"%q is not a user state; a user is active or disabled", s))
}

// User is one user of a tenant.
type User struct {
	ID        id.ID
	Name      string
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Service reads and writes users and tokens.
type Service struct {
	kv  storage.KV
	clk clock.Clock
}

// New returns a service over kv, stamping times from clk.
func New(kv storage.KV, clk clock.Clock) *Service {
	return &Service{kv: kv, clk: clk}
}

// nameTaken is what a write body returns when the name it wants is held.
//
// It is deliberately not an errs.Conflict: txn.Do retries a Conflict on the
// reading that a conflict is a lost race, and "this name is taken" will never
// resolve itself. The body reports the fact, and the caller turns it into the
// Conflict the user is told about, outside the retry (the decision Phase 12
// made for import conflicts).
type nameTaken struct{ holder id.ID }

func (n nameTaken) Error() string { return "name taken by " + n.holder.String() }

func takenError(op, name string, err error) error {
	var taken nameTaken
	if errors.As(err, &taken) {
		return errs.E(errs.Conflict, op, fmt.Errorf(
			"the name %q is held by user %s in this tenant; names are unique, active or disabled",
			name, taken.holder))
	}
	return err
}

// Create adds a user with a new id.
func (s *Service) Create(ctx context.Context, name string) (User, error) {
	const op = "users.Create"
	t, err := tenant.Require(ctx)
	if err != nil {
		return User{}, err
	}
	folded, err := foldName(name)
	if err != nil {
		return User{}, err
	}
	// The id and the time are drawn before the transaction, so a retried body
	// writes the same user rather than a second one (Invariant 9).
	now := s.now()
	u := User{ID: id.New(), Name: name, State: Active, CreatedAt: now, UpdatedAt: now}

	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		if err := claimName(tx, t, folded, u.ID); err != nil {
			return err
		}
		ukey := keys.User(t, ns, u.ID)
		tx.Expect(ukey, nil, false)
		tx.Set(ukey, encodeUser(u))
		return nil
	})
	if err != nil {
		return User{}, takenError(op, name, err)
	}
	return u, nil
}

// Rename changes a user's name and nothing else.
func (s *Service) Rename(ctx context.Context, uid id.ID, name string) (User, error) {
	const op = "users.Rename"
	t, err := tenant.Require(ctx)
	if err != nil {
		return User{}, err
	}
	folded, err := foldName(name)
	if err != nil {
		return User{}, err
	}
	now := s.now()

	var out User
	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		u, raw, err := readUser(tx, t, uid, op)
		if err != nil {
			return err
		}
		old, err := foldName(u.Name)
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("user %s holds a name that does not validate: %w", uid, err))
		}
		if old != folded {
			if err := claimName(tx, t, folded, uid); err != nil {
				return err
			}
			oldKey := keys.UserName(t, ns, old)
			tx.Expect(oldKey, encodeName(uid), true)
			tx.Delete(oldKey)
		}
		u.Name, u.UpdatedAt = name, now
		ukey := keys.User(t, ns, uid)
		tx.Expect(ukey, raw, true)
		tx.Set(ukey, encodeUser(u))
		out = u
		return nil
	})
	if err != nil {
		return User{}, takenError(op, name, err)
	}
	return out, nil
}

// SetState disables or enables a user. Setting the state a user already has
// succeeds and writes nothing, because the post-condition holds.
func (s *Service) SetState(ctx context.Context, uid id.ID, st State) (User, error) {
	const op = "users.SetState"
	t, err := tenant.Require(ctx)
	if err != nil {
		return User{}, err
	}
	if st != Active && st != Disabled {
		return User{}, errs.E(errs.Invalid, op, fmt.Errorf("%d is not a user state", st))
	}
	now := s.now()

	var out User
	err = txn.Do(ctx, s.kv, func(tx txn.Tx) error {
		u, raw, err := readUser(tx, t, uid, op)
		if err != nil {
			return err
		}
		if u.State == st {
			out = u
			return nil
		}
		u.State, u.UpdatedAt = st, now
		ukey := keys.User(t, ns, uid)
		tx.Expect(ukey, raw, true)
		tx.Set(ukey, encodeUser(u))
		out = u
		return nil
	})
	return out, err
}

// Get reads one user.
func (s *Service) Get(ctx context.Context, uid id.ID) (User, error) {
	t, err := tenant.Require(ctx)
	if err != nil {
		return User{}, err
	}
	raw, err := s.kv.Get(ctx, keys.User(t, ns, uid))
	if err != nil {
		return User{}, notFound(err, "users.Get", uid)
	}
	return decodeUser(uid, raw, "users.Get")
}

// List returns every user of the tenant, oldest first.
func (s *Service) List(ctx context.Context) ([]User, error) {
	const op = "users.List"
	t, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi := keys.UserKindRange(t, ns, keys.UserRow)
	it := s.kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()

	var out []User
	for ok := it.First(); ok; ok = it.Next() {
		uid, err := tailID(it.Key(), op)
		if err != nil {
			return nil, err
		}
		u, err := decodeUser(uid, it.Value(), op)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	// Ids are v7 and so roughly time-ordered, but not within a millisecond, and
	// "oldest first" is a promise rather than an accident of the key layout.
	slices.SortFunc(out, func(a, b User) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID.String(), b.ID.String())
	})
	return out, nil
}

// claimName stages the name-index row for folded, or reports who holds it.
//
// The read is inside the body and its absence is expected at commit, so two
// writers claiming one name cannot both win: the loser conflicts, re-runs, and
// is told the truth on the second pass.
func claimName(tx txn.Tx, t tenant.ID, folded string, uid id.ID) error {
	key := keys.UserName(t, ns, folded)
	raw, err := tx.Get(key)
	switch {
	case err == nil:
		holder, derr := decodeName(raw, "users.claimName")
		if derr != nil {
			return derr
		}
		if holder != uid {
			return nameTaken{holder: holder}
		}
		return nil
	case errs.Is(err, errs.NotFound):
		tx.Expect(key, nil, false)
		tx.Set(key, encodeName(uid))
		return nil
	default:
		return err
	}
}

// readUser reads a user through the transaction, returning the bytes it read so
// the write can be conditioned on them.
func readUser(tx txn.Tx, t tenant.ID, uid id.ID, op string) (User, []byte, error) {
	raw, err := tx.Get(keys.User(t, ns, uid))
	if err != nil {
		return User{}, nil, notFound(err, op, uid)
	}
	u, err := decodeUser(uid, raw, op)
	return u, raw, err
}

func notFound(err error, op string, uid id.ID) error {
	if errs.Is(err, errs.NotFound) {
		return errs.E(errs.NotFound, op, fmt.Errorf("no user %s in this tenant", uid))
	}
	return err
}

// now is the time a write stamps, at the millisecond precision rows store, so
// that what a write returns is what a later read of the same row returns.
func (s *Service) now() time.Time { return s.clk.Now().UTC().Truncate(time.Millisecond) }
