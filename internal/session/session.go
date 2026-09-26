// Package session is the MCP session registry.
//
// A session exists because MCP is stateful: a client initialises once, is given
// an id, and presents it on every later call. Remem stores sessions rather than
// holding them in memory, and the reason is operational rather than
// architectural — an in-memory registry strands every connected agent on a
// rolling restart, and a row per session is a rounding error against the
// memories beside it.
//
// # Why absent and unknown are different answers
//
// A request with no session id has never initialised; a request with an id the
// server does not recognise had one that expired or was served by a node that
// has since forgotten it. The first is a client bug and the second is normal
// operation, and conflating them strands every client that idled past the
// sweep — it retries with the same id forever, because nothing told it to
// re-initialise. internal/api/mcp turns the two into 400 and 404.
package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/session/pb"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"google.golang.org/protobuf/proto"
)

// DefaultTTL is how long a session survives without use.
//
// An hour is long enough that an agent pausing between turns keeps its session,
// and short enough that an abandoned one does not accumulate. Expiry is what
// makes the 404 path real, so the value matters more for correctness than for
// storage.
const DefaultTTL = time.Hour

// Session is one MCP session.
type Session struct {
	ID     id.ID
	Tenant tenant.ID

	// Principal names the credential that opened the session, for audit. It is
	// never the secret.
	Principal string

	ClientName      string
	ClientVersion   string
	ProtocolVersion string

	CreatedAt  time.Time
	LastSeenAt time.Time
}

// Registry stores sessions.
type Registry struct {
	kv  storage.KV
	clk clock.Clock
	ttl time.Duration
}

// New returns a registry over kv. A zero ttl takes [DefaultTTL].
func New(kv storage.KV, clk clock.Clock, ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Registry{kv: kv, clk: clk, ttl: ttl}
}

// TTL is how long a session survives without use.
func (r *Registry) TTL() time.Duration { return r.ttl }

// Key is the storage key of a session row.
func Key(t tenant.ID, sid id.ID) []byte {
	return keys.Session(t, tenant.DefaultNamespace, sid)
}

// Create opens a session for the tenant in ctx.
func (r *Registry) Create(ctx context.Context, s Session) (Session, error) {
	const op = "session.Create"

	t, err := tenant.Require(ctx)
	if err != nil {
		return Session{}, err
	}
	now := r.clk.Now().UTC()

	s.ID = id.New()
	s.Tenant = t
	s.CreatedAt = now
	s.LastSeenAt = now

	value, err := marshal(s, op)
	if err != nil {
		return Session{}, err
	}
	if err := r.kv.Set(ctx, Key(t, s.ID), value); err != nil {
		return Session{}, err
	}
	return s, nil
}

// Get returns a session, or errs.NotFound.
//
// An expired session is reported as absent and is not deleted here: a read is
// not the place to take a write, and the sweep is what reclaims it. What
// matters to the caller is that it is gone, and that is what it is told.
func (r *Registry) Get(ctx context.Context, sid id.ID) (Session, error) {
	const op = "session.Get"

	t, err := tenant.Require(ctx)
	if err != nil {
		return Session{}, err
	}
	b, err := r.kv.Get(ctx, Key(t, sid))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return Session{}, errs.E(errs.NotFound, op, fmt.Errorf("no session %s", sid))
		}
		return Session{}, err
	}
	s, err := unmarshal(t, sid, b, op)
	if err != nil {
		return Session{}, err
	}
	if r.expired(s) {
		return Session{}, errs.E(errs.NotFound, op, fmt.Errorf("session %s expired", sid))
	}
	return s, nil
}

// Touch records that a session was used, extending its life.
func (r *Registry) Touch(ctx context.Context, sid id.ID) error {
	const op = "session.Touch"

	s, err := r.Get(ctx, sid)
	if err != nil {
		return err
	}
	s.LastSeenAt = r.clk.Now().UTC()
	value, err := marshal(s, op)
	if err != nil {
		return err
	}
	return r.kv.Set(ctx, Key(s.Tenant, sid), value)
}

// Delete ends a session. Deleting an absent one is not an error.
func (r *Registry) Delete(ctx context.Context, sid id.ID) error {
	t, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	return r.kv.Delete(ctx, Key(t, sid))
}

// SweepExpired removes sessions that have not been used within the TTL, and
// reports how many it removed.
//
// It takes a tenant as a parameter rather than from the context because it is
// maintenance: the job framework drives it across tenants, and forging a
// context per tenant would be ceremony with no reader.
func (r *Registry) SweepExpired(ctx context.Context, t tenant.ID) (int, error) {
	const op = "session.SweepExpired"

	if t == "" {
		return 0, errs.E(errs.Invalid, op, errors.New("a sweep requires a tenant"))
	}

	lower, upper := keys.SpaceRange(t, tenant.DefaultNamespace, keys.SpaceSession)
	it := r.kv.NewIterator(lower, upper)

	var stale [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		var row pb.Session
		if err := proto.Unmarshal(it.Value(), &row); err != nil {
			// A session row that will not decode is expendable by definition,
			// so it is swept rather than raised. This is the one place a
			// decode failure is not Corruption, and it is safe precisely
			// because nothing is lost: the client re-initialises.
			stale = append(stale, append([]byte(nil), it.Key()...))
			continue
		}
		if r.staleAt(time.UnixMilli(int64(row.GetLastSeenAtUnixMs()))) {
			stale = append(stale, append([]byte(nil), it.Key()...))
		}
	}
	err := it.Error()
	_ = it.Close()
	if err != nil {
		return 0, err
	}

	for _, k := range stale {
		if err := r.kv.Delete(ctx, k); err != nil {
			return 0, err
		}
	}
	return len(stale), nil
}

func (r *Registry) expired(s Session) bool { return r.staleAt(s.LastSeenAt) }

func (r *Registry) staleAt(last time.Time) bool {
	return r.clk.Now().UTC().Sub(last.UTC()) > r.ttl
}

func marshal(s Session, op string) ([]byte, error) {
	row := &pb.Session{
		Principal:        s.Principal,
		ProtocolVersion:  s.ProtocolVersion,
		CreatedAtUnixMs:  uint64(s.CreatedAt.UnixMilli()),
		LastSeenAtUnixMs: uint64(s.LastSeenAt.UnixMilli()),
	}
	if s.ClientName != "" {
		row.ClientName = proto.String(s.ClientName)
	}
	if s.ClientVersion != "" {
		row.ClientVersion = proto.String(s.ClientVersion)
	}
	b, err := proto.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the session row: %w", err))
	}
	return b, nil
}

// unmarshal decodes a session row.
//
// A row that will not parse is reported as absent rather than as corruption,
// which is the opposite of every other decode in Remem. That is deliberate and
// is what "expendable" means here: nothing is lost by telling the client to
// re-initialise, where refusing the request would strand it.
func unmarshal(t tenant.ID, sid id.ID, b []byte, op string) (Session, error) {
	var row pb.Session
	if err := proto.Unmarshal(b, &row); err != nil {
		return Session{}, errs.E(errs.NotFound, op, fmt.Errorf("session %s is unreadable", sid))
	}
	return Session{
		ID:              sid,
		Tenant:          t,
		Principal:       row.GetPrincipal(),
		ClientName:      row.GetClientName(),
		ClientVersion:   row.GetClientVersion(),
		ProtocolVersion: row.GetProtocolVersion(),
		CreatedAt:       time.UnixMilli(int64(row.GetCreatedAtUnixMs())).UTC(),
		LastSeenAt:      time.UnixMilli(int64(row.GetLastSeenAtUnixMs())).UTC(),
	}, nil
}
