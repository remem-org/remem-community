// Package tenantkv is the durable [tenant.Directory], stored in the untenanted
// system key space.
//
// # Why this is not in internal/tenant
//
// internal/keys imports internal/tenant for the ID and Namespace types, so
// internal/tenant cannot import internal/keys, and a directory that builds keys
// has to live somewhere else. Plan Task 3.1 lists this file as
// internal/tenant/directory.go, which was written before that edge existed.
// The alternative — a second, private copy of the system key layout inside
// internal/tenant — would duplicate a durable format, which is the one thing
// worth avoiding more than a subpackage.
//
// The shape mirrors internal/storage: the abstraction owns the interface, the
// adapter is a subpackage beside it.
package tenantkv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv/pb"
	"google.golang.org/protobuf/proto"
)

// prefix is where directory rows live. The directory has to be readable before
// any tenant is known — which at open time is always — so it is untenanted,
// like the format manifest beside it.
const prefix = "/tenants/"

// Directory is the durable tenant register.
type Directory struct {
	kv  storage.KV
	clk clock.Clock
}

var _ tenant.Directory = (*Directory)(nil)

// New returns a directory over kv, stamping times from clk.
func New(kv storage.KV, clk clock.Clock) *Directory {
	return &Directory{kv: kv, clk: clk}
}

// Key returns the storage key of a tenant's directory row.
//
// It is exported so a test can corrupt exactly one row, and so that
// remem-admin can name a row it is inspecting. Nothing in the write path uses
// it from outside this package.
func Key(id tenant.ID) []byte { return keys.System(prefix + string(id)) }

func (d *Directory) Create(ctx context.Context, id tenant.ID, m tenant.Meta) error {
	const op = "tenant.Directory.Create"

	if _, err := tenant.Parse(string(id)); err != nil {
		return err
	}
	if _, err := d.Get(ctx, id); err == nil {
		return errs.E(errs.Conflict, op, fmt.Errorf("tenant %q already exists", id))
	} else if !errs.Is(err, errs.NotFound) {
		return err
	}

	now := d.clk.Now().UTC()
	m.ID = id
	if m.SchemaVersion == 0 {
		m.SchemaVersion = tenant.DefaultSchemaVersion
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	m.UpdatedAt = now

	value, err := marshal(m, op)
	if err != nil {
		return err
	}
	return d.kv.Set(ctx, Key(id), value)
}

func (d *Directory) Get(ctx context.Context, id tenant.ID) (tenant.Meta, error) {
	const op = "tenant.Directory.Get"

	b, err := d.kv.Get(ctx, Key(id))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return tenant.Meta{}, errs.E(errs.NotFound, op, fmt.Errorf("no tenant %q", id))
		}
		return tenant.Meta{}, err
	}
	return unmarshal(id, b, op)
}

func (d *Directory) List(ctx context.Context) ([]tenant.Meta, error) {
	var out []tenant.Meta
	err := d.each(ctx, "tenant.Directory.List", func(m tenant.Meta) error {
		out = append(out, m)
		return nil
	})
	return out, err
}

func (d *Directory) ForEach(ctx context.Context, fn func(tenant.ID) error) error {
	return d.each(ctx, "tenant.Directory.ForEach", func(m tenant.Meta) error { return fn(m.ID) })
}

// each walks the directory in key order, decoding each row.
//
// A row that will not decode stops the walk. The alternative — skipping it —
// would mean a rebuild job silently omitting a tenant, which is the failure
// mode that produces "why is customer X's search empty" three weeks later.
func (d *Directory) each(ctx context.Context, op string, fn func(tenant.Meta) error) error {
	lower, upper := keys.PrefixRange(keys.System(prefix))
	it := d.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		id, err := idFromKey(it.Key(), op)
		if err != nil {
			return err
		}
		m, err := unmarshal(id, it.Value(), op)
		if err != nil {
			return err
		}
		if err := fn(m); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}

// idFromKey recovers the tenant id from a directory key.
func idFromKey(k []byte, op string) (tenant.ID, error) {
	want := keys.System(prefix)
	if len(k) <= len(want) || string(k[:len(want)]) != string(want) {
		return "", errs.E(errs.Corruption, op, errors.New("a key in the tenant directory range is not a directory key"))
	}
	return tenant.ID(k[len(want):]), nil
}

func marshal(m tenant.Meta, op string) ([]byte, error) {
	row := &pb.Tenant{
		SchemaVersion:   m.SchemaVersion,
		CreatedAtUnixMs: uint64(m.CreatedAt.UnixMilli()),
		UpdatedAtUnixMs: uint64(m.UpdatedAt.UnixMilli()),
	}
	if m.DisplayName != "" {
		row.DisplayName = proto.String(m.DisplayName)
	}
	b, err := proto.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the tenant row: %w", err))
	}
	return b, nil
}

// unmarshal decodes a directory row.
//
// A row that will not parse is Corruption, never a miss: these bytes came out
// of the store, and reporting NotFound would let a request proceed against a
// tenant whose metadata this binary could not read — the schema version
// included.
func unmarshal(id tenant.ID, b []byte, op string) (tenant.Meta, error) {
	var row pb.Tenant
	if err := proto.Unmarshal(b, &row); err != nil {
		return tenant.Meta{}, errs.E(errs.Corruption, op, fmt.Errorf("the directory row for tenant %q will not parse", id))
	}
	if row.GetSchemaVersion() == 0 {
		return tenant.Meta{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"the directory row for tenant %q carries no schema version; every tenant has one from creation", id))
	}
	return tenant.Meta{
		ID:            id,
		DisplayName:   row.GetDisplayName(),
		SchemaVersion: row.GetSchemaVersion(),
		CreatedAt:     unixMilli(row.GetCreatedAtUnixMs()),
		UpdatedAt:     unixMilli(row.GetUpdatedAtUnixMs()),
	}, nil
}

func unixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
