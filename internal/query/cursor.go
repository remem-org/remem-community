package query

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Cursor resumes an ordered walk within a server-owned snapshot session.
// Session is an unpredictable identity, bound by the memory service to the
// tenant and query. It expires after five minutes and never survives restart.
// All pages read the same snapshot, including their record bodies.
type Cursor struct {
	Tenant tenant.ID
	Slot   uint16
	Desc   bool
	// Value is the ordering value in its encoded, order-preserving form. It is
	// carried encoded because that is what the index key holds and what a
	// resume compares — decoding it in between would be a second encoding of
	// one fact.
	Value   []byte
	ID      id.ID
	Session [16]byte
}

// position is the cursor as a walk resumes from it.
func (c *Cursor) position() *attr.Position {
	if c == nil {
		return nil
	}
	return &attr.Position{Value: c.Value, ID: c.ID}
}

// Encode renders the cursor as an opaque token.
func (c *Cursor) Encode() string {
	if c == nil {
		return ""
	}
	p := make([]byte, 0, 40+len(c.Value))
	p = binary.BigEndian.AppendUint16(p, c.Slot)
	if c.Desc {
		p = append(p, 1)
	} else {
		p = append(p, 0)
	}
	p = codec.AppendLengthPrefixed(p, c.Value)
	p = append(p, c.ID[:]...)
	p = append(p, c.Session[:]...)
	return codec.EncodeToken(codec.TokenListing, c.Tenant, p)
}

// DecodeCursor reads a token and checks it belongs to the tenant using it.
//
// The tenant and kind checks live in internal/codec now; what stays here is
// the payload, which is this package's business. Returning Invalid rather than
// NotFound is deliberate: the token is malformed *for this request*, and
// saying "not found" would suggest that presenting it somewhere else might
// work.
func DecodeCursor(token string, t tenant.ID) (*Cursor, error) {
	const op = "query.DecodeCursor"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	if token == "" {
		return nil, nil
	}
	rest, err := codec.DecodeToken(token, codec.TokenListing, t)
	if err != nil {
		return nil, err
	}
	if len(rest) < 3 {
		return nil, bad("this page token ends before its ordering")
	}
	c := &Cursor{Tenant: t, Slot: binary.BigEndian.Uint16(rest[:2]), Desc: rest[2] == 1}
	rest = rest[3:]

	value, rest, rerr := codec.ReadLengthPrefixed(rest)
	if rerr != nil {
		return nil, bad("this page token ends inside its ordering value")
	}
	c.Value = value

	if len(rest) != 16+16 {
		return nil, bad("this page token ends before its position")
	}
	rid, ierr := id.FromBytes(rest[:16])
	if ierr != nil {
		return nil, bad("this page token holds a malformed record id")
	}
	c.ID = rid
	copy(c.Session[:], rest[16:])
	return c, nil
}

// checkUsable refuses a cursor that cannot mean what it says against this
// query and this store.
func (c *Cursor) checkUsable(q *Query) error {
	const op = "query.Cursor.checkUsable"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	switch {
	case c.Tenant != q.Tenant:
		return bad("this page token was issued for a different tenant")
	case c.Slot != q.OrderBy || c.Desc != q.Desc:
		// Resuming a listing under a different ordering would walk a different
		// index from a position that means nothing in it, and return an
		// arbitrary slice of the corpus that looks like a page.
		return bad("this page token was issued for a different ordering; " +
			"changing the sort starts a new listing")

	}
	return nil
}

// MarshalJSON writes the cursor as its opaque token, so a serialised Query
// carries exactly what a client would have carried.
func (c Cursor) MarshalJSON() ([]byte, error) { return json.Marshal(c.Encode()) }

// UnmarshalJSON reads a token back.
//
// It cannot check the tenant -- that is the caller's, and it is not in scope
// here -- so it reads the tenant the token names and decodes against it,
// leaving [Cursor.checkUsable] to refuse a mismatch before any row is read.
func (c *Cursor) UnmarshalJSON(b []byte) error {
	const op = "query.Cursor.UnmarshalJSON"
	var token string
	if err := json.Unmarshal(b, &token); err != nil {
		return errs.E(errs.Invalid, op, err)
	}
	if token == "" {
		return nil
	}
	name, err := codec.TokenTenant(token)
	if err != nil {
		return err
	}
	got, err := DecodeCursor(token, name)
	if err != nil {
		return err
	}
	*c = *got
	return nil
}
