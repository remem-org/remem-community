package users

import (
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/users/pb"
	"google.golang.org/protobuf/proto"
)

// The encodings of the user space's rows. A row that does not decode is
// errs.Corruption: user rows are canonical, so nothing can rebuild one, and
// reading it as absent would hand its name to the next caller who asks.

func encodeUser(u User) []byte {
	return mustMarshal(&pb.User{
		Name:            u.Name,
		State:           pb.State(u.State),
		CreatedAtUnixMs: uint64(u.CreatedAt.UnixMilli()),
		UpdatedAtUnixMs: uint64(u.UpdatedAt.UnixMilli()),
	})
}

func decodeUser(uid id.ID, raw []byte, op string) (User, error) {
	var m pb.User
	if err := proto.Unmarshal(raw, &m); err != nil {
		return User{}, corrupt(op, "user", uid, err)
	}
	st := State(m.GetState())
	if st != Active && st != Disabled {
		return User{}, corrupt(op, "user", uid, fmt.Errorf("state %d", m.GetState()))
	}
	return User{
		ID:        uid,
		Name:      m.GetName(),
		State:     st,
		CreatedAt: fromMillis(m.GetCreatedAtUnixMs()),
		UpdatedAt: fromMillis(m.GetUpdatedAtUnixMs()),
	}, nil
}

func encodeName(uid id.ID) []byte {
	return mustMarshal(&pb.Name{UserId: uid.Bytes()})
}

func decodeName(raw []byte, op string) (id.ID, error) {
	var m pb.Name
	if err := proto.Unmarshal(raw, &m); err != nil {
		return id.Zero, errs.E(errs.Corruption, op, fmt.Errorf("a name-index row will not decode: %w", err))
	}
	uid, err := id.FromBytes(m.GetUserId())
	if err != nil {
		return id.Zero, errs.E(errs.Corruption, op, fmt.Errorf("a name-index row names no user: %w", err))
	}
	return uid, nil
}

// tailID reads the sixteen-byte id that ends a user or token key.
func tailID(key []byte, op string) (id.ID, error) {
	if len(key) < 16 {
		return id.Zero, errs.E(errs.Corruption, op, fmt.Errorf("a user-space key is %d bytes", len(key)))
	}
	uid, err := id.FromBytes(key[len(key)-16:])
	if err != nil {
		return id.Zero, errs.E(errs.Corruption, op, err)
	}
	return uid, nil
}

func corrupt(op, what string, which id.ID, err error) error {
	return errs.E(errs.Corruption, op, fmt.Errorf("%s %s will not decode: %w", what, which, err))
}

func fromMillis(ms uint64) time.Time { return time.UnixMilli(int64(ms)).UTC() }

// mustMarshal marshals a message this package built from typed fields. It
// cannot fail for these messages, so a failure is a programming error.
func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("users: marshalling %T: %v", m, err))
	}
	return b
}
