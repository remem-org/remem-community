package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
	"google.golang.org/protobuf/proto"
)

// Users travel in their own Go companion section, go.users (41). They are
// canonical identity: an audit event names a user as its actor by id, and a
// restore that dropped users would leave that history pointing at nobody.
//
// Tokens never travel. A snapshot is a portable file, copied and kept for
// years, and a token digest in one is a credential leak waiting for whoever
// finds it. So a restore keeps who everyone is and asks for their tokens to be
// issued again — docs/MIGRATION.md says so, and TestNoTokenMaterialIsInTheFile
// scans an exported file's bytes to hold it.

// UserSource yields a tenant's users, oldest first. *users.Service is one; it
// reads its tenant from the context.
type UserSource interface {
	List(ctx context.Context) ([]users.User, error)
}

// exportUsers writes one tenant's users, a page to a block.
//
// Export calls it for every tenant straight after the tenants block, before any
// record. The order is for binaries from before users. They do not know section
// 41, and their importer writes each block as it reads it, so if users came
// after the records such a binary would import every memory and only then
// refuse, leaving a store with everything but its users. Found by running the
// previous release against a snapshot with users in it. With users first, it
// meets section 41 before any record, and refuses having written no memory,
// vector, edge or event; only the tenant's directory row precedes the refusal. TestUsersAreWrittenBeforeAnyRecord holds the order.
func exportUsers(ctx context.Context, src Sources, sw *Writer, t tenant.ID, opts ExportOpts, got *Stats) error {
	if src.Users == nil {
		return nil
	}
	list, err := src.Users.List(tenant.NewContext(ctx, t))
	if err != nil {
		return err
	}
	if opts.SharedOnly {
		got.UsersDropped += uint64(len(list))
		return nil
	}
	page := opts.pageSize()
	for start := 0; start < len(list); start += page {
		end := min(start+page, len(list))
		batch := make([]proto.Message, 0, end-start)
		for _, u := range list[start:end] {
			batch = append(batch, &pbext.User{
				Tenant:          t.String(),
				Id:              u.ID[:],
				Name:            u.Name,
				State:           u.State.String(),
				CreatedAtUnixMs: unixMs(u.CreatedAt),
				UpdatedAtUnixMs: unixMs(u.UpdatedAt),
			})
		}
		if err := sw.WriteMessages(SectionGoUsers, batch); err != nil {
			return err
		}
		got.Users += uint64(end - start)
	}
	return nil
}

// UserRestorer writes a snapshot's users. *users.Service is one; it reads its
// tenant from the context.
type UserRestorer interface {
	Restore(ctx context.Context, u users.User, mode users.RestoreMode) (users.RestoreOutcome, error)
}

// applyUsers restores one users block, a user at a time: each is its own small
// transaction inside the user service, and its outcome is counted the way a
// record's is.
func (im *importer) applyUsers(ctx context.Context, blk *Block) error {
	const op = "snapshot.Import"
	if im.dst.Users == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"this snapshot carries users and this import has no user store to put them in; "+
				"it is refused rather than dropped, because history names users by id"))
	}
	mode := map[ConflictMode]users.RestoreMode{
		ConflictFail: users.RestoreFail, ConflictSkip: users.RestoreSkip, ConflictOverwrite: users.RestoreOverwrite,
	}[im.opts.conflict()]
	return blk.Each(func(raw []byte) error {
		t, u, err := decodeUser(raw, op)
		if err != nil {
			return err
		}
		im.note(t)
		got, err := im.dst.Users.Restore(tenant.NewContext(ctx, t), u, mode)
		if err != nil {
			return err
		}
		im.report.Stats.Users++
		switch got {
		case users.Unchanged:
			im.report.Unchanged++
		case users.Skipped:
			im.report.Skipped++
		}
		return nil
	})
}

func decodeUser(raw []byte, op string) (tenant.ID, users.User, error) {
	var m pbext.User
	if err := proto.Unmarshal(raw, &m); err != nil {
		return "", users.User{}, errs.E(errs.Corruption, op, fmt.Errorf("decoding a user: %w", err))
	}
	t, err := tenant.Parse(m.GetTenant())
	if err != nil {
		return "", users.User{}, errs.E(errs.Corruption, op, err)
	}
	uid, err := id.FromBytes(m.GetId())
	if err != nil {
		return "", users.User{}, errs.E(errs.Corruption, op, err)
	}
	st, err := users.ParseState(m.GetState())
	if err != nil {
		return "", users.User{}, errs.E(errs.Corruption, op, err)
	}
	return t, users.User{
		ID: uid, Name: m.GetName(), State: st,
		CreatedAt: fromUnixMs(m.GetCreatedAtUnixMs()), UpdatedAt: fromUnixMs(m.GetUpdatedAtUnixMs()),
	}, nil
}

// readUsers remembers the snapshot's users for compareUsers.
func (v *verifier) readUsers(blk *Block) error {
	const op = "snapshot.Verify"
	return blk.Each(func(raw []byte) error {
		var m pbext.User
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, err)
		}
		key, err := scopeOf(m.GetTenant(), m.GetId(), op)
		if err != nil {
			return err
		}
		v.users[key] = &m
		v.report.Snapshot.Users++
		return nil
	})
}

// compareUsers reconciles each tenant's users against the snapshot's, field by
// field. Unlike a record, nothing derives a user's fields on the way in, so a
// user that differs from its snapshot is a user something changed.
func (v *verifier) compareUsers(ctx context.Context, metas []tenant.Meta) error {
	const op = "snapshot.Verify"
	if v.src.Users == nil {
		if len(v.users) > 0 {
			return errs.E(errs.Invalid, op, errors.New(
				"this snapshot carries users and this verification was given no user store to compare them with"))
		}
		return nil
	}
	for _, m := range metas {
		list, err := v.src.Users.List(tenant.NewContext(ctx, m.ID))
		if err != nil {
			return err
		}
		for _, u := range list {
			v.report.Store.Users++
			key := scopedID{tenant: m.ID, id: u.ID}
			want, ok := v.users[key]
			if !ok {
				v.report.add(Finding{Kind: FindingExtra, What: "user", Tenant: m.ID, Ref: u.ID.String(),
					Detail: "the store holds a user the snapshot does not"})
				continue
			}
			delete(v.users, key)
			if field := userDifference(want, u); field != "" {
				v.report.add(Finding{Kind: FindingDiffers, What: "user", Tenant: m.ID, Ref: u.ID.String(),
					Detail: "the user's " + field + " differs from the snapshot's"})
			}
		}
	}
	for key := range v.users {
		v.report.add(Finding{Kind: FindingMissing, What: "user", Tenant: key.tenant, Ref: key.id.String(),
			Detail: "the snapshot holds a user the store does not"})
	}
	return nil
}

func userDifference(m *pbext.User, u users.User) string {
	switch {
	case m.GetName() != u.Name:
		return "name"
	case m.GetState() != u.State.String():
		return "state"
	case m.GetCreatedAtUnixMs() != unixMs(u.CreatedAt):
		return "created_at"
	case m.GetUpdatedAtUnixMs() != unixMs(u.UpdatedAt):
		return "updated_at"
	}
	return ""
}
