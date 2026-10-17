package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
	"github.com/remem-org/remem-go/internal/version"
)

// The user and token verbs administer a stopped server's users: how the first
// user is created, since the routes that do it on a running server need the
// operator credential and nothing else is created at start-up.
//
// They follow the rules the routes do. A user is never deleted. A token's
// secret is printed once and nowhere else, alone on stdout, so a script can
// capture it. And they open the directory the way every writing command here
// does: read-only first, so a mistyped path is refused rather than becoming a
// new database holding a user no server will read.

func usersCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "create":
		return usersCreate(args)
	case "rename":
		return usersRename(args)
	case "disable":
		return usersSetState(args, "disable", users.Disabled)
	case "enable":
		return usersSetState(args, "enable", users.Active)
	case "list":
		return usersList(args)
	case "delete":
		return 0, errors.New("a user is never deleted, because its history refers to it by id; " +
			"disable it instead with `remem-admin users disable`, which refuses all of its tokens")
	default:
		usage()
		return 0, fmt.Errorf("unknown users subcommand %q", sub)
	}
}

func tokensCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "issue":
		return tokensIssue(args)
	case "list":
		return tokensList(args)
	case "revoke":
		return tokensRevoke(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown tokens subcommand %q", sub)
	}
}

// userFlags are the flags every verb takes.
type userFlags struct {
	fs      *flag.FlagSet
	dataDir *string
	tenant  *string
}

func newUserFlags(name string) userFlags {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return userFlags{
		fs:      fs,
		dataDir: fs.String("data-dir", "", "the stopped server's data directory"),
		tenant: fs.String("tenant", "", "the tenant the user belongs to; defaults to "+
			"REMEM_TENANT_DEFAULT, and a single-tenant build refuses any other"),
	}
}

// open opens the directory, settles the tenant, and returns a context scoped to
// it with the user service. write says whether the verb changes anything.
func (f userFlags) open(args []string, write bool) (context.Context, *users.Service, func(), error) {
	if err := f.fs.Parse(args); err != nil {
		return nil, nil, nil, err
	}
	if *f.dataDir == "" {
		return nil, nil, nil, fmt.Errorf("%s needs --data-dir", f.fs.Name())
	}

	var kv storage.KV
	var err error
	if write {
		kv, err = openReadWriteExisting(*f.dataDir)
	} else {
		kv, err = openForInspection(*f.dataDir)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	release := func() { _ = kv.Close() }
	fail := func(err error) (context.Context, *users.Service, func(), error) {
		release()
		return nil, nil, nil, err
	}

	ctx := context.Background()
	if write {
		// A directory a newer binary wrote is refused before this one writes a
		// row into it that the newer one would have to read.
		if _, err := schema.Open(ctx, kv, version.Current()); err != nil {
			return fail(err)
		}
	}
	dir, scoped, err := scope(kv, clock.System(), *f.tenant)
	if err != nil {
		return fail(err)
	}
	t := tenant.ID(scoped)
	if t == "" {
		if t, err = implicitTenant(); err != nil {
			return fail(err)
		}
	}
	if _, err := dir.Get(ctx, t); err != nil {
		return fail(fmt.Errorf("tenant %s: %w; a server provisions its default tenant when it first starts", t, err))
	}
	return tenant.NewContext(ctx, t), users.New(kv, clock.System()), release, nil
}

func usersCreate(args []string) (int, error) {
	f := newUserFlags("users create")
	name := f.fs.String("name", "", "the new user's name, unique within the tenant")
	ctx, svc, release, err := f.open(args, true)
	if err != nil {
		return 0, err
	}
	defer release()
	u, err := svc.Create(ctx, *name)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%s %s\n", u.ID, u.Name)
	return 0, nil
}

func usersRename(args []string) (int, error) {
	f := newUserFlags("users rename")
	user := f.fs.String("user", "", "the user's id")
	name := f.fs.String("name", "", "the new name")
	ctx, svc, release, err := f.open(args, true)
	if err != nil {
		return 0, err
	}
	defer release()
	uid, err := id.Parse(*user)
	if err != nil {
		return 0, fmt.Errorf("--user: %w", err)
	}
	u, err := svc.Rename(ctx, uid, *name)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%s %s\n", u.ID, u.Name)
	return 0, nil
}

func usersSetState(args []string, verb string, st users.State) (int, error) {
	f := newUserFlags("users " + verb)
	user := f.fs.String("user", "", "the user's id")
	ctx, svc, release, err := f.open(args, true)
	if err != nil {
		return 0, err
	}
	defer release()
	uid, err := id.Parse(*user)
	if err != nil {
		return 0, fmt.Errorf("--user: %w", err)
	}
	u, err := svc.SetState(ctx, uid, st)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%s %s %s\n", u.ID, u.Name, u.State)
	return 0, nil
}

func usersList(args []string) (int, error) {
	f := newUserFlags("users list")
	ctx, svc, release, err := f.open(args, false)
	if err != nil {
		return 0, err
	}
	defer release()
	list, err := svc.List(ctx)
	if err != nil {
		return 0, err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, u := range list {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", u.ID, u.Name, u.State, u.CreatedAt.Format(time.RFC3339))
	}
	return 0, w.Flush()
}

func tokensIssue(args []string) (int, error) {
	f := newUserFlags("tokens issue")
	user := f.fs.String("user", "", "the user the token authenticates as")
	expires := f.fs.String("expires", "", "an RFC 3339 time the token stops working at; empty for never")
	ctx, svc, release, err := f.open(args, true)
	if err != nil {
		return 0, err
	}
	defer release()
	uid, err := id.Parse(*user)
	if err != nil {
		return 0, fmt.Errorf("--user: %w", err)
	}
	var at *time.Time
	if *expires != "" {
		e, err := time.Parse(time.RFC3339, *expires)
		if err != nil {
			return 0, fmt.Errorf("--expires: %w", err)
		}
		at = &e
	}
	issued, err := svc.IssueToken(ctx, uid, at)
	if err != nil {
		return 0, err
	}
	// The token alone on stdout, so `TOKEN=$(remem-admin tokens issue ...)`
	// captures it, and a note on stderr that it is not shown again.
	fmt.Println(issued.Secret)
	fmt.Fprintf(os.Stderr, "token %s issued to user %s; it is not shown again\n", issued.Token.ID, uid)
	return 0, nil
}

func tokensList(args []string) (int, error) {
	f := newUserFlags("tokens list")
	user := f.fs.String("user", "", "the user whose tokens to list")
	ctx, svc, release, err := f.open(args, false)
	if err != nil {
		return 0, err
	}
	defer release()
	uid, err := id.Parse(*user)
	if err != nil {
		return 0, fmt.Errorf("--user: %w", err)
	}
	list, err := svc.ListTokens(ctx, uid)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, tok := range list {
		expiry := "never"
		if tok.ExpiresAt != nil {
			expiry = tok.ExpiresAt.Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\tcreated %s\texpires %s\n",
			tok.ID, tok.StateAt(now), tok.CreatedAt.Format(time.RFC3339), expiry)
	}
	return 0, w.Flush()
}

func tokensRevoke(args []string) (int, error) {
	f := newUserFlags("tokens revoke")
	user := f.fs.String("user", "", "the user the token belongs to")
	token := f.fs.String("token", "", "the token's id, as tokens list shows it")
	ctx, svc, release, err := f.open(args, true)
	if err != nil {
		return 0, err
	}
	defer release()
	uid, err := id.Parse(*user)
	if err != nil {
		return 0, fmt.Errorf("--user: %w", err)
	}
	tid, err := id.Parse(*token)
	if err != nil {
		return 0, fmt.Errorf("--token: %w", err)
	}
	tok, err := svc.RevokeToken(ctx, uid, tid)
	if err != nil {
		return 0, err
	}
	fmt.Printf("%s revoked\n", tok.ID)
	return 0, nil
}
