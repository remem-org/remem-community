package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/inspect"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

// inspectCommand examines a data directory without changing it.
//
// check is the corruption diagnosis — internal/inspect, walked over every
// tenant. spaces says where the bytes are. key decodes one key an error message
// or a finding named. None of them prints memory content: a value is described
// by its length, and a text term, which is taken from content, by its length too.
func inspectCommand(args []string) (int, error) {
	sub := ""
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "check":
		return inspectCheck(args)
	case "spaces":
		return inspectSpaces(args)
	case "key":
		return inspectKey(args)
	default:
		usage()
		return 0, fmt.Errorf("unknown inspect subcommand %q", sub)
	}
}

// openForInspection opens a directory read-only, which both matches what
// inspect does and refuses a path holding no database instead of creating one —
// the defect Phase 6 found in `graph verify`, where a mistyped path answered
// "no tenants found" and read as a clean corpus.
func openForInspection(dataDir string) (storage.KV, error) {
	if dataDir == "" {
		return nil, errors.New("inspect needs --data-dir")
	}
	kv, err := pebble.Open(dataDir, pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w\n"+
			"check the path, and note that a data directory in use by a running server may not be "+
			"readable: stop it, or point --data-dir at a backup checkpoint under .backups/", dataDir, err)
	}
	return kv, nil
}

func inspectCheck(args []string) (int, error) {
	fs := flag.NewFlagSet("inspect check", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to check")
	only := fs.String("tenant", "", "check one tenant rather than every tenant")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	kv, err := openForInspection(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	reports, err := checkTenants(context.Background(), kv, dir, tenant.ID(scoped))
	if err != nil {
		return 0, err
	}
	if len(reports) == 0 {
		fmt.Println("no tenants found")
		return 0, nil
	}

	problems := 0
	for _, r := range reports {
		fmt.Printf("tenant %s: %d records, %d problems\n", r.Tenant, r.Records, len(r.Findings))
		for _, f := range r.Findings {
			fmt.Printf("  %s %x: %s\n", f.Space, f.Key, f.Problem)
		}
		if r.Truncated {
			fmt.Printf("  more than %d problems; the rest are not listed\n", inspect.MaxFindings)
		}
		problems += len(r.Findings)
	}
	if problems == 0 {
		return 0, nil
	}
	fmt.Printf("\n%d problems found. A derived index is repaired with `remem-admin rebuild --index`; "+
		"a problem in the record space is damage to data, and needs a decision rather than a rebuild.\n", problems)
	return exitInconsistent, nil
}

// checkTenants runs the checker over one tenant, or every tenant dir names.
//
// The cross-tenant walk is here, not in internal/inspect: Directory.ForEach is
// the single audited cross-tenant path (Invariant 1), and this command is one
// of the callers the boundary guard allows to make it. The directory arrives as
// a parameter because which tenants this build may look at is a policy the
// command settles — see scope.go.
func checkTenants(ctx context.Context, kv storage.KV, dir tenant.Directory, only tenant.ID) ([]inspect.Report, error) {
	if only != "" {
		r, err := inspect.CheckTenant(ctx, kv, only)
		if err != nil {
			return nil, err
		}
		return []inspect.Report{r}, nil
	}
	var reports []inspect.Report
	err := dir.ForEach(ctx, func(t tenant.ID) error {
		r, err := inspect.CheckTenant(ctx, kv, t)
		if err != nil {
			return err
		}
		reports = append(reports, r)
		return nil
	})
	return reports, err
}

func inspectSpaces(args []string) (int, error) {
	fs := flag.NewFlagSet("inspect spaces", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory to measure")
	only := fs.String("tenant", "", "measure one tenant rather than every tenant")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	kv, err := openForInspection(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()
	dir, scoped, err := scope(kv, clock.System(), *only)
	if err != nil {
		return 0, err
	}
	only = &scoped
	ctx := context.Background()

	print := func(t tenant.ID) error {
		rows, err := spaceUsage(ctx, kv, t)
		if err != nil {
			return err
		}
		fmt.Printf("tenant %s\n", t)
		for _, r := range rows {
			fmt.Printf("  %-14s %10d keys %14d bytes\n", r.Space, r.Keys, r.Bytes)
		}
		return nil
	}
	if *only != "" {
		return 0, print(tenant.ID(*only))
	}
	if err := dir.ForEach(ctx, print); err != nil {
		return 0, err
	}

	lower, upper := keys.SystemRange()
	n, size, err := usageOf(ctx, kv, lower, upper)
	if err != nil {
		return 0, err
	}
	fmt.Printf("system: %d keys, %d bytes\n", n, size)
	return 0, nil
}

// spaceRow is one space's share of a tenant.
type spaceRow struct {
	Space string
	Keys  int
	Bytes int64
}

// spaceUsage counts one tenant's keys and their key and value bytes by space,
// in space-byte order. A key that does not parse is counted as "unknown" rather
// than skipped, because it is the one an operator most needs to find.
func spaceUsage(ctx context.Context, kv storage.KV, t tenant.ID) ([]spaceRow, error) {
	lower, upper := keys.NamespaceRange(t, tenant.DefaultNamespace)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var rows []spaceRow
	index := map[string]int{}
	for ok := it.First(); ok; ok = it.Next() {
		name := "unknown"
		if _, _, s, err := keys.ParseSpace(it.Key()); err == nil {
			name = s.String()
		}
		i, seen := index[name]
		if !seen {
			i = len(rows)
			index[name] = i
			rows = append(rows, spaceRow{Space: name})
		}
		rows[i].Keys++
		rows[i].Bytes += int64(len(it.Key()) + len(it.Value()))
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return rows, it.Error()
}

func usageOf(ctx context.Context, kv storage.KV, lower, upper []byte) (int, int64, error) {
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	n, size := 0, int64(0)
	for ok := it.First(); ok; ok = it.Next() {
		n++
		size += int64(len(it.Key()) + len(it.Value()))
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
	}
	return n, size, it.Error()
}

func inspectKey(args []string) (int, error) {
	fs := flag.NewFlagSet("inspect key", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "the Remem data directory the key is in")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if fs.NArg() != 1 {
		return 0, errors.New("inspect key takes one key, in hex, as printed by inspect check")
	}
	kv, err := openForInspection(*dataDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = kv.Close() }()

	desc, err := describeKey(context.Background(), kv, fs.Arg(0))
	if err != nil {
		return 0, err
	}
	fmt.Print(desc)
	return 0, nil
}

// describeKey decodes one key and reports what its value is — its length, never
// its bytes.
func describeKey(ctx context.Context, kv storage.KV, hexKey string) (string, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(hexKey))
	if err != nil {
		return "", fmt.Errorf("the key is not hex: %w", err)
	}

	var b strings.Builder
	t, ns, space, err := keys.ParseSpace(raw)
	if err != nil {
		fmt.Fprintf(&b, "does not parse as a key: %v\n", err)
	} else {
		fmt.Fprintf(&b, "tenant     %q\nnamespace  %q\nspace      %s (%s)\n", t, ns, space, space.Class())
		describeRemainder(&b, space, raw)
	}

	value, err := kv.Get(ctx, raw)
	switch {
	case errs.Is(err, errs.NotFound):
		b.WriteString("value      absent\n")
	case err != nil:
		return "", err
	default:
		fmt.Fprintf(&b, "value      %d bytes (not shown)\n", len(value))
	}
	return b.String(), nil
}

// describeRemainder adds what the space's own parser says about a key.
func describeRemainder(b *strings.Builder, space keys.Space, raw []byte) {
	switch space {
	case keys.SpaceRecord:
		if _, _, typ, rid, err := keys.ParseRecord(raw); err == nil {
			fmt.Fprintf(b, "record     %s %s\n", typ, rid)
		} else {
			fmt.Fprintf(b, "record key does not parse: %v\n", err)
		}
	case keys.SpaceText:
		if _, _, term, rid, err := keys.ParseText(raw); err == nil {
			// A term is taken from a memory's content, so it is not shown.
			fmt.Fprintf(b, "term       %d bytes (not shown)\nrecord     %s\n", len(term), rid)
		} else {
			fmt.Fprintf(b, "text key does not parse: %v\n", err)
		}
	case keys.SpaceJob:
		if _, _, state, due, jid, err := keys.ParseJob(raw); err == nil {
			fmt.Fprintf(b, "job        %s, partition %s, due %s\n",
				jid, state, time.UnixMilli(int64(due)).UTC().Format(time.RFC3339))
		} else {
			fmt.Fprintf(b, "job key does not parse: %v\n", err)
		}
	case keys.SpaceEvent:
		if _, _, subject, ts, seq, err := keys.ParseEvent(raw); err == nil {
			fmt.Fprintf(b, "event      subject %s, at %s, sequence %d\n",
				subject, time.UnixMilli(int64(ts)).UTC().Format(time.RFC3339), seq)
		} else {
			fmt.Fprintf(b, "event key does not parse: %v\n", err)
		}
	}
}
