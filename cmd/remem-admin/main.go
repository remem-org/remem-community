// Command remem-admin operates on a Remem data directory directly.
//
//	remem-admin graph verify --data-dir ./data
//
// It opens the directory itself rather than talking to a server, which is what
// makes it useful for diagnosis and what constrains when it can run: Pebble
// takes an exclusive lock, so the server must be stopped, or the command must be
// pointed at a backup checkpoint (`.backups/pre-<unix>/`) taken from a live one.
//
// Phase 6 ships the graph consistency checker, because the two edge key spaces
// are written together and can therefore only fall apart in ways nothing on the
// read path is looking for. Phase 7 ships the vector index rebuild, because a
// server that finds a damaged index logs that one is needed and deliberately
// does not run it — and a message pointing at a command that does not exist is
// worse than no message. Phase 8 ships the text index rebuild, for the same
// reason and one more: an upgrade that changes how text is split into terms
// makes every posting on disk describe the old rules, and this is what brings a
// corpus forward.
//
// Phase 12 ships export, import, verify and rebuild. The first three are the
// portable snapshot — the only backup format there is, because Invariant 12
// forbids a copy of Pebble's own files from being one — and the fourth is spec
// §43's whole-database, per-tenant, per-index rebuild, which joins the two
// index-specific commands above rather than replacing them: a command a log
// line points at does not get renamed. Phase 13 ships inspect: the corruption
// diagnosis the crash tests also stand on, where the bytes are, and what one key
// means.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/remem-org/remem-go/internal/version"
)

func main() {
	code, err := run(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "remem-admin:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// run returns the process exit code as well as an error, because "the command
// worked and found problems" is neither a success nor a failure: a checker that
// exited zero on a broken corpus could not be used as a check, and one that
// reported a broken corpus as an error would be indistinguishable from one that
// could not open the directory.
func run(args []string) (int, error) {
	command := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}

	switch command {
	case "graph":
		return graphCommand(args)
	case "vector":
		return vectorCommand(args)
	case "text":
		return textCommand(args)
	case "discovery":
		return discoveryCommand(args)
	case "export":
		return exportCommand(args)
	case "import":
		return importCommand(args)
	case "verify":
		return verifyCommand(args)
	case "rebuild":
		return rebuildCommand(args)
	case "inspect":
		return inspectCommand(args)
	case "tenants":
		return tenantsCommand(args)
	case "version":
		fmt.Println("remem-admin", version.Binary)
		fmt.Println(version.Current())
		return 0, nil
	case "", "help", "-h", "--help":
		usage()
		return 0, nil
	default:
		usage()
		return 0, fmt.Errorf("unknown command %q", command)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `remem-admin operates on a Remem data directory directly.

Usage:
  remem-admin graph verify --data-dir <path> [--tenant <id>]
  remem-admin vector rebuild --data-dir <path> [--tenant <id>] [--metric <name>]
  remem-admin text rebuild --data-dir <path> [--tenant <id>]
  remem-admin discovery backfill --data-dir <path> [--tenant <id>] [--per-job <n>]
  remem-admin export --data-dir <path> --out <file> [--tenant <id>] [--shared-only]
  remem-admin import --data-dir <path> --in <file> [--on-conflict fail|skip|overwrite]
                     [--vectors auto|re-embed|verbatim] [--resume]
  remem-admin verify --data-dir <path> --in <file>
  remem-admin rebuild --data-dir <path> --index vector|text|attr|graph-in|all [--tenant <id>]
  remem-admin inspect check --data-dir <path> [--tenant <id>]
  remem-admin inspect spaces --data-dir <path> [--tenant <id>]
  remem-admin inspect key --data-dir <path> <hex key>
  remem-admin tenants inventory --data-dir <path> [--implicit-tenant <id>]
  remem-admin version

The data directory must not be in use: Pebble holds an exclusive lock, so stop
the server first, or point the command at a backup checkpoint taken from a live
one (.backups/pre-<unix>/).

discovery backfill queues work rather than doing it: the rows it writes are
ordinary discovery jobs, which the server drains when it next starts.

export writes the portable snapshot of spec §39 — the only backup format Remem
has, because a copy of Pebble's own files is not an escape route from Pebble
(Invariant 12). import reads one, recomputing the embeddings of any snapshot
this binary's model did not write: Rust pools the model's output differently, so
its vectors are the right width in the wrong space. verify compares a snapshot
against a live store and exits 2 when they disagree, which is the step between
an import and decommissioning the system the file came from.

rebuild rebuilds a derived index. It joins "vector rebuild" and "text rebuild"
rather than replacing them, and adds the two that had no command: the attribute
rows and the in-edge index.

inspect check reports every row a correct sequence of writes cannot leave
behind — a key that does not parse, a record body that does not decode, a
derived row naming a record that does not exist, a record with no attribute
row, and every graph inconsistency — and exits 2 when it finds one. inspect
spaces reports where the bytes are. inspect key decodes one key a finding named.
None of them prints memory content.

tenants inventory lists the tenants a directory holds and exits 2 when it holds
any beyond the one a single-tenant build serves. It is the step before upgrading
to such a build, which refuses to start over a directory it cannot serve all of.

A build that serves one tenant serves the tenant REMEM_TENANT_DEFAULT names, and
refuses to operate on any other: --tenant naming a second one is refused, and so
is a directory or a snapshot holding one. The exception is export, which reads a
tenant this build does not serve because it is how that tenant leaves — it
changes nothing and says so. docs/MIGRATION.md has the procedure.
`)
}
