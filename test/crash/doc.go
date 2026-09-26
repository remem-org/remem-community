// Package crash holds the tests that kill a real Remem process and inspect what
// it left behind (spec §46.5): an interrupted write, an interrupted migration,
// an interrupted job, an interrupted index rebuild, and a restart.
//
// Every test here is behind the `crash` build tag and runs with `make crash`,
// because each one starts and kills processes for tens of seconds. This file
// carries no tag so that a plain `go vet ./...` finds a package here rather
// than a directory whose every file is excluded.
//
// # Why a real process
//
// Phase 10 wrote down, in a package comment and a commit message, that an
// unsynced recall event survived the process dying because it reached the
// write-ahead log. A `kill -9` lost it. A durability claim reasoned from how the
// storage engine ought to behave is not a durability claim, so these tests send
// SIGKILL to an actual child process at a moment the parent chooses, then reopen
// the directory in the parent and check the invariants against the keyspace.
//
// # Shape
//
// The test binary is both parent and child. A parent re-executes os.Args[0]
// with -test.run=^TestCrashChild$ and REMEM_CRASH_ROLE naming a scenario; the
// child runs it against a real Pebble directory and reports progress on stdout
// lines the parent reads to decide when to kill. No production code knows a
// crash test exists.
package crash
