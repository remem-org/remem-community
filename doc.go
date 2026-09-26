// Package remem is the documentation root of the Remem Go implementation: a
// persistent memory layer for LLMs and agents.
//
// There is no library API at this path. Everything is under internal/, with the
// executables in cmd/. The package exists so `go build ./...`, `go doc` and the
// module's godoc landing page have something to address, and so the repository
// compiles before its first subsystem lands.
//
// The architecture and its twelve invariants are specified in
// "Remem Go Rewrite and Distributed Storage Architecture.md"; the phased
// implementation is in docs/superpowers/plans/.
package remem
