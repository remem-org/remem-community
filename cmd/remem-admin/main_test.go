package main

import (
	"os"
	"testing"
)

// This package's fixtures build their corpora in tenant "acme", and a
// single-tenant build refuses to operate on a tenant it does not serve. So the
// implicit tenant for these tests is "acme", set the way an operator sets it:
// REMEM_TENANT_DEFAULT, the same variable that sets tenant.default on the
// server.
//
// Leaving the fixtures in "acme" and telling the commands so is better than
// renaming every fixture to "default", because it exercises the setting. A
// command that ignored REMEM_TENANT_DEFAULT and hard-coded the built-in default
// would fail most of this package.
//
// The tests that are about more than one tenant override it per test.
func TestMain(m *testing.M) {
	if err := os.Setenv("REMEM_TENANT_DEFAULT", "acme"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
