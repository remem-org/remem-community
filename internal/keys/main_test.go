package keys_test

import (
	"flag"
	"os"
	"testing"
)

// Phase 2's completion criteria require at least 1,000 iterations for float
// ordering, float round-trip and string escaping. rapid defaults to 100, and a
// criterion that only holds when someone remembers a flag is not a criterion —
// so the default is raised here instead.
//
// This runs before flag.Parse, so an explicit -rapid.checks on the command line
// still wins: the tighter setting is a floor, not a ceiling.
func TestMain(m *testing.M) {
	if f := flag.Lookup("rapid.checks"); f != nil {
		if err := f.Value.Set("1000"); err != nil {
			panic("raising the rapid check count: " + err.Error())
		}
	}
	os.Exit(m.Run())
}
