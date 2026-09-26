package attr_test

import (
	"flag"
	"os"
	"testing"
)

// The floor for property tests is 1,000 iterations, raised here rather than
// left to a flag: a criterion that only holds when someone remembers a
// command-line argument is not a criterion.
//
// This runs before flag.Parse, so an explicit -rapid.checks still wins. The
// tighter setting is a floor, not a ceiling.
func TestMain(m *testing.M) {
	if f := flag.Lookup("rapid.checks"); f != nil {
		if err := f.Value.Set("1000"); err != nil {
			panic("raising the rapid check count: " + err.Error())
		}
	}
	os.Exit(m.Run())
}
