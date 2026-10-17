package vector_test

import (
	"flag"
	"os"
	"testing"
)

// rapid defaults to 100 iterations and the project's floor for a property test
// is 1,000 (CLAUDE.md, Testing). It is raised here rather than left to a flag,
// and set before flag.Parse, so an explicit -rapid.checks still wins.
func TestMain(m *testing.M) {
	if f := flag.Lookup("rapid.checks"); f != nil {
		if err := f.Value.Set("1000"); err != nil {
			panic("raising the rapid check count: " + err.Error())
		}
	}
	os.Exit(m.Run())
}
