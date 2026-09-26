package hnsw

import (
	"flag"
	"os"
	"testing"
)

// CLAUDE.md's floor is 1,000 iterations, raised here rather than left to a flag
// nobody passes. An explicit -rapid.checks still wins, because this runs before
// flag.Parse: the setting is a floor, not a ceiling.
func TestMain(m *testing.M) {
	if f := flag.Lookup("rapid.checks"); f != nil {
		if err := f.Value.Set("1000"); err != nil {
			panic("raising the rapid check count: " + err.Error())
		}
	}
	os.Exit(m.Run())
}
