package main

import (
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/keys"
)

// TestEveryDerivedSpaceHasARebuildCommand is the stopped-server half of
// Invariant 3. internal/server holds every derived space to a job; this holds
// it to an `--index` name, because a job queue is not reachable with the
// process down and a damaged index on a stopped server still needs a repair.
func TestEveryDerivedSpaceHasARebuildCommand(t *testing.T) {
	for _, s := range keys.AllSpaces() {
		if s.Class() != keys.Derived {
			continue
		}
		name, ok := spaceIndexName(s)
		if !ok {
			t.Errorf("space %s is derived and no `remem-admin rebuild --index` rebuilds it", s)
			continue
		}
		if !slices.Contains(rebuildIndexes, name) {
			t.Errorf("space %s maps to --index %q, which rebuildIndexes does not list", s, name)
		}
	}
}
