package version_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/version"
)

// The edition a binary was built as is decided by the `business` build tag, and
// reported. wantEdition comes from a pair of tag-split test files rather than
// from the constant under test, so the test fails when the two disagree: a
// business build that called itself community, or the reverse, is a release
// shipped under the wrong name.
func TestEditionIsReported(t *testing.T) {
	if version.Edition != wantEdition {
		t.Fatalf("this build reports the %q edition, and was built as %q", version.Edition, wantEdition)
	}
}
