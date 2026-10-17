package snapshot

import (
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
)

// A binary from before users has no section 41 in its table, and must refuse a
// snapshot carrying one rather than restore it without its users. Removing the
// entry is exactly the table such a binary has.
func TestAnOlderSectionTableRefusesTheUsersSection(t *testing.T) {
	if _, err := ParseSection(byte(SectionGoUsers), "test"); err != nil {
		t.Fatalf("this binary refuses its own users section: %v", err)
	}
	saved := sectionNames[SectionGoUsers]
	delete(sectionNames, SectionGoUsers)
	defer func() { sectionNames[SectionGoUsers] = saved }()

	if _, err := ParseSection(41, "test"); !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("an older table read section 41 as %v, want IncompatibleVersion", err)
	}
}
