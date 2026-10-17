package users

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
)

// Two names that a person reads as the same name must fold to one key, or the
// uniqueness the name index exists for holds only for ASCII.
func TestNamesThatReadAlikeFoldAlike(t *testing.T) {
	for _, tc := range []struct{ a, b string }{
		{"Planner", "planner"},
		{"PLANNER", "planner"},
		{"Straße", "STRASSE"},       // full case folding, not lower-casing
		{"caf\u00e9", "cafe\u0301"}, // composed and decomposed é
		{"ΣΊΣΥΦΟΣ", "σίσυφος"},      // final sigma
	} {
		fa, err := foldName(tc.a)
		if err != nil {
			t.Fatalf("%q: %v", tc.a, err)
		}
		fb, err := foldName(tc.b)
		if err != nil {
			t.Fatalf("%q: %v", tc.b, err)
		}
		if fa != fb {
			t.Errorf("%q folds to %q and %q to %q; a person reads them as one name", tc.a, fa, tc.b, fb)
		}
	}
}

func TestDistinctNamesStayDistinct(t *testing.T) {
	fa, _ := foldName("planner")
	fb, _ := foldName("planner-2")
	if fa == fb {
		t.Fatal("two different names folded to one key")
	}
}

func TestMalformedNamesAreRefusedByName(t *testing.T) {
	for _, tc := range []struct{ name, why string }{
		{"", "empty"},
		{strings.Repeat("a", 65), "64"},
		{" planner", "space"},
		{"planner ", "space"},
		{"plan\nner", "control"},
		{"plan\x00ner", "control"},
		{"\xff", "UTF-8"},
	} {
		_, err := foldName(tc.name)
		if !errs.Is(err, errs.Invalid) {
			t.Errorf("%q: got %v, want Invalid", tc.name, err)
			continue
		}
		if !strings.Contains(err.Error(), tc.why) {
			t.Errorf("%q: the refusal %q does not say why (%s)", tc.name, err, tc.why)
		}
	}
}

// The limit is on characters a person counts, not bytes.
func TestTheLengthLimitCountsCharacters(t *testing.T) {
	if _, err := foldName(strings.Repeat("é", 64)); err != nil {
		t.Fatalf("64 two-byte characters were refused: %v", err)
	}
}

// A name an operator reads as another name must be refused or fold to it:
// /history shows names, and two users who both read as "planner" would let one
// pass for the other. Invisible format characters are refused outright, and
// compatibility forms fold to the plain letters they display as.
func TestNamesThatLookAlikeCannotCoexist(t *testing.T) {
	for _, invisible := range []string{
		"plan\u200bner", // zero-width space
		"plan\u200dner", // zero-width joiner
		"\u202eplanner", // right-to-left override
		"planner\u2066", // left-to-right isolate
		"plan\ufeffner", // byte-order mark
	} {
		if _, err := foldName(invisible); !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "invisible") {
			t.Errorf("%q: got %v, want Invalid naming the invisible character", invisible, err)
		}
	}
	plain, _ := foldName("planner")
	for _, compat := range []string{"\uff50\uff4c\uff41\uff4e\uff4e\uff45\uff52", "\uff30\uff2c\uff21\uff2e\uff2e\uff25\uff32"} {
		got, err := foldName(compat)
		if err != nil || got != plain {
			t.Errorf("%q folds to %q (%v), want %q", compat, got, err, plain)
		}
	}
	lig, _ := foldName("o\ufb00ice")
	if office, _ := foldName("office"); lig != office {
		t.Errorf("the ligature ff folds to %q, plain to %q", lig, office)
	}
}
