package text_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/text"
)

// TestUnicodeTokenisation is the plan's test: CJK, accented Latin, emoji and
// mixed scripts tokenise and match.
//
// "Match" is the operative word. Every case asserts that a query built from the
// same text finds the terms the content produced, because that is the only
// property a user can observe — a tokeniser that is self-consistently wrong
// still returns nothing.
func TestUnicodeTokenisation(t *testing.T) {
	cases := []struct {
		name    string
		content string
		query   string
		want    []string // terms the query must find in the content's terms
	}{
		{
			name:    "accented Latin folds case and keeps the accent",
			content: "Café Münster serves Crème Brûlée",
			query:   "CAFÉ crème",
			want:    []string{"café", "crème"},
		},
		{
			name:    "a decomposed accent matches a composed one",
			content: "Cafe\u0301 opens early", // e followed by a combining acute
			query:   "caf\u00e9",              // one precomposed rune
			want:    []string{"caf\u00e9"},
		},
		{
			name:    "CJK segments without spaces",
			content: "東京は日本の首都です",
			query:   "東京",
			want:    []string{"東京"},
		},
		{
			name:    "a single CJK character is findable inside a longer run",
			content: "東京タワー",
			query:   "東",
			want:    []string{"東"},
		},
		{
			name:    "emoji are terms of their own",
			content: "shipped the release 🎉 at last",
			query:   "🎉",
			want:    []string{"🎉"},
		},
		{
			name:    "an emoji with a skin tone modifier stays one term",
			content: "code review 👍🏽 approved",
			query:   "👍🏽",
			want:    []string{"👍🏽"},
		},
		{
			name:    "mixed scripts split at the script boundary",
			content: "Kubernetes は cluster を管理する",
			query:   "kubernetes 管理",
			want:    []string{"kubernetes", "管理"},
		},
		{
			name:    "an identifier splits into its parts and is found whole",
			content: "paid invoice INV-2024-8871 yesterday",
			query:   "INV-2024-8871",
			want:    []string{"inv", "2024", "8871"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			indexed := text.Tokenize(tc.content)
			asked := text.QueryTerms(tc.query)

			for _, want := range tc.want {
				if !slices.Contains(asked, want) {
					t.Errorf("the query %q produced %v, which does not include %q", tc.query, asked, want)
				}
				if !slices.Contains(indexed, want) {
					t.Errorf("the content %q produced %v, which does not include %q", tc.content, indexed, want)
				}
			}
			for _, term := range asked {
				if !slices.Contains(indexed, term) {
					t.Errorf("the query term %q is not among the content's terms %v: "+
						"the two sides do not agree, so this query cannot match this memory", term, indexed)
				}
			}
		})
	}
}

// A term is what a key holds, so a token no caller bounded must not become a
// key no store should hold. It is folded rather than dropped, because dropping
// it makes the memory unfindable by the one word it actually contains.
func TestALongTokenStaysFindableAndBounded(t *testing.T) {
	long := strings.Repeat("supercalifragilistic", 40) // 800 bytes

	indexed := text.Tokenize("prefix " + long + " suffix")
	asked := text.QueryTerms(long)

	if len(asked) != 1 {
		t.Fatalf("a single long token produced %d query terms: %v", len(asked), asked)
	}
	if !slices.Contains(indexed, asked[0]) {
		t.Fatalf("the folded long token %q is not among the content's terms", asked[0])
	}
	if got := len(asked[0]); got > text.MaxTermBytes {
		t.Fatalf("the folded term is %d bytes, above the %d-byte bound", got, text.MaxTermBytes)
	}

	// Two long tokens sharing their first bytes must not fold together: a
	// bounded term that collides is a prefix match wearing an exact match's
	// clothes.
	other := long + "differentending"
	if text.QueryTerms(other)[0] == asked[0] {
		t.Fatal("two different long tokens folded to the same term")
	}
}

// Tags are ordinary terms in a reserved namespace (plan §II.10 row 2), which is
// what removes Rust's length special case: a tag of any length has a posting
// list, so a filter on it never falls back to scanning the payload.
func TestATagOfAnyLengthIsATerm(t *testing.T) {
	short := text.TagTerm("weather")
	long := text.TagTerm(strings.Repeat("x", 120))

	if short == long {
		t.Fatal("two different tags produced the same term")
	}
	for _, term := range []string{short, long} {
		if len(term) > text.MaxTermBytes {
			t.Fatalf("the tag term %q is %d bytes, above the %d-byte bound", term, len(term), text.MaxTermBytes)
		}
		if slices.Contains(text.Tokenize("weather "+strings.Repeat("x", 120)), term) {
			t.Fatalf("content produced the tag term %q; the namespaces are not reserved", term)
		}
	}
}

// A query made entirely of stop words must still be answerable. Stripping them
// is a precision optimisation, and an optimisation that turns a query into no
// query at all has stopped optimising.
func TestStopWordsAreStrippedButNeverLeaveAnEmptyQuery(t *testing.T) {
	if terms := text.Tokenize("the quick brown fox"); slices.Contains(terms, "the") {
		t.Fatalf("content kept the stop word \"the\": %v", terms)
	}
	if terms := text.QueryTerms("to be or not to be"); len(terms) == 0 {
		t.Fatal("a query of nothing but stop words produced no terms, so it can never match")
	}
}

// Folding must be a function of the word, not of how it was typed. Two
// spellings of one word have to produce one term, or the term written on the
// way in is not the term looked up on the way out.
func TestFoldingIsCaseAndFormInsensitive(t *testing.T) {
	// "café" precomposed, and "cafe" followed by a combining acute.
	composed := text.Tokenize("Café")
	decomposed := text.Tokenize("Cafe\u0301")
	upper := text.Tokenize("CAFÉ")

	if !slices.Equal(composed, decomposed) {
		t.Errorf("composed %v and decomposed %v are different terms", composed, decomposed)
	}
	if !slices.Equal(composed, upper) {
		t.Errorf("%v and %v are different terms", composed, upper)
	}
}

// A term is what a key holds, so no term may be empty, over the bound, or
// spelled the way a reserved row is. The reserved prefixes are what keep tags,
// the per-record term row and the corpus statistics from colliding with a word
// somebody wrote.
func TestNoContentTermCanImpersonateAReservedRow(t *testing.T) {
	corpus := []string{
		"ordinary words", "\x00 \x01 control bytes", "\u0000embedded", "",
		"   ", "🎉🎊", "東京", strings.Repeat("q", 500),
	}
	for _, in := range corpus {
		for _, term := range text.Tokenize(in) {
			switch {
			case term == "":
				t.Errorf("%q produced an empty term", in)
			case len(term) > text.MaxTermBytes:
				t.Errorf("%q produced a %d-byte term", in, len(term))
			case term[0] < 0x20:
				t.Errorf("%q produced the reserved term %q", in, term)
			}
		}
	}
}

// A memory must be findable by the words it is made of, whatever those words
// are.
//
// The query side already had this fallback: a query of nothing but stop words
// keeps them, because "to be or not to be" is a real query. The index side did
// not, so a memory whose entire content is stop words — or one short word — got
// no postings at all and could not be found by its own exact text. Phase 8's
// end-to-end run stored "the and of" and searched for "the and of", and got
// nothing back.
//
// The two sides must relax together or they do not meet: whatever makes a
// document unindexable makes the query that would have found it unanswerable.
func TestAMemoryOfNothingButStopWordsIsStillFindable(t *testing.T) {
	for _, content := range []string{"the and of", "it is", "a", "I"} {
		indexed := text.Tokenize(content)
		asked := text.QueryTerms(content)

		if len(indexed) == 0 {
			t.Errorf("the content %q produced no terms at all, so the memory is unfindable "+
				"by the only words it contains", content)
			continue
		}
		if len(asked) == 0 {
			t.Errorf("the query %q produced no terms", content)
			continue
		}
		for _, term := range asked {
			if !slices.Contains(indexed, term) {
				t.Errorf("content %q indexes as %q and queries as %q: the two sides do not meet",
					content, indexed, asked)
				break
			}
		}
	}

	// The relaxation applies only where it is needed. Content with anything
	// substantial in it still drops its stop words, or every memory pays a
	// posting for "the".
	if terms := text.Tokenize("the quick brown fox"); slices.Contains(terms, "the") {
		t.Fatalf("ordinary content kept its stop words: %v", terms)
	}
}
