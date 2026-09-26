package text

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Tokenisation is fixed in code, not configured (see
// docs/architecture/text.md). These constants decide which terms are written
// into the postings key space, so changing one changes what a query returns
// rather than how fast it returns — which is the same reason RRF's k and
// widen_max_factor are constants here. An operator who could turn one of these
// would produce a corpus where memories written before the change are findable
// by a term and memories written after are not, with no error anywhere.
const (
	// MinTermRunes is the shortest Latin-script term indexed. A one-letter
	// term matches most of the corpus and costs a posting per record for it.
	//
	// It is deliberately not applied to CJK or to emoji, where one character
	// is a word.
	MinTermRunes = 2

	// MaxTermBytes bounds a term, and therefore a key. A token longer than
	// this is folded rather than dropped — see [foldLong] — because dropping
	// it makes a memory unfindable by the one word it actually contains.
	MaxTermBytes = 128

	// maxTermPrefixBytes is how much of an over-long token survives verbatim
	// in its folded form. The rest of the budget is the separator and the
	// digest that keeps two long tokens with a shared prefix distinct.
	maxTermPrefixBytes = 96

	// MaxTerms bounds one query. A query is a sentence, not a corpus, and a
	// term costs a scan.
	MaxTerms = 64
)

// Reserved term prefixes.
//
// The tokeniser emits only letters, digits, marks and symbols, so no term it
// produces can begin with a byte below 0x20. That is what lets the text key
// space hold rows that are not postings without a second space or a second
// key shape — and TestNoContentTermCanImpersonateAReservedRow is what keeps it
// true.
const (
	// tagPrefix marks a tag posting. Tags share the index with content and are
	// ordinary terms in a reserved namespace (plan §II.10 row 2), which is what
	// removes Rust's `tag_index_can_answer` and its 100-byte limit: a tag of
	// any length has a posting list, so a filter on one never falls back to
	// scanning the payload.
	tagPrefix = "\x01"
	// docTerm is the reserved term of a record's own term row.
	docTerm = "\x00d"
	// statsTerm is the reserved term of a tenant's corpus statistics.
	statsTerm = "\x00s"
)

// Tokenize turns text into the terms indexed for it, in order and with
// duplicates: a term's frequency in a document is what BM25 scores, so the
// repetition is the signal rather than noise to be deduplicated away.
//
// The same function analyses content on the way in and a query on the way out.
// That is not a tidiness preference — two analysers that disagree write one
// term and look up another, and the symptom is a corpus that simply returns
// nothing for words it plainly contains.
func Tokenize(s string) []string { return relax(s) }

// QueryTerms analyses a query.
//
// It differs from [Tokenize] in two ways, both about not returning an
// unanswerable query. Stop words are put back when removing them would leave
// nothing — "to be or not to be" is a real query, and an optimisation that
// turns it into no query at all has stopped optimising — and the list is capped
// at MaxTerms, because each term costs a scan.
func QueryTerms(s string) []string {
	terms := relax(s)
	if len(terms) > MaxTerms {
		terms = terms[:MaxTerms]
	}
	return terms
}

// relax analyses text, loosening the rules only when they would leave nothing.
//
// The strict pass drops stop words and one-rune terms, which is right for
// almost everything: a posting for "the" on every memory costs a write per
// record and buys a term that tells a ranking nothing.
//
// It is wrong when it empties the text. A memory whose whole content is "the
// and of", or "a", would then have no postings and could not be found by the
// only words it contains — a memory that exists and cannot be found, which is
// the failure this codebase keeps refusing to ship. The end-to-end run of Phase
// 8 stored exactly that and searched for exactly that, and got nothing back.
//
// The two sides relax together, and that is the part that matters. Whatever
// makes a document unindexable makes the query that would have found it
// unanswerable, so index and query must loosen at the same point or they do not
// meet.
func relax(s string) []string {
	if terms := tokenize(s, strict); len(terms) > 0 {
		return terms
	}
	if terms := tokenize(s, keepStopWords); len(terms) > 0 {
		return terms
	}
	return tokenize(s, keepEverything)
}

// rules is how much the tokeniser is allowed to discard.
type rules uint8

const (
	// strict drops stop words and terms below MinTermRunes.
	strict rules = iota
	// keepStopWords keeps them, for text that is nothing else.
	keepStopWords
	// keepEverything also keeps a one-rune term, for text that is nothing else.
	keepEverything
)

// TagTerm is the term a tag is indexed and looked up under.
//
// Tags arrive already trimmed and lower-cased from the write path, and are
// otherwise stored whole: a tag is a label somebody chose, and splitting
// "release notes" into two terms would make it match a memory tagged "notes".
func TagTerm(tag string) string {
	return tagPrefix + foldLong(norm.NFC.String(strings.ToLower(strings.TrimSpace(tag))))
}

// tokenize is the shared pipeline, under the given rules.
func tokenize(s string, mode rules) []string {
	s = norm.NFC.String(s)

	var out []string
	emit := func(term string) {
		if term == "" {
			return
		}
		if mode == strict && stopWords[term] {
			return
		}
		out = append(out, foldLong(term))
	}

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case isCJK(r):
			end := i
			for end < len(s) {
				r, size := utf8.DecodeRuneInString(s[end:])
				if !isCJK(r) {
					break
				}
				end += size
			}
			emitCJK(s[i:end], emit)
			i = end

		case isWordRune(r):
			end := i
			for end < len(s) {
				r, size := utf8.DecodeRuneInString(s[end:])
				if !isWordRune(r) || isCJK(r) {
					break
				}
				end += size
			}
			word := strings.ToLower(s[i:end])
			if mode == keepEverything || utf8.RuneCountInString(word) >= MinTermRunes {
				emit(word)
			}
			i = end

		case isSymbolStart(r):
			end := symbolClusterEnd(s, i)
			emit(s[i:end])
			i = end

		default:
			i += size
		}
	}
	return out
}

// emitCJK indexes a run of ideographs and kana as both unigrams and
// overlapping bigrams.
//
// Neither alone is enough, and the reason is what a user does with the two.
// Unigrams alone make "東京" (Tokyo) match any memory containing 東 or 京
// separately, which over a Japanese corpus is close to everything. Bigrams
// alone make a one-character query — a surname, a single kanji — match nothing,
// because no bigram equals it. Emitting both costs roughly twice the postings
// on CJK text and buys complete recall with the precision a bigram gives.
//
// A dictionary-based segmenter would beat this, and would put a language model
// and its vocabulary in the write path of every memory. That is a decision for
// evidence rather than for a rewrite.
func emitCJK(run string, emit func(string)) {
	runes := []rune(run)
	for i, r := range runes {
		emit(string(r))
		if i+1 < len(runes) {
			emit(string(runes[i : i+2]))
		}
	}
}

// isWordRune is a rune that continues a word: letters, digits, and the
// combining marks that belong to them.
//
// Marks are included because NFC composes only what has a precomposed form.
// Devanagari, Arabic and Thai keep theirs separate, and a mark treated as a
// separator would cut those words into pieces at every vowel sign.
func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) ||
		unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Mc, r)
}

// isCJK reports a script written without spaces, so that a run of it is
// segmented rather than taken whole.
func isCJK(r rune) bool {
	return unicode.Is(unicode.Han, r) ||
		unicode.Is(unicode.Hiragana, r) ||
		unicode.Is(unicode.Katakana, r) ||
		unicode.Is(unicode.Hangul, r)
}

// isSymbolStart reports a rune that begins an emoji or other standalone
// symbol. Emoji are terms: a memory marked 🎉 is findable by 🎉.
func isSymbolStart(r rune) bool { return unicode.Is(unicode.So, r) }

// symbolClusterEnd finds the end of the emoji cluster starting at s[i].
//
// One emoji is frequently several runes — a base symbol, a skin-tone modifier,
// a variation selector, or two symbols joined by a zero-width joiner — and
// splitting them makes 👍🏽 index as 👍 plus a modifier nobody will ever search
// for. Go's standard library has no grapheme segmenter, and this is the part of
// one that matters here.
func symbolClusterEnd(s string, i int) int {
	_, size := utf8.DecodeRuneInString(s[i:])
	end := i + size
	for end < len(s) {
		r, size := utf8.DecodeRuneInString(s[end:])
		switch {
		case r == zeroWidthJoiner:
			// A joiner binds whatever follows into the same emoji, so the
			// next rune is consumed with it rather than examined.
			next, nextSize := utf8.DecodeRuneInString(s[end+size:])
			if nextSize == 0 || !(isSymbolStart(next) || unicode.IsLetter(next)) {
				return end
			}
			end += size + nextSize
		case unicode.Is(unicode.Sk, r) || unicode.Is(unicode.Mn, r) || r == variationSelector16:
			end += size
		default:
			return end
		}
	}
	return end
}

const (
	zeroWidthJoiner     = '‍'
	variationSelector16 = '️'
)

// foldLong bounds a term without losing it.
//
// A term is a key, and a key must be bounded: a base64 blob or a minified
// script pasted into a memory is one token of arbitrary length. Truncating
// alone would make two long tokens sharing a prefix into one term, which is a
// prefix match wearing an exact match's clothes — so the folded form is the
// prefix, a separator no token can contain, and a digest of the whole token.
// Index and query fold identically, so the lookup stays exact.
func foldLong(term string) string {
	if len(term) <= MaxTermBytes {
		return term
	}
	sum := sha256.Sum256([]byte(term))
	prefix := term[:maxTermPrefixBytes]
	// Truncate to a rune boundary so the term stays valid UTF-8 and a log line
	// holding it stays readable.
	for len(prefix) > 0 && !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix + "\x00" + hex.EncodeToString(sum[:8])
}
