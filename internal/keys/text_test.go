package keys_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestAPostingKeyIsItsTermRangeFollowedByTheRecordID pins the one thing
// [keys.TextRecordID] assumes: every posting of a term begins with the lower
// bound of that term's range and ends with the record's sixteen raw bytes, with
// nothing in between and nothing after.
//
// Three builders have to agree for that to hold — Text, TextTermRange and
// PrefixRange — and the whole point of TextRecordID is that a keyword search
// reads the record id out of the tail rather than decoding the key, which costs
// three string allocations per posting. The saving is real and the agreement is
// not free, so it is asserted here: if any of the three ever spells a posting
// differently, this fails in this package rather than returning wrong record ids
// from a search.
func TestAPostingKeyIsItsTermRangeFollowedByTheRecordID(t *testing.T) {
	const (
		tn tenant.ID        = "acme"
		ns tenant.Namespace = tenant.DefaultNamespace
	)
	rid := id.New()
	terms := []string{
		"a",                          // one byte
		"alfa",                       // an ordinary content word
		"\x01atlas",                  // a tag, in internal/text's reserved prefix
		"\x00d",                      // a reserved row
		strings.Repeat("x", 128),     // the longest term internal/text writes
		strings.Repeat("\xff", 128),  // one whose prefix has no successor
		strings.Repeat("y", 200),     // longer than the uvarint term length's first byte
		"a term with spaces in it",   // nothing about a term is escaped
		"\x00\x01\x02 control bytes", // nor are control bytes
	}
	for _, term := range terms {
		lower, upper := keys.TextTermRange(tn, ns, term)
		key := keys.Text(tn, ns, term, rid)

		if len(key) != len(lower)+len(id.Zero) {
			t.Fatalf("term %q: its posting key is %d bytes and its range's lower bound %d, "+
				"so the record id is not the last %d bytes", term, len(key), len(lower), len(id.Zero))
		}
		if !bytes.Equal(key[:len(lower)], lower) {
			t.Fatalf("term %q: its posting key does not begin with its range's lower bound", term)
		}
		if !bytes.Equal(key[len(lower):], rid[:]) {
			t.Fatalf("term %q: its posting key does not end with the record's raw bytes", term)
		}
		if bytes.Compare(key, lower) < 0 || (upper != nil && bytes.Compare(key, upper) >= 0) {
			t.Fatalf("term %q: its own posting key falls outside the range a scan reads it under", term)
		}

		got, err := keys.TextRecordID(key, lower)
		if err != nil || got != rid {
			t.Fatalf("term %q: reading the record out of its own posting key gave %v, %v", term, got, err)
		}
	}
}

// TextRecordID agrees with ParseText about which record a posting names. The
// cheap reader and the decoder are two spellings of one fact, and a corpus read
// through one and rebuilt through the other must not disagree.
func TestTextRecordIDAgreesWithParseText(t *testing.T) {
	const (
		tn tenant.ID        = "acme"
		ns tenant.Namespace = tenant.DefaultNamespace
	)
	for _, term := range []string{"a", "alfa", "\x01atlas", strings.Repeat("z", 150)} {
		rid := id.New()
		lower, _ := keys.TextTermRange(tn, ns, term)
		key := keys.Text(tn, ns, term, rid)

		cheap, err := keys.TextRecordID(key, lower)
		if err != nil {
			t.Fatalf("term %q: %v", term, err)
		}
		gotT, gotNS, gotTerm, decoded, err := keys.ParseText(key)
		if err != nil {
			t.Fatalf("term %q: %v", term, err)
		}
		if cheap != decoded || gotTerm != term || gotT != tn || gotNS != ns {
			t.Fatalf("term %q: TextRecordID read %v, ParseText read %v for %q under %q/%q",
				term, cheap, decoded, gotTerm, gotT, gotNS)
		}
	}
}

// A key inside a term's range that is not one of its postings is corruption, not
// something to hand back a record id for. It cannot happen without a writer this
// package does not know about, which is exactly why it is checked: the
// alternative is a search scoring another term's postings as this one's.
func TestAForeignKeyInsideATermsRangeIsCorruption(t *testing.T) {
	const (
		tn tenant.ID        = "acme"
		ns tenant.Namespace = tenant.DefaultNamespace
	)
	lower, _ := keys.TextTermRange(tn, ns, "alfa")
	idLen := len(id.Zero)
	for name, key := range map[string][]byte{
		"a key that is only the prefix":    bytes.Clone(lower),
		"a key with a short record id":     append(bytes.Clone(lower), 1, 2, 3),
		"a key with a long record id":      append(bytes.Clone(lower), make([]byte, idLen+1)...),
		"a key from another term's prefix": append(bytes.Clone(lower[:len(lower)-1]), make([]byte, idLen+1)...),
		"an empty key":                     {},
	} {
		if _, err := keys.TextRecordID(key, lower); !errs.Is(err, errs.Corruption) {
			t.Fatalf("%s: got %v, want corruption", name, err)
		}
	}
}
