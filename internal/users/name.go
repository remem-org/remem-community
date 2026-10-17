package users

import (
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/remem-org/remem-go/internal/errs"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// MaxNameLen is the longest user name, in characters rather than bytes: a limit
// a person has to stay under should count what a person counts.
const MaxNameLen = 64

// foldName validates a user name and returns the form uniqueness is decided on.
//
// The folded form is NFKC, then full Unicode case folding, then NFKC again,
// because folding can leave a string unnormalised ("ẞ" folds to "ss", and a
// combining mark can end up beside a different base). Compatibility rather than
// canonical normalisation, because a fullwidth "ｐｌａｎｎｅｒ" and a ligature
// "ﬀ" display as the plain letters and must not be a second name. Lower-casing
// would not do either: it leaves "Straße" and "STRASSE" as two names, and a
// person reads them as one. The user row keeps the name as it was written; only
// the index key is folded.
//
// What this does not catch is a confusable from another script: Latin "a" and
// Cyrillic "а" stay two names. Names are created by the operator, never by the
// caller they name, and an id rather than a name is what audit events record.
func foldName(name string) (string, error) {
	const op = "users.name"
	invalid := func(why string) error {
		return errs.E(errs.Invalid, op, fmt.Errorf("the user name %q %s", name, why))
	}

	if !utf8.ValidString(name) {
		return "", errs.E(errs.Invalid, op, errors.New("the user name is not valid UTF-8"))
	}
	n := utf8.RuneCountInString(name)
	switch {
	case n == 0:
		return "", errs.E(errs.Invalid, op, errors.New("the user name is empty"))
	case n > MaxNameLen:
		return "", invalid(fmt.Sprintf("is %d characters; the limit is %d", n, MaxNameLen))
	}
	first, _ := utf8.DecodeRuneInString(name)
	last, _ := utf8.DecodeLastRuneInString(name)
	if unicode.IsSpace(first) || unicode.IsSpace(last) {
		return "", invalid("begins or ends with a space, which would make two names look alike")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", invalid(fmt.Sprintf("contains the control character %U", r))
		}
		// Format characters (Unicode Cf) render as nothing or reorder what
		// surrounds them: a zero-width space, a joiner, a bidi override. A name
		// carrying one displays exactly as a name without it, so two users would
		// read as one person in /history.
		if unicode.Is(unicode.Cf, r) {
			return "", invalid(fmt.Sprintf("contains the invisible formatting character %U", r))
		}
	}
	return norm.NFKC.String(cases.Fold().String(norm.NFKC.String(name))), nil
}
