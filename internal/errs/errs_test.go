package errs_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
)

func TestWrappingPreservesKind(t *testing.T) {
	base := errs.E(errs.NotFound, "record.Get", errors.New("no such key"))
	wrapped := fmt.Errorf("memory.Load: %w", base)
	wrappedTwice := fmt.Errorf("api: %w", wrapped)

	if !errs.Is(wrappedTwice, errs.NotFound) {
		t.Fatalf("kind lost through %%w wrapping: %v", wrappedTwice)
	}
	if got := errs.KindOf(wrappedTwice); got != errs.NotFound {
		t.Fatalf("KindOf = %v, want NotFound", got)
	}
	if !errors.Is(wrappedTwice, base) {
		t.Fatalf("errors.Is must still reach the wrapped error")
	}
}

func TestErrorMessageCarriesOperationAndCause(t *testing.T) {
	err := errs.E(errs.Storage, "pebble.Set", errors.New("disk full"))
	msg := err.Error()
	if !strings.Contains(msg, "pebble.Set") || !strings.Contains(msg, "disk full") {
		t.Fatalf("context lost: %q", msg)
	}
}

func TestCorruptionIsNeverRetryable(t *testing.T) {
	err := errs.E(errs.Corruption, "codec.Decode", errors.New("bad envelope"))
	if errs.Retryable(err) {
		t.Fatal("corruption must never be retryable: retrying cannot repair canonical data")
	}
}

func TestStorageIsRetryable(t *testing.T) {
	err := errs.E(errs.Storage, "pebble.Get", errors.New("io timeout"))
	if !errs.Retryable(err) {
		t.Fatal("storage failures are retryable")
	}
}

func TestUnclassifiedErrorIsStorage(t *testing.T) {
	// An adapter error that nobody wrapped is an infrastructure failure until
	// proven otherwise; classifying it must not panic.
	plain := errors.New("something from a dependency")
	if got := errs.KindOf(plain); got != errs.Storage {
		t.Fatalf("KindOf(unwrapped) = %v, want Storage", got)
	}
	if !errs.Is(plain, errs.Storage) {
		t.Fatal("Is(unwrapped, Storage) must be true")
	}
	if !errs.Retryable(plain) {
		t.Fatal("an unclassified error inherits Storage's retryability")
	}
}

func TestNilIsNoKind(t *testing.T) {
	if got := errs.KindOf(nil); got != errs.Unclassified {
		t.Fatalf("KindOf(nil) = %v, want Unclassified", got)
	}
	if errs.Is(nil, errs.Storage) {
		t.Fatal("nil has no kind")
	}
	if errs.Retryable(nil) {
		t.Fatal("nil is not retryable")
	}
}

func TestEWithNilCauseIsNil(t *testing.T) {
	if err := errs.E(errs.Storage, "op", nil); err != nil {
		t.Fatalf("wrapping nil must stay nil, got %v", err)
	}
}

func TestKindsAreNamed(t *testing.T) {
	for _, k := range []errs.Kind{
		errs.NotFound, errs.Conflict, errs.Invalid, errs.Unauthorized, errs.Forbidden,
		errs.Storage, errs.Corruption, errs.Unavailable, errs.MigrationRequired,
		errs.IncompatibleVersion, errs.NotLeader, errs.Transient,
	} {
		if s := k.String(); s == "" || strings.HasPrefix(s, "Kind(") {
			t.Errorf("kind %d has no name: %q", uint8(k), s)
		}
	}
}
