package pebble

import (
	"errors"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/errs"
)

// translate turns a Pebble error into a classified Remem error.
//
// This file exists on its own because spec §58 forbids a Pebble error reaching
// a caller, and the import-graph guard in internal/arch cannot enforce it: the
// guard sees imports, and a leaked error is a *value* that travels through an
// `error` interface with no import at the receiving end. Nothing but
// discipline and TestPebbleErrorsAreNeverExposed stands here.
//
// The error's text is discarded rather than wrapped. A Pebble message names
// sstables, sequence numbers and manifest offsets; a Remem operator can act on
// none of it, and it is exactly the implementation detail the interface exists
// to hide. Detail belongs in the log the adapter emits, not in the returned
// error.
func translate(op string, err error) error {
	switch {
	case err == nil:
		return nil

	case errors.Is(err, pebbledb.ErrNotFound):
		return errs.E(errs.NotFound, op, errNotFound)

	case errors.Is(err, pebbledb.ErrCorruption):
		// Never retryable: retrying cannot repair a byte (Invariant 3). A
		// caller that sees this reports the key and stops.
		return errs.E(errs.Corruption, op, errCorruption)

	case errors.Is(err, pebbledb.ErrClosed):
		return errs.E(errs.Unavailable, op, errClosed)

	default:
		// Storage is the honest default: the store failed and the data is
		// intact as far as anyone here knows, which makes it retryable.
		return errs.E(errs.Storage, op, errStorage)
	}
}

// The causes are fixed values rather than the Pebble error, so that no Pebble
// string can travel out through Error() either.
var (
	errNotFound   = errors.New("key not found")
	errCorruption = errors.New("stored data is unreadable")
	errClosed     = errors.New("store is closed")
	errStorage    = errors.New("the storage engine failed")
)
