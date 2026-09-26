package pebble

import (
	"context"
	"errors"
	"os"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

var _ storage.Backupper = (*Store)(nil)

// Backup writes a consistent copy of the store into dest.
//
// It is a Pebble checkpoint, not a file copy, and the difference is the whole
// reason this method exists. A recursive copy of a directory Pebble has open
// captures the LOCK file and a write-ahead log mid-write, and the result is a
// directory that may refuse to open — which for a pre-migration backup is the
// one failure that matters, because an operator has planned around it being
// there.
//
// A checkpoint is also nearly free: sstables are hard-linked rather than
// copied, so the backup costs a manifest and the current WAL rather than the
// size of the corpus. That is what makes it affordable to take one before every
// data-rewriting migration instead of asking an operator to remember.
//
// The plan for this phase said to port Rust's recursive copy verbatim. Rust
// owned its own WAL and SSTable tree; this data directory *is* an open Pebble
// database, and the honest port of "copy the data directory" is "ask the engine
// for a consistent copy".
func (s *Store) Backup(_ context.Context, dest string) error {
	const op = "pebble.Backup"

	if s.closed.Load() {
		return errs.E(errs.Unavailable, op, errClosed)
	}
	if dest == "" {
		return errs.E(errs.Invalid, op, errors.New("a backup needs a destination"))
	}
	// Checkpoint refuses a destination that exists, but it reports it as an
	// engine error, and this package never lets one of those out.
	if _, err := os.Stat(dest); err == nil {
		return errs.E(errs.Invalid, op, errors.New("the backup destination already exists; "+
			"overwriting a backup is never what the caller meant"))
	} else if !os.IsNotExist(err) {
		return errs.E(errs.Storage, op, err)
	}

	// WithFlushedWAL, and it is load-bearing rather than defensive. This
	// adapter writes with NoSync — durability is the transaction's decision,
	// taken once by the operator through storage.sync_writes — so recent writes
	// live in a WAL buffer that has not reached the file a checkpoint copies.
	// Without this, the backup silently omits exactly the writes an operator
	// made just before starting the upgrade. It is what
	// TestAPebbleBackupReopensAndHoldsThePreMigrationRecords caught.
	if err := s.db.Checkpoint(dest, pebbledb.WithFlushedWAL()); err != nil {
		return translate(op, err)
	}
	return nil
}
