package memkv

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

var _ storage.Backupper = (*Store)(nil)

// DumpFile is the name of the file a backup of this store writes into its
// destination directory.
const DumpFile = "memkv.dump"

// Backup writes the store's contents into dest as a single [storage.Dump].
//
// An in-memory store has no files to copy, so its backup is the portable
// key-value stream instead. That is worth having rather than refusing: it is
// what lets the migration runner's backup path — take a copy, then rewrite data
// — be exercised in an ordinary unit test rather than only against Pebble,
// and it is the same stream a migration fixture is stored in.
//
// The dump is taken through a snapshot, so it is consistent for the same reason
// Pebble's checkpoint is: concurrent writes are either wholly in it or wholly
// out.
func (s *Store) Backup(ctx context.Context, dest string) error {
	const op = "memkv.Backup"

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errs.E(errs.Unavailable, op, errClosed)
	}
	if dest == "" {
		return errs.E(errs.Invalid, op, errors.New("a backup needs a destination"))
	}
	if _, err := os.Stat(dest); err == nil {
		return errs.E(errs.Invalid, op, errors.New("the backup destination already exists; "+
			"overwriting a backup is never what the caller meant"))
	} else if !os.IsNotExist(err) {
		return errs.E(errs.Storage, op, err)
	}

	if err := os.MkdirAll(dest, 0o755); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	f, err := os.Create(filepath.Join(dest, DumpFile))
	if err != nil {
		return errs.E(errs.Storage, op, err)
	}
	defer func() { _ = f.Close() }()

	if err := storage.Dump(ctx, s, f); err != nil {
		return err
	}
	// The backup exists to survive a crash during the migration that follows,
	// so it must not live only in page cache while that migration rewrites the
	// originals.
	if err := f.Sync(); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	return nil
}
