package schema

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
)

// BackupsDir is where pre-migration backups live, *inside* the data directory.
//
// Inside rather than beside, and that is Rust's decision kept for Rust's
// reason: a sibling of the data directory is not guaranteed to be on the same
// volume. When data_dir is a bind mount, a sibling path is the container's
// writable overlay, and the backup an operator is relying on disappears with
// `docker compose down`.
//
// Backups are never deleted automatically. Disk is cheaper than the corpus.
const BackupsDir = ".backups"

// SkipBackupEnv opts out of the pre-migration backup.
//
// It exists for the operator who has already taken their own snapshot, and for
// a corpus large enough that a second copy will not fit. It is an environment
// variable rather than a configuration key deliberately: it is a decision about
// one upgrade, not a standing property of the deployment.
const SkipBackupEnv = "REMEM_SKIP_MIGRATION_BACKUP"

// BackUp copies the store into <dataDir>/.backups/pre-<unix>/ before a
// migration rewrites data.
//
// It returns the destination, or "" when no backup was taken — because the
// operator opted out, or because the directory holds nothing worth copying.
// Neither is an error.
//
// The copy is asked of the engine ([storage.Backupper]) rather than taken by
// walking the filesystem. A recursive copy of a directory the engine has open
// captures a lock file and a write-ahead log mid-write, and produces a backup
// that may not open — which is the one failure a pre-migration backup cannot
// have, because an operator plans around it being there.
func BackUp(ctx context.Context, kv storage.KV, dataDir string, now time.Time) (string, error) {
	const op = "schema.BackUp"

	if os.Getenv(SkipBackupEnv) == "1" {
		return "", nil
	}

	holds, err := holdsUserData(kv)
	if err != nil {
		return "", err
	}
	if !holds {
		// A manifest and a tenant row are not data. Backing up a fresh install
		// would leave a pile of empty directories that nothing ever deletes.
		return "", nil
	}

	b, ok := kv.(storage.Backupper)
	if !ok {
		return "", errs.E(errs.Invalid, op, fmt.Errorf(
			"this storage engine cannot take a consistent copy of itself, and a migration that rewrites data "+
				"will not run without one. Set %s=1 to proceed after taking your own backup", SkipBackupEnv))
	}

	dest := filepath.Join(dataDir, BackupsDir, "pre-"+strconv.FormatInt(now.Unix(), 10))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return "", errs.E(errs.Storage, op, fmt.Errorf("creating the backup directory: %w", err))
	}
	if _, err := os.Stat(dest); err == nil {
		// A backup for this second already exists — two restarts inside one
		// second, which a crash loop makes ordinary. It is not overwritten and
		// it is not an error: the older copy is the pre-migration one, which is
		// the copy worth keeping. Refusing here would let a crash loop turn a
		// recoverable upgrade into a stuck one.
		return dest, nil
	} else if !os.IsNotExist(err) {
		return "", errs.E(errs.Storage, op, err)
	}
	if err := b.Backup(ctx, dest); err != nil {
		return "", err
	}
	return dest, nil
}

// holdsUserData reports whether the store holds anything a migration could
// destroy.
//
// System rows — the format manifest, the tenant directory, the slot schema, the
// migration state — use an empty tenant and an empty namespace, so they sort
// ahead of every user key. Everything above the system range is therefore user
// data, and one iterator step answers the question.
func holdsUserData(kv storage.KV) (bool, error) {
	_, systemUpper := keys.SystemRange()

	it := kv.NewIterator(systemUpper, nil)
	defer func() { _ = it.Close() }()

	found := it.First()
	if err := it.Error(); err != nil {
		return false, err
	}
	return found, nil
}
