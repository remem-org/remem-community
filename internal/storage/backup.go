package storage

import "context"

// Backupper is a [KV] that can take a consistent copy of itself into a
// directory.
//
// It is an optional interface rather than a method on KV because it is not part
// of what the storage abstraction exists for (spec §10, and the four reasons in
// this package's doc comment). Only one caller needs it — the migration runner,
// before the first step that rewrites data — and only two implementations can
// honour it.
//
// # Consistent, not merely copied
//
// The contract is that reopening dest yields the store as it was at some point
// during the call, with no torn write and nothing missing that Get would have
// returned. That is a stronger promise than "the files were copied", and it is
// the only promise worth making: a pre-migration backup that will not open is
// worse than no backup, because an operator plans around it.
//
// Implementations therefore use whatever mechanism their engine provides for a
// consistent copy rather than walking the filesystem. Pebble has one; the
// in-memory store writes a [Dump], which is consistent because it holds a read
// lock for the duration.
//
// dest must not already exist. Overwriting a backup is never what the caller
// meant, and the runner names each one after the moment it was taken.
type Backupper interface {
	Backup(ctx context.Context, dest string) error
}
