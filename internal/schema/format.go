// Package schema owns the on-disk format manifest and the gate that decides
// whether this binary may open a directory.
//
// It is the answer to the question an operator asks at three in the morning:
// why will the server not start against this data? The manifest is the one
// place that knows, and the error it produces names the format, the two
// version numbers, and which direction to move.
//
// The manifest and the gate live here; the registry and the runner that move
// an older directory forward live beside them in migration.go and runner.go,
// and the slot registry in slots.go. The manifest came first because it is what
// has to exist before anything writes a byte of user data.
package schema

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/version"
)

// formatPrefix is where manifest rows live, under the untenanted system space.
// The manifest describes the whole directory, and it has to be readable before
// any tenant is known — which at open time is always.
const formatPrefix = "/format/"

// rowLen is the encoded width of a [Subsystem]: three big-endian uint32s.
//
// The manifest is deliberately not wrapped in a versioned envelope the way a
// record is. It is the root of the versioning scheme, so an envelope would only
// move the question — something has to be readable without first consulting a
// version, and this is it. A row that is not exactly this long is corruption.
const rowLen = 12

// Subsystem is one durable format's entry in the manifest (spec §19.2).
//
// Current is the version in force. MinReader and MinWriter are the oldest
// binary that may read and write the directory, and they are what allows a
// format bump that did not change how existing bytes are interpreted to leave
// older binaries running. They default to Current, so a bump made without
// thinking about compatibility locks older binaries out rather than letting
// them in.
type Subsystem struct {
	Current   uint32
	MinReader uint32
	MinWriter uint32
}

func (s Subsystem) String() string {
	return fmt.Sprintf("current=%d min_reader=%d min_writer=%d", s.Current, s.MinReader, s.MinWriter)
}

// Format is the manifest: one entry per versioned durable format.
type Format struct {
	Subsystems map[string]Subsystem
}

// CurrentFormat is the manifest this binary stamps on a new directory.
func CurrentFormat(v version.Versions) Format {
	f := Format{Subsystems: make(map[string]Subsystem, len(version.FormatNames()))}
	for _, name := range version.FormatNames() {
		cur, _ := v.Get(name)
		f.Subsystems[name] = Subsystem{Current: cur, MinReader: cur, MinWriter: cur}
	}
	return f
}

// NeedsMigration reports whether any format this binary writes is older, or
// absent, on disk — and therefore whether [Runner] has work to do. Absent reads
// as zero, which is older than anything.
func (f Format) NeedsMigration(want version.Versions) bool {
	for _, name := range version.FormatNames() {
		supported, _ := want.Get(name)
		if f.Subsystems[name].Current < supported {
			return true
		}
	}
	return false
}

// String renders the manifest in a stable order, for logs and for
// `remem-admin inspect`.
func (f Format) String() string {
	names := make([]string, 0, len(f.Subsystems))
	for name := range f.Subsystems {
		names = append(names, name)
	}
	sort.Strings(names)

	out := ""
	for i, name := range names {
		if i > 0 {
			out += " "
		}
		out += name + "{" + f.Subsystems[name].String() + "}"
	}
	return out
}

// ReadFormat reads the manifest.
//
// A directory with no manifest rows is not an error: it is a new directory, or
// one written before the manifest existed. Both present as an empty Format, and
// both are handled identically by [Open].
func ReadFormat(ctx context.Context, kv storage.KV) (Format, error) {
	const op = "schema.ReadFormat"

	lo, hi := keys.PrefixRange(keys.System(formatPrefix))
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()

	f := Format{Subsystems: map[string]Subsystem{}}
	for ok := it.First(); ok; ok = it.Next() {
		name, err := rowName(it.Key(), op)
		if err != nil {
			return Format{}, err
		}
		s, err := decodeRow(it.Value(), name, op)
		if err != nil {
			return Format{}, err
		}
		f.Subsystems[name] = s
	}
	if err := it.Error(); err != nil {
		return Format{}, err
	}
	return f, nil
}

// WriteFormat replaces the manifest, atomically.
//
// Atomicity matters more here than the size of the data suggests: a manifest
// half-written by an interrupted upgrade is a directory that refuses to open
// and cannot explain why.
func WriteFormat(ctx context.Context, kv storage.KV, f Format) error {
	const op = "schema.WriteFormat"

	existing, err := ReadFormat(ctx, kv)
	if err != nil {
		return err
	}

	b := kv.NewBatch()
	defer func() { _ = b.Close() }()

	stageFormat(b, existing, f)
	if err := b.Commit(ctx, true); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	return nil
}

// writer is the part of a batch or a transaction that stages rows. It exists so
// the manifest can be written on its own ([WriteFormat]) or alongside other
// work in one transaction — which is what the migration runner needs, so that
// advancing a version and marking its step done cannot come apart.
type writer interface {
	Set(key, value []byte)
	Delete(key []byte)
}

// stageFormat stages the difference between two manifests into w.
func stageFormat(w writer, existing, f Format) {
	for name := range existing.Subsystems {
		if _, keep := f.Subsystems[name]; !keep {
			w.Delete(keys.System(formatPrefix + name))
		}
	}
	for name, s := range f.Subsystems {
		w.Set(keys.System(formatPrefix+name), encodeRow(s))
	}
}

func encodeRow(s Subsystem) []byte {
	b := make([]byte, 0, rowLen)
	b = binary.BigEndian.AppendUint32(b, s.Current)
	b = binary.BigEndian.AppendUint32(b, s.MinReader)
	return binary.BigEndian.AppendUint32(b, s.MinWriter)
}

func decodeRow(b []byte, name, op string) (Subsystem, error) {
	if len(b) != rowLen {
		// Canonical data. A short row is damage, and reading it as a zero
		// version would silently declare the directory ancient and migrate it.
		return Subsystem{}, errs.E(errs.Corruption, op,
			fmt.Errorf("the manifest row for %q is %d bytes, want %d", name, len(b), rowLen))
	}
	return Subsystem{
		Current:   binary.BigEndian.Uint32(b[0:4]),
		MinReader: binary.BigEndian.Uint32(b[4:8]),
		MinWriter: binary.BigEndian.Uint32(b[8:12]),
	}, nil
}

// rowName extracts the format name from a manifest key.
func rowName(k []byte, op string) (string, error) {
	_, _, space, err := keys.ParseSpace(k)
	if err != nil {
		return "", err
	}
	if space != keys.SpaceSystem {
		return "", errs.E(errs.Corruption, op, errors.New("a manifest scan returned a key outside the system space"))
	}
	// The key is the system prefix followed by "/format/<name>"; the scan was
	// bounded by that prefix, so whatever trails it is the name.
	prefix := keys.System(formatPrefix)
	if len(k) <= len(prefix) {
		return "", errs.E(errs.Corruption, op, errors.New("a manifest row has no format name"))
	}
	return string(k[len(prefix):]), nil
}
