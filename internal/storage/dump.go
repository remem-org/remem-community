package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/remem-org/remem-go/internal/errs"
)

// The portable key-value stream: a magic, a version byte, then length-prefixed
// pairs in key order until the reader is exhausted.
//
//	"RMMD" <version:1> ( <klen:uvarint><key> <vlen:uvarint><value> )*
//
// It is deliberately the simplest thing that can be diffed and regenerated. It
// is not a snapshot format — that is proto/snapshot/v1, the one surface shared
// with Rust Remem — and nothing in the product reads or writes it. It exists
// for two jobs: it is the in-memory store's backup, and it is what lets a
// migration fixture be a readable file rather than a checked-in Pebble
// directory whose format is *Pebble's* version rather than Remem's.
const (
	dumpMagic   = "RMMD"
	dumpVersion = 1
)

// Dump writes every pair in kv to w, in key order.
//
// Two dumps of the same content are byte-identical, which is what makes a
// fixture reviewable in a diff and two backups comparable.
func Dump(ctx context.Context, kv KV, w io.Writer) error {
	const op = "storage.Dump"

	bw := bufio.NewWriter(w)
	if _, err := bw.WriteString(dumpMagic); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	if err := bw.WriteByte(dumpVersion); err != nil {
		return errs.E(errs.Storage, op, err)
	}

	it := kv.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()

	var hdr [binary.MaxVarintLen64]byte
	write := func(b []byte) error {
		n := binary.PutUvarint(hdr[:], uint64(len(b)))
		if _, err := bw.Write(hdr[:n]); err != nil {
			return err
		}
		_, err := bw.Write(b)
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err := write(it.Key()); err != nil {
			return errs.E(errs.Storage, op, err)
		}
		if err := write(it.Value()); err != nil {
			return errs.E(errs.Storage, op, err)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	return nil
}

// Restore reads a stream written by [Dump] into kv.
//
// It writes through a batch, so a stream that fails halfway leaves the store as
// it was rather than half-populated — a store that looks restored and is not is
// worse than one that plainly refused.
func Restore(ctx context.Context, kv KV, r io.Reader) error {
	const op = "storage.Restore"

	br := bufio.NewReader(r)

	header := make([]byte, len(dumpMagic)+1)
	if _, err := io.ReadFull(br, header); err != nil {
		return errs.E(errs.Corruption, op,
			fmt.Errorf("the stream is shorter than its %d-byte header", len(header)))
	}
	if string(header[:len(dumpMagic)]) != dumpMagic {
		return errs.E(errs.Corruption, op,
			fmt.Errorf("the stream does not begin with %q; it is not a Remem key-value dump", dumpMagic))
	}
	if v := header[len(dumpMagic)]; v != dumpVersion {
		if v > dumpVersion {
			return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
				"the stream is version %d and this binary reads version %d; the pairs after the header may be "+
					"framed differently, and a partial restore is a store that looks populated and is not",
				v, dumpVersion))
		}
		return errs.E(errs.Corruption, op, fmt.Errorf("stream version %d is not one this binary knows", v))
	}

	b := kv.NewBatch()
	defer func() { _ = b.Close() }()

	read := func() ([]byte, error) {
		n, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	for {
		key, err := read()
		if errors.Is(err, io.EOF) {
			break // a clean end: the last pair was complete
		}
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("reading a key: %w", err))
		}
		value, err := read()
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("reading the value of a key: %w", err))
		}
		b.Set(key, value)
	}
	return b.Commit(ctx, true)
}
