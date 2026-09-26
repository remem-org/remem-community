package snapshot_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot"
)

// allocatedDuring reports how many bytes fn allocated on the heap.
func allocatedDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// frameAfterHeader returns a valid preamble and header with no blocks yet, and
// the compression it declared, so a test can append a hand-built frame.
func preambleAndHeader(t *testing.T, comp snapshot.Compression) []byte {
	t.Helper()
	full := build(t, comp, header())
	// A snapshot with no blocks is preamble + header + trailer. The trailer is
	// what Next reads first when no block follows, so cut it off.
	r, err := snapshot.NewReader(bytes.NewReader(full))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return full[:r.Offset()]
}

// frame is a block frame header: section, uncompressed length, stored length,
// checksum of the stored bytes. The layout is the one reader.go decodes.
func frame(section snapshot.Section, ulen, clen, crc uint32) []byte {
	b := []byte{byte(section)}
	b = binary.LittleEndian.AppendUint32(b, ulen)
	b = binary.LittleEndian.AppendUint32(b, clen)
	return binary.LittleEndian.AppendUint32(b, crc)
}

// TestAFrameClaimingAHugeBodyDoesNotAllocateIt: a file of a few dozen bytes
// whose frame claims a 512 MiB body must be refused as truncated without the
// reader first allocating the 512 MiB it was promised.
func TestAFrameClaimingAHugeBodyDoesNotAllocateIt(t *testing.T) {
	raw := append(preambleAndHeader(t, snapshot.CompressionNone),
		frame(snapshot.SectionRecords, snapshot.MaxBlockBytes, snapshot.MaxBlockBytes, 0)...)

	var err error
	n := allocatedDuring(func() {
		r, rerr := snapshot.NewReader(bytes.NewReader(raw))
		if rerr != nil {
			err = rerr
			return
		}
		_, err = r.Next()
	})
	if errs.KindOf(err) != errs.Corruption {
		t.Fatalf("a frame with no body: got %v (%v), want Corruption", errs.KindOf(err), err)
	}
	if n > 16<<20 {
		t.Fatalf("refusing a %d-byte file allocated %d MiB: the reader trusted the frame's "+
			"length before reading a byte of the body", len(raw), n>>20)
	}
}

// TestADecompressionBombIsRefused: a block whose stored bytes are a small zstd
// frame expanding past MaxBlockBytes, with a correct checksum — the checksum is
// over the stored bytes and anyone can compute CRC-32 — must be refused as
// Corruption without the reader expanding it.
func TestADecompressionBombIsRefused(t *testing.T) {
	var bomb bytes.Buffer
	enc, err := zstd.NewWriter(&bomb, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(enc, zeros{}, int64(snapshot.MaxBlockBytes)+1<<20); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	stored := bomb.Bytes()

	raw := append(preambleAndHeader(t, snapshot.CompressionZstd),
		frame(snapshot.SectionRecords, 64, uint32(len(stored)), crc32.ChecksumIEEE(stored))...)
	raw = append(raw, stored...)

	var rerr error
	n := allocatedDuring(func() {
		r, err := snapshot.NewReader(bytes.NewReader(raw))
		if err != nil {
			rerr = err
			return
		}
		_, rerr = r.Next()
	})
	if errs.KindOf(rerr) != errs.Corruption {
		t.Fatalf("a %d KiB bomb: got %v (%v), want Corruption", len(stored)>>10, errs.KindOf(rerr), rerr)
	}
	if n > 64<<20 {
		t.Fatalf("refusing a %d KiB block that declares 64 uncompressed bytes allocated %d MiB",
			len(stored)>>10, n>>20)
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
