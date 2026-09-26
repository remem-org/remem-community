package snapshot

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

// Writer streams a snapshot out one block at a time.
//
// The header is written first and carries final counts, which are only known
// once the whole walk is done — so a caller that cannot count in advance stages
// its blocks somewhere and assembles the file afterwards. [Export] does exactly
// that, into a scratch file beside the output: peak memory stays one block, and
// the cost is bounded disk rather than bounded RAM.
type Writer struct {
	dst  *bufio.Writer
	comp Compression
	enc  *zstd.Encoder

	blocks    uint32
	byteTotal uint64
	crcOfCrcs []byte
	closed    bool
}

// WriterOpts configures a [Writer].
type WriterOpts struct {
	// Compression defaults to zstd. This format exists to leave a portable copy
	// of a data directory behind, and paying compression's CPU once for a
	// smaller durable artifact is the right default for that job.
	Compression Compression
}

// NewWriter writes the preamble and header, then returns a writer positioned at
// the first block.
//
// It takes the header rather than accumulating one, because the header is the
// snapshot's promise about itself and a promise assembled from what happened to
// be written is not a check on anything.
func NewWriter(w io.Writer, h *Header, opts WriterOpts) (*Writer, error) {
	const op = "snapshot.NewWriter"
	if h == nil {
		return nil, errs.E(errs.Invalid, op, errNilHeader)
	}
	if _, err := parseCompression(byte(opts.Compression), op); err != nil {
		return nil, err
	}

	raw, err := proto.Marshal(h)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the header: %w", err))
	}
	dst := bufio.NewWriterSize(w, 1<<20)
	pre := make([]byte, preambleLen)
	copy(pre, Magic)
	putUint16(pre[magicLen:], FormatVersion)
	pre[magicLen+2] = byte(opts.Compression)
	putUint32(pre[magicLen+3:], uint32(len(raw)))
	if _, err := dst.Write(pre); err != nil {
		return nil, errs.E(errs.Storage, op, err)
	}
	if _, err := dst.Write(raw); err != nil {
		return nil, errs.E(errs.Storage, op, err)
	}

	out := &Writer{dst: dst, comp: opts.Compression, crcOfCrcs: make([]byte, 0, 64)}
	if opts.Compression == CompressionZstd {
		// SpeedDefault, matching the Rust exporter's zstd level 0. The
		// exporter's job is a durable artifact, not a fast one, and the level
		// is not a setting: a snapshot must decompress with any conforming
		// reader, and the level does not change that.
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, errs.E(errs.Storage, op, err)
		}
		out.enc = enc
	}
	return out, nil
}

// WriteMessages frames msgs as one block.
//
// An empty slice writes nothing rather than an empty block. A section with
// nothing in it is not a fact about the corpus, and a reader that met an empty
// block would have to decide whether it meant "none" or "the exporter stopped".
func (w *Writer) WriteMessages(section Section, msgs []proto.Message) error {
	const op = "snapshot.Writer.WriteMessages"
	if len(msgs) == 0 {
		return nil
	}
	var payload []byte
	for i, m := range msgs {
		raw, err := proto.Marshal(m)
		if err != nil {
			return errs.E(errs.Invalid, op, fmt.Errorf(
				"encoding message %d of the %s block: %w", i, section, err))
		}
		payload = binary.AppendUvarint(payload, uint64(len(raw)))
		payload = append(payload, raw...)
	}
	return w.WriteBlock(section, payload)
}

// WriteBlock frames an already-encoded payload as one block.
func (w *Writer) WriteBlock(section Section, payload []byte) error {
	const op = "snapshot.Writer.WriteBlock"
	if w.closed {
		return errs.E(errs.Invalid, op, fmt.Errorf("the snapshot is already closed"))
	}
	if _, err := ParseSection(byte(section), op); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	if len(payload) > MaxBlockBytes {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"a %s block of %d bytes is past the %d-byte ceiling; page the export more finely",
			section, len(payload), MaxBlockBytes))
	}

	stored := payload
	if w.comp == CompressionZstd {
		stored = w.enc.EncodeAll(payload, make([]byte, 0, len(payload)/3+64))
	}
	crc := checksum(stored)

	frame := make([]byte, blockFrameLen)
	frame[0] = byte(section)
	putUint32(frame[1:], uint32(len(payload)))
	putUint32(frame[5:], uint32(len(stored)))
	putUint32(frame[9:], crc)
	if _, err := w.dst.Write(frame); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	if _, err := w.dst.Write(stored); err != nil {
		return errs.E(errs.Storage, op, err)
	}

	w.blocks++
	w.byteTotal += uint64(blockFrameLen) + uint64(len(stored))
	w.crcOfCrcs = binary.LittleEndian.AppendUint32(w.crcOfCrcs, crc)
	return nil
}

// Close writes the trailer and flushes.
//
// The trailer is what separates a complete snapshot from a truncated one, so it
// is written here rather than by the caller: a close path that could forget it
// would produce files that import cleanly on the day they are written and fail
// on the day they are needed.
func (w *Writer) Close() error {
	const op = "snapshot.Writer.Close"
	if w.closed {
		return nil
	}
	w.closed = true
	if w.enc != nil {
		defer func() { _ = w.enc.Close() }()
	}

	buf := make([]byte, trailerLen)
	copy(buf, TrailerMagic)
	putUint32(buf[trailerMagicLen:], w.blocks)
	putUint64(buf[trailerMagicLen+4:], w.byteTotal)
	putUint32(buf[trailerMagicLen+12:], checksum(w.crcOfCrcs))
	if _, err := w.dst.Write(buf); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	if err := w.dst.Flush(); err != nil {
		return errs.E(errs.Storage, op, err)
	}
	return nil
}

// Blocks reports how many blocks have been written.
func (w *Writer) Blocks() uint32 { return w.blocks }
