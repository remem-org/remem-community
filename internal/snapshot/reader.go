package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/remem-org/remem-go/internal/errs"
	"google.golang.org/protobuf/proto"
)

// Reader streams a snapshot one block at a time.
//
// Peak memory is one block, not one corpus: the exporter pages a thousand
// records into each, so a 250,000-record snapshot is read in a few hundred
// bounded steps. That is the only reason a logical dump is a viable backup
// format at all.
//
// Every failure it can report is [errs.Corruption] or [errs.IncompatibleVersion]
// and names where the file stopped being readable. A snapshot is the last copy
// of data by the time anyone reads one, so "this file is bad" is never a
// sufficient message.
type Reader struct {
	src    *bufio.Reader
	header *Header
	comp   Compression
	dec    *zstd.Decoder

	// offset is how many bytes have been consumed, so a failure can say where.
	offset int64
	// last is the section of the previous block, which is what makes a
	// companion section checkable: one is only meaningful immediately after
	// the block it annotates.
	last Section

	// blocks and byteTotal accumulate what the trailer is checked against.
	blocks    uint32
	byteTotal uint64
	crcOfCrcs []byte

	done bool
}

// Block is one framed section of a snapshot.
//
// Payload is the concatenation of length-delimited protobuf messages that the
// block carries, already decompressed and already checked against its recorded
// checksum. It is owned by the caller until the next [Reader.Next].
type Block struct {
	Section Section
	Payload []byte
	// Offset is where this block's frame began, which is what a resume cursor
	// stores and what a truncation message names.
	Offset int64
	// Index is the block's ordinal, counting from zero.
	Index uint32
}

// Each splits a block into the messages it carries.
//
// The framing is a varint length followed by that many bytes, repeated until
// the payload is exhausted — prost's `encode_length_delimited` on the writing
// side. A length that runs past the end of the payload is corruption naming the
// message ordinal, never a short iteration that returns what it managed to read.
func (b *Block) Each(fn func(raw []byte) error) error {
	const op = "snapshot.Block.Each"
	for off, n := 0, 0; off < len(b.Payload); n++ {
		size, adv := binary.Uvarint(b.Payload[off:])
		if adv <= 0 {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"block %d (%s): message %d has an unreadable length prefix at byte %d of the block",
				b.Index, b.Section, n, off))
		}
		start := off + adv
		end := start + int(size)
		if size > uint64(len(b.Payload)) || end > len(b.Payload) || end < start {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"block %d (%s): message %d claims %d bytes but only %d remain in the block",
				b.Index, b.Section, n, size, len(b.Payload)-start))
		}
		if err := fn(b.Payload[start:end]); err != nil {
			return err
		}
		off = end
	}
	return nil
}

// NewReader validates the preamble and decodes the header.
//
// It reads nothing beyond the header, so a caller that only wants to know what
// a file claims — `remem-admin verify`, or an operator checking a transfer
// arrived — pays for the header and stops.
func NewReader(r io.Reader) (*Reader, error) {
	const op = "snapshot.NewReader"
	src := bufio.NewReaderSize(r, 1<<20)

	pre := make([]byte, preambleLen)
	if n, err := io.ReadFull(src, pre); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errShortRead(op, "opening header", preambleLen, n)
		}
		return nil, errs.E(errs.Storage, op, err)
	}
	if got := string(pre[:magicLen]); got != Magic {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"this is not a Remem snapshot: it opens with %q where every snapshot opens with %q",
			got, Magic))
	}
	version := binary.LittleEndian.Uint16(pre[magicLen:])
	if version != FormatVersion {
		return nil, errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"snapshot format version %d, and this binary reads version %d. A newer snapshot is "+
				"refused rather than partially read: upgrade Remem to a build that lists version "+
				"%d, or re-export from the version that wrote it",
			version, FormatVersion, version))
	}
	comp, err := parseCompression(pre[magicLen+2], op)
	if err != nil {
		return nil, err
	}
	hlen := binary.LittleEndian.Uint32(pre[magicLen+3:])
	if hlen > MaxBlockBytes {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the header claims %d bytes, past the %d-byte ceiling: the file is damaged in its "+
				"first %d bytes", hlen, MaxBlockBytes, preambleLen))
	}
	raw := make([]byte, hlen)
	if n, err := io.ReadFull(src, raw); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errShortRead(op, "header", int(hlen), n)
		}
		return nil, errs.E(errs.Storage, op, err)
	}
	header := &Header{}
	if err := proto.Unmarshal(raw, header); err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf("the header does not decode: %w", err))
	}

	rd := &Reader{
		src:       src,
		header:    header,
		comp:      comp,
		offset:    int64(preambleLen) + int64(hlen),
		crcOfCrcs: make([]byte, 0, 64),
	}
	if comp == CompressionZstd {
		// One goroutine: decompress stops reading as soon as a block exceeds its
		// declared size, and a concurrent stream decoder would still be working
		// ahead on bytes nobody will read.
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, errs.E(errs.Storage, op, err)
		}
		rd.dec = dec
	}
	return rd, nil
}

// Header returns what the snapshot claims about itself: who wrote it, when, and
// how many of each thing it holds. The counts are a claim, not a proof — every
// caller that cares compares them against what it actually read.
func (r *Reader) Header() *Header { return r.header }

// Compression reports how the blocks are stored.
func (r *Reader) Compression() Compression { return r.comp }

// Next returns the next block, or io.EOF once the trailer has been read and
// checked.
//
// The trailer is not optional and is not skipped at EOF: a file that simply
// stops after its last block is a truncated file, and the difference between
// that and a complete one is the whole reason a trailer exists.
func (r *Reader) Next() (*Block, error) {
	const op = "snapshot.Reader.Next"
	if r.done {
		return nil, io.EOF
	}

	// A block frame opens with a section byte, 1..6; the trailer opens with
	// "REMEMEND", whose first byte is 'R' and is not a section. Peeking is what
	// lets one stream hold both without a length field for the block sequence.
	peek, err := r.src.Peek(trailerMagicLen)
	switch {
	case err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF):
		return nil, errs.E(errs.Storage, op, err)
	case len(peek) == 0:
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"the snapshot ends after %d complete block(s), at byte %d, with no trailer. It was "+
				"truncated in transfer or the export never finished; re-export or re-transfer it",
			r.blocks, r.offset))
	case bytes.HasPrefix([]byte(TrailerMagic), peek):
		// A complete match is the trailer. A prefix of it, cut short, is a file
		// truncated inside the trailer — which is a *complete* set of blocks
		// and worth saying, because it is the one truncation that costs no data.
		if len(peek) < trailerMagicLen {
			return nil, errShortRead(op, "trailer", trailerLen, len(peek))
		}
		return nil, r.readTrailer()
	}

	frameStart := r.offset
	frame := make([]byte, blockFrameLen)
	if n, err := io.ReadFull(r.src, frame); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, r.truncated(op, frameStart, "block frame header", blockFrameLen, n)
		}
		return nil, errs.E(errs.Storage, op, err)
	}
	section, err := ParseSection(frame[0], op)
	if err != nil {
		return nil, err
	}
	ulen := binary.LittleEndian.Uint32(frame[1:])
	clen := binary.LittleEndian.Uint32(frame[5:])
	want := binary.LittleEndian.Uint32(frame[9:])
	if clen > MaxBlockBytes || ulen > MaxBlockBytes {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"block %d (%s) at byte %d claims %d stored / %d uncompressed bytes, past the "+
				"%d-byte ceiling: its frame header is damaged",
			r.blocks, section, frameStart, clen, ulen, MaxBlockBytes))
	}
	r.offset += int64(blockFrameLen)

	// The body is read through a limit rather than into a slice of the claimed
	// size. clen is a number from the file and MaxBlockBytes is 512 MiB, so
	// allocating it up front let a 57-byte file make the reader take half a
	// gigabyte before discovering there was no body.
	stored, err := io.ReadAll(io.LimitReader(r.src, int64(clen)))
	if err != nil {
		return nil, errs.E(errs.Storage, op, err)
	}
	if len(stored) < int(clen) {
		return nil, r.truncated(op, frameStart, "block body", int(clen), len(stored))
	}
	r.offset += int64(clen)

	if got := checksum(stored); got != want {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"block %d (%s) at byte %d fails its checksum: recorded %#08x, computed %#08x. "+
				"The %d block(s) before it read cleanly, so the damage starts here",
			r.blocks, section, frameStart, want, got, r.blocks))
	}

	payload := stored
	if r.comp == CompressionZstd {
		payload, err = r.decompress(stored, ulen)
		if err != nil {
			return nil, errs.E(errs.Corruption, op, fmt.Errorf(
				"block %d (%s) at byte %d passed its checksum but does not decompress: %w",
				r.blocks, section, frameStart, err))
		}
	}
	switch {
	case uint64(len(payload)) > uint64(ulen):
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"block %d (%s) at byte %d declares %d uncompressed bytes and decompresses to more; "+
				"it was not expanded further",
			r.blocks, section, frameStart, ulen))
	case uint32(len(payload)) != ulen:
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"block %d (%s) at byte %d declares %d uncompressed bytes and produced %d",
			r.blocks, section, frameStart, ulen, len(payload)))
	}

	// A companion section annotates the block before it, and only that one.
	// Checking it here rather than in the importer is what lets the importer
	// hold one page rather than a map of the whole corpus.
	if annotates, ok := section.Annotates(); ok && r.last != annotates {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"block %d at byte %d is a %s block, which annotates the %s block before it, and the "+
				"block before it is %s. The blocks were reordered, or the export is from a "+
				"version that framed them differently",
			r.blocks, frameStart, section, annotates, describePrevious(r.blocks, r.last)))
	}

	b := &Block{Section: section, Payload: payload, Offset: frameStart, Index: r.blocks}
	r.last = section
	r.blocks++
	r.byteTotal += uint64(blockFrameLen) + uint64(clen)
	r.crcOfCrcs = binary.LittleEndian.AppendUint32(r.crcOfCrcs, want)
	return b, nil
}

// truncated reports a file that stopped inside a block, naming the last block
// that was whole. That is the one fact an operator needs: a snapshot cut at
// block 900 of 1,000 is 900 blocks of recoverable data, and a message that only
// says "corrupt" throws them away.
// decompress expands one block's stored bytes, reading at most one byte past
// what the frame declares.
//
// The checksum covers the *stored* bytes and anyone can compute CRC-32, so a
// block that passes it proves nothing about how far it expands. DecodeAll has
// no ceiling of its own that fits a block — a 104 KiB block declaring 64 bytes
// allocated 2.8 GiB before its length was compared — so the stream is read
// through a limit instead, and the caller refuses anything that reached it.
// The limit rather than a decoder window cap, because a single-segment frame
// legitimately declares a window as large as its content.
func (r *Reader) decompress(stored []byte, ulen uint32) ([]byte, error) {
	if err := r.dec.Reset(bytes.NewReader(stored)); err != nil {
		return nil, err
	}
	return io.ReadAll(io.LimitReader(r.dec, int64(ulen)+1))
}

func (r *Reader) truncated(op string, frameStart int64, what string, want, got int) error {
	return errs.E(errs.Corruption, op, fmt.Errorf(
		"the snapshot ends inside the %s of block %d, at byte %d: wanted %d more bytes, found %d. "+
			"The last complete block is %s",
		what, r.blocks, frameStart, want, got, describeLastBlock(r.blocks)))
}

func describePrevious(blocks uint32, last Section) string {
	if blocks == 0 {
		return "nothing — it is the first block"
	}
	return last.String()
}

func describeLastBlock(blocks uint32) string {
	if blocks == 0 {
		return "none — the file ends before its first block"
	}
	return fmt.Sprintf("block %d", blocks-1)
}

// readTrailer consumes and checks the trailer, then marks the reader done.
func (r *Reader) readTrailer() error {
	const op = "snapshot.Reader.readTrailer"
	buf := make([]byte, trailerLen)
	if n, err := io.ReadFull(r.src, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errShortRead(op, "trailer", trailerLen, n)
		}
		return errs.E(errs.Storage, op, err)
	}
	r.offset += int64(trailerLen)
	r.done = true

	blocks := binary.LittleEndian.Uint32(buf[trailerMagicLen:])
	total := binary.LittleEndian.Uint64(buf[trailerMagicLen+4:])
	crcs := binary.LittleEndian.Uint32(buf[trailerMagicLen+12:])

	switch {
	case blocks != r.blocks:
		return errs.E(errs.Corruption, op, fmt.Errorf(
			"the trailer counts %d block(s) and %d were read: the file is missing blocks or "+
				"carries extra ones", blocks, r.blocks))
	case total != r.byteTotal:
		return errs.E(errs.Corruption, op, fmt.Errorf(
			"the trailer accounts for %d block byte(s) and %d were read", total, r.byteTotal))
	case crcs != checksum(r.crcOfCrcs):
		return errs.E(errs.Corruption, op, fmt.Errorf(
			"the trailer's checksum over every block checksum does not match: recorded %#08x, "+
				"computed %#08x. Every block passed its own checksum, so the blocks were "+
				"reordered or one was substituted wholesale", crcs, checksum(r.crcOfCrcs)))
	}

	// Anything after the trailer is not part of this snapshot. Reporting it
	// matters because the common cause is two snapshots concatenated by a
	// shell redirect, which otherwise imports the first and silently drops the
	// second.
	if extra, err := r.src.Peek(1); err == nil && len(extra) > 0 {
		return errs.E(errs.Corruption, op, errors.New(
			"the file continues past its trailer: this looks like two snapshots concatenated, "+
				"of which only the first would be imported"))
	}
	return io.EOF
}

// Close releases the decompressor. It is safe to call more than once.
func (r *Reader) Close() error {
	if r.dec != nil {
		r.dec.Close()
		r.dec = nil
	}
	return nil
}

// Blocks reports how many blocks have been read so far. It is what a resume
// cursor records alongside the byte offset.
func (r *Reader) Blocks() uint32 { return r.blocks }

// Offset reports how many bytes have been consumed.
func (r *Reader) Offset() int64 { return r.offset }

// ReadHeader decodes just the header of a snapshot file's bytes.
func ReadHeader(r io.Reader) (*Header, error) {
	rd, err := NewReader(r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rd.Close() }()
	return rd.Header(), nil
}

// SplitMessages returns each length-delimited message in a payload. It is Each
// collected, for the tests and the small callers that want a slice.
func SplitMessages(b *Block) ([][]byte, error) {
	var out [][]byte
	err := b.Each(func(raw []byte) error {
		out = append(out, bytes.Clone(raw))
		return nil
	})
	return out, err
}
