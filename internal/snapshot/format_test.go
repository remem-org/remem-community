package snapshot_test

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"google.golang.org/protobuf/proto"
)

// build writes a small snapshot and returns its bytes.
func build(t *testing.T, comp snapshot.Compression, h *pb.Header, blocks ...blockSpec) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := snapshot.NewWriter(&buf, h, snapshot.WriterOpts{Compression: comp})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, b := range blocks {
		if err := w.WriteMessages(b.section, b.msgs); err != nil {
			t.Fatalf("WriteMessages(%s): %v", b.section, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

type blockSpec struct {
	section snapshot.Section
	msgs    []proto.Message
}

func records(n int) blockSpec {
	msgs := make([]proto.Message, 0, n)
	for i := range n {
		msgs = append(msgs, &pb.Record{
			Tenant:  "default",
			Id:      bytes.Repeat([]byte{byte(i)}, 16),
			Content: strings.Repeat("content ", i%7+1),
			Policy:  "short_term",
		})
	}
	return blockSpec{section: snapshot.SectionRecords, msgs: msgs}
}

func readAll(t *testing.T, b []byte) (*pb.Header, []*snapshot.Block, error) {
	t.Helper()
	r, err := snapshot.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = r.Close() }()
	var out []*snapshot.Block
	for {
		blk, err := r.Next()
		if errors.Is(err, io.EOF) {
			return r.Header(), out, nil
		}
		if err != nil {
			return r.Header(), out, err
		}
		out = append(out, blk)
	}
}

func header() *pb.Header {
	return &pb.Header{
		CreatedAtUnixMs: 1700000000000,
		SourceImpl:      "go",
		SourceVersion:   "test",
		Tenants:         []string{"default"},
		RecordCount:     3,
	}
}

func TestRoundTripThroughTheFraming(t *testing.T) {
	for _, comp := range []snapshot.Compression{snapshot.CompressionNone, snapshot.CompressionZstd} {
		t.Run(comp.String(), func(t *testing.T) {
			raw := build(t, comp, header(), records(3), blockSpec{
				section: snapshot.SectionTenants,
				msgs:    []proto.Message{&pb.Tenant{Id: "default", DisplayName: "default"}},
			})
			h, blocks, err := readAll(t, raw)
			if err != nil {
				t.Fatalf("reading back: %v", err)
			}
			if h.GetSourceImpl() != "go" || h.GetRecordCount() != 3 {
				t.Fatalf("header did not survive: %+v", h)
			}
			if len(blocks) != 2 {
				t.Fatalf("got %d blocks, want 2", len(blocks))
			}
			msgs, err := snapshot.SplitMessages(blocks[0])
			if err != nil {
				t.Fatalf("splitting: %v", err)
			}
			if len(msgs) != 3 {
				t.Fatalf("got %d records, want 3", len(msgs))
			}
			var rec pb.Record
			if err := proto.Unmarshal(msgs[1], &rec); err != nil {
				t.Fatalf("decoding record 1: %v", err)
			}
			if rec.GetContent() != "content content " {
				t.Fatalf("record 1 content is %q", rec.GetContent())
			}
		})
	}
}

// An empty section writes no block. A reader that met an empty block would have
// to guess whether it meant "none" or "the export stopped here".
func TestAnEmptySectionWritesNoBlock(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(),
		blockSpec{section: snapshot.SectionRecords, msgs: nil},
		blockSpec{section: snapshot.SectionVectors, msgs: nil})
	_, blocks, err := readAll(t, raw)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("got %d blocks, want none", len(blocks))
	}
}

func TestUnknownSectionFails(t *testing.T) {
	raw := build(t, snapshot.CompressionNone, header(), records(2))
	// The section byte is the first byte of the first frame, which sits
	// immediately after the preamble and the header protobuf.
	hlen := binary.LittleEndian.Uint32(raw[12:])
	at := 16 + int(hlen)
	if raw[at] != byte(snapshot.SectionRecords) {
		t.Fatalf("frame does not start where the layout says: byte %d is %d", at, raw[at])
	}
	raw[at] = 99

	_, _, err := readAll(t, raw)
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
	for _, want := range []string{"99", "3 (records)", "refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q: %v", want, err)
		}
	}
}

func TestTruncatedSnapshotFailsAtTheBlock(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(4), records(4), records(4))
	_, whole, err := readAll(t, raw)
	if err != nil || len(whole) != 3 {
		t.Fatalf("the intact snapshot did not read: %d blocks, %v", len(whole), err)
	}

	// Cut inside the third block's body: two blocks are whole and the message
	// has to say so, because two blocks of recoverable data is the difference
	// between a recoverable incident and a lost corpus.
	cut := int(whole[2].Offset) + 5
	_, blocks, err := readAll(t, raw[:cut])
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("got %d blocks before the cut, want 2", len(blocks))
	}
	if !strings.Contains(err.Error(), "last complete block is block 1") {
		t.Fatalf("the error does not name the last good block: %v", err)
	}
}

// A file that simply stops after its last block is truncated, and the trailer is
// what makes that distinguishable from a complete one.
func TestASnapshotWithNoTrailerIsTruncated(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(4))
	_, _, err := readAll(t, raw[:len(raw)-24])
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "no trailer") {
		t.Fatalf("the error does not say the trailer is missing: %v", err)
	}
}

func TestCorruptBlockChecksumFails(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(4), records(4))
	_, blocks, err := readAll(t, raw)
	if err != nil {
		t.Fatalf("the intact snapshot did not read: %v", err)
	}
	// Flip one bit inside the second block's stored bytes.
	at := int(blocks[1].Offset) + 13 + 3
	raw[at] ^= 0x01

	_, got, err := readAll(t, raw)
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d clean blocks, want 1", len(got))
	}
	if !strings.Contains(err.Error(), "fails its checksum") {
		t.Fatalf("the error does not name the checksum: %v", err)
	}
}

func TestNewerSnapshotVersionIsRefused(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(2))
	binary.LittleEndian.PutUint16(raw[9:], 2)

	_, err := snapshot.NewReader(bytes.NewReader(raw))
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("want IncompatibleVersion, got %v", err)
	}
	for _, want := range []string{"version 2", "upgrade Remem"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q: %v", want, err)
		}
	}
}

func TestSomethingElseEntirelyIsRefusedByName(t *testing.T) {
	_, err := snapshot.NewReader(strings.NewReader("this is a tar file, honestly, at least 20 bytes"))
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "not a Remem snapshot") {
		t.Fatalf("the refusal does not say what the file is not: %v", err)
	}
}

func TestUnknownCompressionIsRefusedAtTheHeader(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(2))
	raw[11] = 7
	_, err := snapshot.NewReader(bytes.NewReader(raw))
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "compression byte 7") {
		t.Fatalf("the refusal does not name the byte: %v", err)
	}
}

// The trailer's checksum-over-checksums catches a substitution that every
// individual block checksum accepts.
func TestReorderedBlocksFailTheTrailer(t *testing.T) {
	raw := build(t, snapshot.CompressionNone, header(),
		blockSpec{section: snapshot.SectionRecords, msgs: []proto.Message{
			&pb.Record{Tenant: "default", Id: bytes.Repeat([]byte{1}, 16), Content: "aaaa"},
		}},
		blockSpec{section: snapshot.SectionRecords, msgs: []proto.Message{
			&pb.Record{Tenant: "default", Id: bytes.Repeat([]byte{2}, 16), Content: "bbbb"},
		}})
	_, blocks, err := readAll(t, raw)
	if err != nil || len(blocks) != 2 {
		t.Fatalf("the intact snapshot did not read: %v", err)
	}
	first, second := int(blocks[0].Offset), int(blocks[1].Offset)
	size := second - first
	swapped := append([]byte(nil), raw...)
	copy(swapped[first:], raw[second:second+size])
	copy(swapped[first+size:], raw[first:second])

	_, _, err = readAll(t, swapped)
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "reordered") {
		t.Fatalf("the error does not name the cause: %v", err)
	}
}

func TestTwoSnapshotsConcatenatedAreRefused(t *testing.T) {
	raw := build(t, snapshot.CompressionZstd, header(), records(2))
	_, _, err := readAll(t, append(append([]byte(nil), raw...), raw...))
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "concatenated") {
		t.Fatalf("the error does not name the cause: %v", err)
	}
}

// A message length that runs past the end of its block is corruption naming the
// message, never a short iteration returning what it managed to read.
func TestAMessageRunningPastItsBlockIsCorruption(t *testing.T) {
	raw := build(t, snapshot.CompressionNone, header(), records(2))
	_, blocks, err := readAll(t, raw)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	blocks[0].Payload[0] = 0x7f // a length far past the block

	err = blocks[0].Each(func([]byte) error { return nil })
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("want Corruption, got %v", err)
	}
	if !strings.Contains(err.Error(), "message 0") {
		t.Fatalf("the error does not name the message: %v", err)
	}
}

// The Rust golden: this is the only test that proves Go reads what Rust wrote
// rather than what Go writes. See rustgolden_test.go for why it is committed.
func TestGoReadsWhatRustWrote(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(rustGoldenHead)
	if err != nil {
		t.Fatalf("decoding the golden: %v", err)
	}

	r, err := snapshot.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Rust's preamble did not read: %v", err)
	}
	defer func() { _ = r.Close() }()

	h := r.Header()
	if h.GetSourceImpl() != "rust" {
		t.Fatalf("source_impl is %q, want \"rust\"", h.GetSourceImpl())
	}
	if got := h.GetTenants(); len(got) != 1 || got[0] != "default" {
		t.Fatalf("tenants are %v", got)
	}
	if h.GetRecordCount() != 50 || h.GetVectorCount() != 50 || h.GetEdgeCount() != 120 {
		t.Fatalf("counts are %d/%d/%d, want 50/50/120",
			h.GetRecordCount(), h.GetVectorCount(), h.GetEdgeCount())
	}
	if r.Compression() != snapshot.CompressionZstd {
		t.Fatalf("compression is %s", r.Compression())
	}

	// The block. Reading it at all proves the frame layout, the zstd frame and
	// — since Next() rejects a checksum mismatch — that the checksum is
	// CRC-32/IEEE and not the CRC-32C the plan's §II.7 specifies.
	blk, err := r.Next()
	if err != nil {
		t.Fatalf("Rust's first block did not read: %v", err)
	}
	if blk.Section != snapshot.SectionRecords {
		t.Fatalf("first block is %s, want records", blk.Section)
	}
	msgs, err := snapshot.SplitMessages(blk)
	if err != nil {
		t.Fatalf("splitting Rust's block: %v", err)
	}
	if len(msgs) != rustGoldenRecords {
		t.Fatalf("got %d records, want %d", len(msgs), rustGoldenRecords)
	}

	var rec pb.Record
	if err := proto.Unmarshal(msgs[0], &rec); err != nil {
		t.Fatalf("decoding Rust's first record: %v", err)
	}
	if rec.GetContent() != "tiny fixture record 0 (seed 42)" {
		t.Fatalf("content is %q", rec.GetContent())
	}
	if rec.GetPolicy() != "short_term" || rec.GetImportance() != 0.5 || rec.GetHealth() != 100 {
		t.Fatalf("lifecycle fields did not survive: %+v", &rec)
	}
	if len(rec.GetId()) != 16 {
		t.Fatalf("id is %d bytes, want 16", len(rec.GetId()))
	}

	// And the checksum Rust wrote, transcribed from the file, is the one this
	// binary computes over the same bytes.
	if got := binary.LittleEndian.Uint32(raw[62:]); got != rustGoldenBlockCRC {
		t.Fatalf("the golden's frame CRC is %#08x, want %#08x", got, rustGoldenBlockCRC)
	}

	// The golden is a cut file, so the next read is the truncation path.
	if _, err := r.Next(); !errs.Is(err, errs.Corruption) {
		t.Fatalf("a cut file should report truncation, got %v", err)
	}
}
