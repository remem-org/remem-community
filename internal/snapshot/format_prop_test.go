package snapshot_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// Whatever sequence of blocks goes in comes out, in order, with the same
// sections and the same messages. The framing carries a length, a compressed
// length and a checksum per block, and an off-by-one in any of them produces a
// file that reads back plausibly for some inputs and not others — which is
// exactly the shape a property test finds and a fixed example does not.
func TestEveryBlockSequenceRoundTrips(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		comp := snapshot.CompressionNone
		if rapid.Bool().Draw(t, "zstd") {
			comp = snapshot.CompressionZstd
		}
		sections := []snapshot.Section{
			snapshot.SectionTenants, snapshot.SectionRecords,
			snapshot.SectionVectors, snapshot.SectionEdges,
		}

		nblocks := rapid.IntRange(0, 8).Draw(t, "blocks")
		want := make([][]string, 0, nblocks)
		wantSections := make([]snapshot.Section, 0, nblocks)

		var buf bytes.Buffer
		w, err := snapshot.NewWriter(&buf, &pb.Header{SourceImpl: "go"},
			snapshot.WriterOpts{Compression: comp})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		for i := range nblocks {
			section := rapid.SampledFrom(sections).Draw(t, "section")
			n := rapid.IntRange(0, 20).Draw(t, "messages")
			msgs := make([]proto.Message, 0, n)
			contents := make([]string, 0, n)
			for range n {
				// Content is drawn, and it is the only thing that varies: a
				// generator seeded from a clock or an id would make a failure
				// unreproducible, which rapid reports as a flaky test.
				c := rapid.String().Draw(t, "content")
				contents = append(contents, c)
				msgs = append(msgs, &pb.Record{Tenant: "default", Content: c})
			}
			if err := w.WriteMessages(section, msgs); err != nil {
				t.Fatalf("block %d: %v", i, err)
			}
			if n > 0 {
				want = append(want, contents)
				wantSections = append(wantSections, section)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		r, err := snapshot.NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		defer func() { _ = r.Close() }()

		got := make([][]string, 0, len(want))
		gotSections := make([]snapshot.Section, 0, len(want))
		for {
			blk, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			gotSections = append(gotSections, blk.Section)
			var contents []string
			if err := blk.Each(func(raw []byte) error {
				var rec pb.Record
				if err := proto.Unmarshal(raw, &rec); err != nil {
					return err
				}
				contents = append(contents, rec.GetContent())
				return nil
			}); err != nil {
				t.Fatalf("Each: %v", err)
			}
			got = append(got, contents)
		}

		if len(got) != len(want) {
			t.Fatalf("got %d blocks, want %d", len(got), len(want))
		}
		for i := range want {
			if gotSections[i] != wantSections[i] {
				t.Fatalf("block %d is %s, want %s", i, gotSections[i], wantSections[i])
			}
			if len(got[i]) != len(want[i]) {
				t.Fatalf("block %d has %d messages, want %d", i, len(got[i]), len(want[i]))
			}
			for j := range want[i] {
				if got[i][j] != want[i][j] {
					t.Fatalf("block %d message %d is %q, want %q", i, j, got[i][j], want[i][j])
				}
			}
		}
	})
}

// Flipping any single byte of a block payload or of the trailer is caught: the
// per-block checksum covers the payload, and the trailer's block count, byte
// total and checksum-over-checksums cover the shape of the sequence.
//
// Two regions are deliberately outside this property, and both are properties
// of the shared format rather than of this reader.
//
// The header carries no checksum — Rust's does not either, and the file is
// byte-shared, so Go cannot add one.
//
// Neither does a block's *frame header*: the section byte and the two lengths
// sit outside the CRC that follows them. A flipped bit in a section byte
// therefore relabels a block without any framing check noticing — records read
// back as tenants, for instance, because both put a string in field 1.
//
// What catches both is that nothing trusts the header: [Import] and [Verify]
// compare every count it claims against what they actually read, and a
// relabelled or damaged header fails there. That is asserted where it happens,
// not here, by TestADamagedHeaderCountIsCaught and TestARelabelledBlockDoesNotPass.
func TestASingleFlippedByteInAPayloadOrTrailerIsAlwaysCaught(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var buf bytes.Buffer
		w, err := snapshot.NewWriter(&buf, &pb.Header{SourceImpl: "go"},
			snapshot.WriterOpts{Compression: snapshot.CompressionNone})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		for range 3 {
			if err := w.WriteMessages(snapshot.SectionRecords, []proto.Message{
				&pb.Record{Tenant: "default", Content: rapid.String().Draw(t, "content")},
			}); err != nil {
				t.Fatalf("WriteMessages: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		raw := buf.Bytes()
		at := rapid.SampledFrom(checkedBytes(t, raw)).Draw(t, "byte")
		bit := rapid.IntRange(0, 7).Draw(t, "bit")
		damaged := append([]byte(nil), raw...)
		damaged[at] ^= 1 << bit

		r, err := snapshot.NewReader(bytes.NewReader(damaged))
		if err != nil {
			return // refused at the header, which counts as caught
		}
		defer func() { _ = r.Close() }()
		for {
			blk, err := r.Next()
			if errors.Is(err, io.EOF) {
				t.Fatalf("a flipped bit at byte %d (bit %d) read back as a whole snapshot", at, bit)
			}
			if err != nil {
				return // caught
			}
			if err := blk.Each(func([]byte) error { return nil }); err != nil {
				return // caught
			}
		}
	})
}

// checkedBytes returns every offset the framing actually protects: each block's
// stored payload, and the whole trailer. It walks the frames rather than
// assuming their sizes, so it stays honest if the layout ever moves.
func checkedBytes(t *rapid.T, raw []byte) []int {
	t.Helper()
	var out []int
	at := 16 + int(binary.LittleEndian.Uint32(raw[12:]))
	for at < len(raw)-24 {
		clen := int(binary.LittleEndian.Uint32(raw[at+5:]))
		for i := at + 13; i < at+13+clen; i++ {
			out = append(out, i)
		}
		at += 13 + clen
	}
	for i := len(raw) - 24; i < len(raw); i++ {
		out = append(out, i)
	}
	return out
}
