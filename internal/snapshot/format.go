// Package snapshot is Remem's portable logical export: the only backup format
// there is, and the only place the Go and Rust implementations meet.
//
// # What it is, and what it is not
//
// A snapshot is a logical dump of canonical data — records, canonical vectors
// and out-edges — framed so that a reader who knows nothing about Pebble can
// consume it (spec §39, Invariant 12). It is neither canonical nor derived: it
// is written from canonical rows and read back into them, and nothing ever
// queries it. Derived rows — attribute rows and their slot entries, text
// postings, in-edges, the HNSW graph — are never exported, because they are
// rebuilt on import by the same write path that builds them every day.
//
// It is emphatically not a copy of Pebble's files. Invariant 12 forbids that,
// and the reason is the whole point of the format: a backup that only Pebble
// can read is not an escape route from Pebble.
//
// # Two implementations, one file
//
// Only Rust can read Rust's on-disk layout and only Go can read Go's (spec
// §45), so migration goes through this format and nothing else: Rust exports,
// Go imports. The wire contract is proto/snapshot/v1/snapshot.proto, which is
// byte-shared and checksummed in both repositories.
//
// # The framing
//
//	magic     "REMEMSNAP" (9 bytes)
//	u16       snapshot format version = 1
//	u8        compression: 0 none, 1 zstd
//	u32       header length, then the header protobuf
//	then a sequence of framed, optionally-compressed blocks:
//	  u8 section, u32 uncompressed_len, u32 compressed_len, u32 crc32, bytes
//	sections: 1 tenants  2 schema  3 records  4 vectors  5 edges  6 events
//	trailer   "REMEMEND" + u32 block count + u64 total bytes + u32 crc32 of all block crcs
//
// Every multi-byte integer is little-endian. A block body is the concatenation
// of length-delimited protobuf messages, which is what lets a reader hold one
// block at a time however large the corpus is.
//
// # The checksum is CRC-32/IEEE, and the plan says otherwise
//
// Implementation plan §II.7 calls this field crc32c. The exporter that wrote
// every snapshot in existence uses crc32fast, which is CRC-32/IEEE, and its own
// module documentation records the choice and the reason. The wire wins over
// the document: a verifier written from §II.7 would reject every real file.
// This is recorded rather than silently matched because the next reader of
// §II.7 deserves to know.
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
)

// Header is the snapshot's opening protobuf. It is an alias rather than a
// second struct: two descriptions of one durable format are two things that can
// disagree, and this one is generated from the schema both implementations
// compile.
type Header = pb.Header

// The file's fixed markers and the version this binary writes.
const (
	// Magic opens every snapshot.
	Magic = "REMEMSNAP"
	// TrailerMagic opens the trailer.
	TrailerMagic = "REMEMEND"
	// FormatVersion is the framing version this binary writes and the newest
	// it reads. A file claiming a higher one is refused (Invariant 4).
	FormatVersion uint16 = 1
)

// Frame sizes, named so the arithmetic in reader.go and writer.go is checkable
// against the layout comment above rather than against itself.
const (
	magicLen        = len(Magic)
	trailerMagicLen = len(TrailerMagic)
	// preambleLen is magic + version + compression byte + header length.
	preambleLen = magicLen + 2 + 1 + 4
	// blockFrameLen is section + uncompressed_len + compressed_len + crc32.
	blockFrameLen = 1 + 4 + 4 + 4
	// trailerLen is the magic + block count + total bytes + crc of crcs.
	trailerLen = trailerMagicLen + 4 + 8 + 4
)

// MaxBlockBytes bounds one block's stored payload.
//
// It exists so a corrupt length field cannot make a reader allocate a gigabyte
// before it discovers the checksum is wrong. The exporter pages a thousand
// records at a time and a record's content is capped at a megabyte, so a real
// block is orders of magnitude below this.
const MaxBlockBytes = 512 << 20

// Compression names how a block's bytes are stored.
type Compression uint8

// The two compression settings. The byte is durable: it is read before any
// block, and a reader that does not recognise it cannot interpret what follows.
const (
	CompressionNone Compression = 0
	CompressionZstd Compression = 1
)

func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionZstd:
		return "zstd"
	default:
		return fmt.Sprintf("compression(%d)", uint8(c))
	}
}

// parseCompression rejects a byte this binary cannot act on. It is checked for
// its own sake even by a caller that only wants the header: an unrecognised
// setting means the blocks are uninterpretable, and saying so at the header is
// better than saying it at block one.
func parseCompression(b byte, op string) (Compression, error) {
	switch Compression(b) {
	case CompressionNone:
		return CompressionNone, nil
	case CompressionZstd:
		return CompressionZstd, nil
	default:
		return 0, errs.E(errs.Corruption, op, fmt.Errorf(
			"unknown compression byte %d: this snapshot was written by a newer or different "+
				"exporter, and its blocks cannot be read", b))
	}
}

// Section identifies what a block carries.
//
// The numbers are a durable format. A number is never reused for a different
// meaning, and a retired section stays retired — an importer must be able to
// name a section it does not understand, which is the whole reason
// [ParseSection] refuses rather than skips.
//
// # The id space has two halves
//
// **1 to 6 are the migration contract.** They carry the messages of
// proto/snapshot/v1/snapshot.proto, the file byte-shared with the frozen Rust
// implementation and checksummed in both repositories. Rust writes 1, 3, 4 and
// 5; Go writes exactly the same bytes for the same data. Sections 2 and 6 are
// declared there and written by nobody: Rust has no per-tenant user schema and
// no durable event log, and Go's audit stream does not fit the shape v1 gives
// an event (see [SectionGoEvents]).
//
// **7 upward are Go-native companions.** The shared schema predates Go's graph
// and its lifecycle, so it has no field for connection metadata, an edge's
// update time, a record's archive time, the per-tenant schema version, or an
// audit event in the shape Go keeps one. Dropping those would make Remem's only
// backup format lossy, and Invariant 12 calls that format the escape route from
// Pebble. So Go writes the shared sections exactly as Rust would and writes
// these alongside them: strip the companions from a Go snapshot and what
// remains is precisely a snapshot.v1 file.
//
// A companion block always immediately follows the block it annotates, which is
// what bounds an importer's memory to one page rather than one corpus.
type Section uint8

// The sections. See the type comment for why the space has two halves.
const (
	SectionTenants Section = 1
	SectionSchema  Section = 2
	SectionRecords Section = 3
	SectionVectors Section = 4
	SectionEdges   Section = 5
	SectionEvents  Section = 6

	// SectionGoRecordExt annotates the records block before it.
	SectionGoRecordExt Section = 7
	// SectionGoEdgeExt annotates the edges block before it.
	SectionGoEdgeExt Section = 8
	// SectionGoTenantExt annotates the tenants block before it.
	SectionGoTenantExt Section = 9
	// SectionGoEvents is the lifecycle audit stream (plan §II.10 row 14). It
	// is its own section rather than section 6 because it carries a different
	// message: v1's Event flattens an event into a kind and free text, where
	// Go's records what moved, who moved it and why. One id, one message.
	SectionGoEvents Section = 10
)

var sectionNames = map[Section]string{
	SectionTenants:     "tenants",
	SectionSchema:      "schema",
	SectionRecords:     "records",
	SectionVectors:     "vectors",
	SectionEdges:       "edges",
	SectionEvents:      "events",
	SectionGoRecordExt: "go.record_ext",
	SectionGoEdgeExt:   "go.edge_ext",
	SectionGoTenantExt: "go.tenant_ext",
	SectionGoEvents:    "go.events",
}

// Shared reports whether a section belongs to the byte-shared migration
// contract. A snapshot made only of shared sections is what any implementation
// compiling proto/snapshot/v1 can read.
func (s Section) Shared() bool { return s >= SectionTenants && s <= SectionEvents }

// Annotates returns the section a companion block must immediately follow, and
// whether the section is a companion at all.
func (s Section) Annotates() (Section, bool) {
	switch s {
	case SectionGoRecordExt:
		return SectionRecords, true
	case SectionGoEdgeExt:
		return SectionEdges, true
	case SectionGoTenantExt:
		return SectionTenants, true
	default:
		return 0, false
	}
}

func (s Section) String() string {
	if n, ok := sectionNames[s]; ok {
		return n
	}
	return fmt.Sprintf("section(%d)", uint8(s))
}

// Sections returns every section in number order, for the message a refusal
// prints.
func Sections() []Section {
	return []Section{
		SectionTenants, SectionSchema, SectionRecords, SectionVectors, SectionEdges, SectionEvents,
		SectionGoRecordExt, SectionGoEdgeExt, SectionGoTenantExt, SectionGoEvents,
	}
}

// ParseSection refuses a section id this binary does not know.
//
// It fails rather than skipping, and that is spec §59 rather than strictness
// for its own sake: a section a reader skips is data the operator believes was
// migrated. The one thing worse than a failed import is a successful one that
// left something behind.
func ParseSection(b byte, op string) (Section, error) {
	if _, ok := sectionNames[Section(b)]; ok {
		return Section(b), nil
	}
	names := make([]string, 0, len(sectionNames))
	for _, s := range Sections() {
		names = append(names, fmt.Sprintf("%d (%s)", uint8(s), s))
	}
	return 0, errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
		"unknown snapshot section %d: this binary understands %v. The snapshot was written by a "+
			"newer Remem and carries data this one would silently lose, so it is refused rather "+
			"than partially imported — upgrade before importing", b, names))
}

// checksum is the block checksum: CRC-32/IEEE over the stored (possibly
// compressed) bytes. See the package comment for why it is not CRC-32C.
func checksum(stored []byte) uint32 { return crc32.ChecksumIEEE(stored) }

// errShortRead reports a file that ended inside a structure, naming what was
// being read and how far the file got. It is Corruption rather than Storage:
// a truncated snapshot does not become whole on a retry.
func errShortRead(op, what string, want, got int) error {
	return errs.E(errs.Corruption, op, fmt.Errorf(
		"the snapshot ends inside its %s: wanted %d more bytes, found %d", what, want, got))
}

// putUint32 and friends keep every multi-byte write little-endian in one place,
// so a mistake is one mistake rather than one per field.
func putUint16(b []byte, v uint16) { binary.LittleEndian.PutUint16(b, v) }
func putUint32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }
func putUint64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }

var errNilHeader = errors.New("header is nil")
