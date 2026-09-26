package snapshot_test

// rustGoldenHead is the first 1,465 bytes of a real Rust export, verbatim,
// base64-encoded.
//
// It is the `tiny` reference corpus (seed 42) written by remem-development at
// pre-go-freeze, cut immediately after its first complete block: the preamble,
// the header protobuf, and one zstd-compressed records block carrying fifty
// records, with the CRC that export recorded.
//
// # Why this is committed when the corpora are not
//
// The fixture corpora are generated on demand (`make fixtures`), so the tests
// that import them skip on a machine that has not built the Rust reference —
// which is most CI runs. Without something committed, nothing would ever prove
// that Go reads Rust's *framing*: the magic, the little-endian version, the
// compression byte, the header protobuf, the frame field order, and above all
// that the block checksum is CRC-32/IEEE rather than the CRC-32C the
// implementation plan's §II.7 specifies. Each of those is a total failure if it
// is wrong, and not one of them is exercised by Go reading its own output.
//
// A wire test vector is not a corpus. This is 1.4 KB, it holds nothing anyone
// would query, and it exists to keep a contract with a frozen repository.
//
// To regenerate, from the remem-development tree at pre-go-freeze:
//
//	cargo build --release -p remem-server --bin remem-fixture
//	remem-fixture --profile tiny --seed 42 --data-dir <dir> --out tiny.rsnap
//
// then base64 the bytes up to the second block frame. 1,465 is that offset for
// this corpus: 9 magic + 2 version + 1 compression + 4 header length + 37
// header + 13 frame + 1,399 stored block bytes. The corpus digest is pinned in
// remem-development's docs/FIXTURES.md and was reproduced byte for byte on the
// host that captured these bytes.
const rustGoldenHead = "" +
	"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" +
	"weDoS1KU8BuqAVuHpCGtAg=="

// What the golden's one block holds, and the checksum Rust wrote into its frame
// at byte 62. Both are transcribed from the file rather than computed here: a
// test that recomputes the thing it is checking proves only that it can compute
// it twice the same way.
const (
	rustGoldenRecords  = 50
	rustGoldenBlockCRC = 0xf53d9a19
)
