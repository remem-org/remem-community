// Package test holds checks that belong to no single package.
//
// The one here pins the wire contract between the two implementations.
// proto/snapshot/v1/snapshot.proto is byte-shared with the frozen Rust
// repository, which vendors the same file and checksums it in
// crates/remem-server/tests/proto_checksum.rs. Two checksums over one file turn
// silent drift into a failed build in whichever repository moved first —
// without them, an exporter and an importer disagree about a format neither
// reports as wrong, and the symptom is a migration that loses a field.
package test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

// expectedSnapshotProtoSHA256 is updated deliberately, in both repositories, in
// the same change. It is the value remem-development's proto_checksum.rs holds.
const expectedSnapshotProtoSHA256 = "1e6b5fe67e4c05376cd4e275457386883756234f3c65bcba55f9046fc5fda092"

func TestSnapshotProtoIsUnchanged(t *testing.T) {
	bytes, err := os.ReadFile("../proto/snapshot/v1/snapshot.proto")
	if err != nil {
		t.Fatalf("reading the schema: %v", err)
	}
	sum := sha256.Sum256(bytes)
	got := hex.EncodeToString(sum[:])
	if got != expectedSnapshotProtoSHA256 {
		t.Fatalf("proto/snapshot/v1/snapshot.proto changed.\n"+
			"  got  %s\n  want %s\n"+
			"Update the constant here AND in remem-development's "+
			"crates/remem-server/tests/proto_checksum.rs in the same change, or the exporter "+
			"and the importer will disagree about a format neither reports as wrong.", got, expectedSnapshotProtoSHA256)
	}
}
