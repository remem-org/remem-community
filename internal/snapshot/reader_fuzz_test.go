package snapshot_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"google.golang.org/protobuf/proto"
)

// FuzzSnapshotReader holds the reader to two rules: no input panics it, and
// every refusal is Corruption or IncompatibleVersion.
//
// A snapshot is the last copy of data by the time anyone reads one, so an
// unclassified error is a message nobody can act on — and a reader that can be
// made to allocate without bound by a small file is a reader an import endpoint
// cannot safely expose.
func FuzzSnapshotReader(f *testing.F) {
	golden, err := base64.StdEncoding.DecodeString(rustGoldenHead)
	if err != nil {
		f.Fatalf("decoding the Rust golden: %v", err)
	}
	f.Add(golden)
	for _, comp := range []snapshot.Compression{snapshot.CompressionNone, snapshot.CompressionZstd} {
		f.Add(writtenForFuzzing(f, comp))
	}
	f.Add([]byte(snapshot.Magic))
	f.Add([]byte{})
	// A whole preamble with the wrong magic. The short seeds above are refused
	// as truncated before the magic is compared, so without this one no seed
	// reaches that refusal at all — which is how a deliberately misclassified
	// magic error first passed this target.
	f.Add(append([]byte("NOTREMEM!"), writtenForFuzzing(f, snapshot.CompressionNone)[len(snapshot.Magic):]...))

	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := snapshot.NewReader(bytes.NewReader(b))
		if err != nil {
			refusedByName(t, err)
			return
		}
		defer func() { _ = r.Close() }()
		for i := 0; i < 1<<16; i++ {
			blk, err := r.Next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				refusedByName(t, err)
				return
			}
			if err := blk.Each(func([]byte) error { return nil }); err != nil {
				refusedByName(t, err)
				return
			}
		}
		t.Fatal("65,536 blocks from one fuzz input: the reader is not bounded by its input")
	})
}

// writtenForFuzzing is a complete two-block snapshot, so the fuzzer starts from
// a file that reaches the trailer rather than one that stops at the preamble.
func writtenForFuzzing(f *testing.F, comp snapshot.Compression) []byte {
	f.Helper()
	var buf bytes.Buffer
	w, err := snapshot.NewWriter(&buf, header(), snapshot.WriterOpts{Compression: comp})
	if err != nil {
		f.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteMessages(snapshot.SectionTenants,
		[]proto.Message{&pb.Tenant{Id: "default", DisplayName: "default"}}); err != nil {
		f.Fatalf("WriteMessages: %v", err)
	}
	if err := w.WriteMessages(snapshot.SectionRecords, records(3).msgs); err != nil {
		f.Fatalf("WriteMessages: %v", err)
	}
	if err := w.Close(); err != nil {
		f.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func refusedByName(t *testing.T, err error) {
	t.Helper()
	switch errs.KindOf(err) {
	case errs.Corruption, errs.IncompatibleVersion:
	default:
		t.Fatalf("refused as %v (%v), want Corruption or IncompatibleVersion", errs.KindOf(err), err)
	}
}
