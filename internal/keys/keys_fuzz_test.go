package keys_test

import (
	"bytes"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// FuzzKeyDecoders holds every parser to two rules: it never panics, and it
// rejects rather than mis-parses.
//
// The second rule is checked without an oracle. A key a parser accepts must
// re-encode to exactly the bytes it was given: a parser that accepts a spelling
// its encoder cannot produce has admitted two byte strings for one key, and a
// prefix scan built from the encoder sees only one of them.
func FuzzKeyDecoders(f *testing.F) {
	t, ns := tenant.ID("acme"), tenant.DefaultNamespace
	// id.New is fine here: these are seeds, and the fuzzer's inputs are its own
	// bytes derived from them.
	rid := id.New()
	f.Add(keys.Record(t, ns, keys.RecordMemory, rid))
	f.Add(keys.Text(t, ns, "raft", rid))
	f.Add(keys.Job(t, ns, keys.JobPending, 1_700_000_000_000, rid))
	f.Add(keys.Event(t, ns, rid, 1_700_000_000_000, 7))
	f.Add(keys.System("format"))
	f.Add([]byte{})
	// A non-minimal uvarint for a zero-length tenant.
	f.Add([]byte{0x80, 0x00})

	f.Fuzz(func(tt *testing.T, k []byte) {
		if tid, n, typ, r, err := keys.ParseRecord(k); err != nil {
			mustBeCorruption(tt, "ParseRecord", k, err)
		} else if got := keys.Record(tid, n, typ, r); !bytes.Equal(got, k) {
			tt.Fatalf("ParseRecord accepted %x, which re-encodes as %x", k, got)
		}
		if tid, n, term, r, err := keys.ParseText(k); err != nil {
			mustBeCorruption(tt, "ParseText", k, err)
		} else if got := keys.Text(tid, n, term, r); !bytes.Equal(got, k) {
			tt.Fatalf("ParseText accepted %x, which re-encodes as %x", k, got)
		}
		if tid, n, st, due, j, err := keys.ParseJob(k); err != nil {
			mustBeCorruption(tt, "ParseJob", k, err)
		} else if got := keys.Job(tid, n, st, due, j); !bytes.Equal(got, k) {
			tt.Fatalf("ParseJob accepted %x, which re-encodes as %x", k, got)
		}
		if tid, n, subj, ts, seq, err := keys.ParseEvent(k); err != nil {
			mustBeCorruption(tt, "ParseEvent", k, err)
		} else if got := keys.Event(tid, n, subj, ts, seq); !bytes.Equal(got, k) {
			tt.Fatalf("ParseEvent accepted %x, which re-encodes as %x", k, got)
		}
		if _, _, s, err := keys.ParseSpace(k); err != nil {
			mustBeCorruption(tt, "ParseSpace", k, err)
		} else if s.String() == "unknown" {
			tt.Fatalf("ParseSpace accepted %x as space %#x, which is not declared", k, byte(s))
		}
	})
}

func mustBeCorruption(t *testing.T, parser string, k []byte, err error) {
	t.Helper()
	if kind := errs.KindOf(err); kind != errs.Corruption {
		t.Fatalf("%s refused %x as %v (%v), want Corruption: bytes from the store that do not "+
			"decode are damage, not a bad request", parser, k, kind, err)
	}
}
