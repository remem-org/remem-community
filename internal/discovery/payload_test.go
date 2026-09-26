package discovery_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"pgregory.net/rapid"
)

func TestPayloadRoundTrips(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Seeded from a drawn value, never from id.New(): rapid replays draws
		// and nothing else, so a subject drawn from a generator outside its
		// control shrinks to nothing and reports "can not reproduce a failure".
		n := rapid.IntRange(1, 64).Draw(t, "count")
		subjects := make([]id.ID, n)
		for i := range subjects {
			b := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(t, "id")
			copy(subjects[i][:], b)
		}

		encoded, err := discovery.EncodePayload(subjects)
		if err != nil {
			t.Fatalf("encoding %d subjects: %v", n, err)
		}
		got, err := discovery.DecodePayload(encoded)
		if err != nil {
			t.Fatalf("decoding %d subjects: %v", n, err)
		}
		if len(got) != len(subjects) {
			t.Fatalf("round trip returned %d subjects, wrote %d", len(got), len(subjects))
		}
		for i := range subjects {
			if got[i] != subjects[i] {
				t.Fatalf("subject %d round-tripped as %s, was %s", i, got[i], subjects[i])
			}
		}
	})
}

func TestAnEmptyPayloadIsRefused(t *testing.T) {
	if _, err := discovery.EncodePayload(nil); errs.KindOf(err) != errs.Invalid {
		t.Fatalf("encoding no subjects returned %v; a discovery job with nothing to do is a bug at the enqueue", err)
	}
}

// A payload that will not parse names the problem rather than panicking. It is
// read on the way into a handler, from a durable row that a newer binary may
// have written, so it is a parsing surface like any other.
func TestACorruptPayloadIsCorruption(t *testing.T) {
	good, err := discovery.EncodePayload([]id.ID{id.New(), id.New()})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}

	cases := map[string][]byte{
		"empty":            {},
		"unknown version":  append([]byte{9}, good[1:]...),
		"truncated mid-id": good[:len(good)-3],
		"count overruns":   append(append([]byte{}, good[:1]...), 0x40),
		"trailing bytes":   append(append([]byte{}, good...), 0xff),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := discovery.DecodePayload(b); errs.KindOf(err) != errs.Corruption {
				t.Fatalf("decoding a %s payload returned %v, want a Corruption error", name, err)
			}
		})
	}
}

// The checkpoint is the same encoding holding what is left, so a resumed job
// never repeats a subject and there is one decoder rather than two.
func TestTheCheckpointIsThePayloadEncoding(t *testing.T) {
	subjects := []id.ID{id.New(), id.New(), id.New()}
	all, err := discovery.EncodePayload(subjects)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	rest, err := discovery.EncodePayload(subjects[2:])
	if err != nil {
		t.Fatalf("encoding the remainder: %v", err)
	}
	if len(rest) >= len(all) {
		t.Fatalf("a two-subject remainder encoded to %d bytes, the whole three to %d", len(rest), len(all))
	}
	got, err := discovery.DecodePayload(rest)
	if err != nil {
		t.Fatalf("decoding the remainder: %v", err)
	}
	if len(got) != 1 || got[0] != subjects[2] {
		t.Fatalf("the remainder decoded to %v, want just %s", got, subjects[2])
	}
}
