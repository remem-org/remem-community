package id_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
)

func TestNewIDsSortByCreationTime(t *testing.T) {
	a := id.New()
	time.Sleep(2 * time.Millisecond)
	b := id.New()
	if bytes.Compare(a[:], b[:]) >= 0 {
		t.Fatalf("v7 ids must sort by time: %s !< %s", a, b)
	}
}

func TestLegacyV4RoundTrips(t *testing.T) {
	const legacy = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
	got, err := id.Parse(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != legacy {
		t.Fatalf("imported ids must keep their identity: %s != %s", got, legacy)
	}
}

func TestParseAcceptsUppercaseAndPreservesCanonicalForm(t *testing.T) {
	const legacy = "F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6"
	got, err := id.Parse(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != strings.ToLower(legacy) {
		t.Fatalf("String() must be canonical lowercase: %s", got)
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "not-a-uuid", "f81d4fae7dec11d0a76500a0c91e6bf", strings.Repeat("f", 40)} {
		if _, err := id.Parse(s); !errs.Is(err, errs.Invalid) {
			t.Errorf("Parse(%q): want Invalid, got %v", s, err)
		}
	}
}

func TestFromBytesRequiresSixteenBytes(t *testing.T) {
	if _, err := id.FromBytes(make([]byte, 15)); !errs.Is(err, errs.Invalid) {
		t.Errorf("15 bytes must be rejected, got %v", err)
	}
	if _, err := id.FromBytes(make([]byte, 17)); !errs.Is(err, errs.Invalid) {
		t.Errorf("17 bytes must be rejected, got %v", err)
	}
	want := id.New()
	got, err := id.FromBytes(want[:])
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("FromBytes round trip: %s != %s", got, want)
	}
}

func TestFromBytesCopiesItsInput(t *testing.T) {
	// A key decoder hands us a slice into a Pebble iterator's buffer, which is
	// reused after the next Next(). An ID that aliases it would change value.
	buf := make([]byte, 16)
	copy(buf, id.New().Bytes())
	got, err := id.FromBytes(buf)
	if err != nil {
		t.Fatal(err)
	}
	before := got.String()
	for i := range buf {
		buf[i] = 0xff
	}
	if got.String() != before {
		t.Fatal("FromBytes must copy, not alias, its input")
	}
}

func TestTimeOfV7IsTheCreationTime(t *testing.T) {
	before := time.Now().Add(-time.Millisecond)
	got := id.New().Time()
	after := time.Now().Add(time.Millisecond)
	if got.Before(before) || got.After(after) {
		t.Fatalf("v7 timestamp %v outside [%v, %v]", got, before, after)
	}
}

func TestTimeOfLegacyV4IsZero(t *testing.T) {
	// A v4 id carries no time. Reporting a plausible-looking one would be worse
	// than reporting none.
	v4, err := id.Parse("f81d4fae-7dec-41d0-a765-00a0c91e6bf6")
	if err != nil {
		t.Fatal(err)
	}
	if !v4.Time().IsZero() {
		t.Fatalf("v4 ids carry no creation time, got %v", v4.Time())
	}
}

func TestZeroValue(t *testing.T) {
	var zero id.ID
	if !zero.IsZero() {
		t.Fatal("the zero ID must report itself as zero")
	}
	if id.New().IsZero() {
		t.Fatal("a generated ID is never zero")
	}
	if zero.String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("unexpected zero rendering: %s", zero)
	}
}

func TestConcurrentGenerationIsUniqueAndSafe(t *testing.T) {
	// spec §49: generation is uncoordinated and safe under concurrency.
	const goroutines, each = 8, 500
	var wg sync.WaitGroup
	out := make([][]id.ID, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]id.ID, each)
			for i := range ids {
				ids[i] = id.New()
			}
			out[g] = ids
		}()
	}
	wg.Wait()

	seen := make(map[id.ID]bool, goroutines*each)
	for _, ids := range out {
		for _, v := range ids {
			if seen[v] {
				t.Fatalf("duplicate id %s", v)
			}
			seen[v] = true
		}
	}
}

func TestTextRoundTrip(t *testing.T) {
	type payload struct{ Ref id.ID }
	want := payload{Ref: id.New()}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), want.Ref.String()) {
		t.Fatalf("ids marshal as their canonical string, got %s", b)
	}
	var got payload
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip: %v != %v", got, want)
	}
	if err := json.Unmarshal([]byte(`{"Ref":"nonsense"}`), &got); !errs.Is(err, errs.Invalid) {
		t.Fatalf("unmarshalling garbage must be Invalid, got %v", err)
	}
}
