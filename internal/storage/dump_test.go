package storage_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
)

func ctx() context.Context { return context.Background() }

func filled(t *testing.T, pairs map[string]string) *memkv.Store {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	for k, v := range pairs {
		if err := kv.Set(ctx(), []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	return kv
}

func TestDumpAndRestoreRoundTripEveryPair(t *testing.T) {
	pairs := map[string]string{
		"":             "an empty key is legal and must survive",
		"a":            "",
		"\x00\xff\x00": "keys are arbitrary bytes, not strings",
		"z":            "last",
	}
	src := filled(t, pairs)

	var buf bytes.Buffer
	if err := storage.Dump(ctx(), src, &buf); err != nil {
		t.Fatal(err)
	}

	dst := memkv.New()
	defer func() { _ = dst.Close() }()
	if err := storage.Restore(ctx(), dst, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}

	for k, want := range pairs {
		got, err := dst.Get(ctx(), []byte(k))
		if err != nil {
			t.Fatalf("key %q: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("key %q: got %q want %q", k, got, want)
		}
	}
}

func TestDumpIsOrdered(t *testing.T) {
	src := filled(t, map[string]string{"c": "3", "a": "1", "b": "2"})

	var buf bytes.Buffer
	if err := storage.Dump(ctx(), src, &buf); err != nil {
		t.Fatal(err)
	}

	// The stream is the store's own order, so two dumps of the same content are
	// byte-identical — which is what makes a fixture diffable and a backup
	// comparable.
	var again bytes.Buffer
	if err := storage.Dump(ctx(), src, &again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), again.Bytes()) {
		t.Fatal("two dumps of one store differ")
	}
	if bytes.Index(buf.Bytes(), []byte("1")) > bytes.Index(buf.Bytes(), []byte("2")) {
		t.Fatal("the stream is not in key order")
	}
}

func TestRestoreRefusesAStreamItDoesNotRecognise(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"wrong magic", []byte("NOPE\x01")},
		{"truncated header", []byte("RMMD")},
		{"truncated pair", append([]byte("RMMD\x01"), 0x05, 'a')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := memkv.New()
			defer func() { _ = dst.Close() }()
			if err := storage.Restore(ctx(), dst, bytes.NewReader(tc.in)); !errs.Is(err, errs.Corruption) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

// A stream written by a later version is refused rather than half-read: the
// pairs after the header may be framed differently, and a partial restore is a
// store that looks populated and is not.
func TestRestoreRefusesANewerStream(t *testing.T) {
	dst := memkv.New()
	defer func() { _ = dst.Close() }()

	err := storage.Restore(ctx(), dst, bytes.NewReader([]byte("RMMD\xff")))
	if !errs.Is(err, errs.IncompatibleVersion) {
		t.Fatalf("got %v", err)
	}
}

func TestDumpReportsAWriteFailure(t *testing.T) {
	src := filled(t, map[string]string{"a": "1"})
	if err := storage.Dump(ctx(), src, failingWriter{}); err == nil {
		t.Fatal("a dump that could not be written reported success")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }
