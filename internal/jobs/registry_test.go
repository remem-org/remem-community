package jobs_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
)

func nop(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil }

func TestARegisteredTypeIsFoundByName(t *testing.T) {
	reg := jobs.NewRegistry()
	if err := reg.Register(jobs.Entry{
		Type:        "vector.rebuild",
		Handler:     jobs.HandlerFunc(nop),
		Description: "rebuild the approximate vector index from the canonical vectors",
		MaxAttempts: 2,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := reg.Lookup("vector.rebuild")
	if !ok {
		t.Fatal("a registered type was not found")
	}
	if got.MaxAttempts != 2 || got.Description == "" {
		t.Fatalf("the entry lost its configuration: %+v", got)
	}
	if _, ok := reg.Lookup("text.rebuild"); ok {
		t.Fatal("an unregistered type was found")
	}
}

func TestRegisteringATypeTwiceIsRefused(t *testing.T) {
	// A second registration that silently replaced the first would send work
	// enqueued against one handler to another, and nothing would report it.
	reg := jobs.NewRegistry()
	e := jobs.Entry{Type: "vector.rebuild", Handler: jobs.HandlerFunc(nop)}
	if err := reg.Register(e); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(e); !errs.Is(err, errs.Conflict) {
		t.Fatalf("a duplicate registration was accepted: %v", err)
	}
}

func TestRegistrationRefusesWhatCannotBeRun(t *testing.T) {
	reg := jobs.NewRegistry()
	for name, e := range map[string]jobs.Entry{
		"no handler":        {Type: "vector.rebuild"},
		"no type":           {Handler: jobs.HandlerFunc(nop)},
		"bad type":          {Type: "Vector Rebuild", Handler: jobs.HandlerFunc(nop)},
		"negative interval": {Type: "jobs.reap", Handler: jobs.HandlerFunc(nop), Every: -time.Second},
	} {
		if err := reg.Register(e); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: want Invalid, got %v", name, err)
		}
	}
}

func TestTypesAreListedInAStableOrder(t *testing.T) {
	// The admin surface renders this list, and a listing whose order changes
	// per process is a listing nobody can diff.
	reg := jobs.NewRegistry()
	for _, typ := range []jobs.Type{"text.rebuild", "jobs.reap", "vector.rebuild"} {
		if err := reg.Register(jobs.Entry{Type: typ, Handler: jobs.HandlerFunc(nop)}); err != nil {
			t.Fatal(err)
		}
	}
	want := []jobs.Type{"jobs.reap", "text.rebuild", "vector.rebuild"}
	for range 5 {
		if got := reg.Types(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Types() = %v, want %v", got, want)
		}
	}
	if entries := reg.Entries(); len(entries) != 3 || entries[0].Type != "jobs.reap" {
		t.Fatalf("Entries() = %v", entries)
	}
}

func TestAnUnregisteredTypeIsRefusedRatherThanDropped(t *testing.T) {
	// A job whose type nothing handles must not look like a job that has been
	// done. It is the shape of an upgrade that removed a handler while rows of
	// that type were still queued.
	reg := jobs.NewRegistry()
	if _, err := reg.Handler("text.rebuild"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an unregistered type resolved: %v", err)
	}
}

func TestAnEntryWithoutAnAttemptBoundTakesTheDefault(t *testing.T) {
	reg := jobs.NewRegistry()
	if err := reg.Register(jobs.Entry{Type: "vector.rebuild", Handler: jobs.HandlerFunc(nop)}); err != nil {
		t.Fatal(err)
	}
	got, _ := reg.Lookup("vector.rebuild")
	if got.MaxAttempts != jobs.DefaultMaxAttempts {
		t.Fatalf("an entry with no bound has MaxAttempts %d, want the default %d",
			got.MaxAttempts, jobs.DefaultMaxAttempts)
	}
}
