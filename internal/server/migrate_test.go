package server_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/server"
	pebblekv "github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/version"
)

// A directory older than this binary, with no step that can close the gap, is
// refused before anything listens.
//
// The alternative — starting and serving an un-migrated directory — is the
// failure mode the whole format manifest exists to prevent: reads that return
// something plausible from bytes this binary interprets differently than the
// binary that wrote them.
func TestServerRefusesADirectoryWithNoMigrationPath(t *testing.T) {
	cfg := testConfig(t)

	// Start once so the directory exists and is stamped, then wind one format
	// back beyond anything the shipped registry can reach.
	srv := mustNew(t, cfg)
	stopServer(t, srv)

	store, err := pebblekv.Open(cfg.Storage.Path, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := schema.ReadFormat(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	// The record envelope, not the attribute slot schema: this build ships a
	// step that advances attr_schema from 1 to 2, so winding *that* one back
	// produces a reachable directory rather than an unreachable one. What is
	// under test here is the refusal, so the format has to be one no step can
	// close the gap on.
	back := version.RecordEnvelope - 1
	f.Subsystems["record_envelope"] = schema.Subsystem{Current: back, MinReader: back, MinWriter: back}
	if err := schema.WriteFormat(context.Background(), store, f); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = server.New(cfg, server.WithEmbedder(embeddingtest.New()), server.WithClock(clock.System()))
	if !errs.Is(err, errs.MigrationRequired) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "record envelope") {
		t.Fatalf("the error must name the format an operator has to act on: %v", err)
	}
}

// The ordinary path: a directory this binary wrote opens, needs nothing, and
// the server starts. It is worth asserting because the migration wiring runs on
// every single start, and a bug there is an outage for every deployment rather
// than only for the ones upgrading.
func TestAFreshDirectoryNeedsNoMigration(t *testing.T) {
	cfg := testConfig(t)
	srv := mustNew(t, cfg)
	stopServer(t, srv)

	store, err := pebblekv.Open(cfg.Storage.Path, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	f, err := schema.ReadFormat(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if f.NeedsMigration(version.Current()) {
		t.Fatalf("a directory this binary just created needs migrating: %s", f)
	}
	states, err := schema.ListStates(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("a fresh directory recorded %d migrations", len(states))
	}
}

// stopServer runs the server long enough to bind and then shuts it down, so a
// test can inspect the directory it left behind.
func stopServer(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
