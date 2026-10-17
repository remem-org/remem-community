package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/server"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/users"
)

// usersDataDir is a stopped server's data directory holding the given tenants.
func usersDataDir(t *testing.T, ids ...tenant.ID) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := tenantkv.New(kv, clock.System())
	for _, id := range ids {
		if err := d.Create(context.Background(), id, tenant.Meta{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// The first user is created on a stopped server, and the token the command
// prints is the token: nothing else on stdout, so a script can capture it, and
// it verifies against the directory exactly as the server will.
func TestTheFirstUserAndTokenAreCreatedOffline(t *testing.T) {
	dir := usersDataDir(t, "acme")

	out, code, err := captureRun(t, "users", "create", "--data-dir", dir, "--name", "planner")
	if err != nil || code != 0 {
		t.Fatalf("users create: %d %v", code, err)
	}
	uid := strings.Fields(out)[0]

	tok, code, err := captureRun(t, "tokens", "issue", "--data-dir", dir, "--user", uid)
	if err != nil || code != 0 {
		t.Fatalf("tokens issue: %d %v", code, err)
	}
	tok = strings.TrimSpace(tok)
	if !strings.HasPrefix(tok, users.TokenPrefix) || strings.ContainsAny(tok, " \n\t") {
		t.Fatalf("tokens issue printed %q, want the token and nothing else", tok)
	}

	// Closed before the next command runs: Pebble's lock is exclusive.
	kv, err := pebble.Open(dir, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	who, err := users.New(kv, clock.System()).Verify(context.Background(), tok)
	_ = kv.Close()
	if err != nil || who.Tenant != "acme" || who.User.String() != uid {
		t.Fatalf("the offline token verifies as %+v, %v", who, err)
	}

	listed, _, err := captureRun(t, "users", "list", "--data-dir", dir)
	if err != nil || !strings.Contains(listed, uid) || !strings.Contains(listed, "planner") {
		t.Fatalf("users list: %v\n%s", err, listed)
	}
	tokens, _, err := captureRun(t, "tokens", "list", "--data-dir", dir, "--user", uid)
	if err != nil || strings.Contains(tokens, tok[4:]) || !strings.Contains(tokens, "active") {
		t.Fatalf("tokens list: %v\n%s", err, tokens)
	}
}

func TestUsersCanBeRenamedDisabledAndEnabledOffline(t *testing.T) {
	dir := usersDataDir(t, "acme")
	out, _, _ := captureRun(t, "users", "create", "--data-dir", dir, "--name", "agent-a")
	uid := strings.Fields(out)[0]
	tok, _, _ := captureRun(t, "tokens", "issue", "--data-dir", dir, "--user", uid)
	tok = strings.TrimSpace(tok)

	for _, args := range [][]string{
		{"users", "rename", "--data-dir", dir, "--user", uid, "--name", "planner"},
		{"users", "disable", "--data-dir", dir, "--user", uid},
	} {
		if _, code, err := captureRun(t, args...); err != nil || code != 0 {
			t.Fatalf("%v: %d %v", args, code, err)
		}
	}
	verify := func() error {
		kv, err := pebble.Open(dir, pebble.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = kv.Close() }()
		_, err = users.New(kv, clock.System()).Verify(context.Background(), tok)
		return err
	}
	if err := verify(); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("a disabled user's token: %v", err)
	}
	if _, _, err := captureRun(t, "users", "enable", "--data-dir", dir, "--user", uid); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err != nil {
		t.Fatalf("after enabling: %v", err)
	}
	listed, _, _ := captureRun(t, "users", "list", "--data-dir", dir)
	if !strings.Contains(listed, "planner") || strings.Contains(listed, "agent-a") {
		t.Fatalf("the rename did not take:\n%s", listed)
	}

	tokens, _, _ := captureRun(t, "tokens", "list", "--data-dir", dir, "--user", uid)
	tid := strings.Fields(tokens)[0]
	if _, _, err := captureRun(t, "tokens", "revoke", "--data-dir", dir, "--user", uid, "--token", tid); err != nil {
		t.Fatal(err)
	}
	if err := verify(); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("a revoked token: %v", err)
	}
}

// A mistyped path is refused and nothing is created, for the reason every
// command here opens read-only first: a new empty database would otherwise
// hold a user nobody's server will ever read.
func TestAMistypedDataDirectoryCreatesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-a-data-directory")
	if _, _, err := captureRun(t, "users", "create", "--data-dir", dir, "--name", "planner"); err == nil {
		t.Fatal("a path with no database was accepted")
	}
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("the refused command created the directory")
	}
}

// A single-tenant build will not create a user in a tenant it does not serve,
// and says so rather than writing somewhere no server will read.
func TestAnUnservedTenantIsRefused(t *testing.T) {
	dir := usersDataDir(t, "acme")
	_, _, err := captureRun(t, "users", "create", "--data-dir", dir, "--name", "planner", "--tenant", "beta")
	if !errs.Is(err, errs.Forbidden) || !strings.Contains(err.Error(), "beta") {
		t.Fatalf("got %v, want Forbidden naming beta", err)
	}
}

// A user cannot be deleted from the command line either, and the refusal says
// what to do instead.
func TestThereIsNoDeleteVerb(t *testing.T) {
	dir := usersDataDir(t, "acme")
	_, _, err := captureRun(t, "users", "delete", "--data-dir", dir)
	if err == nil || !strings.Contains(err.Error(), "disable") {
		t.Fatalf("users delete: %v, want a refusal naming disable", err)
	}
}

// The whole offline path, ending where an operator's does: a server started
// over the directory accepts the printed token, as a user of the implicit
// tenant, on its first request.
func TestATokenIssuedOfflineAuthenticatesAgainstAServer(t *testing.T) {
	dir := usersDataDir(t, "acme")
	out, _, err := captureRun(t, "users", "create", "--data-dir", dir, "--name", "planner")
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := captureRun(t, "tokens", "issue", "--data-dir", dir, "--user", strings.Fields(out)[0])
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Storage.Path = dir
	cfg.Storage.SyncWrites = false
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Server.APIKey = "an-operator-key-long-enough-for-this-test"
	cfg.Tenant.Default = "acme"
	cfg.Log.Level = "error"
	srv, err := server.New(cfg, server.WithEmbedder(embeddingtest.New()), server.WithClock(clock.System()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for deadline := time.Now().Add(10 * time.Second); !srv.Ready() || srv.Addr() == ""; {
		if time.Now().After(deadline) {
			t.Fatal("the server never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}

	req, _ := http.NewRequest("GET", "http://"+srv.Addr()+"/api/v1/memories", nil)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(tok))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the offline token was answered %d, want 200", resp.StatusCode)
	}
}

// The commands an operator backs up and restores with must carry users. A
// source left unwired would write a backup with no users and say nothing, so
// this goes through export, import and verify exactly as an operator does.
func TestUsersTravelThroughExportImportAndVerify(t *testing.T) {
	src := usersDataDir(t, "acme")
	out, _, err := captureRun(t, "users", "create", "--data-dir", src, "--name", "planner")
	if err != nil {
		t.Fatal(err)
	}
	uid := strings.Fields(out)[0]
	file := filepath.Join(t.TempDir(), "corpus.rsnap")

	if out, _, err := captureRun(t, "export", "--data-dir", src, "--out", file); err != nil || !strings.Contains(out, "1 user(s)") {
		t.Fatalf("export: %v\n%s", err, out)
	}
	dst := filepath.Join(t.TempDir(), "restored")
	if out, _, err := captureRun(t, "import", "--data-dir", dst, "--in", file); err != nil || !strings.Contains(out, "1 user(s)") ||
		!strings.Contains(out, "tokens issue") {
		t.Fatalf("import: %v\n%s", err, out)
	}
	if out, code, err := captureRun(t, "verify", "--data-dir", dst, "--in", file); err != nil || code != 0 {
		t.Fatalf("verify: %d %v\n%s", code, err, out)
	}
	listed, _, err := captureRun(t, "users", "list", "--data-dir", dst)
	if err != nil || !strings.Contains(listed, uid) || !strings.Contains(listed, "planner") {
		t.Fatalf("the restored directory lists: %v\n%s", err, listed)
	}
}
