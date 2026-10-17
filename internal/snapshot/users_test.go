package snapshot_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
	userspb "github.com/remem-org/remem-go/internal/users/pb"
	"google.golang.org/protobuf/proto"
)

// withUsers is the populated world plus two users of "default", one disabled,
// and a token for the first.
func withUsers(t *testing.T) (*world, users.User, users.Issued) {
	t.Helper()
	w := populated(t)
	ctx := tenant.NewContext(context.Background(), "default")
	planner, err := w.users.Create(ctx, "planner")
	must(t, err)
	researcher, err := w.users.Create(ctx, "researcher")
	must(t, err)
	_, err = w.users.SetState(ctx, researcher.ID, users.Disabled)
	must(t, err)
	issued, err := w.users.IssueToken(ctx, planner.ID, nil)
	must(t, err)
	return w, planner, issued
}

func TestUsersAreExportedWithEveryFieldTheyHave(t *testing.T) {
	w, planner, _ := withUsers(t)
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{})
	if stats.Users != 2 {
		t.Fatalf("exported %d users, want 2", stats.Users)
	}
	got := decode(t, raw)
	if len(got.users) != 2 {
		t.Fatalf("the file holds %d users", len(got.users))
	}
	u := got.users[0]
	if u.GetTenant() != "default" || !bytes.Equal(u.GetId(), planner.ID[:]) || u.GetName() != "planner" ||
		u.GetState() != "active" || u.GetCreatedAtUnixMs() != uint64(planner.CreatedAt.UnixMilli()) {
		t.Fatalf("the first user exported as %v", u)
	}
	if got.users[1].GetState() != "disabled" {
		t.Fatalf("the disabled user exported as %v", got.users[1])
	}
}

// A snapshot is a portable file. Nothing token-shaped may be in it: not the
// secret, not the token id, not the digest the store keeps. The file is written
// uncompressed so a byte scan sees everything.
func TestNoTokenMaterialIsInTheFile(t *testing.T) {
	w, _, issued := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{Compression: snapshot.CompressionNone})

	row, err := w.kv.Get(context.Background(), keys.UserToken("default", tenant.DefaultNamespace, issued.Token.ID))
	must(t, err)
	var stored userspb.Token
	must(t, proto.Unmarshal(row, &stored))

	for what, b := range map[string][]byte{
		"the secret":   []byte(issued.Secret),
		"the token id": issued.Token.ID[:],
		"the digest":   stored.GetDigest(),
		"the index":    keys.TokenIndex(issued.Token.ID),
	} {
		if bytes.Contains(raw, b) {
			t.Errorf("the snapshot contains %s", what)
		}
	}
}

func TestASharedOnlyExportDropsUsersAndCountsThem(t *testing.T) {
	w, _, _ := withUsers(t)
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{SharedOnly: true})
	if stats.Users != 0 || stats.UsersDropped != 2 {
		t.Fatalf("a shared-only export wrote %d users and dropped %d, want 0 and 2", stats.Users, stats.UsersDropped)
	}
	for _, s := range decode(t, raw).sections {
		if s == snapshot.SectionGoUsers {
			t.Fatal("a shared-only export wrote a users block")
		}
	}
}

func TestAnExportWithoutAUserSourceWritesNone(t *testing.T) {
	w, _, _ := withUsers(t)
	src, done := w.sources()
	defer done()
	src.Users = nil
	var buf bytes.Buffer
	stats, err := snapshot.Export(context.Background(), src, &buf, snapshot.ExportOpts{})
	must(t, err)
	if stats.Users != 0 || len(decode(t, buf.Bytes()).users) != 0 {
		t.Fatal("an export with no user source wrote users")
	}
}

func TestUsersSurviveAnExportAndAnImport(t *testing.T) {
	w, _, _ := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if rep.Stats.Users != 2 {
		t.Fatalf("imported %d users, want 2", rep.Stats.Users)
	}
	ctx := tenant.NewContext(context.Background(), "default")
	want, _ := w.users.List(ctx)
	got, err := d.users.List(ctx)
	must(t, err)
	if len(got) != len(want) {
		t.Fatalf("%d users after import, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("user %d is %+v after import, want %+v", i, got[i], want[i])
		}
	}
	// The name rows were rebuilt, so a name is still held.
	if _, err := d.users.Create(ctx, "Planner"); err == nil {
		t.Fatal("a restored user's name is not held")
	}

	// And a second import of the same file changes nothing.
	again := d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if again.Skipped != 0 {
		t.Fatalf("a second import skipped %d rows", again.Skipped)
	}
}

// Every token is reissued after a restore: the one that worked before the
// export does not work after the import.
func TestATokenDoesNotSurviveARestore(t *testing.T) {
	w, _, issued := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if _, err := d.users.Verify(context.Background(), issued.Secret); err == nil {
		t.Fatal("a token from before the export authenticates after the import")
	}
}

func TestAConflictingUserFollowsTheConflictModeAndNamesTheField(t *testing.T) {
	w, planner, _ := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	ctx := tenant.NewContext(context.Background(), "default")
	_, err := d.users.Rename(ctx, planner.ID, "renamed-since")
	must(t, err)

	_, err = snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim, OnConflict: snapshot.ConflictFail})
	if !errs.Is(err, errs.Conflict) || !strings.Contains(err.Error(), "name") {
		t.Fatalf("fail mode: %v, want Conflict naming the field", err)
	}
	rep := d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim, OnConflict: snapshot.ConflictSkip})
	if rep.Skipped == 0 {
		t.Fatal("skip mode skipped nothing")
	}
	if u, _ := d.users.Get(ctx, planner.ID); u.Name != "renamed-since" {
		t.Fatalf("skip mode changed the user to %+v", u)
	}
	d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim, OnConflict: snapshot.ConflictOverwrite})
	if u, _ := d.users.Get(ctx, planner.ID); u.Name != "planner" {
		t.Fatalf("overwrite mode left %+v", u)
	}
}

// A users block an import has nowhere to put is refused rather than dropped,
// as the audit stream is.
func TestAnImportWithNoUserStoreRefusesUsers(t *testing.T) {
	w, _, _ := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	d.dst.Users = nil
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	if err == nil || !strings.Contains(err.Error(), "user") {
		t.Fatalf("got %v, want a refusal naming users", err)
	}
}

func TestVerifyComparesUsersFieldByField(t *testing.T) {
	w, planner, _ := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})

	verify := func() snapshot.VerifyReport {
		snap := d.kv.NewSnapshot()
		defer func() { _ = snap.Close() }()
		rep, err := snapshot.Verify(context.Background(), snapshot.Sources{
			Snap: snap, Tenants: d.dst.Tenants, Records: d.repo, Users: d.users,
		}, bytes.NewReader(raw))
		must(t, err)
		return rep
	}
	if rep := verify(); rep.Snapshot.Users != 2 || rep.Store.Users != 2 {
		t.Fatalf("verify counted %d/%d users", rep.Snapshot.Users, rep.Store.Users)
	} else {
		for _, f := range rep.Findings {
			if f.What == "user" {
				t.Fatalf("a clean import has a user finding: %v", f)
			}
		}
	}

	ctx := tenant.NewContext(context.Background(), "default")
	_, err := d.users.Rename(ctx, planner.ID, "renamed-since")
	must(t, err)
	extra, err := d.users.Create(ctx, "a-new-user")
	must(t, err)
	found := map[string]bool{}
	for _, f := range verify().Findings {
		if f.What == "user" {
			found[f.Kind.String()+" "+f.Ref] = true
		}
	}
	if !found["differs "+planner.ID.String()] || !found["extra "+extra.ID.String()] {
		t.Fatalf("verify found %v, want planner differing and the new user extra", found)
	}
}

// A binary from before users does not know section 41, and its importer writes
// each block as it reads it. If users came after the records, such a binary
// would import every memory and then refuse — leaving a store with everything
// but its users, the partial restore the refusal exists to prevent. Found by
// the real-binary run against the previous release. So the users block comes
// straight after the tenants, before anything an older reader would write.
func TestUsersAreWrittenBeforeAnyRecord(t *testing.T) {
	w, _, _ := withUsers(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	firstUsers, firstRecords := -1, -1
	for i, s := range decode(t, raw).sections {
		if s == snapshot.SectionGoUsers && firstUsers < 0 {
			firstUsers = i
		}
		if s == snapshot.SectionRecords && firstRecords < 0 {
			firstRecords = i
		}
	}
	if firstUsers < 0 || firstRecords < 0 || firstUsers > firstRecords {
		t.Fatalf("the first users block is block %d and the first records block %d; users must come first",
			firstUsers, firstRecords)
	}
}
