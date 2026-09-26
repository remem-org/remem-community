package text_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

const (
	acme tenant.ID        = "acme"
	ns   tenant.Namespace = tenant.DefaultNamespace
)

func newIndex(t *testing.T) (*text.Index, storage.KV) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return text.New(), kv
}

// stage runs one indexing operation in its own transaction, as the write path
// does. A nil rec removes.
func stage(t *testing.T, ix *text.Index, kv storage.KV, tn tenant.ID, rid id.ID, rec *record.Record) {
	t.Helper()
	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.Stage(context.Background(), tx, tn, ns, rid, rec); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

func memoryRecord(rid id.ID, tn tenant.ID, content string, tags ...string) *record.Record {
	return &record.Record{
		ID: rid, Tenant: tn, Namespace: ns, Type: record.TypeMemory,
		Content:   content,
		Fields:    record.Fields{Tags: tags}.WithDefaults(),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
}

// postingsFor returns every (term, record) pair in a tenant's text space, so a
// test can assert about the whole index rather than about the terms it thought
// to look up.
func postingsFor(t *testing.T, kv storage.KV, tn tenant.ID) map[string][]id.ID {
	t.Helper()
	lower, upper := keys.SpaceRange(tn, ns, keys.SpaceText)
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	out := map[string][]id.ID{}
	for ok := it.First(); ok; ok = it.Next() {
		_, _, term, rid, err := keys.ParseText(it.Key())
		if err != nil {
			t.Fatalf("parsing a text key: %v", err)
		}
		out[term] = append(out[term], rid)
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	return out
}

// contentPostings drops the reserved rows, leaving the postings a query scans.
func contentPostings(t *testing.T, kv storage.KV, tn tenant.ID) map[string][]id.ID {
	t.Helper()
	out := map[string][]id.ID{}
	for term, ids := range postingsFor(t, kv, tn) {
		if term[0] >= 0x20 || term[0] == 0x01 {
			out[term] = ids
		}
	}
	return out
}

// TestRemovalClearsEveryPosting is the plan's test. "Every" is the word that
// matters: a posting left behind points at a record that no longer exists, and
// the search that finds it reports a memory the user deleted.
func TestRemovalClearsEveryPosting(t *testing.T) {
	ix, kv := newIndex(t)
	rid := id.New()

	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme,
		"the quarterly revenue report for the Berlin office", "finance", "berlin"))
	if len(contentPostings(t, kv, acme)) == 0 {
		t.Fatal("indexing a memory produced no postings at all")
	}

	stage(t, ix, kv, acme, rid, nil)

	for term, ids := range postingsFor(t, kv, acme) {
		if slices.Contains(ids, rid) {
			t.Errorf("after removal the term %q still lists the record", term)
		}
	}
}

// An update must withdraw what the previous version wrote. A posting nothing
// withdraws is not detectably wrong — it points at a record that exists — it
// simply returns that memory for a word it no longer contains.
func TestAnUpdateWithdrawsThePostingsItReplaced(t *testing.T) {
	ix, kv := newIndex(t)
	rid := id.New()

	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme, "kubernetes cluster autoscaling", "infra"))
	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme, "postgres replication lag", "database"))

	postings := postingsFor(t, kv, acme)
	for _, gone := range []string{"kubernetes", "cluster", "autoscaling", text.TagTerm("infra")} {
		if slices.Contains(postings[gone], rid) {
			t.Errorf("the withdrawn term %q still lists the record", gone)
		}
	}
	for _, kept := range []string{"postgres", "replication", "lag", text.TagTerm("database")} {
		if !slices.Contains(postings[kept], rid) {
			t.Errorf("the new term %q does not list the record", kept)
		}
	}
}

// Postings are staged into the caller's transaction and land with the record
// (spec §12). This is the property that removes the drift class the vector
// index has to design against: there is no window in which a stored memory is
// unfindable by a word it contains.
func TestPostingsAreInvisibleUntilTheRecordCommits(t *testing.T) {
	ix, kv := newIndex(t)
	ctx := context.Background()
	rid := id.New()

	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.Stage(ctx, tx, acme, ns, rid, memoryRecord(rid, acme, "atomic visibility")); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if got := len(postingsFor(t, kv, acme)); got != 0 {
		t.Fatalf("%d rows were visible before the transaction committed", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing: %v", err)
	}
	if len(contentPostings(t, kv, acme)) == 0 {
		t.Fatal("no postings after the commit")
	}
}

// Tags and content share one index (plan §II.10 row 2). A tag is a term in a
// reserved namespace rather than a second structure with its own limits, which
// is what removes Rust's tag_index_can_answer and its 100-byte cut-off.
func TestTagsAndContentShareOneIndex(t *testing.T) {
	ix, kv := newIndex(t)
	rid := id.New()

	long := "a-tag-far-longer-than-any-hardwired-hundred-byte-limit-would-ever-have-allowed-through-the-index"
	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme, "weather forecast", "weather", long))

	postings := postingsFor(t, kv, acme)
	if !slices.Contains(postings["weather"], rid) {
		t.Error("the content term is missing")
	}
	if !slices.Contains(postings[text.TagTerm("weather")], rid) {
		t.Error("the tag term is missing")
	}
	if !slices.Contains(postings[text.TagTerm(long)], rid) {
		t.Errorf("a %d-byte tag produced no posting, so a filter on it cannot be answered", len(long))
	}
	// The two must not be the same row: a memory tagged "weather" and one
	// merely mentioning the word are different answers to a tag filter.
	if text.TagTerm("weather") == "weather" {
		t.Fatal("the tag namespace is not reserved")
	}
}

// Every user-owned row carries a tenant (Invariant 1). One tenant's postings
// must be unreachable from another's range, or a keyword search is a
// cross-tenant read.
func TestPostingsAreTenantScoped(t *testing.T) {
	ix, kv := newIndex(t)
	mine, theirs := id.New(), id.New()

	stage(t, ix, kv, acme, mine, memoryRecord(mine, acme, "shared vocabulary"))
	stage(t, ix, kv, "globex", theirs, memoryRecord(theirs, "globex", "shared vocabulary"))

	for _, rid := range postingsFor(t, kv, acme)["shared"] {
		if rid == theirs {
			t.Fatal("another tenant's record is inside this tenant's postings")
		}
	}
	if len(postingsFor(t, kv, "globex")["shared"]) != 1 {
		t.Fatal("the other tenant's own posting is missing")
	}
}

// A record with no terms — punctuation, or a single letter — must still leave
// the index consistent. It has no postings, and nothing must be left over from
// a version that did.
func TestARecordWithNoTermsLeavesNothingBehind(t *testing.T) {
	ix, kv := newIndex(t)
	rid := id.New()

	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme, "kubernetes"))
	stage(t, ix, kv, acme, rid, memoryRecord(rid, acme, "... ?!"))

	if ids := postingsFor(t, kv, acme)["kubernetes"]; slices.Contains(ids, rid) {
		t.Fatal("a record rewritten to have no terms kept its old posting")
	}
}
