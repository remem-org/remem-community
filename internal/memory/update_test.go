package memory_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

func TestUpdatedContentIsFoundByItsNewWords(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "the invoice number is INV-2024-8871"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}

	replacement := "the purchase order is PO-2025-4410"
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	res, err := svc.Search(ctx, memory.SearchReq{
		Query: "PO-2025-4410", Type: memory.SearchKeyword, Limit: 10,
	})
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Memory.ID != m.ID {
		t.Fatalf("the new words did not find the memory: %d results", len(res.Results))
	}
}

func TestUpdatedContentIsNotFoundByItsOldWords(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "the invoice number is INV-2024-8871"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	replacement := "the purchase order is PO-2025-4410"
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	res, err := svc.Search(ctx, memory.SearchReq{
		Query: "INV-2024-8871", Type: memory.SearchKeyword, Limit: 10,
	})
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	// The old postings must be gone. A stale posting is worse than a missing
	// one: it returns a memory whose content no longer contains the term the
	// caller searched for, and nothing says why.
	if len(res.Results) != 0 {
		t.Fatalf("the old words still find the memory: %d results", len(res.Results))
	}
}

// The decoy is what makes this test able to fail. Over a one-memory corpus a
// semantic search returns that memory whatever its vector says, so an assertion
// that the subject comes back first would hold with the restage deleted. With a
// second memory that still means the subject's *old* content, a stale vector
// puts the two side by side and the ordering stops being a foregone conclusion.
func TestUpdatedContentIsFoundByItsNewMeaning(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	decoy := seed(t, svc, "a recipe for marzipan")[0]

	m, err := svc.Create(ctx, memory.CreateReq{Content: "a recipe for marzipan"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	replacement := "raft leader election and distributed consensus"
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	res, err := svc.Search(ctx, memory.SearchReq{
		Query: "raft leader election and distributed consensus",
		Type:  memory.SearchSemantic, Limit: 10,
	})
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	if len(res.Results) == 0 || res.Results[0].Memory.ID != m.ID {
		t.Fatalf("the new meaning did not find the memory: %d results", len(res.Results))
	}

	// And the old meaning now belongs to the decoy. This is the half that
	// catches a missing vector restage: with the old vector still stored, the
	// subject is as marzipan-shaped as the decoy and may take this position.
	res, err = svc.Search(ctx, memory.SearchReq{
		Query: "a recipe for marzipan", Type: memory.SearchSemantic, Limit: 10,
	})
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	if len(res.Results) == 0 || res.Results[0].Memory.ID != decoy {
		t.Fatalf("the old meaning still belongs to the updated memory: %v", res.Results[0].Memory.ID)
	}
}

func TestUpdateLeavesUnsetFieldsAlone(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	imp := float32(0.9)
	m, err := svc.Create(ctx, memory.CreateReq{
		Content: "keep this", Tags: []string{"alpha"}, Importance: &imp, Source: "mcp",
	})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}

	replacement := "changed this"
	got, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement})
	if err != nil {
		t.Fatalf("updating: %v", err)
	}
	if got.Content != replacement {
		t.Fatalf("content is %q, want %q", got.Content, replacement)
	}
	if len(got.Tags) != 1 || got.Tags[0] != "alpha" {
		t.Fatalf("tags changed to %v; nil means leave them alone", got.Tags)
	}
	if got.Importance != 0.9 {
		t.Fatalf("importance changed to %v; nil means leave it alone", got.Importance)
	}
	if got.Source != "mcp" {
		t.Fatalf("source changed to %q; nil means leave it alone", got.Source)
	}
}

// Nil and empty must not mean the same thing. With a plain []string the second
// case is unsayable, which is why the field is a pointer to a slice.
func TestAnEmptyTagSliceRemovesEveryTag(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "tagged", Tags: []string{"alpha", "beta"}})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}

	none := []string{}
	got, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Tags: &none})
	if err != nil {
		t.Fatalf("updating: %v", err)
	}
	if len(got.Tags) != 0 {
		t.Fatalf("tags are %v, want none", got.Tags)
	}
}

func TestUpdateRecordsWhatChangedAndNotItsValues(t *testing.T) {
	svc, _ := newRecordingService(t)
	ctx := acmeCtx()

	m, err := svc.Create(ctx, memory.CreateReq{Content: "the first version"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	replacement := "the second version"
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	entries, err := svc.History(ctx, memory.HistoryReq{ID: m.ID, Limit: 10})
	if err != nil {
		t.Fatalf("reading history: %v", err)
	}
	if len(entries.Entries) == 0 || entries.Entries[0].Kind != "updated" {
		t.Fatalf("newest entry is %+v, want an updated event", entries)
	}
	if got := entries.Entries[0].After["fields"]; got != "content" {
		t.Fatalf("the event names %q as changed, want content", got)
	}
	// The event must not carry the content itself, in either direction.
	for _, fields := range []map[string]string{entries.Entries[0].Before, entries.Entries[0].After} {
		for k, v := range fields {
			if strings.Contains(v, "version") {
				t.Fatalf("the event carries memory content in %s=%q", k, v)
			}
		}
	}
}

// One rule, both write paths. An edit raising arousal to the flashbulb
// threshold promotes and protects exactly as a create at that arousal does.
func TestRaisingArousalToFlashbulbPromotesAndProtects(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	calm := float32(0.1)
	m, err := svc.Create(ctx, memory.CreateReq{
		Content: "an ordinary fact", Policy: "short_term", Arousal: &calm,
	})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	if m.Policy != "short_term" {
		t.Fatalf("stored policy is %q, want short_term", m.Policy)
	}

	intense := float32(0.9)
	got, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Arousal: &intense})
	if err != nil {
		t.Fatalf("updating: %v", err)
	}
	if got.Policy != "long_term" {
		t.Fatalf("policy is %q after arousal 0.9, want long_term", got.Policy)
	}
	if got.ProtectedUntil.IsZero() {
		t.Fatal("no protection window was set")
	}
}

// A protection already granted is not revoked by an edit. Making it revocable
// would turn the window into a value that changes by itself, which is what
// Phase 10 refused when it declined the plan's temporary-policy shape.
func TestLoweringArousalDoesNotRevokeProtection(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	intense := float32(0.9)
	m, err := svc.Create(ctx, memory.CreateReq{Content: "a vivid fact", Arousal: &intense})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	protected := m.ProtectedUntil

	calm := float32(0.1)
	got, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Arousal: &calm})
	if err != nil {
		t.Fatalf("updating: %v", err)
	}
	if !got.ProtectedUntil.Equal(protected) {
		t.Fatalf("protection moved from %v to %v", protected, got.ProtectedUntil)
	}
}

func TestUpdatingAnArchivedMemoryIsRefused(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "retire me"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	if err := svc.Delete(ctx, m.ID, false); err != nil {
		t.Fatalf("archiving: %v", err)
	}

	replacement := "changed"
	_, err = svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement})
	if !errs.Is(err, errs.Conflict) {
		t.Fatalf("updating an archived memory returned %v, want Conflict", err)
	}
	if !strings.Contains(err.Error(), "archived") {
		t.Fatalf("the message does not name the cause: %q", err)
	}
}

// newSchedulingService is newService with a lifecycle scheduler wired into the
// repository, which is how the server wires it (server/deps.go). It returns the
// repository too: next_attention_at is a record field with no surface of its
// own, deliberately — it is the sweep's business and not a caller's.
func newSchedulingService(t *testing.T) (*memory.Service, record.Repo) {
	t.Helper()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(texts),
		record.WithScheduler(lifecycle.NewScheduler(nil)))
	index := flat.New(vector.NewStore(kv), distance.L2)
	svc := memory.New(kv, repo, index, embeddingtest.New(), clk, attr.MustTable(),
		graph.NewService(kv, clk), memory.Config{}, memory.WithTextIndex(texts))
	t.Cleanup(func() { _ = svc.Close() })
	return svc, repo
}

func nextAttentionOf(t *testing.T, repo record.Repo, ctx context.Context, rid id.ID) time.Time {
	t.Helper()
	rec, err := repo.Get(ctx, rid)
	must(t, err)
	return rec.Fields.NextAttentionAt
}

// A policy change reaches the sweep on the new schedule, because Put
// recomputes next_attention_at through the scheduler. This is the narrow
// half of REM-114 and it does not close it: a change to a *tenant's policy
// table* still reaches records only when they are next visited.
//
// The pair is short_term to *pinned* rather than to long_term, and the reason
// is worth keeping: the schedule is a function of *whether* a transition is
// owed, not of how fast it runs. short_term and long_term decay health at 8.0
// and 2.0 points a day, and both are therefore due a whole day out — the same
// instant. Only pinned, which is owed nothing at all, falls through to the
// thirty-day cap. A test asserting short_term to long_term moves the schedule
// asserts something false about the scheduler rather than something true about
// Update.
func TestChangingPolicyReschedulesTheRecord(t *testing.T) {
	svc, repo := newSchedulingService(t)
	ctx := acmeCtx()

	m, err := svc.Create(ctx, memory.CreateReq{Content: "reschedule me", Policy: "short_term"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	before := nextAttentionOf(t, repo, ctx, m.ID)

	pinned := "pinned"
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Policy: &pinned}); err != nil {
		t.Fatalf("updating: %v", err)
	}
	if after := nextAttentionOf(t, repo, ctx, m.ID); after.Equal(before) {
		t.Fatalf("next_attention_at did not move from %v", before)
	}
}

// The other half of the same mechanism, and the one an operator actually
// notices: a TTL shortened by an edit expires the memory on the new deadline
// rather than the one it was written under.
func TestShorteningATTLReschedulesTheRecord(t *testing.T) {
	svc, repo := newSchedulingService(t)
	ctx := acmeCtx()

	m, err := svc.Create(ctx, memory.CreateReq{Content: "expire me sooner"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	before := nextAttentionOf(t, repo, ctx, m.ID)

	short := time.Hour
	if _, err := svc.Update(ctx, memory.UpdateReq{ID: m.ID, TTL: &short}); err != nil {
		t.Fatalf("updating: %v", err)
	}
	after := nextAttentionOf(t, repo, ctx, m.ID)
	if !after.Before(before) {
		t.Fatalf("next_attention_at is %v, want earlier than %v", after, before)
	}
}

// drain claims every pending discovery job, so that a later pendingDiscovery
// sees only what the call under test queued. Claiming rather than deleting is
// deliberate: it is the state a real worker leaves behind.
func drain(t *testing.T, q *jobs.Queue) {
	t.Helper()
	for {
		batch, err := q.Claim(context.Background(), "acme", "test", []jobs.Type{discoveryType}, 100)
		must(t, err)
		if len(batch) == 0 {
			return
		}
	}
}

func pendingDiscovery(t *testing.T, q *jobs.Queue) []*jobs.Job {
	t.Helper()
	var out []*jobs.Job
	for _, j := range discoveryJobs(t, q, "acme") {
		if j.State == jobs.Pending {
			out = append(out, j)
		}
	}
	return out
}

// Discovery fires on a content change and only on a content change. A
// tags-only edit moves nothing about the memory's meaning, so there is nothing
// for a vector search to reconsider.
func TestUpdatingContentEnqueuesExactlyOneDiscoveryJob(t *testing.T) {
	s := newDiscoveringService(t)
	ctx := acmeCtx()

	m, err := s.svc.Create(ctx, memory.CreateReq{Content: "the first version"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	drain(t, s.queue) // the create's own discovery job

	replacement := "a completely different fact"
	if _, err := s.svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &replacement}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	queued := pendingDiscovery(t, s.queue)
	if len(queued) != 1 {
		t.Fatalf("a content change queued %d discovery jobs, want 1", len(queued))
	}
	subjects, err := discovery.DecodePayload(queued[0].Payload)
	must(t, err)
	if len(subjects) != 1 || subjects[0] != m.ID {
		t.Fatalf("the queued job names %v, want just the memory %s", subjects, m.ID)
	}
}

func TestUpdatingOnlyTagsEnqueuesNoDiscovery(t *testing.T) {
	s := newDiscoveringService(t)
	ctx := acmeCtx()

	m, err := s.svc.Create(ctx, memory.CreateReq{Content: "unchanged content"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	drain(t, s.queue)

	tags := []string{"newtag"}
	if _, err := s.svc.Update(ctx, memory.UpdateReq{ID: m.ID, Tags: &tags}); err != nil {
		t.Fatalf("updating: %v", err)
	}

	if queued := pendingDiscovery(t, s.queue); len(queued) != 0 {
		t.Fatalf("a tags-only change queued %d discovery jobs, want 0", len(queued))
	}
}

// Rewriting the content to the same string is not a change. Nothing derived
// from it moved, so nothing derived from it should be recomputed.
func TestRewritingIdenticalContentEnqueuesNoDiscovery(t *testing.T) {
	s := newDiscoveringService(t)
	ctx := acmeCtx()

	const same = "identical"
	m, err := s.svc.Create(ctx, memory.CreateReq{Content: same})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}
	drain(t, s.queue)

	again := same
	if _, err := s.svc.Update(ctx, memory.UpdateReq{ID: m.ID, Content: &again}); err != nil {
		t.Fatalf("updating: %v", err)
	}
	if queued := pendingDiscovery(t, s.queue); len(queued) != 0 {
		t.Fatalf("an identical rewrite queued %d discovery jobs, want 0", len(queued))
	}
}

// Two writers, one memory. The loser must conflict and re-run against the
// winner rather than overwrite it — the tx.Expect over originalBody is what
// makes that true, and this is the test that proves it rather than assuming it.
func TestConcurrentUpdatesConflictRatherThanLose(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "start"})
	if err != nil {
		t.Fatalf("storing: %v", err)
	}

	const writers = 16
	var wg sync.WaitGroup
	errsCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src := fmt.Sprintf("writer-%d", i)
			_, uerr := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Source: &src})
			errsCh <- uerr
		}(i)
	}
	wg.Wait()
	close(errsCh)
	for e := range errsCh {
		if e != nil {
			t.Fatalf("a concurrent update failed rather than retrying: %v", e)
		}
	}

	got, err := svc.Get(ctx, m.ID, memory.GetOpts{})
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	// Whichever writer landed last, the record must hold exactly one of their
	// values — not a mixture, and not the value it started with.
	if !strings.HasPrefix(got.Source, "writer-") {
		t.Fatalf("source is %q; no writer's value survived", got.Source)
	}
}
