package memory_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/memory"
)

func TestBudgetedRecallRespectsTheBudget(t *testing.T) {
	svc := newService(t)
	// Twelve memories of roughly a hundred bytes each, so a budget of a
	// hundred tokens can hold three or four and no more.
	contents := make([]string, 12)
	for i := range contents {
		contents[i] = "distributed consensus and raft leader election, " + strings.Repeat("x", 90)
	}
	seed(t, svc, contents...)

	const budget = 100
	got, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Context:     "raft leader election",
		Type:        memory.SearchHybrid,
		TokenBudget: budget,
	})
	must(t, err)

	if len(got.Memories) == 0 {
		t.Fatal("the recall returned nothing at all")
	}
	if got.UsedTokens > budget {
		t.Fatalf("the recall used %d tokens against a budget of %d", got.UsedTokens, budget)
	}
	// The cost the caller is told is the cost of what came back.
	spent := 0
	for _, m := range got.Memories {
		spent += memory.TokensOf(m.Memory.Content)
	}
	if spent != got.UsedTokens {
		t.Errorf("reported %d tokens used, the returned memories cost %d", got.UsedTokens, spent)
	}
	if got.OmittedCount == 0 {
		t.Fatalf("returned %d of 12 relevant memories and reported nothing omitted; an agent "+
			"cannot tell that from \"there was nothing else\"", len(got.Memories))
	}
}

// A budget large enough for everything omits nothing, so omitted_count is a
// real signal rather than a number that is always positive.
func TestABudgetThatFitsOmitsNothing(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "raft leader election", "paxos", "two phase commit")

	got, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Context:     "consensus",
		Type:        memory.SearchHybrid,
		TokenBudget: 10_000,
	})
	must(t, err)
	if got.OmittedCount != 0 {
		t.Fatalf("omitted %d memories from a budget of 10,000 tokens", got.OmittedCount)
	}
	if len(got.Memories) != 3 {
		t.Fatalf("returned %d of 3 memories", len(got.Memories))
	}
}

func TestBudgetedRecallExcludesAlreadyHeld(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, "raft leader election", "paxos made simple", "two phase commit")

	held := []id.ID{ids[0], ids[2]}
	got, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Context:     "consensus",
		Type:        memory.SearchHybrid,
		TokenBudget: 10_000,
		AlreadyHave: held,
	})
	must(t, err)

	for _, m := range got.Memories {
		for _, h := range held {
			if m.Memory.ID == h {
				t.Fatalf("memory %s was already held and came back anyway", h)
			}
		}
	}
	if len(got.Memories) != 1 {
		t.Fatalf("returned %d memories, want the one not already held", len(got.Memories))
	}
}

// A memory too large to fit is skipped and counted, and a later smaller one is
// still taken. That is what makes this a budget rather than a cutoff.
func TestALargeMemoryDoesNotStopTheBudgetBeingFilled(t *testing.T) {
	svc := newService(t)
	seed(t, svc,
		"raft leader election "+strings.Repeat("y", 4000),
		"raft leader election, briefly")

	got, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Context:     "raft leader election",
		Type:        memory.SearchHybrid,
		TokenBudget: 200,
	})
	must(t, err)

	if len(got.Memories) != 1 {
		t.Fatalf("returned %d memories, want the small one", len(got.Memories))
	}
	if !strings.HasSuffix(got.Memories[0].Memory.Content, "briefly") {
		t.Fatalf("returned %q", got.Memories[0].Memory.Content)
	}
	if got.OmittedCount != 1 {
		t.Errorf("omitted %d, want the memory that did not fit", got.OmittedCount)
	}
}

// search_type is required here for the same reason it is required on search:
// the three modes answer different questions, and a default on one surface and
// a refusal on another is the split default Phase 8 removed.
func TestRecallRefusesAMissingSearchType(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "anything")

	_, err := svc.Recall(acmeCtx(), memory.RecallReq{Context: "x", TokenBudget: 100})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	for _, mode := range memory.SearchTypes() {
		if !strings.Contains(err.Error(), mode) {
			t.Errorf("the refusal does not name %q: %v", mode, err)
		}
	}
}

func TestRecallRefusesAMissingBudget(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "anything")

	_, err := svc.Recall(acmeCtx(), memory.RecallReq{Context: "x", Type: memory.SearchHybrid})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Errorf("the refusal does not mention the budget: %v", err)
	}
}

// Recall records no recall: the caller did not name these memories, the ranking
// chose them. It is the line search sits on, and moving it would make
// access_count a measure of how often an agent asked a question.
func TestRecallRecordsNoRecall(t *testing.T) {
	svc, _ := newRecordingService(t)
	ids := seed(t, svc, "raft leader election")

	_, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Context: "consensus", Type: memory.SearchHybrid, TokenBudget: 10_000,
	})
	must(t, err)

	entries, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(entries.Entries) != 0 {
		t.Fatalf("a budgeted recall wrote %d events: %+v", len(entries.Entries), entries.Entries)
	}

	// And a Get does, which is what makes the distinction visible rather than
	// merely asserted.
	if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	entries, err = svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(entries.Entries) != 1 || entries.Entries[0].Kind != "recalled" {
		t.Fatalf("fetching by id produced %+v, want one recall", entries)
	}
}

// A search records nothing either, for the same reason.
func TestSearchRecordsNoRecall(t *testing.T) {
	svc, _ := newRecordingService(t)
	ids := seed(t, svc, "raft leader election")

	_, err := svc.Search(acmeCtx(), memory.SearchReq{Query: "consensus", Type: memory.SearchSemantic})
	must(t, err)

	entries, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(entries.Entries) != 0 {
		t.Fatalf("a search wrote %d events", len(entries.Entries))
	}
}

// A recall is visible on the very next read, without waiting for a sweep.
//
// This is the defect Phase 10's end-to-end run found. A recall appends an event
// and does not touch the record, and the record is next visited when it is owed
// something — for anything that decays, a day away. So the stored count lagged
// by a day rather than by a sweep interval, and a caller who fetched a memory
// ten times and saw zero would be right to call that broken.
//
// Rust reaches the same answer for the same reason (search_engine.rs:165-171):
// a read reports the pending delta and never consumes it.
func TestARecallIsVisibleOnTheNextReadWithoutASweep(t *testing.T) {
	svc, clk := newRecordingService(t)
	ids := seed(t, svc, "a memory worth using")

	for i := 0; i < 3; i++ {
		clk.Advance(2 * lifecycle.DefaultRecallWindow)
		if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	got, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{})
	must(t, err)
	// Three recall sessions, not four reads. The final fetch shares an instant
	// with the third, so it falls inside the coalescing window and adds nothing
	// — which is the same rule, seen from the other side.
	if got.AccessCount != 3 {
		t.Fatalf("access count is %d after three recall sessions and no sweep, want 3", got.AccessCount)
	}
	if got.LastRecalledAt.IsZero() {
		t.Fatal("last_recalled_at is unset after four recalls")
	}
}

// The peek does not write. A read that consumed a recall would make the count
// depend on how often somebody looked at the memory, and it would fail on a
// read-only store.
func TestThePeekDoesNotConsumeTheRecall(t *testing.T) {
	svc, clk := newRecordingService(t)
	ids := seed(t, svc, "a memory")

	clk.Advance(2 * lifecycle.DefaultRecallWindow)
	if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Reading many times inside one coalescing window adds no recalls, so the
	// count must not climb with the reads.
	var counts []uint32
	for i := 0; i < 5; i++ {
		got, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{})
		must(t, err)
		counts = append(counts, got.AccessCount)
	}
	for i, n := range counts {
		if n != counts[0] {
			t.Fatalf("read %d reported %d recalls, the first reported %d; the peek is "+
				"consuming what it reads", i, n, counts[0])
		}
	}

	// And the stream still holds exactly the one recall.
	entries, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(entries.Entries) != 1 {
		t.Fatalf("the stream holds %d events, want 1", len(entries.Entries))
	}
}

// Search reports the same count as a fetch. Rust's comment is the reason: "the
// same memory shows a different use count depending on which endpoint asked" is
// the failure, and one number everywhere is the fix.
func TestSearchReportsTheSameCountAsAFetch(t *testing.T) {
	svc, clk := newRecordingService(t)
	ids := seed(t, svc, "raft leader election")

	for i := 0; i < 2; i++ {
		clk.Advance(2 * lifecycle.DefaultRecallWindow)
		if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	fetched, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{})
	must(t, err)
	found, err := svc.Search(acmeCtx(), memory.SearchReq{Query: "consensus", Type: memory.SearchSemantic})
	must(t, err)
	if len(found.Results) == 0 {
		t.Fatal("the search found nothing")
	}
	if found.Results[0].Memory.AccessCount != fetched.AccessCount {
		t.Fatalf("search reports %d recalls, a fetch reports %d",
			found.Results[0].Memory.AccessCount, fetched.AccessCount)
	}
}
