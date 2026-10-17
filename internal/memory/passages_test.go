package memory_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// Passage rows, through the memory service and the fake embedder, whose
// passages are embeddingtest.PassageWords words long. These hold the write
// path's half of the passage contract: what is stored, replaced and removed,
// and when. That a search finds a memory by its passage is the vector index's
// half, held where the index reads these rows.

// longContent is n passages of filler, with fact as word `at`.
func longContent(passages, at int, fact string) string {
	words := make([]string, passages*embeddingtest.PassageWords)
	for i := range words {
		words[i] = fmt.Sprintf("filler%d", i%7)
	}
	if fact != "" {
		words[at] = fact
	}
	return strings.Join(words, " ")
}

// passageRows reads every stored passage row of rid, in passage order.
func passageRows(t *testing.T, kv storage.KV, tn tenant.ID, rid id.ID) []vector.PassageRow {
	t.Helper()
	lo, hi := keys.PassagesOfRange(tn, tenant.DefaultNamespace, rid)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	var out []vector.PassageRow
	for ok := it.First(); ok; ok = it.Next() {
		_, _, _, p, err := keys.ParsePassage(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		if int(p) != len(out) {
			t.Fatalf("passage %d stored where %d was expected: a memory's passages are contiguous from 0", p, len(out))
		}
		row, err := vector.DecodePassage(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rawPassages is the stored bytes of every passage row, for asserting that a
// write left them exactly as they were.
func rawPassages(t *testing.T, kv storage.KV, tn tenant.ID, rid id.ID) [][]byte {
	t.Helper()
	lo, hi := keys.PassagesOfRange(tn, tenant.DefaultNamespace, rid)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	var out [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, append(bytes.Clone(it.Key()), it.Value()...))
	}
	return out
}

func spanText(content string, r vector.PassageRow) string { return content[r.Start:r.End] }

// A fact in the third passage is stored in the third passage's row, with the
// span that bounds it, and every row carries the model's id.
func TestALongMemorysLaterPassageIsStoredWithItsSpan(t *testing.T) {
	var kv storage.KV
	svc := newService(t, withStore(&kv))
	content := longContent(3, 2*embeddingtest.PassageWords+5, "zanzibar")
	ids := seed(t, svc, content)

	rows := passageRows(t, kv, "acme", ids[0])
	if len(rows) != 3 {
		t.Fatalf("%d passage rows for a three-passage memory", len(rows))
	}
	if !strings.Contains(spanText(content, rows[2]), "zanzibar") {
		t.Fatalf("passage 2 spans %q, which does not hold the fact", spanText(content, rows[2]))
	}
	for i, r := range rows[:2] {
		if strings.Contains(spanText(content, r), "zanzibar") {
			t.Fatalf("passage %d holds the fact as well", i)
		}
	}
	if rows[0].Start != 0 || int(rows[2].End) != len(content) {
		t.Fatalf("passages span [%d,%d) of %d bytes", rows[0].Start, rows[2].End, len(content))
	}
	for i, r := range rows {
		if r.Vector.ModelID != embedding.Model || r.Vector.Dim != embedding.Dim {
			t.Fatalf("passage %d is stamped %q/%d", i, r.Vector.ModelID, r.Vector.Dim)
		}
	}
}

// A memory that fits one passage is indexed by its canonical vector, which is
// that passage, so nothing else is written for it.
func TestAOnePassageMemoryWritesNoPassageRows(t *testing.T) {
	var kv storage.KV
	svc := newService(t, withStore(&kv))
	ids := seed(t, svc, "a short memory about the harbour")
	if rows := passageRows(t, kv, "acme", ids[0]); len(rows) != 0 {
		t.Fatalf("a one-passage memory wrote %d passage rows", len(rows))
	}
}

// Rewriting the content replaces the passages: the new content's, all of them,
// and none of the old. A shorter content must not leave the old tail behind.
func TestAContentUpdateReplacesThePassages(t *testing.T) {
	var kv storage.KV
	svc := newService(t, withStore(&kv))
	old := longContent(4, 3*embeddingtest.PassageWords+1, "zanzibar")
	ids := seed(t, svc, old)
	if got := len(passageRows(t, kv, "acme", ids[0])); got != 4 {
		t.Fatalf("%d rows before the update", got)
	}

	updated := longContent(2, embeddingtest.PassageWords+3, "timbuktu")
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: ids[0], Content: &updated}); err != nil {
		t.Fatal(err)
	}
	rows := passageRows(t, kv, "acme", ids[0])
	if len(rows) != 2 {
		t.Fatalf("%d passage rows after rewriting four passages as two; the old tail survived", len(rows))
	}
	for i, r := range rows {
		if int(r.End) > len(updated) {
			t.Fatalf("passage %d spans past the new content", i)
		}
		if strings.Contains(spanText(updated, r), "zanzibar") {
			t.Fatalf("passage %d still holds the old fact", i)
		}
	}
	if !strings.Contains(spanText(updated, rows[1]), "timbuktu") {
		t.Fatal("the new fact is not in the new second passage")
	}

	// Rewritten as one passage, the memory has no passage rows at all.
	short := "now a short memory"
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: ids[0], Content: &short}); err != nil {
		t.Fatal(err)
	}
	if rows := passageRows(t, kv, "acme", ids[0]); len(rows) != 0 {
		t.Fatalf("%d passage rows after rewriting as one passage", len(rows))
	}
}

// Writes that do not change the content leave the passages exactly as they
// were: a tag change, a policy change and an archive each rewrite the record
// and must not touch rows only a content change can make stale.
func TestWritesThatKeepTheContentKeepThePassages(t *testing.T) {
	var kv storage.KV
	svc := newService(t, withStore(&kv))
	ids := seed(t, svc, longContent(3, 40, "zanzibar"))
	before := rawPassages(t, kv, "acme", ids[0])
	if len(before) != 3 {
		t.Fatalf("%d rows to begin with", len(before))
	}
	same := func(what string) {
		t.Helper()
		after := rawPassages(t, kv, "acme", ids[0])
		if len(after) != len(before) {
			t.Fatalf("after %s: %d rows, was %d", what, len(after), len(before))
		}
		for i := range after {
			if !bytes.Equal(after[i], before[i]) {
				t.Fatalf("after %s: passage %d changed", what, i)
			}
		}
	}

	tags := []string{"harbour"}
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: ids[0], Tags: &tags}); err != nil {
		t.Fatal(err)
	}
	same("a tag change")
	policy := "long_term"
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: ids[0], Policy: &policy}); err != nil {
		t.Fatal(err)
	}
	same("a policy change")
	if err := svc.Delete(acmeCtx(), ids[0], false); err != nil {
		t.Fatal(err)
	}
	same("an archive")
}

// A hard delete takes the passages with the record, in the same transaction:
// a passage row whose memory is gone is an index entry for nothing.
func TestAHardDeleteRemovesThePassages(t *testing.T) {
	var kv storage.KV
	svc := newService(t, withStore(&kv))
	ids := seed(t, svc, longContent(3, 40, "zanzibar"), longContent(2, 10, "other"))
	if err := svc.Delete(acmeCtx(), ids[0], true); err != nil {
		t.Fatal(err)
	}
	if rows := passageRows(t, kv, "acme", ids[0]); len(rows) != 0 {
		t.Fatalf("%d passage rows survived a hard delete", len(rows))
	}
	if rows := passageRows(t, kv, "acme", ids[1]); len(rows) != 2 {
		t.Fatalf("the other memory's passages went too: %d rows", len(rows))
	}
}

// beyondTheFirstSegment is a memory whose only mention of fact is passage 17,
// past the first segment's sixteen passages. Its document vector — the mean of
// the first segment — knows nothing of the fact, so only a passage row can
// find it. The passage is the fact alone, so under the fake embedder it matches
// the query exactly.
func beyondTheFirstSegment(fact string) string {
	words := strings.Fields(longContent(20, 0, ""))
	for i := 17 * embeddingtest.PassageWords; i < 18*embeddingtest.PassageWords; i++ {
		words[i] = fact
	}
	return strings.Join(words, " ")
}

// The search half of the passage contract, which 3.3 left to the index: a fact
// that only a passage row holds finds its memory, ahead of a decoy that merely
// mentions the word. The decoy is what makes this a test: without passages the
// long memory scores nothing for the query and the decoy wins.
func TestAFactOnlyAPassageHoldsFindsItsMemory(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, beyondTheFirstSegment("zanzibar"), "zanzibar is a spice island")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "zanzibar", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 || res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("the memory holding the fact in passage 17 was not found first: %v", res.Results)
	}
}

// And a rewrite takes the old content's passages with it: the fact the memory
// no longer holds no longer finds it, and the decoy is what the search returns.
func TestAFactOnlyTheOldContentHeldNoLongerFindsTheMemory(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, beyondTheFirstSegment("zanzibar"), "zanzibar is a spice island")
	rewritten := beyondTheFirstSegment("timbuktu")
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: ids[0], Content: &rewritten}); err != nil {
		t.Fatal(err)
	}

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "zanzibar", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 || res.Results[0].Memory.ID != ids[1] {
		t.Fatalf("the old fact still found the rewritten memory: %v", res.Results)
	}
	res, err = svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "timbuktu", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 || res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("the new fact does not find the rewritten memory: %v", res.Results)
	}
}

// Hybrid search fuses by memory, and rank fusion adds 1/(k+rank+1) for every
// time a memory appears in a source's list. A memory whose fact is in several
// passages must therefore appear once in the vector source, ranked by its
// nearest passage — or it collects a contribution per passage, and its fused
// score outranks a memory that is equally relevant but shorter. Appearing once
// on the page is not the property: Fuse collapses ids either way, so a check of
// the page passed with the index returning one hit per passage. The number of
// contributions is.
func TestHybridSearchFusesAMultiPassageMemoryOncePerSource(t *testing.T) {
	svc := newService(t)
	words := strings.Fields(longContent(20, 0, ""))
	for _, p := range []int{1, 5, 17} {
		for i := p * embeddingtest.PassageWords; i < (p+1)*embeddingtest.PassageWords; i++ {
			words[i] = "zanzibar"
		}
	}
	ids := seed(t, svc, strings.Join(words, " "), "zanzibar is a spice island", "a harbour note")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchHybrid, Query: "zanzibar",
		Limit: 10, Explain: true})
	must(t, err)
	var found bool
	for _, r := range res.Results {
		if r.Memory.ID != ids[0] {
			continue
		}
		found = true
		per := map[string]int{}
		for _, src := range r.Sources {
			per[src.Source]++
		}
		if per["vector"] != 1 || per["text"] != 1 {
			t.Fatalf("the three-passage memory was fused from %v contributions; want one per source", per)
		}
	}
	if !found {
		t.Fatal("the memory holding the fact was not returned")
	}
}

// fifthPassageContent is a memory whose passage 4 is the fact alone, so under
// the fake embedder a search for the fact ranks the memory by that passage.
func fifthPassageContent(fact string) string {
	words := strings.Fields(longContent(8, 0, ""))
	for i := 4 * embeddingtest.PassageWords; i < 5*embeddingtest.PassageWords; i++ {
		words[i] = fact
	}
	return strings.Join(words, " ")
}

// A result names the passage that ranked its memory, and the span bounds
// exactly that passage's text.
func TestAResultNamesThePassageThatRankedIt(t *testing.T) {
	svc := newService(t)
	content := fifthPassageContent("zanzibar")
	ids := seed(t, svc, content, "a harbour note")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "zanzibar", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 || res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("results = %v", res.Results)
	}
	p := res.Results[0].Passage
	if p == nil {
		t.Fatal("a semantic result carries no passage")
	}
	if p.Index != 4 {
		t.Fatalf("passage index %d, want 4", p.Index)
	}
	want := strings.Repeat("zanzibar ", embeddingtest.PassageWords-1) + "zanzibar"
	if got := content[p.Start:p.End]; got != want {
		t.Fatalf("the span bounds %q, want exactly the fifth passage's text", got)
	}
}

// A one-passage memory's passage is its whole content, and costs no read: its
// span is known from the content itself.
func TestAOnePassageMemorysPassageSpansItsContent(t *testing.T) {
	svc := newService(t)
	content := "the harbour authority recorded the tide tables"
	seed(t, svc, content)
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "tide tables", Limit: 1})
	must(t, err)
	p := res.Results[0].Passage
	if p == nil || p.Index != 0 || p.Start != 0 || int(p.End) != len(content) {
		t.Fatalf("passage = %+v, want index 0 spanning [0,%d)", p, len(content))
	}
}

// A keyword hit has no vector evidence, so it names no passage rather than a
// made-up one.
func TestAKeywordHitNamesNoPassage(t *testing.T) {
	svc := newService(t)
	seed(t, svc, fifthPassageContent("zanzibar"))
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchKeyword, Query: "zanzibar", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 {
		t.Fatalf("%d results", len(res.Results))
	}
	if res.Results[0].Passage != nil {
		t.Fatalf("a keyword hit names passage %+v", res.Results[0].Passage)
	}
}

// Spans are byte offsets, and after multi-byte characters they must still fall
// on character boundaries, or slicing the content for display corrupts it.
func TestASpanAfterMultiByteCharactersSlicesValidText(t *testing.T) {
	svc := newService(t)
	words := make([]string, 3*embeddingtest.PassageWords)
	for i := range words {
		words[i] = []string{"Grüße", "日本語", "🙂👍🏽", "Straße"}[i%4]
	}
	for i := 2 * embeddingtest.PassageWords; i < 3*embeddingtest.PassageWords; i++ {
		words[i] = "zürich"
	}
	content := strings.Join(words, " ")
	seed(t, svc, content)
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "zürich", Limit: 1})
	must(t, err)
	p := res.Results[0].Passage
	if p == nil || p.Index != 2 {
		t.Fatalf("passage = %+v, want index 2", p)
	}
	span := content[p.Start:p.End]
	if !utf8.ValidString(span) || !strings.HasPrefix(span, "zürich") || !strings.HasSuffix(span, "zürich") {
		t.Fatalf("the span %q is not the third passage's text", span)
	}
}
