package memory_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/memory"
)

// Two callers archive the same memory at once — a double-clicked delete, or a
// client retrying one it thinks timed out. Both must succeed: archiving an
// already-archived memory is not an error, because the post-condition holds.
//
// This is the read-modify-write Update got wrong first, in Delete's soft path.
// The record was read before Service.write was entered, so its originalBody —
// the bytes Put conditions the commit on — was fixed before the transaction
// existed, and txn.Do re-runs only the body. A loser therefore re-issued the
// same stale expectation on every attempt and failed after eight, rather than
// re-reading, finding the memory already archived, and returning.
func TestConcurrentArchivesAllSucceed(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "archive me from several places"})
	must(t, err)

	const archivers = 16
	var wg sync.WaitGroup
	failures := make(chan error, archivers)
	for i := 0; i < archivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failures <- svc.Delete(ctx, m.ID, false)
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatalf("a concurrent archive failed rather than finding its work done: %v", err)
		}
	}

	got, err := svc.Get(ctx, m.ID, memory.GetOpts{IncludeArchived: true})
	must(t, err)
	if !got.Archived {
		t.Fatal("sixteen archives succeeded and the memory is not archived")
	}
}

// The case that matters in practice: a memory archived while something else is
// writing it — an agent editing it, or the lifecycle sweep visiting it. Every
// archive must land. An update is allowed to lose, but only in the one way that
// is true: refused because the memory is archived by the time it looks. A
// conflict that exhausted its retries is neither, and is the defect.
func TestAnArchiveRacingUpdatesStillLands(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)

	m, err := svc.Create(ctx, memory.CreateReq{Content: "edited and retired at once"})
	must(t, err)

	const each = 8
	var wg sync.WaitGroup
	archiveErrs := make(chan error, each)
	updateErrs := make(chan error, each)
	for i := 0; i < each; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			archiveErrs <- svc.Delete(ctx, m.ID, false)
		}()
		go func(i int) {
			defer wg.Done()
			src := fmt.Sprintf("editor-%d", i)
			_, uerr := svc.Update(ctx, memory.UpdateReq{ID: m.ID, Source: &src})
			updateErrs <- uerr
		}(i)
	}
	wg.Wait()
	close(archiveErrs)
	close(updateErrs)

	for err := range archiveErrs {
		if err != nil {
			t.Fatalf("an archive racing updates failed: %v", err)
		}
	}
	for err := range updateErrs {
		if err == nil {
			continue
		}
		if !errs.Is(err, errs.Conflict) || !strings.Contains(err.Error(), "archived") {
			t.Fatalf("an update failed for a reason other than finding the memory archived: %v", err)
		}
	}

	got, err := svc.Get(ctx, m.ID, memory.GetOpts{IncludeArchived: true})
	must(t, err)
	if !got.Archived {
		t.Fatal("every archive succeeded and the memory is not archived")
	}
}
