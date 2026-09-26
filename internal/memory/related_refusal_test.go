package memory

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
)

// A related cursor is refused in its own name.
//
// Search and related share one session registry and one token format, and both
// refusals used to say "memory.Search ... start the search again" -- which is
// what the Task 4.2 end-to-end run showed a caller paging related memories
// after a restart. Phase 9 met the same shape on the admin routes: a shared
// refusal has to take its subject from the request, or the second surface to
// use it sends people to look at the first.
func TestARelatedCursorIsRefusedInItsOwnName(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	anchor, err := svc.Create(ctx, CreateReq{Content: "anchor"})
	if err != nil {
		t.Fatal(err)
	}
	var gone [16]byte
	gone[0] = 1
	for name, token := range map[string]string{
		"a session that does not exist": encodeSearchCursor("acme", gone, 5),
		"a position that ends early":    codec.EncodeToken(codec.TokenSearch, "acme", gone[:8]),
	} {
		_, err := svc.Related(ctx, RelatedReq{ID: anchor.ID, Cursor: token})
		if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "memory.Related") ||
			strings.Contains(err.Error(), "search") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
