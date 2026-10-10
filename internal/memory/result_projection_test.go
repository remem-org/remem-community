package memory

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"pgregory.net/rapid"
)

// TestResultsAreIdenticalWhicheverDecodeBuiltThem is the guard on plan item
// 17's two decoders.
//
// repo.GetBodyFrom decodes a result page's bodies down to the fields a
// response reads; repo.GetFrom decodes them in full. Both feed fromRecord,
// which is what every Search, Related and List result is projected through, so
// the memories the two produce must be indistinguishable. If they ever differ,
// the API answers the same question two ways depending on which endpoint asked
// — and the difference would show up as a field quietly missing from a search
// result and present in a Get.
//
// It draws the corpus rather than fixing it so that the shapes a body can
// reach — absent optionals, an empty policy that triggers the lifecycle
// defaults, no tags at all — are covered as well as the fully-populated one.
func TestResultsAreIdenticalWhicheverDecodeBuiltThem(t *testing.T) {
	const tn tenant.ID = "acme"
	ctx := tenant.NewContext(context.Background(), tn)

	rapid.Check(t, func(rt *rapid.T) {
		kv := memkv.New()
		defer func() { _ = kv.Close() }()
		repo := record.NewRepo(kv)

		recs := rapid.SliceOfN(rapid.Just(0), 1, 8).Draw(rt, "corpus")
		ids := make([]id.ID, len(recs))
		tx := txn.New(kv)
		for i := range recs {
			rec := drawRecord(rt, tn, i)
			ids[i] = rec.ID
			if err := repo.Put(ctx, tx, rec); err != nil {
				tx.Close()
				rt.Fatalf("storing record %d: %v", i, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			tx.Close()
			rt.Fatalf("committing the corpus: %v", err)
		}
		tx.Close()

		snap := kv.NewSnapshot()
		defer func() { _ = snap.Close() }()
		lower, upper := keys.TenantRange(tn)
		reader := storage.NewSeekReader(snap, lower, upper)
		defer func() { _ = reader.Close() }()

		for i, rid := range ids {
			full, err := repo.GetFrom(ctx, snap, rid)
			if err != nil {
				rt.Fatalf("the full read of record %d failed: %v", i, err)
			}
			projected, err := repo.GetBodyFrom(ctx, reader, rid)
			if err != nil {
				rt.Fatalf("the result-page read of record %d failed: %v", i, err)
			}

			want, got := fromRecord(full), fromRecord(projected)
			if reflect.DeepEqual(want, got) {
				continue
			}
			rt.Errorf("record %d: the two decodes build different memories%s", i, diffMemories(want, got))
		}
	})
}

// diffMemories names the fields that differ, so a failure says which field
// the projection lost rather than printing two structs side by side.
func diffMemories(want, got *Memory) string {
	wv, gv := reflect.ValueOf(*want), reflect.ValueOf(*got)
	out := ""
	for i := 0; i < wv.NumField(); i++ {
		w, g := wv.Field(i).Interface(), gv.Field(i).Interface()
		if reflect.DeepEqual(w, g) {
			continue
		}
		out += fmt.Sprintf("\n\t%s: the full decode says %v, the result-page decode says %v",
			wv.Type().Field(i).Name, w, g)
	}
	if out == "" {
		// DeepEqual disagreed with a field-by-field walk, which means the
		// difference is in a field this walk cannot see. Say so rather than
		// reporting no difference.
		return "\n\t(no field differs; the two differ in something this comparison cannot name)"
	}
	return out
}

// drawRecord draws one record to store. Times are the zero time or a positive
// millisecond count, never the epoch itself: zero milliseconds is how the body
// encodes "unset", so the epoch is the one instant the encoding cannot carry.
func drawRecord(rt *rapid.T, tn tenant.ID, n int) *record.Record {
	label := func(s string) string { return s + "-" + string(rune('a'+n%26)) }
	drawTime := func(name string) time.Time {
		if rapid.Bool().Draw(rt, label(name)+"-unset") {
			return time.Time{}
		}
		return time.UnixMilli(rapid.Int64Range(1, 4_000_000_000_000).Draw(rt, label(name))).UTC()
	}
	drawFloat := func(name string, lo, hi float64) float32 {
		return float32(rapid.Float64Range(lo, hi).Draw(rt, label(name)))
	}
	// Content and the creation time are not drawn as absent: record.validate
	// refuses a record without either, so a draw that produced one would test
	// the validator rather than the decoders.
	return &record.Record{
		ID:            id.New(),
		Tenant:        tn,
		Namespace:     tenant.DefaultNamespace,
		Type:          record.TypeMemory,
		Content:       rapid.StringN(1, 64, -1).Draw(rt, label("content")),
		SchemaVersion: rapid.Uint32Range(0, 9).Draw(rt, label("schema-version")),
		Fields: record.Fields{
			Tags:       rapid.SliceOfN(rapid.StringMatching(`[a-z]{1,8}`), 0, 5).Draw(rt, label("tags")),
			Source:     rapid.String().Draw(rt, label("source")),
			Archived:   rapid.Bool().Draw(rt, label("archived")),
			ArchivedAt: drawTime("archived-at"),

			Policy:          rapid.SampledFrom([]string{"", "short_term", "long_term", "pinned"}).Draw(rt, label("policy")),
			Importance:      drawFloat("importance", 0, 1),
			Health:          drawFloat("health", 0, 100),
			Valence:         drawFloat("valence", -1, 1),
			Arousal:         drawFloat("arousal", 0, 1),
			AccessedAt:      drawTime("accessed-at"),
			AccessCount:     rapid.Uint32().Draw(rt, label("access-count")),
			LastRecalledAt:  drawTime("last-recalled-at"),
			NextAttentionAt: drawTime("next-attention-at"),

			TTL:               time.Duration(rapid.Int64Range(0, 1<<40).Draw(rt, label("ttl"))),
			ProtectedUntil:    drawTime("protected-until"),
			LastDecayAt:       drawTime("last-decay-at"),
			LastHealthCheckAt: drawTime("last-health-check-at"),
		},
		CreatedAt: time.UnixMilli(rapid.Int64Range(1, 4_000_000_000_000).Draw(rt, label("created-at"))).UTC(),
		UpdatedAt: drawTime("updated-at"),
	}
}
