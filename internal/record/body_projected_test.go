package record

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"pgregory.net/rapid"
)

// fullyPopulated is the shape a lifecycle-managed memory reaches: tags, a
// source, and every optional timestamp present. It is what both decoders are
// compared over, and what the benchmark measures, because a body with half its
// fields absent measures the decoder skipping work rather than doing it.
func fullyPopulated(rid id.ID) *Record {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	return &Record{
		ID:            rid,
		Tenant:        "acme",
		Namespace:     tenant.DefaultNamespace,
		Type:          TypeMemory,
		Content:       "the quarterly review moved to the first Tuesday of the month, and Dana owns the agenda",
		SchemaVersion: 3,
		Fields: Fields{
			Tags:       []string{"meeting", "quarterly", "dana", "agenda"},
			Source:     "slack://team-ops/1700000000",
			Archived:   true,
			ArchivedAt: base.Add(900 * time.Hour),

			Policy:          "long_term",
			Importance:      0.75,
			Health:          82.5,
			Valence:         -0.25,
			Arousal:         0.6,
			AccessedAt:      base.Add(48 * time.Hour),
			AccessCount:     17,
			LastRecalledAt:  base.Add(47 * time.Hour),
			NextAttentionAt: base.Add(72 * time.Hour),

			TTL:               36 * time.Hour,
			ProtectedUntil:    base.Add(720 * time.Hour),
			LastDecayAt:       base.Add(24 * time.Hour),
			LastHealthCheckAt: base.Add(25 * time.Hour),
		},
		CreatedAt: base,
		UpdatedAt: base.Add(time.Hour),
	}
}

// TestProjectedDecodeAgreesWithFullDecodeOnResultFields pins the projection to
// the full decoder on every field a result page reads. The two decoders are
// only safe to run side by side for as long as this holds.
func TestProjectedDecodeAgreesWithFullDecodeOnResultFields(t *testing.T) {
	rid := id.New()
	body, err := encodeBody(fullyPopulated(rid))
	if err != nil {
		t.Fatalf("encoding the body: %v", err)
	}

	want, err := decodeBody("acme", tenant.DefaultNamespace, rid, body, "test")
	if err != nil {
		t.Fatalf("the full decode failed: %v", err)
	}
	got, err := decodeBodyProjected("acme", tenant.DefaultNamespace, rid, body, "test")
	if err != nil {
		t.Fatalf("the projected decode failed: %v", err)
	}

	for _, c := range resultFieldChecks(want, got) {
		if c.got != c.want {
			t.Errorf("%s: the projected decode says %v, the full decode says %v", c.name, c.got, c.want)
		}
	}
	if n := len(got.Fields.Tags); n != len(want.Fields.Tags) {
		t.Fatalf("tags: the projected decode read %d, the full decode read %d", n, len(want.Fields.Tags))
	}
	for i := range want.Fields.Tags {
		if got.Fields.Tags[i] != want.Fields.Tags[i] {
			t.Errorf("tag %d: the projected decode says %q, the full decode says %q",
				i, got.Fields.Tags[i], want.Fields.Tags[i])
		}
	}
}

// TestProjectedDecodeAgreesWithFullDecodeForAnyRecord is the same guarantee
// over a drawn corpus rather than one fixture: the fixture pins the shape a
// body reaches in production, and this pins the shapes it can reach at all —
// absent optionals, an empty policy that triggers the defaults, no tags, empty
// strings.
func TestProjectedDecodeAgreesWithFullDecodeForAnyRecord(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		rid := id.New()
		body, err := encodeBody(drawRecord(rt, rid))
		if err != nil {
			rt.Fatalf("encoding the body: %v", err)
		}

		want, err := decodeBody("acme", tenant.DefaultNamespace, rid, body, "test")
		if err != nil {
			rt.Fatalf("the full decode failed: %v", err)
		}
		got, err := decodeBodyProjected("acme", tenant.DefaultNamespace, rid, body, "test")
		if err != nil {
			rt.Fatalf("the projected decode failed: %v", err)
		}

		for _, c := range resultFieldChecks(want, got) {
			if c.got != c.want {
				rt.Errorf("%s: the projected decode says %v, the full decode says %v", c.name, c.got, c.want)
			}
		}
		if !slices.Equal(got.Fields.Tags, want.Fields.Tags) {
			rt.Errorf("tags: the projected decode says %q, the full decode says %q",
				got.Fields.Tags, want.Fields.Tags)
		}
	})
}

// drawRecord draws a record whose every field is in the range the encoder
// accepts. Times are drawn as either the zero time or a positive millisecond
// count, never UnixMilli(0): zero is how "unset" is stored, so the epoch is
// the one instant the encoding cannot represent, and drawing it would test the
// encoder's known lossiness rather than the two decoders' agreement.
func drawRecord(rt *rapid.T, rid id.ID) *Record {
	drawTime := func(name string) time.Time {
		if rapid.Bool().Draw(rt, name+"-unset") {
			return time.Time{}
		}
		return time.UnixMilli(rapid.Int64Range(1, 4_000_000_000_000).Draw(rt, name)).UTC()
	}
	drawFloat := func(name string, lo, hi float64) float32 {
		return float32(rapid.Float64Range(lo, hi).Draw(rt, name))
	}
	return &Record{
		ID:            rid,
		Tenant:        "acme",
		Namespace:     tenant.DefaultNamespace,
		Type:          TypeMemory,
		Content:       rapid.String().Draw(rt, "content"),
		SchemaVersion: rapid.Uint32Range(0, 9).Draw(rt, "schema-version"),
		Fields: Fields{
			Tags:       rapid.SliceOfN(rapid.String(), 0, 6).Draw(rt, "tags"),
			Source:     rapid.String().Draw(rt, "source"),
			Archived:   rapid.Bool().Draw(rt, "archived"),
			ArchivedAt: drawTime("archived-at"),

			Policy:          rapid.SampledFrom([]string{"", "short_term", "long_term", "pinned", "custom"}).Draw(rt, "policy"),
			Importance:      drawFloat("importance", 0, 1),
			Health:          drawFloat("health", 0, 100),
			Valence:         drawFloat("valence", -1, 1),
			Arousal:         drawFloat("arousal", 0, 1),
			AccessedAt:      drawTime("accessed-at"),
			AccessCount:     rapid.Uint32().Draw(rt, "access-count"),
			LastRecalledAt:  drawTime("last-recalled-at"),
			NextAttentionAt: drawTime("next-attention-at"),

			TTL:               time.Duration(rapid.Int64Range(0, 1<<40).Draw(rt, "ttl")),
			ProtectedUntil:    drawTime("protected-until"),
			LastDecayAt:       drawTime("last-decay-at"),
			LastHealthCheckAt: drawTime("last-health-check-at"),
		},
		CreatedAt: drawTime("created-at"),
		UpdatedAt: drawTime("updated-at"),
	}
}

// TestProjectedDecodeLeavesTheWriteOnlyFieldsUnset pins the other half of the
// contract. These are the fields only a writing path needs, and a projected
// record is never written back; if one of them starts arriving, the comment on
// decodeBodyProjected that says a projected record must not be written has
// become a comment nobody has to believe.
func TestProjectedDecodeLeavesTheWriteOnlyFieldsUnset(t *testing.T) {
	rid := id.New()
	body, err := encodeBody(fullyPopulated(rid))
	if err != nil {
		t.Fatalf("encoding the body: %v", err)
	}
	got, err := decodeBodyProjected("acme", tenant.DefaultNamespace, rid, body, "test")
	if err != nil {
		t.Fatalf("the projected decode failed: %v", err)
	}

	if got.SchemaVersion != 0 {
		t.Errorf("SchemaVersion is %d; a projected record carries no schema version", got.SchemaVersion)
	}
	if got.Type != 0 {
		t.Errorf("Type is %d; a projected record carries no record type", got.Type)
	}
	if !got.Fields.NextAttentionAt.IsZero() {
		t.Errorf("NextAttentionAt is %v; it is a sweep's field, not a response's", got.Fields.NextAttentionAt)
	}
	if !got.Fields.LastDecayAt.IsZero() {
		t.Errorf("LastDecayAt is %v; it is a sweep's field, not a response's", got.Fields.LastDecayAt)
	}
	if !got.Fields.LastHealthCheckAt.IsZero() {
		t.Errorf("LastHealthCheckAt is %v; it is a sweep's field, not a response's", got.Fields.LastHealthCheckAt)
	}
	if got.originalBody != nil {
		t.Error("originalBody is set; a projected record must not be presentable as a concurrency token")
	}
	if got.Vectors != nil {
		t.Error("Vectors is allocated; GetBodyFrom reads no vectors, so the map is an allocation for nothing")
	}
}

// TestProjectedDecodeRefusesTheSameBodiesAsTheFullDecode keeps the two
// decoders' refusals in step. A value the full decoder rejects must not be
// quietly accepted by the one a search uses.
func TestProjectedDecodeRefusesTheSameBodiesAsTheFullDecode(t *testing.T) {
	rid, other := id.New(), id.New()
	body, err := encodeBody(fullyPopulated(rid))
	if err != nil {
		t.Fatalf("encoding the body: %v", err)
	}

	cases := []struct {
		name  string
		value []byte
		rid   id.ID
	}{
		{"a body stored under another record's key", body, other},
		{"a value shorter than the envelope", body[:3], rid},
		{"a value that is not a record", append([]byte("XXXX"), body[4:]...), rid},
		{"a truncated body", body[:len(body)-3], rid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, fullErr := decodeBody("acme", tenant.DefaultNamespace, c.rid, c.value, "test")
			_, projErr := decodeBodyProjected("acme", tenant.DefaultNamespace, c.rid, c.value, "test")
			if fullErr == nil {
				t.Fatalf("the full decode accepted %s; the case no longer tests anything", c.name)
			}
			if projErr == nil {
				t.Fatalf("the projected decode accepted %s, which the full decode refused as %v", c.name, fullErr)
			}
			if errs.KindOf(projErr) != errs.KindOf(fullErr) {
				t.Errorf("the projected decode calls it %v, the full decode calls it %v",
					errs.KindOf(projErr), errs.KindOf(fullErr))
			}
		})
	}
}

// TestPutRefusesARecordThatCameFromGetBodyFrom enforces what the doc comments
// on Repo.GetBodyFrom and decodeBodyProjected promise.
//
// A projected record carries no schema version and no original body, so Put
// would stamp it with the version in force and fall back to a
// read-modify-write condition — quietly dropping the record's own schema
// version and every field this binary does not understand. The write is
// refused rather than left to a reviewer noticing the comment.
func TestPutRefusesARecordThatCameFromGetBodyFrom(t *testing.T) {
	kv, repo, ctx := putHarness(t)
	rec := fullyPopulated(id.New())
	commit(t, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) })

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	projected, err := repo.GetBodyFrom(ctx, snap, rec.ID)
	if err != nil {
		t.Fatalf("reading the record back: %v", err)
	}

	err = commit(t, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, projected) })
	if err == nil {
		t.Fatal("Put accepted a record read through GetBodyFrom; it would have " +
			"written away the record's schema version and its unknown fields")
	}
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("Put refused it as %v; a caller handing over the wrong record is an Invalid", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), "GetBodyFrom") {
		t.Errorf("the refusal does not name GetBodyFrom, so it does not tell the caller "+
			"which read to swap: %v", err)
	}
}

// TestPutAcceptsARecordFromTheFullDecode is the other half: the guard must
// refuse only projected records. Get, GetFrom and Scan all decode in full, and
// a record from any of them is the ordinary read-modify-write subject.
func TestPutAcceptsARecordFromTheFullDecode(t *testing.T) {
	kv, repo, ctx := putHarness(t)
	rec := fullyPopulated(id.New())
	commit(t, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) })

	full, err := repo.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("reading the record back: %v", err)
	}
	full.Content = "edited"
	if err := commit(t, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, full) }); err != nil {
		t.Fatalf("Put refused a record from the full decode: %v", err)
	}
}

// TestCloneKeepsTheProjectionMarker matters because Service.peek clones every
// record on the result path. A clone that lost the marker would be a projected
// record the guard no longer recognises, which is the one way a projected
// record could still reach Put.
func TestCloneKeepsTheProjectionMarker(t *testing.T) {
	rid := id.New()
	body, err := encodeBody(fullyPopulated(rid))
	if err != nil {
		t.Fatalf("encoding the body: %v", err)
	}
	projected, err := decodeBodyProjected("acme", tenant.DefaultNamespace, rid, body, "test")
	if err != nil {
		t.Fatalf("the projected decode failed: %v", err)
	}
	if !projected.projected {
		t.Fatal("decodeBodyProjected did not mark the record it built")
	}
	if !projected.Clone().projected {
		t.Error("Clone dropped the marker; a cloned projected record would be writable")
	}

	full, err := decodeBody("acme", tenant.DefaultNamespace, rid, body, "test")
	if err != nil {
		t.Fatalf("the full decode failed: %v", err)
	}
	if full.projected || full.Clone().projected {
		t.Error("the full decode marks its record as projected, which would refuse every write")
	}
}

func putHarness(t *testing.T) (storage.KV, Repo, context.Context) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return kv, NewRepo(kv), tenant.NewContext(context.Background(), "acme")
}

// commit runs one staged write to completion, returning the first error of the
// stage and the commit so a test can assert on a refusal from either.
func commit(t *testing.T, kv storage.KV, stage func(txn.Tx) error) error {
	t.Helper()
	tx := txn.New(kv)
	defer tx.Close()
	if err := stage(tx); err != nil {
		return err
	}
	return tx.Commit(context.Background())
}

type fieldCheck struct {
	name      string
	got, want any
}

// resultFieldChecks names every record field a Search, Related or List
// response is projected from. Nothing outside this list is read downstream of
// GetBodyFrom, which is why the projection is allowed to leave it unset.
func resultFieldChecks(want, got *Record) []fieldCheck {
	return []fieldCheck{
		{"ID", got.ID, want.ID},
		{"Tenant", got.Tenant, want.Tenant},
		{"Namespace", got.Namespace, want.Namespace},
		{"Content", got.Content, want.Content},
		{"CreatedAt", got.CreatedAt, want.CreatedAt},
		{"UpdatedAt", got.UpdatedAt, want.UpdatedAt},
		{"Fields.Source", got.Fields.Source, want.Fields.Source},
		{"Fields.Archived", got.Fields.Archived, want.Fields.Archived},
		{"Fields.ArchivedAt", got.Fields.ArchivedAt, want.Fields.ArchivedAt},
		{"Fields.Policy", got.Fields.Policy, want.Fields.Policy},
		{"Fields.Importance", got.Fields.Importance, want.Fields.Importance},
		{"Fields.Health", got.Fields.Health, want.Fields.Health},
		{"Fields.Valence", got.Fields.Valence, want.Fields.Valence},
		{"Fields.Arousal", got.Fields.Arousal, want.Fields.Arousal},
		{"Fields.AccessedAt", got.Fields.AccessedAt, want.Fields.AccessedAt},
		{"Fields.AccessCount", got.Fields.AccessCount, want.Fields.AccessCount},
		{"Fields.LastRecalledAt", got.Fields.LastRecalledAt, want.Fields.LastRecalledAt},
		{"Fields.TTL", got.Fields.TTL, want.Fields.TTL},
		{"Fields.ProtectedUntil", got.Fields.ProtectedUntil, want.Fields.ProtectedUntil},
	}
}
