package memory_test

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
)

func f32p(v float32) *float32 { return &v }

func TestCreateCarriesTheLifecycleFields(t *testing.T) {
	svc := newService(t)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{
		Content:    "a fact worth keeping",
		Policy:     lifecycle.ShortTerm,
		Importance: f32p(0.9),
		Valence:    f32p(-0.4),
		Arousal:    f32p(0.2),
		TTL:        2 * time.Hour,
	})
	must(t, err)

	switch {
	case m.Policy != lifecycle.ShortTerm:
		t.Errorf("policy is %q", m.Policy)
	case m.Importance != 0.9:
		t.Errorf("importance is %v", m.Importance)
	case m.Valence != -0.4:
		t.Errorf("valence is %v", m.Valence)
	case m.Arousal != 0.2:
		t.Errorf("arousal is %v", m.Arousal)
	case m.TTL != 2*time.Hour:
		t.Errorf("ttl is %v", m.TTL)
	case m.Health != record.DefaultHealth:
		t.Errorf("health is %v, want the default %v", m.Health, record.DefaultHealth)
	}

	got, err := svc.Get(acmeCtx(), m.ID, memory.GetOpts{})
	must(t, err)
	if got.Importance != 0.9 || got.TTL != 2*time.Hour || got.Policy != lifecycle.ShortTerm {
		t.Fatalf("the stored memory is %+v", got)
	}
}

// A TTL belongs to a short-term memory and to nothing else. Rust drops it from
// any memory that ends up long-term (services/memory_manager.rs:149-150) and
// expires only short-term memories (services/types.rs:1135-1145), and Go's own
// rule is that promotion clears the TTL (internal/lifecycle/ttl.go). A flashbulb
// memory is a promotion at birth.
//
// Until Phase 13, Go kept the requested TTL through that promotion, so a memory
// stored with arousal 0.8 and a three-second TTL was protected for thirty days
// and archived three seconds later. Found by the differential harness's
// lifecycle surface on every seed; Rust kept the memory live and long-term.
func TestAMemoryThatIsNotShortTermKeepsNoTTL(t *testing.T) {
	svc := newService(t)
	for _, tc := range []struct {
		name    string
		req     memory.CreateReq
		policy  string
		keepTTL bool
	}{
		{"flashbulb", memory.CreateReq{Content: "the day the building shook",
			Policy: lifecycle.ShortTerm, Arousal: f32p(lifecycle.FlashbulbArousal), TTL: 3 * time.Second},
			lifecycle.LongTerm, false},
		{"long term asked for", memory.CreateReq{Content: "a decision of record",
			Policy: lifecycle.LongTerm, TTL: 5 * time.Second}, lifecycle.LongTerm, false},
		{"pinned asked for", memory.CreateReq{Content: "never forget this",
			Policy: lifecycle.Pinned, TTL: time.Hour}, lifecycle.Pinned, false},
		{"short term keeps it", memory.CreateReq{Content: "a passing note",
			Policy: lifecycle.ShortTerm, Arousal: f32p(0.79), TTL: 3 * time.Second}, lifecycle.ShortTerm, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := svc.Create(acmeCtx(), tc.req)
			must(t, err)
			want := time.Duration(0)
			if tc.keepTTL {
				want = tc.req.TTL
			}
			if m.Policy != tc.policy || m.TTL != want {
				t.Fatalf("created as %q with ttl %v, want %q with ttl %v", m.Policy, m.TTL, tc.policy, want)
			}
			got, err := svc.Get(acmeCtx(), m.ID, memory.GetOpts{})
			must(t, err)
			if got.TTL != want {
				t.Fatalf("stored with ttl %v, want %v", got.TTL, want)
			}
		})
	}
}

// An importance of zero is a value a caller may mean — "ignore this in ranking"
// — and a defaulting decoder cannot tell it from "I did not say". That is why
// the request fields are pointers.
func TestAZeroImportanceIsAValueNotAnOmission(t *testing.T) {
	svc := newService(t)

	zero, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "unimportant", Importance: f32p(0)})
	must(t, err)
	if zero.Importance != 0 {
		t.Fatalf("an explicit importance of 0 became %v", zero.Importance)
	}

	unset, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "unstated"})
	must(t, err)
	if unset.Importance != record.DefaultImportance {
		t.Fatalf("an omitted importance became %v, want the default %v",
			unset.Importance, record.DefaultImportance)
	}
}

// Rust's flashbulb rule, kept exactly: arousal at or above 0.8 makes the memory
// long-term whatever the caller asked for, and protects it for thirty days. The
// threshold is inclusive and there is no gradation around it.
func TestFlashbulbPromotionOverridesTheRequestedPolicy(t *testing.T) {
	svc := newService(t)

	hot, err := svc.Create(acmeCtx(), memory.CreateReq{
		Content: "the day the building shook",
		Policy:  lifecycle.ShortTerm,
		Arousal: f32p(lifecycle.FlashbulbArousal),
	})
	must(t, err)
	if hot.Policy != lifecycle.LongTerm {
		t.Fatalf("policy is %q; arousal >= %v overrides the requested policy",
			hot.Policy, lifecycle.FlashbulbArousal)
	}
	if hot.ProtectedUntil.Sub(hot.CreatedAt) != lifecycle.FlashbulbProtection {
		t.Errorf("protected for %v, want %v",
			hot.ProtectedUntil.Sub(hot.CreatedAt), lifecycle.FlashbulbProtection)
	}

	// And 0.79 does neither.
	cool, err := svc.Create(acmeCtx(), memory.CreateReq{
		Content: "an ordinary tuesday",
		Policy:  lifecycle.ShortTerm,
		Arousal: f32p(0.79),
	})
	must(t, err)
	if cool.Policy != lifecycle.ShortTerm {
		t.Errorf("arousal 0.79 promoted the memory to %q", cool.Policy)
	}
	if !cool.ProtectedUntil.IsZero() {
		t.Errorf("arousal 0.79 protected the memory until %v", cool.ProtectedUntil)
	}
}

// Out of range is refused, not clamped. A caller who sent importance 5 meant
// something, and storing 1 silently makes every later ranking a lie they have
// no way to notice.
func TestOutOfRangeLifecycleValuesAreRefused(t *testing.T) {
	svc := newService(t)
	tests := []struct {
		name string
		req  memory.CreateReq
		want string
	}{
		{"importance above one", memory.CreateReq{Content: "x", Importance: f32p(5)}, "importance"},
		{"importance below zero", memory.CreateReq{Content: "x", Importance: f32p(-1)}, "importance"},
		{"valence out of range", memory.CreateReq{Content: "x", Valence: f32p(2)}, "valence"},
		{"arousal out of range", memory.CreateReq{Content: "x", Arousal: f32p(-0.5)}, "arousal"},
		{"negative ttl", memory.CreateReq{Content: "x", TTL: -time.Hour}, "ttl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Create(acmeCtx(), tc.req)
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v, want Invalid", err)
			}
			if !contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not mention %q: %v", tc.want, err)
			}
		})
	}
}

// A memory created with an explicit policy and nothing else must arrive with
// full health.
//
// The defect this pins was live for one commit and is worth naming.
// record.Fields.WithDefaults treats an empty policy as its signal that the
// whole lifecycle group is unset — correct for a record decoded from disk,
// where a health of 0 in a record that *has* a policy is a real 0. Applied at
// creation it means a caller who names a policy gets importance 0 and health 0,
// and health 0 archives on the first sweep. The memory would be retired within
// the hour, and the only visible symptom would be that it had vanished.
func TestNamingAPolicyDoesNotArriveWithNoHealth(t *testing.T) {
	svc := newService(t)
	for _, policy := range []string{lifecycle.ShortTerm, lifecycle.LongTerm, lifecycle.Pinned} {
		t.Run(policy, func(t *testing.T) {
			m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "a memory", Policy: policy})
			must(t, err)
			if m.Health != record.DefaultHealth {
				t.Fatalf("health is %v, want %v — this memory would be archived on the "+
					"first sweep", m.Health, record.DefaultHealth)
			}
			if m.Importance != record.DefaultImportance {
				t.Fatalf("importance is %v, want %v", m.Importance, record.DefaultImportance)
			}
		})
	}
}
