// Package lifecycle is what happens to a memory when nobody is looking at it:
// expiry, promotion, decay, archival and cleanup, driven by one job per tenant.
//
// # Retention policies replace the type enum
//
// Rust decides a memory's fate from `MemoryType{ShortTerm, LongTerm}`, matched
// on in five sweeps. Here it is a named [Policy] carried on the record (plan
// §II.6), so adding a retention behaviour is data rather than a new variant
// every match arm has to learn about, and a tenant can be given different rules
// without a code path of its own.
//
// # One walk, not five
//
// Every memory carries `next_attention_at` — attribute slot 9, indexed since
// Phase 5 — and one handler per tenant walks that index once per run. Rust has
// the same shared attention time and then walks it five times, each pass
// discarding the memories another sweep owns (services/attrs.rs:44-51). The
// defect is five walks of one index, and the fix is to ask each due memory what
// it is owed rather than asking the corpus five separate questions.
//
// # Nothing here destroys user data on a judgement
//
// Plan §II.10 row 8: a heuristic archives, and only a stated retention over an
// already-archived record deletes. `pinned` never reaches even that. The
// distinction is not a technicality — "your memory was retired and can be
// restored" and "your memory is gone" are different sentences to say to
// somebody, and only one of them is recoverable.
//
// # The lifecycle never moves updated_at
//
// It is a content-edit timestamp and one of the five orderings Phase 5 ships.
// A decay pass that moved it would reorder a user's list for nothing, and an
// archive that moved it would claim the memory was edited. Cleanup schedules
// from `archived_at` instead, which is why Go needs no exception where Rust
// has one.
package lifecycle

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/record"
)

// The Rust constants, read from the source at pre-go-freeze rather than copied
// from plan §II.6 — whose table gets two of them wrong. Each cites where it
// came from, because the next person to change one will want to check.
const (
	// ImportanceDecayPerDay is the multiplier applied per whole elapsed day.
	// services/lifecycle_manager.rs:251.
	ImportanceDecayPerDay = 0.995
	// HealthDecayShortTerm and HealthDecayLongTerm are points of health lost
	// per whole elapsed day. services/lifecycle_manager.rs:420-423. Plan §II.6
	// says 0 and 1.0; both are wrong, and the long-term one would let an
	// untouched memory survive twice as long as Rust allows.
	HealthDecayShortTerm = 8.0
	HealthDecayLongTerm  = 2.0
	// PromoteAtRecalls is the access count at which an expired short-term
	// memory is promoted rather than archived. services/lifecycle_manager.rs:57.
	PromoteAtRecalls uint32 = 3
	// CleanupAfter is how long an archived record is kept before it is removed.
	// services/attrs.rs:38.
	CleanupAfter = 30 * 24 * time.Hour
	// ArchiveAtHealth is the health at or below which a memory is retired.
	// services/lifecycle_manager.rs:426.
	ArchiveAtHealth float32 = 0
	// MaxHealth is the top of the scale, matching Rust's 0..100 so an imported
	// corpus needs no rescaling.
	MaxHealth float32 = 100
	// FlashbulbArousal is the arousal at or above which a memory stored through
	// the API is protected from decay and forgetting.
	// services/memory_manager.rs:114. The comparison is >= and there is no
	// gradation around it: 0.8 protects, 0.79 does not.
	FlashbulbArousal float32 = 0.8
	// FlashbulbProtection is how long that protection lasts.
	// services/memory_manager.rs:115.
	FlashbulbProtection = 30 * 24 * time.Hour
	// Day is the unit every decay reasons in. Rust truncates to whole days and
	// writes nothing for a pass under one (lifecycle_manager.rs:279-282), which
	// is what keeps a sweep from rewriting the corpus on every run.
	Day = 24 * time.Hour
)

// The built-in policy names. They are durable — written into every record of
// that policy, and into the snapshot — so a name is never reused for different
// rules and a retired one stays retired.
const (
	// ShortTerm is the default. Without a TTL on the record it never expires,
	// exactly as in Rust; what retires it is health, at 8 points a day.
	ShortTerm = "short_term"
	// LongTerm decays importance and health slowly and never expires.
	LongTerm = "long_term"
	// Pinned is new in Go and costs nothing: nothing about a pinned memory
	// moves. It is what a caller wants when they mean "never touch this", and
	// it is what plan §II.6 reaches for to express flashbulb protection —
	// which is a window on the record instead, so that a policy stays a
	// decision a user made rather than a value that changes by itself.
	Pinned = "pinned"
)

// Policy is a named set of retention rules.
//
// Nil pointers mean "never": no TTL, no promotion, no archival by health, no
// cleanup. That is the safe direction for every one of them — a policy this
// binary reads and half-understands leaves memories alone rather than acting on
// a guess.
type Policy struct {
	Name string

	// TTL is how long a memory of this policy lives before it expires. Nil
	// means it never does. It is a *default*: a record carrying its own TTL
	// uses that, which is where Rust puts it and where the API surface sets it.
	TTL *time.Duration

	// PromoteAtRecalls is the access count at which an expired memory is
	// promoted into PromoteTo instead of being archived. Nil never promotes.
	PromoteAtRecalls *uint32
	PromoteTo        string

	// ImportanceDecay is the per-day multiplier. 1.0 is no decay. It is
	// applied per whole elapsed day, so a value at or below zero would erase
	// importance in one pass and one above it would grow without bound —
	// both are refused.
	ImportanceDecay float64

	// HealthDecay is points of health lost per whole elapsed day. Zero is no
	// decay.
	HealthDecay float64

	// ArchiveAtHealth is the health at or below which the memory is retired.
	// Nil means health never archives it.
	ArchiveAtHealth *float32

	// CleanupAfter is how long an archived record is retained before it is
	// removed. Nil means never — the record survives until somebody asks for
	// it to go.
	CleanupAfter *time.Duration
}

// builtins is the shipped table.
//
// It is built per call rather than shared, because a Policy holds pointers and
// a package-level value would hand every caller the same *time.Duration to
// point at. A policy table is read once per sweep, not once per memory.
func builtins() map[string]Policy {
	ttlNone := (*time.Duration)(nil)
	promoteAt := PromoteAtRecalls
	archiveAt := ArchiveAtHealth
	cleanup := CleanupAfter

	return map[string]Policy{
		ShortTerm: {
			Name:             ShortTerm,
			TTL:              ttlNone,
			PromoteAtRecalls: &promoteAt,
			PromoteTo:        LongTerm,
			ImportanceDecay:  1.0,
			HealthDecay:      HealthDecayShortTerm,
			ArchiveAtHealth:  &archiveAt,
			CleanupAfter:     &cleanup,
		},
		LongTerm: {
			Name:            LongTerm,
			ImportanceDecay: ImportanceDecayPerDay,
			HealthDecay:     HealthDecayLongTerm,
			ArchiveAtHealth: &archiveAt,
			CleanupAfter:    &cleanup,
		},
		Pinned: {
			Name:            Pinned,
			ImportanceDecay: 1.0,
			HealthDecay:     0,
		},
	}
}

// Builtin returns a shipped policy by name.
func Builtin(name string) (Policy, bool) {
	p, ok := builtins()[name]
	return p, ok
}

// Builtins returns every shipped policy, in name order, for the admin surface
// and for a test that must not miss one.
func Builtins() []Policy {
	all := builtins()
	names := make([]string, 0, len(all))
	for n := range all {
		names = append(names, n)
	}
	sort.Strings(names)

	out := make([]Policy, 0, len(names))
	for _, n := range names {
		out = append(out, all[n])
	}
	return out
}

// inert is what an unrecognised policy resolves to: nothing happens.
//
// A record whose policy this binary does not know is a rolling upgrade — a
// newer node wrote a policy this one has never heard of — and the only safe
// answer to "I do not know this memory's rules" is to leave it alone. Falling
// back to the default would apply somebody else's retention to it, and the
// visible outcome would be memories archived on rules their owner never chose.
func inert(name string) Policy {
	return Policy{Name: name, ImportanceDecay: 1.0}
}

// Policies is one tenant's resolved policy table: the built-ins, with any
// overrides that tenant has recorded.
//
// An override states a whole policy rather than a patch. The durable form has
// to be unambiguous — "no TTL" and "TTL unstated" are different things and a
// partial record cannot tell them apart — so the patching happens at the API
// edge, where JSON null and JSON absent are distinguishable, and what is stored
// is complete.
type Policies struct {
	table map[string]Policy
}

// NewPolicies resolves a tenant's table, validating every override.
//
// Validation happens here, at the point an override is loaded or written, and
// never at sweep time. An override that is only checked when a memory falls due
// is one whose typo surfaces as "nothing decays" weeks later, with nothing
// pointing at the cause.
func NewPolicies(overrides map[string]Policy) (*Policies, error) {
	const op = "lifecycle.NewPolicies"

	table := builtins()
	for name, p := range overrides {
		if err := checkName(name, op); err != nil {
			return nil, err
		}
		p.Name = name
		table[name] = p
	}

	// Two passes: every policy has to exist before PromoteTo can be checked
	// against the table, or an override adding a policy and another promoting
	// into it would be refused in one order and accepted in the other.
	names := make([]string, 0, len(overrides))
	for name := range overrides {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validate(table[name], table, op); err != nil {
			return nil, err
		}
	}
	return &Policies{table: table}, nil
}

// Get returns a policy by name, or [errs.NotFound].
//
// It is the form for a caller naming a policy explicitly — an API request, an
// override's PromoteTo — where an unknown name is a mistake worth reporting.
// A record's policy goes through [Policies.Resolve] instead.
func (p *Policies) Get(name string) (Policy, error) {
	pol, ok := p.table[name]
	if !ok {
		return Policy{}, errs.E(errs.NotFound, "lifecycle.Policies.Get", fmt.Errorf(
			"no retention policy named %q; this tenant has %v", name, p.Names()))
	}
	return pol, nil
}

// Resolve returns the policy governing a record, never failing.
//
// An empty name is the default policy: records written before Phase 5 carry
// none, and the default is what they were created under. An unrecognised name
// is [inert] — see its comment for why that is not the same fallback.
func (p *Policies) Resolve(name string) Policy {
	if name == "" {
		name = record.DefaultPolicy
	}
	if pol, ok := p.table[name]; ok {
		return pol
	}
	return inert(name)
}

// For returns the policy governing a record.
func (p *Policies) For(rec *record.Record) Policy {
	return p.Resolve(rec.Fields.Policy)
}

// Known reports whether this binary recognises the name, so a caller can tell
// "left alone deliberately" from "left alone because nothing knew what to do".
func (p *Policies) Known(name string) bool {
	if name == "" {
		name = record.DefaultPolicy
	}
	_, ok := p.table[name]
	return ok
}

// Names returns every policy this tenant has, in order.
func (p *Policies) Names() []string {
	out := make([]string, 0, len(p.table))
	for n := range p.table {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// MaxPolicyNameLen bounds a policy name. It is stored on every record of that
// policy and travels through the attribute row, so an unbounded one is
// unbounded overhead per memory.
const MaxPolicyNameLen = 64

func checkName(name, op string) error {
	if name == "" {
		return errs.E(errs.Invalid, op, errors.New("a retention policy must have a name"))
	}
	if len(name) > MaxPolicyNameLen {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"policy name %q is %d bytes, the limit is %d", name, len(name), MaxPolicyNameLen))
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
		default:
			return errs.E(errs.Invalid, op, fmt.Errorf(
				"policy name %q contains %q; only [a-z0-9_] is allowed", name, string(c)))
		}
	}
	return nil
}

// validate refuses a policy that would misbehave rather than fail.
//
// Every rule here has a symptom attached, because that is what a reader needs:
// the reason a decay of 1.5 is refused is not that it is out of range, it is
// that importance would grow without bound and every ranking with it.
func validate(p Policy, table map[string]Policy, op string) error {
	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}

	switch {
	case p.ImportanceDecay <= 0:
		return bad("policy %q decays importance by %v a day, which erases it in one pass; "+
			"1.0 is no decay", p.Name, p.ImportanceDecay)
	case p.ImportanceDecay > 1:
		return bad("policy %q decays importance by %v a day, which grows it without bound; "+
			"a decay multiplier is at most 1.0", p.Name, p.ImportanceDecay)
	case p.HealthDecay < 0:
		return bad("policy %q decays health by %v a day, which heals a memory nobody touches; "+
			"0 is no decay", p.Name, p.HealthDecay)
	case p.HealthDecay > float64(MaxHealth):
		return bad("policy %q decays health by %v a day, more than the whole %v scale; "+
			"every memory of this policy would be archived on its first sweep",
			p.Name, p.HealthDecay, MaxHealth)
	}

	if p.TTL != nil && *p.TTL <= 0 {
		return bad("policy %q has a ttl of %v; a memory cannot expire before it exists", p.Name, *p.TTL)
	}
	if p.ArchiveAtHealth != nil {
		if h := *p.ArchiveAtHealth; h < 0 || h > MaxHealth {
			return bad("policy %q archives at health %v, which is off the 0..%v scale, so it "+
				"would archive everything or nothing", p.Name, h, MaxHealth)
		}
	}
	if p.CleanupAfter != nil && *p.CleanupAfter <= 0 {
		return bad("policy %q cleans up after %v, which would delete a memory the moment it is "+
			"archived; nil means never", p.Name, *p.CleanupAfter)
	}

	if p.PromoteAtRecalls != nil {
		if p.PromoteTo == "" {
			return bad("policy %q promotes at %d recalls with nowhere to promote to",
				p.Name, *p.PromoteAtRecalls)
		}
		if _, ok := table[p.PromoteTo]; !ok {
			return bad("policy %q promotes into %q, which is not a policy this tenant has",
				p.Name, p.PromoteTo)
		}
	} else if p.PromoteTo != "" {
		return bad("policy %q promotes into %q but has no recall threshold to promote at",
			p.Name, p.PromoteTo)
	}
	return nil
}
