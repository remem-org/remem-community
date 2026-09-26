package schema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/version"
)

// noop is a migration body that does nothing. The registry's job is ordering
// and refusal, and neither depends on what a step actually does.
func noop(context.Context, *schema.Context) error { return nil }

// step builds a migration advancing one subsystem from one version to the next.
func step(id, subsystem string, from, to uint32) schema.Migration {
	return schema.Migration{
		ID:       id,
		Requires: map[string]uint32{subsystem: from},
		Advances: map[string]uint32{subsystem: to},
		Strategy: schema.StrategyInstant,
		Run:      noop,
	}
}

// at builds a manifest pinning the named subsystems and leaving every other one
// at the version this binary writes.
func manifestAt(pins map[string]uint32) schema.Format {
	f := schema.CurrentFormat(version.Current())
	for name, v := range pins {
		f.Subsystems[name] = schema.Subsystem{Current: v, MinReader: v, MinWriter: v}
	}
	return f
}

// writes is the set of versions the binary under test writes. The real one is
// version.Current(); these tests raise the two formats they exercise so a step
// to version 2 is a step the binary could actually have shipped.
func writes() version.Versions {
	v := version.Current()
	v.AttrSchema = 4
	v.RecordEnvelope = 2
	return v
}

func ids(plan []schema.Migration) []string {
	out := make([]string, 0, len(plan))
	for _, m := range plan {
		out = append(out, m.ID)
	}
	return out
}

// Declaration order is not execution order. A registry is a set of steps with
// declared preconditions; the order falls out of the preconditions, so that
// inserting a step later cannot silently reorder the ones already shipped.
func TestPlanOrdersByDependency(t *testing.T) {
	// Registered backwards, and named backwards too: the step that must run
	// second sorts first by id. Without that the test would pass against a
	// planner that merely sorted alphabetically and understood nothing.
	second := schema.Migration{
		ID:       "a-needs-attr-2",
		Requires: map[string]uint32{"attr_schema": 2},
		Advances: map[string]uint32{"attr_schema": 3},
		Strategy: schema.StrategyInstant,
		Run:      noop,
	}
	first := step("z-advances-attr-to-2", "attr_schema", 1, 2)

	reg, err := schema.NewRegistry(writes(), second, first)
	if err != nil {
		t.Fatal(err)
	}

	want := version.Current()
	want.AttrSchema = 3

	plan, err := reg.Plan(manifestAt(map[string]uint32{"attr_schema": 1}), want)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(plan)
	if len(got) != 2 || got[0] != first.ID || got[1] != second.ID {
		t.Fatalf("plan ran in %v; the step advancing attr to 2 must precede the one requiring it", got)
	}
}

// A plan that cannot reach the target is not a short plan — it is a directory
// nothing can migrate, and opening it would mean operating on data this binary
// half understands.
func TestPlanRefusesAGap(t *testing.T) {
	reg, err := schema.NewRegistry(writes(), step("attr-1-to-2", "attr_schema", 1, 2))
	if err != nil {
		t.Fatal(err)
	}

	want := version.Current()
	want.AttrSchema = 4 // nothing gets from 2 to 4

	_, err = reg.Plan(manifestAt(map[string]uint32{"attr_schema": 1}), want)
	if !errs.Is(err, errs.MigrationRequired) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "attribute slot schema") {
		t.Fatalf("the error must name the subsystem that cannot be reached: %v", err)
	}
	if !strings.Contains(err.Error(), "2") || !strings.Contains(err.Error(), "4") {
		t.Fatalf("the error must name where it got to and where it had to reach: %v", err)
	}
}

func TestPlanIsEmptyWhenCurrent(t *testing.T) {
	reg, err := schema.NewRegistry(writes(), step("attr-1-to-2", "attr_schema", 1, 2))
	if err != nil {
		t.Fatal(err)
	}

	plan, err := reg.Plan(schema.CurrentFormat(writes()), writes())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 0 {
		t.Fatalf("a directory already at current has no work, got %v", ids(plan))
	}
}

// A directory with no manifest at all reads every subsystem as absent, which is
// older than anything. It must still plan to the target rather than refuse.
func TestPlanFromAnAbsentManifest(t *testing.T) {
	reg, err := schema.NewRegistry(writes(),
		step("adopt", "attr_schema", 0, 1),
	)
	if err != nil {
		t.Fatal(err)
	}

	want := version.Versions{AttrSchema: 1}
	plan, err := reg.Plan(schema.Format{Subsystems: map[string]schema.Subsystem{}}, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 || plan[0].ID != "adopt" {
		t.Fatalf("got %v", ids(plan))
	}
}

// The plan is a total order even when two steps are independently applicable,
// so two runs of the same binary against the same directory do the same thing
// in the same sequence — which is what makes a crashed migration resumable.
func TestPlanIsDeterministicAcrossIndependentSteps(t *testing.T) {
	a := step("z-record-envelope", "record_envelope", 1, 2)
	b := step("a-attr-schema", "attr_schema", 1, 2)

	want := version.Current()
	want.RecordEnvelope = 2
	want.AttrSchema = 2
	from := manifestAt(map[string]uint32{"record_envelope": 1, "attr_schema": 1})

	forward, err := schema.NewRegistry(writes(), a, b)
	if err != nil {
		t.Fatal(err)
	}
	backward, err := schema.NewRegistry(writes(), b, a)
	if err != nil {
		t.Fatal(err)
	}

	one, err := forward.Plan(from, want)
	if err != nil {
		t.Fatal(err)
	}
	two, err := backward.Plan(from, want)
	if err != nil {
		t.Fatal(err)
	}
	if got, other := ids(one), ids(two); got[0] != other[0] || got[1] != other[1] {
		t.Fatalf("registration order changed the plan: %v then %v", got, other)
	}
}

// --- construction-time refusals ---------------------------------------------
//
// Every one of these is a programming error in the binary's own migration list.
// Caught at construction it is a start-up failure with a name in it; caught at
// run time it is a half-migrated directory.

func TestRegistryRefusesTwoMigrationsAdvancingTheSameSubsystem(t *testing.T) {
	_, err := schema.NewRegistry(writes(),
		step("one", "attr_schema", 1, 2),
		step("two", "attr_schema", 1, 2),
	)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "attr_schema") {
		t.Fatalf("the error must name the subsystem: %v", err)
	}
}

func TestRegistryRefusesADuplicateID(t *testing.T) {
	_, err := schema.NewRegistry(writes(),
		step("same", "attr_schema", 1, 2),
		step("same", "record_envelope", 1, 2),
	)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a migration id is how a crashed run finds its own state; got %v", err)
	}
}

func TestRegistryRefusesAStepThatDoesNotAdvance(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    schema.Migration
	}{
		{"advances nothing", schema.Migration{ID: "x", Strategy: schema.StrategyInstant, Run: noop}},
		{"advances backwards", step("x", "attr_schema", 2, 1)},
		{"advances to the same version", step("x", "attr_schema", 2, 2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := schema.NewRegistry(writes(), tc.m); !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestRegistryRefusesAnUnknownSubsystem(t *testing.T) {
	_, err := schema.NewRegistry(writes(), step("x", "attr_scheme", 1, 2)) // typo
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a typo in a subsystem name would make the step unreachable rather than wrong; got %v", err)
	}
	if !strings.Contains(err.Error(), "attr_scheme") {
		t.Fatalf("the error must quote what was written: %v", err)
	}
}

// A step producing a version this binary does not write would leave a directory
// this same binary then refuses to open. That is a bug in the migration list,
// and it is caught before a byte moves.
func TestRegistryRefusesAdvancingPastWhatTheBinaryWrites(t *testing.T) {
	supported := writes()
	_, err := schema.NewRegistry(supported, step("x", "attr_schema", supported.AttrSchema, supported.AttrSchema+1))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "attribute slot schema") {
		t.Fatalf("the error must name the format: %v", err)
	}
}

// Plan means "to here", not "as far as the list goes". A step beyond the target
// is a step for a later binary and must not be selected — otherwise a partial
// migration would run past what the caller asked for.
func TestPlanStopsAtTheTarget(t *testing.T) {
	reg, err := schema.NewRegistry(writes(),
		step("attr-1-to-2", "attr_schema", 1, 2),
		step("attr-2-to-3", "attr_schema", 2, 3),
	)
	if err != nil {
		t.Fatal(err)
	}

	want := version.Current()
	want.AttrSchema = 2

	plan, err := reg.Plan(manifestAt(map[string]uint32{"attr_schema": 1}), want)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(plan); len(got) != 1 || got[0] != "attr-1-to-2" {
		t.Fatalf("got %v", got)
	}
}

func TestRegistryRefusesAMalformedStep(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    schema.Migration
	}{
		{"no id", schema.Migration{Advances: map[string]uint32{"attr_schema": 2}, Strategy: schema.StrategyInstant, Run: noop}},
		{"no run", schema.Migration{ID: "x", Advances: map[string]uint32{"attr_schema": 2}, Strategy: schema.StrategyInstant}},
		{"no strategy", schema.Migration{ID: "x", Advances: map[string]uint32{"attr_schema": 2}, Run: noop}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := schema.NewRegistry(writes(), tc.m); !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

// A Background step is where start-up stops. Everything after it runs alongside
// serving, in order — splitting by strategy rather than by position would run a
// later Instant step before the Background one it declares a dependency on.
func TestSplitStopsAtTheFirstBackgroundStep(t *testing.T) {
	plan := []schema.Migration{
		step("a", "attr_schema", 1, 2),
		func() schema.Migration {
			m := step("b", "attr_schema", 2, 3)
			m.Strategy = schema.StrategyBackground
			return m
		}(),
		step("c", "attr_schema", 3, 4),
	}

	startup, background := schema.Split(plan)
	if got := ids(startup); len(got) != 1 || got[0] != "a" {
		t.Fatalf("startup ran %v", got)
	}
	if got := ids(background); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("background ran %v; the tail keeps the plan's order", got)
	}
}

func TestSplitLeavesAPlanWithoutBackgroundStepsAtStartup(t *testing.T) {
	plan := []schema.Migration{step("a", "attr_schema", 1, 2)}
	startup, background := schema.Split(plan)
	if len(startup) != 1 || len(background) != 0 {
		t.Fatalf("startup=%v background=%v", ids(startup), ids(background))
	}
}
