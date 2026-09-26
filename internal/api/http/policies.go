package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/tenant"
)

// PolicyResponse is one retention policy as an operator sees it.
//
// Every "never" is an absent field rather than a zero, which is the same
// distinction the durable row makes: a cleanup_after of 0 would delete a memory
// the instant it was archived, and its absence means the record is kept.
type PolicyResponse struct {
	Name string `json:"name"`

	TTLSeconds       *uint64 `json:"ttl_seconds,omitempty"`
	PromoteAtRecalls *uint32 `json:"promote_at_recalls,omitempty"`
	PromoteTo        string  `json:"promote_to,omitempty"`

	ImportanceDecay float64 `json:"importance_decay"`
	HealthDecay     float64 `json:"health_decay"`

	ArchiveAtHealth  *float32 `json:"archive_at_health,omitempty"`
	CleanupAfterSecs *uint64  `json:"cleanup_after_seconds,omitempty"`

	// Overridden says this tenant has changed the policy from the built-in, so
	// an operator reading the list can tell configuration from default without
	// knowing the built-in numbers by heart.
	Overridden bool `json:"overridden"`
}

// PoliciesResponse is a tenant's whole resolved table.
type PoliciesResponse struct {
	Tenant   string           `json:"tenant"`
	Policies []PolicyResponse `json:"policies"`
}

// PolicyPatch changes one policy. Every field is a pointer, because this is a
// patch over the resolved policy and JSON absent has to be distinguishable from
// JSON null: absent leaves the field alone, and null sets it to "never".
//
// The durable row stores a whole policy, not a patch. The ambiguity lives here,
// at the one place a request can express it.
type PolicyPatch struct {
	TTLSeconds       *uint64  `json:"ttl_seconds"`
	PromoteAtRecalls *uint32  `json:"promote_at_recalls"`
	PromoteTo        *string  `json:"promote_to"`
	ImportanceDecay  *float64 `json:"importance_decay"`
	HealthDecay      *float64 `json:"health_decay"`
	ArchiveAtHealth  *float32 `json:"archive_at_health"`
	CleanupAfterSecs *uint64  `json:"cleanup_after_seconds"`
}

// PatchPoliciesRequest changes one or more of a tenant's policies.
type PatchPoliciesRequest struct {
	Policies map[string]PolicyPatch `json:"policies"`
}

// listPolicies shows a tenant's resolved retention table.
func (d Deps) listPolicies(w http.ResponseWriter, r *http.Request) {
	if !d.crossTenant(w, r) {
		return
	}
	t, ok := d.policyTenant(w, r)
	if !ok {
		return
	}

	table, err := d.resolvePolicies(r, t)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	overrides, err := d.Policies.Get(r.Context(), t)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := PoliciesResponse{Tenant: string(t)}
	for _, name := range table.Names() {
		p, err := table.Get(name)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		_, overridden := overrides[name]
		out.Policies = append(out.Policies, toPolicyResponse(p, overridden))
	}
	writeJSON(w, r, http.StatusOK, out)
}

// patchPolicies changes a tenant's retention rules.
//
// It is a patch over the *resolved* policy rather than over the stored one, so
// an operator changing one number on a built-in does not have to restate the
// other six — and what reaches disk is a complete policy, because a partial
// durable record cannot say "no TTL" and "TTL unstated" apart.
func (d Deps) patchPolicies(w http.ResponseWriter, r *http.Request) {
	if !d.crossTenant(w, r) {
		return
	}
	t, ok := d.policyTenant(w, r)
	if !ok {
		return
	}

	var body PatchPoliciesRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if len(body.Policies) == 0 {
		WriteMalformed(w, r, "name at least one policy to change")
		return
	}

	table, err := d.resolvePolicies(r, t)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	next, err := d.Policies.Get(r.Context(), t)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	for name, patch := range body.Policies {
		base, err := table.Get(name)
		if err != nil {
			// A policy nothing defines. Refused rather than created, because a
			// typo that silently adds a policy no record names is a policy
			// nothing ever uses and nobody notices.
			WriteError(w, r, errs.E(errs.Invalid, "http.patchPolicies", fmt.Errorf(
				"no retention policy named %q for tenant %s; this tenant has %v",
				name, t, table.Names())))
			return
		}
		next[name] = patch.applyTo(base)
	}

	if err := d.Policies.Put(r.Context(), t, next); err != nil {
		WriteError(w, r, err)
		return
	}
	// The scheduler caches a tenant's table, because it is consulted on every
	// record write. This is the one surface that changes one, so it is the one
	// that drops the cache.
	if d.PolicyCache != nil {
		d.PolicyCache.Invalidate(t)
	}

	d.listPolicies(w, r)
}

// applyTo folds a patch into a resolved policy.
//
// Absent leaves the field alone; present sets it. For the four nullable fields
// a JSON null and an absent field are both decoded as a nil pointer by
// encoding/json, so "set this to never" is spelled by naming the field with a
// zero — a ttl_seconds of 0 means the policy no longer expires. That is stated
// here because it is the one place the API is not self-evident.
func (p PolicyPatch) applyTo(base lifecycle.Policy) lifecycle.Policy {
	if p.TTLSeconds != nil {
		base.TTL = secondsOrNil(*p.TTLSeconds)
	}
	if p.PromoteAtRecalls != nil {
		if *p.PromoteAtRecalls == 0 {
			base.PromoteAtRecalls, base.PromoteTo = nil, ""
		} else {
			n := *p.PromoteAtRecalls
			base.PromoteAtRecalls = &n
		}
	}
	if p.PromoteTo != nil {
		base.PromoteTo = *p.PromoteTo
	}
	if p.ImportanceDecay != nil {
		base.ImportanceDecay = *p.ImportanceDecay
	}
	if p.HealthDecay != nil {
		base.HealthDecay = *p.HealthDecay
	}
	if p.ArchiveAtHealth != nil {
		h := *p.ArchiveAtHealth
		if h < 0 {
			base.ArchiveAtHealth = nil
		} else {
			base.ArchiveAtHealth = &h
		}
	}
	if p.CleanupAfterSecs != nil {
		base.CleanupAfter = secondsOrNil(*p.CleanupAfterSecs)
	}
	return base
}

// secondsOrNil reads zero as "never", which is what makes a duration turnable
// off through a field that cannot carry null.
func secondsOrNil(secs uint64) *time.Duration {
	if secs == 0 {
		return nil
	}
	d := time.Duration(secs) * time.Second
	return &d
}

// policyTenant resolves which tenant the request is about.
//
// The path names it, rather than the X-Remem-Tenant header the job routes use.
// A retention policy is a property *of* a tenant rather than work done inside
// one, so it reads the way the tenant routes beside it read — and an operator
// changing the wrong tenant's decay because a header was left over from the
// last request is a mistake with no undo.
func (d Deps) policyTenant(w http.ResponseWriter, r *http.Request) (tenant.ID, bool) {
	// A build without an override store answers by name rather than panicking
	// on a nil interface, so "not built" and "wrong URL" stay distinguishable —
	// the disposition the job routes already have.
	if d.Policies == nil {
		WriteError(w, r, errs.E(errs.Invalid, "http.policies", fmt.Errorf(
			"this process does not store retention overrides; the built-in policies are in force")))
		return "", false
	}
	t, err := tenant.Parse(r.PathValue("id"))
	if err != nil {
		WriteError(w, r, err)
		return "", false
	}
	if _, err := d.Tenants.Get(r.Context(), t); err != nil {
		WriteError(w, r, err)
		return "", false
	}
	return t, true
}

func (d Deps) resolvePolicies(r *http.Request, t tenant.ID) (*lifecycle.Policies, error) {
	overrides, err := d.Policies.Get(r.Context(), t)
	if err != nil {
		return nil, err
	}
	return lifecycle.NewPolicies(overrides)
}

func toPolicyResponse(p lifecycle.Policy, overridden bool) PolicyResponse {
	out := PolicyResponse{
		Name:             p.Name,
		PromoteAtRecalls: p.PromoteAtRecalls,
		PromoteTo:        p.PromoteTo,
		ImportanceDecay:  p.ImportanceDecay,
		HealthDecay:      p.HealthDecay,
		ArchiveAtHealth:  p.ArchiveAtHealth,
		Overridden:       overridden,
	}
	if p.TTL != nil {
		secs := uint64(*p.TTL / time.Second)
		out.TTLSeconds = &secs
	}
	if p.CleanupAfter != nil {
		secs := uint64(*p.CleanupAfter / time.Second)
		out.CleanupAfterSecs = &secs
	}
	return out
}
