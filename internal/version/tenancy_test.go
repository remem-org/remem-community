package version_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
)

// The capability a binary was built with is decided by the `enterprise` build
// tag and reported. wantTenantCapability comes from a pair of tag-split test
// files rather than from the constant under test, so the test fails when the
// two disagree — a build that served one tenant while claiming to resolve
// tenants from an identity would be a release shipped under the wrong contract.
func TestTenantCapabilityIsReported(t *testing.T) {
	if version.TenantCapability != wantTenantCapability {
		t.Fatalf("this build declares the %q tenant capability, and was built as %q",
			version.TenantCapability, wantTenantCapability)
	}
}

// The string is not free-form: the composition root turns it into a policy, and
// a name nothing parses would fail closed at start-up rather than at build time.
func TestTheDeclaredCapabilityParses(t *testing.T) {
	c, err := tenant.ParseCapability(version.TenantCapability)
	if err != nil {
		t.Fatalf("this build's declared capability does not parse: %v", err)
	}
	if c == tenant.NoCapability {
		t.Fatal("the declared capability parsed to no capability")
	}
}

// The business edition is observability around the same engine. Mapping its
// build tag to the identity capability would advertise an isolation layer the
// binary does not contain, so the two tags are independent and this is the
// check that keeps them so.
func TestTheBusinessTagDoesNotImplyIdentityTenancy(t *testing.T) {
	if version.Edition == "business" && version.TenantCapability != "single-tenant" {
		t.Fatalf("a business build declares the %q capability; the business edition packages "+
			"observability, not an identity and authorization layer", version.TenantCapability)
	}
}
