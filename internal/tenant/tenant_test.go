package tenant_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestParseAcceptsWellFormedIDs(t *testing.T) {
	for _, s := range []string{"acme", "acme-corp", "acme_corp", "a", "t1", strings.Repeat("a", tenant.MaxIDLen)} {
		if _, err := tenant.Parse(s); err != nil {
			t.Errorf("Parse(%q) = %v, want accepted", s, err)
		}
	}
}

func TestParseRefusesMalformedIDs(t *testing.T) {
	cases := map[string]string{
		"empty":       "",
		"uppercase":   "Acme",
		"space":       "acme corp",
		"slash":       "acme/corp",
		"dot":         "acme.corp",
		"nul":         "acme\x00corp",
		"too long":    strings.Repeat("a", tenant.MaxIDLen+1),
		"unicode":     "acmé",
		"path escape": "../etc",
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := tenant.Parse(s); err == nil {
				t.Fatalf("Parse(%q) was accepted", s)
			} else if !errs.Is(err, errs.Invalid) {
				t.Fatalf("Parse(%q) = %v, want Invalid: a bad id is caller input", s, err)
			}
		})
	}
}

func TestDefaultNamespaceIsValid(t *testing.T) {
	if !tenant.DefaultNamespace.Valid() {
		t.Fatal("the namespace every record uses must itself be well formed")
	}
}

func TestNamespaceFollowsTheSameRules(t *testing.T) {
	if _, err := tenant.ParseNamespace("Staging"); err == nil {
		t.Fatal("a namespace must be held to the same character set as a tenant id")
	}
	if _, err := tenant.ParseNamespace("staging"); err != nil {
		t.Fatalf("ParseNamespace(staging) = %v", err)
	}
}

func TestIdentifiersRenderAsThemselves(t *testing.T) {
	// They appear in log fields and error messages; a String that decorated
	// them would make a grep for a tenant id fail against its own logs.
	if got := tenant.ID("acme").String(); got != "acme" {
		t.Errorf("ID.String() = %q", got)
	}
	if got := tenant.DefaultNamespace.String(); got != "default" {
		t.Errorf("Namespace.String() = %q", got)
	}
}

func TestValidMatchesParse(t *testing.T) {
	for _, s := range []string{"acme", "", "Acme", "a-b_c", "acme corp"} {
		_, err := tenant.Parse(s)
		if want := err == nil; tenant.ID(s).Valid() != want {
			t.Errorf("ID(%q).Valid() = %v but Parse err = %v", s, tenant.ID(s).Valid(), err)
		}
		_, err = tenant.ParseNamespace(s)
		if want := err == nil; tenant.Namespace(s).Valid() != want {
			t.Errorf("Namespace(%q).Valid() disagrees with ParseNamespace", s)
		}
	}
}
