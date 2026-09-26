package config_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

// API keys: how they bind to tenants, how they are refused, and how they are
// kept out of logs. Split from config_test.go in Phase 13 to keep that file
// under the project's size limit; the tests did not change.

func TestAPIKeysBindCredentialsToTenants(t *testing.T) {
	c := config.Default()
	c.Server.APIKey = "root-key"
	c.Server.APIKeys = []string{"key-acme@acme", "key-other@other", "second-root"}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	got := c.Credentials()
	if len(got) != 4 {
		t.Fatalf("Credentials returned %d entries: %+v", len(got), got)
	}
	if got[0].Secret != "root-key" || got[0].Tenant != "" || !got[0].CrossTenant {
		t.Fatalf("the singular api_key must be the unbound operator key, got %+v", got[0])
	}
	if got[1].Secret != "key-acme" || got[1].Tenant != "acme" || got[1].CrossTenant {
		t.Fatalf("a bound key must not be cross-tenant, got %+v", got[1])
	}
	if got[3].Tenant != "" || !got[3].CrossTenant {
		t.Fatalf("an unbound entry is another operator key, got %+v", got[3])
	}
}

func TestMalformedAPIKeysAreRefusedNamingTheEntry(t *testing.T) {
	c := config.Default()
	c.Server.APIKeys = []string{"fine@acme", "@acme", "key@Acme"}
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate accepted malformed credentials")
	}
	for _, want := range []string{"server.api_keys[1]", "server.api_keys[2]"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %s: %v", want, err)
		}
	}
}

// A production server with no singular api_key but a bound key list is
// authenticated, and refusing it would push operators back to one shared key.
func TestProductionAcceptsABoundKeyListWithoutTheSingularKey(t *testing.T) {
	c := config.Default()
	c.Server.Env = "production"
	c.Server.APIKeys = []string{"key-acme-long-enough@acme"}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestAPIKeysAreRedacted(t *testing.T) {
	c := config.Default()
	c.Server.APIKeys = []string{"key-acme@acme"}
	if strings.Contains(c.Redacted(), "key-acme") {
		t.Fatal("Redacted printed an API key")
	}
}

// A production server refuses a credential that is too short to be secret or
// is a placeholder copied from an example, as Rust's production validation did
// (config.rs, validate_production_config: 16 characters and a placeholder list).
// The refusal names the setting and never the credential: startup errors are
// logged.
//
// Found in Phase 13's security review. Go refused a production server with no
// key at all and accepted one of a single character.
func TestAProductionServerRefusesAWeakCredential(t *testing.T) {
	for _, tc := range []struct {
		name, secret string
		set          func(*config.Config, string)
		want         string
	}{
		{"a short api_key", "only-15-chars!!", func(c *config.Config, s string) { c.Server.APIKey = s }, "server.api_key"},
		{"Rust's placeholder", "change-this-secret-key-in-production", func(c *config.Config, s string) { c.Server.APIKey = s }, "server.api_key"},
		{"our own documentation's placeholder", "replace-with-a-long-random-secret", func(c *config.Config, s string) { c.Server.APIKey = s }, "server.api_key"},
		{"a short api_keys entry", "short-secret", func(c *config.Config, s string) { c.Server.APIKeys = []string{s + "@acme"} }, "server.api_keys[0]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Default()
			c.Server.Env = "production"
			tc.set(&c, tc.secret)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error naming %q, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), tc.secret) {
				t.Fatalf("the refusal repeats the credential: %v", err)
			}
		})
	}

	// Development keeps accepting them, as Rust does: the rule is about what is
	// exposed, and a laptop with key "dev" is not.
	c := config.Default()
	c.Server.APIKey = "dev"
	if err := c.Validate(); err != nil {
		t.Fatalf("a development server refused a short key: %v", err)
	}
	c.Server.Env = "production"
	c.Server.APIKey = "a-real-looking-key-of-sufficient-length"
	if err := c.Validate(); err != nil {
		t.Fatalf("a production server refused a sound key: %v", err)
	}
}

// An explicit tenant set is how a deployment states a cross-tenant authority
// rather than leaving it to be inferred from a key with no binding. One tenant
// after the "@" is still a binding: a set of one is what a binding already is.
func TestAnAPIKeyCanNameAnExplicitTenantSet(t *testing.T) {
	c := config.Default()
	c.Server.APIKeys = []string{"key-acme@acme", "key-two@acme,globex", "key-spaced@ acme , beta "}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	got := c.Credentials()
	if len(got) != 3 {
		t.Fatalf("Credentials returned %d entries: %+v", len(got), got)
	}
	if got[0].Tenant != "acme" || len(got[0].Authorized) != 0 || got[0].CrossTenant {
		t.Fatalf("one tenant after the @ must stay a binding, got %+v", got[0])
	}
	if got[1].Tenant != "" || !got[1].CrossTenant {
		t.Fatalf("a set must be cross-tenant and unbound, got %+v", got[1])
	}
	if len(got[1].Authorized) != 2 || got[1].Authorized[0] != "acme" || got[1].Authorized[1] != "globex" {
		t.Fatalf("the authorised set is %v, want [acme globex] in configuration order", got[1].Authorized)
	}
	if got[1].ID != "acme+globex" {
		t.Fatalf("the credential names itself %q; a log line has to say which key this was", got[1].ID)
	}
	if len(got[2].Authorized) != 2 || got[2].Authorized[0] != "acme" || got[2].Authorized[1] != "beta" {
		t.Fatalf("whitespace around a tenant id was not trimmed: %v", got[2].Authorized)
	}
}

func TestAMalformedTenantSetIsRefusedNamingTheEntry(t *testing.T) {
	for _, tc := range []struct {
		entry, want string
	}{
		{"key@acme,Globex", "Globex"},
		{"key@acme,", `""`},
		{"key@acme,acme", "twice"},
	} {
		c := config.Default()
		c.Server.APIKeys = []string{tc.entry}
		err := c.Validate()
		if err == nil {
			t.Fatalf("Validate accepted %q", tc.entry)
		}
		if !strings.Contains(err.Error(), "server.api_keys[0]") || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("the refusal of %q does not name the entry and the cause: %v", tc.entry, err)
		}
	}
}
