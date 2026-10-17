package config_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
)

// How the environment reaches configuration: legacy names, aliases, retired
// settings refused by name, typos, and two names for one setting.

// An upgrade from Rust Remem must not be a rewrite of every deployment
// manifest: REMEM_API_KEY and REMEM_DATA_DIR are in every existing
// environment.
func TestLegacyEnvironmentVariablesStillWork(t *testing.T) {
	c, err := config.Load(nil, []string{
		"REMEM_API_KEY=from-the-old-deployment",
		"REMEM_DATA_DIR=/srv/remem",
		"REMEM_ENV=development",
	}, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.APIKey != "from-the-old-deployment" {
		t.Errorf("server.api_key = %q", c.Server.APIKey)
	}
	if c.Storage.Path != "/srv/remem" {
		t.Errorf("storage.path = %q", c.Storage.Path)
	}
}

// Rust's rate-limit variables were refused while Go had no limiter. Now they
// are aliases again — same numbers, counted per tenant rather than per address.
func TestRateLimitVariablesAreAliasesAndOnByDefault(t *testing.T) {
	def := config.Default()
	if def.Server.RateLimitRPS != 100 || def.Server.RateLimitBurst != 50 {
		t.Fatalf("defaults are %d rps, burst %d; Rust shipped 100 and 50, on",
			def.Server.RateLimitRPS, def.Server.RateLimitBurst)
	}

	c, err := config.Load(nil, []string{
		"REMEM_RATE_LIMIT_RPS=7",
		"REMEM_RATE_LIMIT_BURST=3",
	}, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.RateLimitRPS != 7 || c.Server.RateLimitBurst != 3 {
		t.Fatalf("server.rate_limit_rps = %d, server.rate_limit_burst = %d; want 7 and 3",
			c.Server.RateLimitRPS, c.Server.RateLimitBurst)
	}
}

func TestAnUnservableRateLimitIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		key  string
	}{
		{"negative rate", []string{"REMEM_SERVER_RATE_LIMIT_RPS=-1"}, "server.rate_limit_rps"},
		{"no burst while limiting", []string{"REMEM_SERVER_RATE_LIMIT_BURST=0"}, "server.rate_limit_burst"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(nil, tc.env, nil)
			if err == nil {
				t.Fatalf("%v was accepted", tc.env)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("the refusal does not name %s: %v", tc.key, err)
			}
		})
	}

	// A burst of zero is fine when limiting is off: nothing reads it.
	if _, err := config.Load(nil, []string{
		"REMEM_SERVER_RATE_LIMIT_RPS=0", "REMEM_SERVER_RATE_LIMIT_BURST=0",
	}, nil); err != nil {
		t.Fatalf("limiting off with a zero burst was refused: %v", err)
	}
}

// A setting that is gone must say so, not be ignored. Each of these was
// security-relevant, and an operator whose CORS restriction silently stopped
// being enforced would find out from an incident.
func TestRetiredEnvironmentVariablesRefuseWithAnExplanation(t *testing.T) {
	for _, name := range []string{
		"REMEM_ALLOW_AUTH_DISABLED=true",
		"REMEM_CORS_ORIGINS=https://example.com",
		"REMEM_TRUST_PROXY_HEADERS=true",
		"REMEM_PORT=9000",
	} {
		_, err := config.Load(nil, []string{name}, nil)
		if err == nil {
			t.Errorf("%s was ignored", name)
			continue
		}
		key, _, _ := strings.Cut(name, "=")
		if !strings.Contains(err.Error(), key) {
			t.Errorf("the error does not name %s: %v", key, err)
		}
	}
}

// The three tokeniser dials were removed because they decided what was written
// to disk rather than how fast a query ran. A config file still carrying one is
// refused by name and told why — an operator whose setting silently stopped
// being read would find out from a support ticket instead.
func TestRetiredTokeniserSettingsRefuseWithAnExplanation(t *testing.T) {
	for _, env := range []string{
		"REMEM_TEXT_MIN_TOKEN_LENGTH=3",
		"REMEM_TEXT_MAX_TOKEN_LENGTH=100",
		"REMEM_TEXT_REMOVE_STOP_WORDS=false",
	} {
		_, err := config.Load(nil, []string{env}, nil)
		if err == nil {
			t.Errorf("%s was ignored", env)
			continue
		}
		if !strings.Contains(err.Error(), "no longer") {
			t.Errorf("%s was refused as an unknown setting rather than a retired one: %v", env, err)
		}
	}

	// text.enabled stays: turning the index off is a deployment decision, not
	// a change to what a query means.
	cfg, err := config.Load(nil, []string{"REMEM_TEXT_ENABLED=false"}, nil)
	if err != nil {
		t.Fatalf("text.enabled was refused: %v", err)
	}
	if cfg.Text.Enabled {
		t.Fatal("text.enabled=false did not take effect")
	}
}

// remem-mcp is routinely launched from the same shell as the server it talks
// to, so its variables must not stop the server from starting.
func TestTheBridgesVariablesAreIgnoredByTheServer(t *testing.T) {
	if _, err := config.Load(nil, []string{
		"REMEM_MCP_URL=http://localhost:4545/mcp",
		"REMEM_MCP_API_KEY=secret",
		"REMEM_MCP_TENANT=acme",
	}, nil); err != nil {
		t.Fatalf("the bridge's variables stopped the server: %v", err)
	}
}

// The strict rule still holds for everything else: a typo silently dropped is
// a setting an operator believes is in force and is not.
func TestATypoInAnEnvironmentVariableIsStillRefused(t *testing.T) {
	if _, err := config.Load(nil, []string{"REMEM_SERVER_HTTP_ADDRESS=1.2.3.4:80"}, nil); err == nil {
		t.Fatal("a misspelled setting was ignored")
	}
}

// A secret list nobody configured must read as unset, not as "(set,
// redacted)": telling an operator they have credentials they do not have is
// worse than saying nothing.
func TestAnUnsetSecretListReadsAsUnset(t *testing.T) {
	c := config.Default()
	if !strings.Contains(c.Redacted(), "server.api_keys = (unset)") {
		t.Fatalf("api_keys is not reported as unset:\n%s", c.Redacted())
	}
	c.Server.APIKeys = []string{"key@acme"}
	if !strings.Contains(c.Redacted(), "server.api_keys = (set, redacted)") {
		t.Fatalf("a configured api_keys is not reported as set:\n%s", c.Redacted())
	}
}

func TestTwoNamesForOneSettingAreRefusedRatherThanRaced(t *testing.T) {
	// A legacy variable and its canonical name are two spellings of one
	// setting, and os.Environ() has no defined order, so the winner would
	// otherwise be whichever the operating system handed over second.
	//
	// Found by the Phase 9 verification run: a REMEM_API_KEY left over in the
	// shell silently overrode the REMEM_SERVER_API_KEY the run had just
	// exported, and the server rejected its own operator credential with
	// "the presented credential is not one this server issued". Nothing said
	// anything, and both variables were plainly visible in the environment.
	env := []string{
		"REMEM_SERVER_API_KEY=the-one-i-just-set-000000",
		"REMEM_API_KEY=the-one-left-over-from-before",
	}
	_, err := config.Load(nil, env, nil)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("two names for one setting were resolved silently: %v", err)
	}
	for _, want := range []string{"REMEM_API_KEY", "REMEM_SERVER_API_KEY", "server.api_key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %s: %v", want, err)
		}
	}

	// Reversing the order must refuse identically. A rule that depends on
	// which came first is the same coin flip wearing a different hat.
	if _, err := config.Load(nil, []string{env[1], env[0]}, nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("the refusal depends on the order of the environment: %v", err)
	}

	// The same value under both names is not a conflict: it is a deployment
	// that set the belt and the braces, and refusing it would break exactly
	// the upgrade the legacy names exist to protect.
	agreeing := []string{
		"REMEM_SERVER_API_KEY=the-same-value-under-both-000",
		"REMEM_API_KEY=the-same-value-under-both-000",
	}
	cfg, err := config.Load(nil, agreeing, nil)
	if err != nil {
		t.Fatalf("two names agreeing on one value were refused: %v", err)
	}
	if cfg.Server.APIKey != "the-same-value-under-both-000" {
		t.Fatalf("the agreed value did not land: %q", cfg.Server.APIKey)
	}
}

// embedding.max_sequence_tokens claimed a 256-token window that the embedder
// never applied: MiniLM's tokenizer truncated at 128 before the setting was
// read. Its successor is the passage format, which is fixed in code because it
// decides what is written to disk. A file or a variable still naming it is
// refused by name, saying so.
func TestTheRetiredEmbeddingWindowIsRefusedByName(t *testing.T) {
	check := func(how string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("embedding.max_sequence_tokens set by %s was accepted", how)
		}
		for _, want := range []string{"max_sequence_tokens", "no longer", "passage"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("by %s, the refusal does not say %q: %v", how, want, err)
			}
		}
	}
	_, err := config.Load(nil, []string{"REMEM_EMBEDDING_MAX_SEQUENCE_TOKENS=512"}, nil)
	check("the environment", err)

	path := writeTOML(t, "[embedding]\nmax_sequence_tokens = 512\n")
	_, err = config.Load([]string{path}, nil, nil)
	check("a configuration file", err)
}
