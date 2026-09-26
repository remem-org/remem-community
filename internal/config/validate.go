package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
)

// tenantIDPattern is what a tenant id may look like: lowercase, no spaces, no
// punctuation beyond - and _, short enough to prefix every key it scopes.
var tenantIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// Validate checks the whole configuration and reports every problem at once.
//
// Spec §51 asks for validation at startup and a fast failure on invalid
// values. Two properties make that useful rather than merely correct: each
// message names the key at fault, in the same "section.key" spelling the file,
// the environment variable and the flag use; and a configuration with three
// mistakes reports three, not one per restart.
func (c Config) Validate() error {
	var p problems

	// Storage.
	p.require(c.Storage.Path != "", "storage.path", "must name a data directory")
	p.oneOf(c.Storage.Engine, []string{"pebble", "memory"}, "storage.engine")
	if c.Server.Env == "production" && c.Storage.Engine == "memory" {
		// Decided with the user in Phase 13's security review: the memory
		// engine keeps nothing across a restart and slows down super-linearly
		// past a few thousand memories, so a production server on it loses every
		// tenant's memories at its next restart.
		p.add("storage.engine", `"memory" keeps nothing across a restart; a production server needs "pebble"`)
	}
	p.require(c.Storage.CacheSizeBytes >= 0, "storage.cache_size_bytes", "cannot be negative")

	// Server.
	p.oneOf(c.Server.Env, []string{"development", "production"}, "server.env")
	p.require(c.Server.HTTPAddr != "", "server.http_addr", "must name a listen address")
	if c.Server.Env == "production" && c.Server.APIKey == "" && len(c.Server.APIKeys) == 0 {
		p.add("server.api_key", "must be set in production: a production server without authentication would serve every tenant's memories to anyone who can reach it")
	}
	if c.Server.Env == "production" && c.Server.APIKey != "" {
		if why := weakSecret(c.Server.APIKey); why != "" {
			p.add("server.api_key", why)
		}
	}
	for i, entry := range c.Server.APIKeys {
		cred, err := parseAPIKey(entry)
		if err != nil {
			p.add(fmt.Sprintf("server.api_keys[%d]", i), err.Error())
			continue
		}
		if c.Server.Env == "production" {
			if why := weakSecret(cred.Secret); why != "" {
				p.add(fmt.Sprintf("server.api_keys[%d]", i), why)
			}
		}
	}
	p.positive(c.Server.ReadTimeout, "server.read_timeout")
	p.positive(c.Server.WriteTimeout, "server.write_timeout")
	p.positive(c.Server.ShutdownTimeout, "server.shutdown_timeout")
	p.require(c.Server.MaxRequestBytes > 0, "server.max_request_bytes", "must be greater than zero")
	p.require(c.Server.RateLimitRPS >= 0, "server.rate_limit_rps", "cannot be negative; zero turns rate limiting off")
	p.require(c.Server.RateLimitRPS == 0 || c.Server.RateLimitBurst >= 1,
		"server.rate_limit_burst", "must be at least 1 while server.rate_limit_rps is set, or no request could ever be served")

	// Tenant. Invariant 1: there is always an explicit tenant, so the default
	// one must itself be a legal tenant id.
	if !tenantIDPattern.MatchString(c.Tenant.Default) {
		p.add("tenant.default", fmt.Sprintf("%q is not a tenant id: use lowercase letters, digits, - and _, up to 63 characters", c.Tenant.Default))
	}

	// Embedding and vector. The model and its width are fixed constants: a
	// vector produced at another width is not comparable with the ones already
	// stored, and the mismatch would only surface as bad search results.
	p.require(c.Embedding.Model != "", "embedding.model", "must name a model")
	p.require(c.Embedding.BatchSize > 0, "embedding.batch_size", "must be greater than zero")
	p.require(c.Embedding.MaxSequenceTokens > 0, "embedding.max_sequence_tokens", "must be greater than zero")
	p.require(c.Embedding.CacheSize >= 0, "embedding.cache_size", "cannot be negative")
	p.require(c.Embedding.Workers > 0, "embedding.workers", "must be greater than zero")
	p.positive(c.Embedding.FillWindow, "embedding.fill_window")

	p.require(c.Vector.Dimension > 0, "vector.dimension", "must be greater than zero")
	if c.Embedding.Model == EmbeddingModel && c.Vector.Dimension != EmbeddingDimension {
		p.add("vector.dimension", fmt.Sprintf("is %d, but embedding.model %q produces %d-dimensional vectors",
			c.Vector.Dimension, EmbeddingModel, EmbeddingDimension))
	}
	p.oneOf(c.Vector.Index, []string{"flat", "hnsw"}, "vector.index")
	p.oneOf(c.Vector.Metric, []string{"cosine", "dot", "l2"}, "vector.metric")
	if c.Vector.Index == "hnsw" {
		p.require(c.Vector.HNSWM > 0, "vector.hnsw_m", "must be greater than zero when vector.index is hnsw")
		p.require(c.Vector.HNSWEfConstruction > 0, "vector.hnsw_ef_construction", "must be greater than zero when vector.index is hnsw")
		p.require(c.Vector.HNSWEfSearch > 0, "vector.hnsw_ef_search", "must be greater than zero when vector.index is hnsw")
		p.require(c.Vector.ResidentBudgetMB > 0, "vector.resident_budget_mb",
			"must be greater than zero when vector.index is hnsw: an index allowed no memory can hold no tenant")
	}

	// Text.

	// Search.
	p.require(c.Search.DefaultLimit > 0, "search.default_limit", "must be greater than zero")
	p.require(c.Search.MaxLimit >= c.Search.DefaultLimit, "search.max_limit", "cannot be below search.default_limit")
	p.require(c.Search.MaxPageDepth > 0, "search.max_page_depth", "must be greater than zero")
	p.positive(c.Paging.RankedTTL, "paging.ranked_ttl")
	p.require(c.Search.RRFK > 0, "search.rrf_k", "must be greater than zero")
	p.require(c.Search.WidenMaxFactor >= 1, "search.widen_max_factor", "must be at least one: a search that cannot widen its candidate set cannot answer a filtered query")
	p.require(c.Search.ListMaxFactor >= 1, "search.list_max_factor", "must be at least one: a listing that cannot widen cannot fill a page under a filter")
	p.ratio(c.Search.SimilarityThreshold, "search.similarity_threshold")

	// Lifecycle.
	p.positive(c.Lifecycle.SweepInterval, "lifecycle.sweep_interval")
	p.require(c.Lifecycle.RunBudget > 0, "lifecycle.run_budget",
		"must be greater than zero: a run that may act on nothing never drains a backlog")
	p.positive(c.Lifecycle.RecallWindow, "lifecycle.recall_window")
	p.positive(c.Lifecycle.EventRetention, "lifecycle.event_retention")

	// Discovery. The threshold is refused outside [0, 1] rather than clamped,
	// for the reason a memory's importance is: a caller who set 2 meant
	// something, and quietly storing 1 makes every relationship it produced a
	// number nobody can account for.
	p.require(c.Discovery.Threshold >= 0 && c.Discovery.Threshold <= 1,
		"discovery.threshold", "is a cosine, so it runs from 0 to 1")
	p.require(c.Discovery.TopK > 0, "discovery.top_k",
		"must be greater than zero: a discovery that may link nothing is discovery turned off, and discovery.enabled is how to say that")
	p.require(c.Discovery.MaxCandidates >= c.Discovery.TopK, "discovery.max_candidates",
		"cannot be below discovery.top_k: a subject could then never gain the relationships the bound allows")

	// Jobs.
	p.require(c.Jobs.Workers > 0, "jobs.workers", "must be greater than zero")
	p.positive(c.Jobs.LeaseDuration, "jobs.lease_duration")
	p.require(c.Jobs.MaxRetries >= 0, "jobs.max_retries", "cannot be negative")
	p.positive(c.Jobs.PollInterval, "jobs.poll_interval")
	p.positive(c.Jobs.Retention, "jobs.retention")
	p.positive(c.Jobs.DrainTimeout, "jobs.drain_timeout")

	// Observability.
	if _, ok := logLevels[c.Log.Level]; !ok {
		p.add("log.level", fmt.Sprintf("%q is not a level; use debug, info, warn or error", c.Log.Level))
	}
	p.oneOf(c.Log.Format, []string{"json", "text"}, "log.format")
	if c.Metrics.Enabled {
		switch path := c.Metrics.Path; {
		case !strings.HasPrefix(path, "/"):
			p.add("metrics.path", fmt.Sprintf("%q must begin with /", path))
		case !metricsPath.MatchString(path):
			p.add("metrics.path", fmt.Sprintf("%q must be a plain path such as /metrics: segments of "+
				"letters, digits, '.', '_', '~' and '-', and not the root", path))
		case path == "/api" || strings.HasPrefix(path, "/api/") || path == "/mcp" || strings.HasPrefix(path, "/mcp/"):
			p.add("metrics.path", fmt.Sprintf("%q is inside /api or /mcp, which other routes own", path))
		}
		if addr := c.Metrics.Addr; addr != "" {
			// Port 0 on both is two different ephemeral ports, not a collision.
			if _, port, err := net.SplitHostPort(addr); err != nil {
				p.add("metrics.addr", fmt.Sprintf("%q is not a host:port listen address: %v", addr, err))
			} else if addr == c.Server.HTTPAddr && port != "0" {
				p.add("metrics.addr", fmt.Sprintf("%q is server.http_addr; leave metrics.addr empty to serve "+
					"the metrics on the main listener", addr))
			}
		}
	}

	if len(p) == 0 {
		return nil
	}
	return errs.E(errs.Invalid, "config.Validate", errors.Join(p...))
}

// problems accumulates configuration errors so one startup reports all of them.
type problems []error

func (p *problems) add(key, why string) {
	*p = append(*p, fmt.Errorf("%s %s", key, why))
}

func (p *problems) require(ok bool, key, why string) {
	if !ok {
		p.add(key, why)
	}
}

func (p *problems) positive(d time.Duration, key string) {
	if d <= 0 {
		p.add(key, "must be a positive duration")
	}
}

func (p *problems) ratio(v float64, key string) {
	if v < 0 || v > 1 {
		p.add(key, fmt.Sprintf("is %v; it must be between 0 and 1", v))
	}
}

func (p *problems) oneOf(value string, allowed []string, key string) {
	for _, a := range allowed {
		if value == a {
			return
		}
	}
	p.add(key, fmt.Sprintf("%q is not one of %s", value, strings.Join(allowed, ", ")))
}

// credential is one parsed server.api_keys entry.
type credential struct {
	Secret string
	Tenant string
	// Authorized is the explicit tenant set of a credential written with more
	// than one tenant after the "@". A single tenant is a binding and arrives
	// in Tenant instead.
	Authorized []string
}

// parseAPIKey reads "<secret>@<tenant>", "<secret>@<tenant>,<tenant>,…" for a
// credential authorised across an explicit set, or "<secret>" for an unbound
// operator key.
//
// The compound string exists because config is deliberately two levels deep and
// scalar (see walk): a list of TOML tables would be a third level, and the
// package's own doc comment says configuration that nests further is
// configuration nobody can hold in their head. One string per credential is the
// form that fits, and it stays readable in a file, an environment variable and
// a flag alike.
//
// The comma form is what lets a deployment state a cross-tenant authority
// instead of leaving it to be inferred from an unbound key. One tenant is a
// binding, several are a set the request chooses within, and none is the
// operator credential that configuration has deliberately not narrowed.
func parseAPIKey(entry string) (credential, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return credential{}, errors.New(`is empty; write "<secret>@<tenant>", "<secret>@<tenant>,<tenant>" or "<secret>" for an unbound operator key`)
	}
	secret, tenantList, bound := strings.Cut(entry, "@")
	if secret == "" {
		return credential{}, errors.New(`has no secret before the "@"`)
	}
	if !bound {
		return credential{Secret: secret}, nil
	}

	var ids []string
	seen := make(map[string]bool)
	for _, part := range strings.Split(tenantList, ",") {
		id := strings.TrimSpace(part)
		if !tenantIDPattern.MatchString(id) {
			return credential{}, fmt.Errorf("names %q, which is not a tenant id: use lowercase letters, digits, - and _, up to 63 characters", id)
		}
		if seen[id] {
			return credential{}, fmt.Errorf("names tenant %q twice", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if len(ids) == 1 {
		return credential{Secret: secret, Tenant: ids[0]}, nil
	}
	return credential{Secret: secret, Authorized: ids}, nil
}

// Credentials returns the configured credentials in the form internal/auth
// builds a keyring from: the singular api_key first, then api_keys in order.
//
// It returns them as plain structs rather than importing internal/auth,
// because config sits below every subsystem and importing one of them would
// invert that. The composition root does the conversion.
// Credential is one configured credential, as a plain struct rather than an
// internal/auth type: config sits below every subsystem and importing one of
// them would invert that. The composition root does the conversion.
type Credential struct {
	ID          string
	Secret      string
	Tenant      string
	CrossTenant bool
	// Authorized is the explicit tenant set of a credential written with
	// several tenants after the "@". Empty means configuration did not narrow
	// the credential's authority.
	Authorized []string
}

func (c Config) Credentials() []Credential {
	var out []Credential
	if c.Server.APIKey != "" {
		out = append(out, Credential{ID: "operator", Secret: c.Server.APIKey, CrossTenant: true})
	}
	for i, entry := range c.Server.APIKeys {
		parsed, err := parseAPIKey(entry)
		if err != nil {
			continue // Validate already reported it; Credentials runs after.
		}
		id := parsed.Tenant
		if id == "" && len(parsed.Authorized) > 0 {
			id = strings.Join(parsed.Authorized, "+")
		}
		if id == "" {
			id = fmt.Sprintf("operator-%d", i+1)
		}
		out = append(out, Credential{
			ID:          id,
			Secret:      parsed.Secret,
			Tenant:      parsed.Tenant,
			CrossTenant: parsed.Tenant == "",
			Authorized:  parsed.Authorized,
		})
	}
	return out
}

// metricsPath is the shape a metrics path may take: one or more plain segments.
// It keeps the path clear of the router's pattern syntax (`{name}`, a method
// prefix, a trailing slash that matches a whole subtree), and of the root,
// which every unrouted request falls through to.
var metricsPath = regexp.MustCompile(`^(/[A-Za-z0-9._~-]+)+$`)

// MinProductionSecretLen is the shortest credential a production server takes.
// It is Rust's number (config.rs, validate_production_config).
const MinProductionSecretLen = 16

// placeholderSecrets are credentials copied from an example rather than chosen:
// Rust's list, and the placeholders this repository's own README and compose
// files print.
var placeholderSecrets = map[string]bool{
	"change-this-secret-key-in-production": true,
	"dev":                                  true,
	"test":                                 true,
	"replace-with-a-long-random-secret":    true,
	"replace-with-another-secret":          true,
}

// weakSecret says why a credential is not fit for production, or "" when it
// is. The reason never contains the credential: configuration errors are
// logged, and a refused key may still be a real one.
func weakSecret(secret string) string {
	switch {
	case placeholderSecrets[secret]:
		return "is a placeholder copied from an example; a production server needs a secret of its own"
	case len(secret) < MinProductionSecretLen:
		return fmt.Sprintf("is %d characters; a production server needs at least %d", len(secret), MinProductionSecretLen)
	}
	return ""
}
