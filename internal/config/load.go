package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/remem-org/remem-go/internal/errs"
)

// EnvPrefix is the prefix of every environment variable Remem reads. A
// variable without it is somebody else's.
const EnvPrefix = "REMEM_"

// Load builds the configuration from, in increasing precedence: the defaults,
// the first readable file among paths, the environment, and the flags. The
// result is validated, so a Config that comes back from Load is usable.
//
//   - paths are search paths. One that does not exist is skipped: a server
//     looks in several places and finding none is how an unconfigured Remem
//     starts.
//   - env is the environment in os.Environ form. Pass os.Environ(); nil reads
//     nothing, which is what a test wants.
//   - args are command-line arguments without the program name. `--config
//     <path>` names a file explicitly: it replaces the search paths and must
//     exist, because an operator who names a file and gets defaults has been
//     lied to.
//
// Every value is set through one string-parsing path, whatever its source, so
// "45s" means the same thing in a file, a variable and a flag, and every error
// names the key at fault.
func Load(paths []string, env []string, args []string) (Config, error) {
	const op = "config.Load"

	c := Default()
	fields := walk(reflect.ValueOf(&c).Elem())

	explicit, flags, err := parseArgs(args, fields)
	if err != nil {
		return Config{}, errs.E(errs.Invalid, op, err)
	}
	if explicit != "" {
		if _, statErr := os.Stat(explicit); statErr != nil {
			return Config{}, errs.E(errs.Invalid, op,
				fmt.Errorf("--config %s: %w", explicit, statErr))
		}
		paths = []string{explicit}
	}

	for _, path := range paths {
		values, err := readFile(path)
		if err != nil {
			return Config{}, errs.E(errs.Invalid, op, err)
		}
		if values == nil {
			continue // Missing search path; an empty readable file is non-nil.
		}
		if err := apply(fields, values, "in "+path); err != nil {
			return Config{}, errs.E(errs.Invalid, op, err)
		}
		break
	}

	envValues, err := readEnv(env, fields)
	if err != nil {
		return Config{}, errs.E(errs.Invalid, op, err)
	}
	if err := apply(fields, envValues, "from the environment"); err != nil {
		return Config{}, errs.E(errs.Invalid, op, err)
	}

	if err := apply(fields, flags, "on the command line"); err != nil {
		return Config{}, errs.E(errs.Invalid, op, err)
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// field is one configurable value: where it lives, and whether printing it
// would print a secret.
type field struct {
	value  reflect.Value
	secret bool
}

// walk maps "section.key" to the settable field behind it. It is deliberately
// two levels deep: configuration that nests further is configuration nobody
// can hold in their head.
func walk(cfg reflect.Value) map[string]field {
	out := make(map[string]field)
	t := cfg.Type()
	for i := range t.NumField() {
		section := t.Field(i)
		name := tagName(section)
		if name == "" {
			continue
		}
		sv := cfg.Field(i)
		st := sv.Type()
		for j := range st.NumField() {
			sf := st.Field(j)
			key := tagName(sf)
			if key == "" {
				continue
			}
			out[name+"."+key] = field{
				value:  sv.Field(j),
				secret: sf.Tag.Get("secret") == "true",
			}
		}
	}
	return out
}

func tagName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("toml"), ",")
	if name == "-" {
		return ""
	}
	return name
}

// readFile decodes a TOML file into "section.key" strings. A file that does
// not exist contributes nothing; one that exists and is malformed is an error.
func readFile(path string) (map[string]string, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var raw map[string]any
	if _, err := toml.DecodeFile(path, &raw); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	out := make(map[string]string)
	for section, body := range raw {
		table, ok := body.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("in %s: %q is not a section; every setting belongs to one", path, section)
		}
		for key, value := range table {
			s, err := scalarString(value)
			if err != nil {
				return nil, fmt.Errorf("in %s: %s.%s: %w", path, section, key, err)
			}
			out[section+"."+key] = s
		}
	}
	return out, nil
}

// scalarString renders a decoded TOML value as the string the setters parse.
func scalarString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			s, err := scalarString(e)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, ","), nil
	default:
		return "", fmt.Errorf("unsupported value %v of type %T", v, v)
	}
}

// legacyEnv maps the variable names Rust Remem used onto Go settings.
//
// They exist because an upgrade must not be a rewrite of every deployment
// manifest. REMEM_API_KEY and REMEM_DATA_DIR are in every existing
// environment, in every compose file and in every MCP client configuration
// that ever pointed at Remem; refusing to start because of them would make the
// first thing a Go server did be to break a working deployment.
var legacyEnv = map[string]string{
	"REMEM_API_KEY":           "server.api_key",
	"REMEM_API_KEY_SECONDARY": "server.api_keys",
	"REMEM_DATA_DIR":          "storage.path",
	"REMEM_ENV":               "server.env",
	// Same numbers, one difference an operator should know: Rust limited per
	// client address, Go limits per tenant.
	"REMEM_RATE_LIMIT_RPS":   "server.rate_limit_rps",
	"REMEM_RATE_LIMIT_BURST": "server.rate_limit_burst",
}

// retiredEnv names variables Rust Remem read that Go has no equivalent for,
// and what to do instead.
//
// Each one refuses startup rather than being ignored, and that is the point.
// Every entry here is security-relevant — whether authentication may be off,
// which origins may call, whether proxy headers are trusted — and an operator
// whose CORS restriction silently stopped being enforced would find out from
// an incident rather than from a message. A one-time edit is the cheaper
// outcome.
var retiredEnv = map[string]string{
	"REMEM_ALLOW_AUTH_DISABLED": "authentication is off exactly when no credential is configured, and server.env=production refuses that outright",
	"REMEM_CORS_ORIGINS":        "no CORS policy is implemented yet; put Remem behind a proxy that sets one",
	"REMEM_TRUST_PROXY_HEADERS": "no proxy headers are trusted yet",
	"REMEM_PORT":                "set server.http_addr, which names the interface as well as the port",
	"REMEM_DEFAULT_PARTITION":   "a Rust partition is a Go namespace, not a tenant; namespaces are fixed at \"default\" until a product decision introduces more",
}

// retiredSettings are names that were settings and are not any more, with the
// reason each went.
//
// They refuse start-up rather than being ignored, for the reason retiredEnv
// does: an operator whose dial silently stopped being read finds out from a
// support ticket rather than from a message, and a one-time edit is the cheaper
// outcome. The three text settings went because they were not performance dials
// — each decided what was written into the postings key space, so turning one
// on a live corpus made half the memories findable by a term and half not.
var retiredSettings = map[string]string{
	"text.min_token_length": "how text is split into terms is fixed in code, because changing it " +
		"changes what is written to disk rather than how fast a query runs; a corpus written under " +
		"one setting and searched under another answers differently for older and newer memories",
	"text.max_token_length": "there is no length above which a term is dropped any more: a long token " +
		"is folded to a bounded key, so a tag of any length filters correctly",
	"text.remove_stop_words": "stop words are a fixed list in internal/text, for the same reason as " +
		"text.min_token_length",
	"lifecycle.decay_half_life": "importance decay is a per-day multiplier on a named retention " +
		"policy, not a half-life, and it is set per tenant through " +
		"PATCH /api/v1/tenants/{id}/policies. A global dial that silently lost to a policy " +
		"would be a setting an operator believes is in force and is not",
	"lifecycle.promotion_threshold": "a memory is promoted at a number of recalls, not at an " +
		"importance, and the count belongs to the policy it is promoting out of. See " +
		"promote_at_recalls on PATCH /api/v1/tenants/{id}/policies",
	"lifecycle.archive_at_health": "the health that archives is a field on each retention policy " +
		"now, so one tenant can retire aggressively while another does not. See " +
		"archive_at_health on PATCH /api/v1/tenants/{id}/policies",
	"jobs.queue_capacity": "the job queue is durable, so its capacity is the disk rather than a " +
		"number; the bounded in-memory channel this bounded is the thing the job framework replaced, " +
		"because it dropped work under load and only counted the drops. See jobs.retention for how " +
		"long finished jobs are kept, and jobs.workers for how much runs at once",
}

// foreignEnv are variables that belong to another Remem binary sharing this
// environment. The server ignores them rather than refusing: remem-mcp is
// routinely launched from the same shell as the server it talks to.
var foreignEnv = map[string]bool{
	"REMEM_MCP_URL":     true,
	"REMEM_MCP_API_KEY": true,
	"REMEM_MCP_TENANT":  true,
}

// readEnv turns REMEM_SECTION_KEY variables into "section.key" values.
//
// Section names are single words, so the first underscore separates the
// section from the key: REMEM_SERVER_HTTP_ADDR is server.http_addr.
//
// An unrecognised REMEM_ variable refuses startup rather than being ignored,
// because a typo that is silently dropped is a setting an operator believes is
// in force and is not. The three tables above are what keep that rule from
// being merely obstructive.
func readEnv(env []string, fields map[string]field) (map[string]string, error) {
	out := make(map[string]string)
	// setBy remembers which variable last wrote each setting, so that two names
	// for one setting can be caught rather than resolved by luck.
	setBy := make(map[string]string)

	set := func(full, value, from string) error {
		// A legacy name and its canonical name are two spellings of one
		// setting, and os.Environ() has no defined order — so without this the
		// winner is whichever the operating system happened to hand over
		// second. That is the failure this package exists to prevent, in its
		// purest form: a setting an operator can see in their environment,
		// believe is in force, and watch be ignored with nothing said.
		//
		// Found by a Phase 9 verification run, where a REMEM_API_KEY left over
		// in the shell silently overrode the REMEM_SERVER_API_KEY the run had
		// just set, and the server refused its own operator credential.
		if prev, seen := setBy[full]; seen && prev != from && out[full] != value {
			return fmt.Errorf("%s and %s both set %s, to different values; "+
				"which one wins depends on the order the operating system returns the environment in, "+
				"so set one of them and unset the other", prev, from, full)
		}
		out[full] = value
		setBy[full] = from
		return nil
	}

	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(name, EnvPrefix) {
			continue
		}
		if foreignEnv[name] {
			continue
		}
		if why, retired := retiredEnv[name]; retired {
			return nil, fmt.Errorf("%s is no longer a Remem setting: %s", name, why)
		}
		if full, legacy := legacyEnv[name]; legacy {
			if err := set(full, value, name); err != nil {
				return nil, err
			}
			continue
		}
		section, key, ok := strings.Cut(strings.ToLower(strings.TrimPrefix(name, EnvPrefix)), "_")
		if !ok {
			return nil, fmt.Errorf("%s does not name a setting; the form is %sSECTION_KEY", name, EnvPrefix)
		}
		full := section + "." + key
		if _, known := fields[full]; !known {
			if why, retired := retiredSettings[full]; retired {
				return nil, fmt.Errorf("%s is no longer a Remem setting (it would be %s): %s", name, full, why)
			}
			return nil, fmt.Errorf("%s is not a Remem setting (it would be %s)", name, full)
		}
		if err := set(full, value, name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// aliases are short flag spellings for settings people reach for constantly.
//
// There is one, and adding a second is a decision rather than a habit: every
// alias is a second name for one thing, and a configuration with two names for
// each setting is one nobody can search. This one exists because
// `remem start --data-dir ./data` is the command in the specification and in
// every piece of documentation, and making a new user type
// `--storage.path` instead would be pedantry with a cost.
var aliases = map[string]string{
	"data-dir": "storage.path",
}

// parseArgs reads --section.key=value, --section.key value, --config=path and
// --config path. A single leading dash is accepted too, as Go's own flag
// package does.
func parseArgs(args []string, fields map[string]field) (configPath string, values map[string]string, err error) {
	values = make(map[string]string)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			return "", nil, fmt.Errorf("unexpected argument %q; every setting is given as --section.key=value", arg)
		}
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if name == "" {
			return "", nil, fmt.Errorf("unexpected argument %q", arg)
		}
		if full, ok := aliases[name]; ok {
			name = full
		}

		f, known := fields[name]
		if !known && name != "config" {
			return "", nil, fmt.Errorf("--%s is not a Remem setting", name)
		}

		if !hasValue {
			// A bool flag may stand alone; anything else needs the next word.
			if known && f.value.Kind() == reflect.Bool && (i+1 >= len(args) || strings.HasPrefix(args[i+1], "-")) {
				value = "true"
			} else {
				if i+1 >= len(args) {
					return "", nil, fmt.Errorf("--%s needs a value", name)
				}
				i++
				value = args[i]
			}
		}

		if name == "config" {
			configPath = value
			continue
		}
		values[name] = value
	}
	return configPath, values, nil
}

// apply sets each named value, reporting where it came from so an operator
// knows which file or variable to fix.
func apply(fields map[string]field, values map[string]string, origin string) error {
	var problems []error
	for name, raw := range values {
		f, ok := fields[name]
		if !ok {
			if why, retired := retiredSettings[name]; retired {
				problems = append(problems, fmt.Errorf(
					"%s is no longer a Remem setting (%s): %s", name, origin, why))
				continue
			}
			problems = append(problems, fmt.Errorf("%s is not a Remem setting (%s)", name, origin))
			continue
		}
		if err := set(f.value, raw); err != nil {
			problems = append(problems, fmt.Errorf("%s (%s): %w", name, origin, err))
		}
	}
	return errors.Join(problems...)
}

// set parses raw into the field, which is where every source converges.
func set(v reflect.Value, raw string) error {
	if v.Type() == reflect.TypeOf(time.Duration(0)) {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("%q is not a duration; write it as 30s, 5m or 2h", raw)
		}
		v.SetInt(int64(d))
		return nil
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("%q is not true or false", raw)
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not a whole number", raw)
		}
		if v.OverflowInt(n) {
			return fmt.Errorf("%q does not fit in %s", raw, v.Type())
		}
		v.SetInt(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("%q is not a number", raw)
		}
		v.SetFloat(f)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported setting type %s", v.Type())
		}
		if raw == "" {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			return nil
		}
		parts := strings.Split(raw, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		v.Set(reflect.ValueOf(parts))
	default:
		return fmt.Errorf("unsupported setting type %s", v.Type())
	}
	return nil
}
