// Command remem is the Remem server.
//
//	remem start --storage.path ./data
//
// Every setting is a flag, an environment variable or a line in a TOML file,
// with exactly the same name in all three (internal/config). There is no
// setting that exists in only one of them, and none that is read from anywhere
// else.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/server"
	"github.com/remem-org/remem-go/internal/version"
)

// configPaths are searched in order. A path that does not exist is skipped:
// looking in several places and finding none is how an unconfigured Remem
// starts, which Invariant 10 requires to work.
var configPaths = []string{"remem.toml", "/etc/remem/remem.toml"}

// startProfiler is set only by a build with the pprof tag (profiler.go). It is
// nil in every build that ships.
var startProfiler func(log *slog.Logger) error

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "remem:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "start"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}

	switch command {
	case "start":
		return start(args)
	case "version":
		fmt.Printf("remem %s (%s edition, %s tenancy)\n",
			version.Binary, version.Edition, version.TenantCapability)
		fmt.Println(version.Current())
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func start(args []string) error {
	env := os.Environ()
	cfg, err := config.Load(configPaths, env, args)
	if err != nil {
		return err
	}

	log := obs.NewLogger(cfg.LogConfig())
	obs.SetDefault(log)

	// The whole configuration, once, at startup, with secrets redacted. This is
	// the "inspectable" half of spec §51: "what was this process actually
	// running with" is answerable from the logs, and the API key is not one of
	// the answers.
	log.Info("configuration", "version", version.Binary, "edition", version.Edition,
		"tenancy", version.TenantCapability, "settings", cfg.Redacted())

	// Before the data directory is opened, because both settings govern the
	// memory everything after this allocates.
	applyRuntimeSettings(cfg.Runtime, env, log)

	if startProfiler != nil {
		if err := startProfiler(log); err != nil {
			return fmt.Errorf("the diagnostic profiler: %w", err)
		}
	}

	srv, err := server.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := server.SignalContext(context.Background())
	defer stop()

	if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// applyRuntimeSettings puts the [runtime] section into force.
//
// It lives here and not in internal/server because both settings are
// process-global: a constructor that re-paced the collector would re-pace it
// for every test binary that builds a server, and that is not a decision a
// constructor gets to make. Starting a process is.
//
// The environment wins. GOGC and GOMEMLIMIT are the Go runtime's own names, and
// an operator who set one in a unit file or a container manifest has already
// said what they want; overriding it from a file would be two names for one
// setting, with the file silently winning — the defect internal/config exists
// to prevent. Either way the value in force is logged, so "what is this process
// actually pacing against" is answerable from the logs.
func applyRuntimeSettings(rt config.RuntimeConfig, env []string, log *slog.Logger) {
	d := decideRuntimeSettings(rt, env)

	if d.gcSource == fromConfig {
		debug.SetGCPercent(d.gcPercent)
		log.Info("gc percent in force", "source", d.gcSource, "runtime.gc_percent", d.gcPercent)
	} else {
		log.Info("gc percent comes from the environment; runtime.gc_percent is not applied",
			"source", d.gcSource, "GOGC", d.envGOGC, "runtime.gc_percent", d.gcPercent)
	}

	switch d.memoryLimitSource {
	case fromConfig:
		debug.SetMemoryLimit(d.memoryLimit)
		log.Info("soft memory limit in force",
			"source", d.memoryLimitSource, "runtime.memory_limit_bytes", d.memoryLimit)
	case fromEnvironment:
		log.Info("the soft memory limit comes from the environment; runtime.memory_limit_bytes is not applied",
			"source", d.memoryLimitSource, "GOMEMLIMIT", d.envGOMEMLIMIT,
			"runtime.memory_limit_bytes", d.memoryLimit)
	default:
		log.Info("no soft memory limit; the heap is bounded only by runtime.gc_percent",
			"source", d.memoryLimitSource, "runtime.gc_percent", d.gcPercent)
	}
}

// runtimeSource says where a dial's value came from. unset is the zero value so
// that a decision nobody filled in changes nothing.
type runtimeSource int

const (
	unset           runtimeSource = iota // nothing sets it
	fromConfig                           // this process sets it, from [runtime]
	fromEnvironment                      // the environment set it, and wins
)

func (s runtimeSource) String() string {
	switch s {
	case fromConfig:
		return "config"
	case fromEnvironment:
		return "environment"
	default:
		return "unset"
	}
}

// runtimeDecision is what the [runtime] section and the environment together
// settle. It is separate from applying it because applying it cannot be
// observed afterwards: debug.SetGCPercent returns the value it replaced, not the
// one now in force, so the decision is the only part of this a test can hold.
type runtimeDecision struct {
	gcPercent int
	gcSource  runtimeSource
	// envGOGC is the environment's GOGC, when that is what won.
	envGOGC string

	memoryLimit       int64
	memoryLimitSource runtimeSource
	// envGOMEMLIMIT is the environment's GOMEMLIMIT, when that is what won.
	envGOMEMLIMIT string
}

// decideRuntimeSettings works out which of the two dials this process sets, and
// to what. It reads the environment and nothing else, so it is a pure function
// of its two arguments.
func decideRuntimeSettings(rt config.RuntimeConfig, env []string) runtimeDecision {
	d := runtimeDecision{gcPercent: rt.GCPercent, memoryLimit: rt.MemoryLimitBytes}

	if value, set := lookupEnv(env, "GOGC"); set {
		d.gcSource, d.envGOGC = fromEnvironment, value
	} else {
		d.gcSource = fromConfig
	}

	switch value, set := lookupEnv(env, "GOMEMLIMIT"); {
	case set:
		d.memoryLimitSource, d.envGOMEMLIMIT = fromEnvironment, value
	case rt.MemoryLimitBytes > 0:
		d.memoryLimitSource = fromConfig
	}
	return d
}

// lookupEnv reads one variable out of an os.Environ slice. os.Getenv would do,
// but start already holds the environment it passed to config.Load, and reading
// the same slice is what makes "the environment wins" mean the same environment.
func lookupEnv(env []string, name string) (string, bool) {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, name+"="); ok {
			return value, true
		}
	}
	return "", false
}

func usage() {
	fmt.Fprint(os.Stderr, `remem — a persistent memory layer for agents

Usage:
  remem start [settings]     serve HTTP and MCP
  remem version              print the binary and format versions
  remem help                 this text

Settings take the form --section.key=value, and the same names work as
REMEM_SECTION_KEY in the environment or as keys in remem.toml. --config <path>
names a configuration file explicitly.

  remem start --storage.path ./data --server.http_addr 127.0.0.1:4545

`)
}
