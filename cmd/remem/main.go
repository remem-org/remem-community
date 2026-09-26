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
	"os"
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
	cfg, err := config.Load(configPaths, os.Environ(), args)
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
