// Command remem-mcp bridges a stdio MCP client to a Remem server.
//
// It is a pipe, deliberately. Every MCP client speaks stdio and Remem is a
// server; the alternative — embedding the engine here — would mean two
// processes writing one data directory whenever a user also ran the server,
// which is the single thing a storage engine cannot survive.
//
//	remem-mcp --url http://localhost:4545/mcp --api-key KEY
//
// Configuration comes from flags or from REMEM_MCP_URL, REMEM_MCP_API_KEY and
// REMEM_MCP_TENANT, because an MCP client launches this process from a config
// file where environment variables are the only thing that travels.
//
// The REMEM_MCP_ prefix is not decoration. The server reads REMEM_ variables as
// its own configuration and refuses any it does not recognise, so a bridge that
// claimed REMEM_API_KEY would stop the server from starting in any shell where
// both were used. internal/config reserves these three names for exactly that
// reason.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/version"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// stderr, never stdout: stdout is the protocol stream, and one stray
		// line on it corrupts the client's parse for the rest of the session.
		fmt.Fprintln(os.Stderr, "remem-mcp:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("remem-mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	url := fs.String("url", envOr("REMEM_MCP_URL", "http://127.0.0.1:4545/mcp"),
		"the Remem server's MCP endpoint")
	key := fs.String("api-key", os.Getenv("REMEM_MCP_API_KEY"),
		"credential presented to the server")
	tenantID := fs.String("tenant", os.Getenv("REMEM_MCP_TENANT"),
		"tenant to act for; honoured only for a credential authorised across tenants")
	showVersion := fs.Bool("version", false, "print the version and exit")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintln(os.Stderr, "remem-mcp", version.Binary)
		return nil
	}

	// A signal ends the bridge cleanly, so a client that stops the process gets
	// a closed pipe rather than a half-written message.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &mcp.StdioClient{Endpoint: *url, Credential: *key, Tenant: *tenantID}
	return client.Run(ctx, os.Stdin, os.Stdout)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
