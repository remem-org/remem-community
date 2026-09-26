// Package bench holds Remem's performance benchmarks, and nothing else: every
// file is a test file, so nothing here is linked into a shipped binary.
//
// The write path and vector retrieval mirror Rust Remem's two Criterion
// benchmarks at pre-go-freeze (crates/remem-server/benches), so the two can be
// compared on the same host; docs/BENCHMARKS.md has the comparison. Listing and
// keyword search have no Rust counterpart and are baselines for later Go work.
// Run them through scripts/benchmark.sh or `make bench`.
package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	// writeConcurrency is how many creates one wave sends at once. Rust's
	// WRITE_BENCHMARK_CONCURRENCY.
	writeConcurrency = 50
	// defaultTotalWrites is one timed operation. Rust's default.
	defaultTotalWrites = 1000
)

// resolveDataDir is where a benchmark writes its database.
//
// An override must be persistent and empty: /tmp is refused because a tmpfs
// measures memory rather than the disk a deployment writes to, and a non-empty
// directory is refused because a corpus left by an earlier run changes what is
// measured. Without an override it is a fresh directory under the repository's
// ignored .benchmark-data/, kept after the run for inspection. Rust's runners
// make the same two refusals, in the same words where they can.
func resolveDataDir(repoRoot, override string) (string, error) {
	if override == "" {
		parent := filepath.Join(repoRoot, ".benchmark-data")
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return "", err
		}
		dir, err := os.MkdirTemp(parent, "run-")
		if err != nil {
			return "", err
		}
		return filepath.Abs(dir)
	}

	dir, err := filepath.Abs(override)
	if err != nil {
		return "", err
	}
	if dir == "/tmp" || strings.HasPrefix(dir, "/tmp/") {
		return "", fmt.Errorf("REMEM_BENCHMARK_DATA_DIR must be persistent storage, not /tmp: %s", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	if len(entries) > 0 {
		return "", fmt.Errorf("REMEM_BENCHMARK_DATA_DIR must be empty at benchmark startup: %s", dir)
	}
	return dir, nil
}

// parseTotalWrites reads REMEM_BENCHMARK_TOTAL_WRITES: a positive multiple of
// the wave size, so every wave is full. Rust's WriteBenchmarkConfig.
func parseTotalWrites(raw string) (int, error) {
	if raw == "" {
		return defaultTotalWrites, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n%writeConcurrency != 0 {
		return 0, fmt.Errorf("REMEM_BENCHMARK_TOTAL_WRITES must be a positive multiple of %d", writeConcurrency)
	}
	return n, nil
}

// benchDataDir resolves the data directory for a running benchmark, failing
// it rather than measuring somewhere the numbers would not mean anything.
func benchDataDir(b *testing.B, name string) string {
	b.Helper()
	override := os.Getenv("REMEM_BENCHMARK_DATA_DIR")
	if override != "" {
		override = filepath.Join(override, name)
	}
	dir, err := resolveDataDir("..", override)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("benchmark data: %s", dir)
	return dir
}
