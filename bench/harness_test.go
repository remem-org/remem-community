package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A benchmark's number is only as trustworthy as the directory it wrote into.
// Rust's runners refuse /tmp, because a tmpfs measures memory rather than the
// disk a deployment writes to, and refuse a non-empty directory, because a
// corpus left by a previous run silently changes what is measured
// (scripts/benchmark-in-container.sh at pre-go-freeze). The Go harness keeps
// both refusals.

func TestBenchmarkDataDirRefusesTmp(t *testing.T) {
	for _, dir := range []string{"/tmp", "/tmp/remem-bench"} {
		if _, err := resolveDataDir("..", dir); err == nil || !strings.Contains(err.Error(), "/tmp") {
			t.Errorf("resolveDataDir(%q) = %v; want a refusal naming /tmp", dir, err)
		}
	}
}

func TestBenchmarkDataDirRefusesANonEmptyDirectory(t *testing.T) {
	// Not t.TempDir(): that lives under /tmp, and the /tmp refusal would pass
	// this test for the wrong reason.
	dir := repoScratch(t)
	if dir == "/tmp" || strings.HasPrefix(dir, "/tmp/") {
		// A checkout under /tmp has nowhere inside it that is not under /tmp,
		// and there the /tmp refusal comes first, so this one cannot be reached.
		// Found releasing an edition into a scratch repository under /tmp.
		// TestBenchmarkDataDirRefusesTmp covers the refusal that can.
		t.Skip("this checkout is under /tmp, where every data directory is refused as /tmp first")
	}
	if err := os.WriteFile(filepath.Join(dir, "left-over"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveDataDir("..", dir); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("resolveDataDir over a non-empty directory = %v; want a refusal saying it must be empty", err)
	}
}

func TestBenchmarkDataDirDefaultsToAFreshDirectoryUnderTheRepository(t *testing.T) {
	root := repoScratch(t)
	a, err := resolveDataDir(root, "")
	if err != nil {
		t.Fatalf("resolveDataDir with no override: %v", err)
	}
	b, err := resolveDataDir(root, "")
	if err != nil {
		t.Fatalf("resolveDataDir a second time: %v", err)
	}
	want := filepath.Join(root, ".benchmark-data") + string(filepath.Separator)
	if !strings.HasPrefix(a, want) || !strings.HasPrefix(b, want) || a == b {
		t.Fatalf("defaults were %q and %q; want two distinct fresh directories under %q", a, b, want)
	}
}

func TestTotalWritesMustBeAPositiveMultipleOfFifty(t *testing.T) {
	if n, err := parseTotalWrites(""); err != nil || n != 1000 {
		t.Fatalf("parseTotalWrites(\"\") = %d, %v; want Rust's default of 1000", n, err)
	}
	if n, err := parseTotalWrites("2000"); err != nil || n != 2000 {
		t.Fatalf("parseTotalWrites(\"2000\") = %d, %v", n, err)
	}
	for _, raw := range []string{"0", "-50", "75", "lots"} {
		if _, err := parseTotalWrites(raw); err == nil ||
			!strings.Contains(err.Error(), "positive multiple of 50") {
			t.Errorf("parseTotalWrites(%q) = %v; want Rust's refusal", raw, err)
		}
	}
}

// repoScratch is an empty directory inside the repository's ignored
// .benchmark-data/, removed after the test.
func repoScratch(t *testing.T) string {
	t.Helper()
	parent := filepath.Join("..", ".benchmark-data")
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "harness-test-")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(abs) })
	return abs
}
