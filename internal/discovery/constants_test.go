package discovery_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/discovery"
)

// Rust's two discovery numbers, pinned as literals (config.rs:274-275). The
// behaviour tests use the constants by name, so a changed number would change
// their expectation with it; this is where a change fails. The quantity the
// threshold is compared against is Go's cosine, not Rust's 1/(1+d): §II.10
// row 17, and docs/PARITY.md.
func TestDiscoveryDefaultsAreRustsNumbers(t *testing.T) {
	threshold, topK := float64(discovery.DefaultThreshold), int(discovery.DefaultTopK)
	if threshold != 0.7 {
		t.Errorf("the discovery threshold is %v, want Rust's 0.7", threshold)
	}
	if topK != 5 {
		t.Errorf("discovery's top-k is %d, want Rust's 5", topK)
	}
}
