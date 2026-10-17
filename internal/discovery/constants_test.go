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
	topK := int(discovery.DefaultTopK)
	if topK != 5 {
		t.Errorf("discovery's top-k is %d, want Rust's 5", topK)
	}
}

// The threshold is not Rust's any more. It was 0.7 until the passage model, and
// is 0.6 because that is the middle of the gap between paraphrases and
// topic-mates measured in the new space (see DefaultThreshold). Pinned as a
// literal for the reason Rust's numbers are: this is where a change fails.
func TestTheDiscoveryThresholdIsTheOneMeasuredForThisModel(t *testing.T) {
	if threshold := float64(discovery.DefaultThreshold); threshold != 0.6 {
		t.Errorf("the discovery threshold is %v, want the measured 0.6", threshold)
	}
}
