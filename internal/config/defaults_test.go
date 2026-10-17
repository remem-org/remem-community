package config_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

// The defaults an unconfigured Remem runs with, and the fixed constants.

func TestDefaultsAreValid(t *testing.T) {
	// A development default that does not pass its own validation is a trap.
	if err := config.Default().Validate(); err != nil {
		t.Fatalf("the defaults must validate: %v", err)
	}
}

// The [runtime] section trades resident memory for fewer collections, so the
// numbers it ships with are the trade-off and belong in a test.
func TestRuntimeDefaultsTradeMemoryForFewerCollections(t *testing.T) {
	c := config.Default()
	// GOGC 200 lets the heap grow to roughly three times live data between
	// collections. Go's own default of 100 collects at twice, and the
	// performance review measured the collector at over 40% of CPU in the
	// search benchmark under it.
	if c.Runtime.GCPercent != 200 {
		t.Errorf("runtime.gc_percent = %d, want 200", c.Runtime.GCPercent)
	}
	// No limit unless an operator sets one: a soft limit a server did not ask
	// for turns a memory spike into a collector that will not stop running.
	if c.Runtime.MemoryLimitBytes != 0 {
		t.Errorf("runtime.memory_limit_bytes = %d, want 0", c.Runtime.MemoryLimitBytes)
	}
}

// Pebble's own defaults are no filter and a 4 MiB memtable, which are tuned for
// a short-lived test instance rather than a server, so Remem names its own.
func TestStorageDefaultsSizeTheMemtableAndTheBloomFilter(t *testing.T) {
	c := config.Default()
	if c.Storage.MemTableSizeBytes != 64<<20 {
		t.Errorf("storage.memtable_size_bytes = %d, want 64 MiB", c.Storage.MemTableSizeBytes)
	}
	// Ten bits per key is the usual bloom size: about a 1% false-positive rate
	// for about 1.25 bytes of filter per key.
	if c.Storage.BloomBitsPerKey != 10 {
		t.Errorf("storage.bloom_bits_per_key = %d, want 10", c.Storage.BloomBitsPerKey)
	}
}

// Zero is "take the engine's default" for one and "write no filter" for the
// other, so neither may be refused for being zero.
func TestZeroMemtableAndBloomAreTheEnginesDefaults(t *testing.T) {
	c := config.Default()
	c.Storage.MemTableSizeBytes = 0
	c.Storage.BloomBitsPerKey = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("zero means the engine default for both keys: %v", err)
	}
}

// -1 is the Go runtime's own spelling of "do not collect", so it is a value
// rather than a mistake — which is why validation names it instead of refusing
// every negative number.
func TestGCPercentOfMinusOneTurnsTheCollectorOff(t *testing.T) {
	c := config.Default()
	c.Runtime.GCPercent = -1
	if err := c.Validate(); err != nil {
		t.Fatalf("runtime.gc_percent = -1 must be accepted: %v", err)
	}
}

func TestFixedConstantsAreTheDefaults(t *testing.T) {
	c := config.Default()
	if c.Embedding.Model != "pplx-embed-context-v1-0.6b" {
		t.Errorf("embedding model = %q", c.Embedding.Model)
	}
	if c.Vector.Dimension != 1024 {
		t.Errorf("vector dimension = %d, want 1024", c.Vector.Dimension)
	}
	if c.Search.RRFK != 60 {
		t.Errorf("rrf k = %d, want 60", c.Search.RRFK)
	}
	if c.Search.WidenMaxFactor != 32 {
		t.Errorf("widen_max_factor = %d, want 32", c.Search.WidenMaxFactor)
	}
	// Rust's config.rs:300. Deliberately larger than the search bound: a
	// listing candidate is far cheaper than a widened search's.
	if c.Search.ListMaxFactor != 128 {
		t.Errorf("list_max_factor = %d, want 128", c.Search.ListMaxFactor)
	}
}

// embedding.workers and the ONNX intra-op thread count are different things,
// and were once the same knob: the model runs one inference at a time, so
// workers bounds how many batches queue for it and buys no parallelism inside
// a run. An operator raising one must not be silently raising the other.
func TestTheIntraOpThreadCountIsItsOwnSetting(t *testing.T) {
	if got := config.Default().Embedding.IntraOpThreads; got != config.DefaultEmbeddingIntraOpThreads {
		t.Errorf("embedding.intra_op_threads = %d, want %d", got, config.DefaultEmbeddingIntraOpThreads)
	}
	if config.DefaultEmbeddingIntraOpThreads != 0 {
		t.Errorf("the default is %d; zero is what derives the count from the CPUs available",
			config.DefaultEmbeddingIntraOpThreads)
	}

	c, err := config.Load(nil, []string{"REMEM_EMBEDDING_INTRA_OP_THREADS=4"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Embedding.IntraOpThreads != 4 {
		t.Errorf("embedding.intra_op_threads = %d, want 4", c.Embedding.IntraOpThreads)
	}
	if c.Embedding.Workers != config.DefaultEmbeddingWorkers {
		t.Errorf("the thread count moved embedding.workers to %d", c.Embedding.Workers)
	}
}

func TestTheEmbeddingProviderDefaultsToONNXAndLoadsFromTheEnvironment(t *testing.T) {
	if got := config.Default().Embedding.Provider; got != config.ProviderONNX {
		t.Fatalf("default provider = %q, want %q", got, config.ProviderONNX)
	}
	c, err := config.Load(nil, []string{
		"REMEM_EMBEDDING_PROVIDER=external",
		"REMEM_EMBEDDING_EXTERNAL_MODEL_ID=text-embedding-3-small",
		"REMEM_VECTOR_DIMENSION=1536",
	}, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Embedding.Provider != config.ProviderExternal || c.Embedding.ExternalModelID != "text-embedding-3-small" || c.Vector.Dimension != 1536 {
		t.Fatalf("loaded %+v / dimension %d", c.Embedding, c.Vector.Dimension)
	}
}
