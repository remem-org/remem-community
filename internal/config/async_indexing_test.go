package config_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

// Asynchronous indexing is off by default, and the bound is the queue's own.
// Turned on, a memory becomes vector-searchable only once the queue applies its
// insert, so a search issued straight after a create can miss it; that is a
// trade an operator opts into (config/remem.toml, docs/architecture/vector.md),
// not one a default makes for them. Read-your-writes with the queue on is
// openspec/changes/async-vector-indexing.
func TestAsyncIndexingIsOffByDefaultWithABoundedQueue(t *testing.T) {
	c := config.Default()
	if c.Vector.AsyncIndexing {
		t.Error("vector.async_indexing is on by default; a write would then be unsearchable until the queue applies it")
	}
	if c.Vector.IndexQueueSize != config.DefaultIndexQueueSize {
		t.Errorf("vector.index_queue_size = %d, want %d",
			c.Vector.IndexQueueSize, config.DefaultIndexQueueSize)
	}
	if config.DefaultIndexQueueSize != 1024 {
		t.Errorf("the default bound is %d; 1024 is the number the queue ships with",
			config.DefaultIndexQueueSize)
	}
}

// A queue of nothing is not a queue: a server configured that way would make
// every create wait for an index insert it believed it had moved off the
// request path, which is worse than turning the setting off.
func TestAnUnservableIndexQueueSizeIsRefused(t *testing.T) {
	c := config.Default()
	c.Vector.AsyncIndexing = true
	c.Vector.IndexQueueSize = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("vector.index_queue_size = 0 was accepted")
	}
	if !strings.Contains(err.Error(), "vector.index_queue_size") {
		t.Fatalf("the failure does not name the setting: %v", err)
	}

	// With asynchronous indexing off there is no queue to size, so the number
	// is not consulted and must not be refused.
	c.Vector.AsyncIndexing = false
	if err := c.Validate(); err != nil {
		t.Fatalf("the size is only a setting when the queue exists: %v", err)
	}
}

// The bound has an upper end too. Each slot holds a queued embedding and the
// channel is allocated at that depth per worker, so a slipped digit is
// gigabytes of resident memory a server would otherwise reserve on the way up
// and never explain.
func TestAnIndexQueueSizeAboveTheBoundIsRefused(t *testing.T) {
	c := config.Default()
	c.Vector.AsyncIndexing = true
	c.Vector.IndexQueueSize = config.MaxIndexQueueSize + 1
	err := c.Validate()
	if err == nil {
		t.Fatalf("vector.index_queue_size = %d was accepted", c.Vector.IndexQueueSize)
	}
	if !strings.Contains(err.Error(), "vector.index_queue_size") {
		t.Fatalf("the failure does not name the setting: %v", err)
	}

	// The bound itself is a value, not the first refusal.
	c.Vector.IndexQueueSize = config.MaxIndexQueueSize
	if err := c.Validate(); err != nil {
		t.Fatalf("the bound itself was refused: %v", err)
	}

	// And it is only consulted when there is a queue, like the lower end.
	c.Vector.IndexQueueSize = config.MaxIndexQueueSize + 1
	c.Vector.AsyncIndexing = false
	if err := c.Validate(); err != nil {
		t.Fatalf("the size is only a setting when the queue exists: %v", err)
	}
}

func TestTheAsyncIndexingSettingsLoadAcrossSources(t *testing.T) {
	c, err := config.Load(nil, []string{
		"REMEM_VECTOR_ASYNC_INDEXING=false",
		"REMEM_VECTOR_INDEX_QUEUE_SIZE=64",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Vector.AsyncIndexing {
		t.Error("REMEM_VECTOR_ASYNC_INDEXING=false did not turn it off")
	}
	if c.Vector.IndexQueueSize != 64 {
		t.Errorf("vector.index_queue_size = %d, want 64", c.Vector.IndexQueueSize)
	}

	c, err = config.Load(nil, nil, []string{"--vector.index_queue_size", "7"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Vector.IndexQueueSize != 7 {
		t.Errorf("--vector.index_queue_size 7 gave %d", c.Vector.IndexQueueSize)
	}
}
