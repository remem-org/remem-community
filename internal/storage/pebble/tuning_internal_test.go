package pebble

import (
	"testing"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
)

// Pebble inherits a level's filter policy from the level above only when the
// level's own is nil, and it fills every unset field in during Open. So the
// options are asserted before Open, where "what Remem asked for" is still
// distinguishable from "what Pebble decided".
func TestTuningWritesABloomFilterOnEveryLevel(t *testing.T) {
	po := &pebbledb.Options{}
	applyTuning(po, Options{MemTableSizeBytes: 64 << 20, BloomBitsPerKey: 10})

	if po.MemTableSize != 64<<20 {
		t.Errorf("MemTableSize = %d, want %d", po.MemTableSize, int64(64<<20))
	}
	want := pebbledb.FilterPolicy(bloom.FilterPolicy(10))
	for level := range po.Levels {
		if got := po.Levels[level].FilterPolicy; got != want {
			t.Errorf("level %d filter policy = %v, want %v", level, got, want)
		}
	}
}

// Zero means "the engine decides" for both, and the way to say that to Pebble
// is to leave the field alone: a zero MemTableSize is what Open reads as "use
// 4 MiB", and a nil policy is what it reads as "no filter".
func TestZeroTuningLeavesPebblesDefaultsAlone(t *testing.T) {
	po := &pebbledb.Options{}
	applyTuning(po, Options{})

	if po.MemTableSize != 0 {
		t.Errorf("MemTableSize = %d, want it untouched at 0", po.MemTableSize)
	}
	for level := range po.Levels {
		if po.Levels[level].FilterPolicy != nil {
			t.Errorf("level %d has a filter policy, want none", level)
		}
	}
}
