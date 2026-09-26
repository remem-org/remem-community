package embedding

import (
	"container/list"
	"crypto/sha256"
	"sync"
)

// cache is a bounded LRU from text to vector.
//
// It is keyed by the SHA-256 of the text rather than the text itself, for two
// reasons. A key is then 32 bytes whatever the memory's length, so the cache's
// footprint does not depend on how long the user's memories are; and the cache
// never holds a second copy of the user's content, which matters because a
// heap dump of the server would otherwise contain it.
//
// Every read returns a copy. A caller that normalises or scales a vector in
// place would otherwise corrupt the entry for every later caller, and the bug
// would present as gradually worsening search results.
type cache struct {
	mu       sync.Mutex
	capacity int
	entries  map[[32]byte]*list.Element
	order    *list.List // front is most recently used

	hits, misses uint64
}

type entry struct {
	key [32]byte
	vec []float32
}

func newCache(capacity int) *cache {
	if capacity <= 0 {
		capacity = 0
	}
	return &cache{
		capacity: capacity,
		entries:  make(map[[32]byte]*list.Element, capacity),
		order:    list.New(),
	}
}

func cacheKey(text string) [32]byte { return sha256.Sum256([]byte(text)) }

func (c *cache) get(text string) ([]float32, bool) {
	if c.capacity == 0 {
		return nil, false
	}
	k := cacheKey(text)

	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[k]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.order.MoveToFront(el)
	return append([]float32(nil), el.Value.(*entry).vec...), true
}

func (c *cache) put(text string, vec []float32) {
	if c.capacity == 0 {
		return
	}
	k := cacheKey(text)
	stored := append([]float32(nil), vec...)

	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[k]; ok {
		el.Value.(*entry).vec = stored
		c.order.MoveToFront(el)
		return
	}
	c.entries[k] = c.order.PushFront(&entry{key: k, vec: stored})
	for c.order.Len() > c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*entry).key)
	}
}

func (c *cache) stats() (hits, misses uint64, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.order.Len()
}
