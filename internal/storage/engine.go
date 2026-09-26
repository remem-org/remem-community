package storage

// EngineStats are facts about a storage engine as a physical thing: how much
// disk it occupies, how many compactions it has run, how its block cache has
// served reads.
//
// They carry no tenant, and cannot: a compaction rewrites a range of SSTables
// that holds many tenants' rows at once, and attributing it to one of them would
// be inventing the attribution. That is why they are sampled from the engine at
// scrape time rather than counted per operation like reads and writes.
//
// The counts are cumulative since the engine was opened.
type EngineStats struct {
	DiskBytes   uint64
	Compactions int64
	CacheHits   int64
	CacheMisses int64
}

// StatsReporter is a store that can report EngineStats. The in-memory store is
// not one: it has no disk, no compactions and no block cache, and reporting
// zeros for them would be a measurement of nothing dressed as a measurement.
type StatsReporter interface {
	EngineStats() EngineStats
}
