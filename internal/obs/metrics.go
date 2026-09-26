package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// namespace prefixes every metric Remem defines.
const namespace = "remem"

// Metrics holds every collector the process exposes, grouped by the domains
// spec §50 enumerates. It is constructed once by the composition root and
// passed down; there is no package-level registry, so two servers — or two
// tests — in one process do not fight over the same collectors.
//
// Labelling follows one rule: a metric that counts work done on a tenant's
// behalf carries a tenant label (Invariant 1 reaches the metrics, not only the
// keyspace). A metric describing the storage engine as a physical thing —
// compaction, block cache, file size — carries a component label instead,
// because those events belong to the engine and cannot be attributed to a
// tenant without inventing the attribution.
//
// Cluster metrics (leaders, elections, replication lag, proposal latency)
// arrive with Phase 14; spec §50 lists them as later work.
type Metrics struct {
	reg *prometheus.Registry
	// engine owns the three storage-engine collectors, so that a scrape samples
	// the engine and reports what it sampled in one pass.
	engine *engineCollector
	// collectors is everything registered, kept so DeclaredNames can ask each
	// what it declares: a registry gathers only families that have samples.
	collectors []prometheus.Collector

	API        APIMetrics
	Storage    StorageMetrics
	Search     SearchMetrics
	Jobs       JobMetrics
	Lifecycle  LifecycleMetrics
	Discovery  DiscoveryMetrics
	Migrations MigrationMetrics
}

// APIMetrics covers the delivery surfaces. The route label is the registered
// pattern, never the request path: a path carries record ids, and one time
// series per memory would take the metrics store down within a day.
type APIMetrics struct {
	RequestsTotal *prometheus.CounterVec
	Duration      *prometheus.HistogramVec
	// InFlight is what tells an operator the difference between a slow server
	// and a stopped one.
	InFlight *prometheus.GaugeVec
}

// StorageMetrics covers spec §50's storage list: writes, reads, scan latency,
// storage size, compaction and cache behaviour.
type StorageMetrics struct {
	// WritesTotal counts durable writes, by tenant and keyspace.
	WritesTotal *prometheus.CounterVec
	// ReadsTotal counts point reads, by tenant and keyspace.
	ReadsTotal *prometheus.CounterVec
	// ScanDuration measures range scans, which are the ones that get slow.
	ScanDuration *prometheus.HistogramVec
	// SizeBytes is on-disk size by component — an engine fact, not a tenant's.
	SizeBytes *prometheus.GaugeVec
	// CompactionsTotal counts compactions the engine ran.
	CompactionsTotal *prometheus.CounterVec
	// CacheAccessesTotal counts block cache accesses by result (hit/miss).
	CacheAccessesTotal *prometheus.CounterVec
}

// SearchMetrics covers vector, keyword and hybrid latency and the number of
// candidates considered. One histogram with a mode label rather than three
// metrics: the modes are compared against each other far more often than they
// are read alone.
type SearchMetrics struct {
	Duration             *prometheus.HistogramVec
	CandidatesConsidered *prometheus.HistogramVec
	// TruncatedTotal counts searches that stopped looking before they could
	// say they had found everything — the `truncated: true` a response carries.
	// Divided by the duration histogram's count it is the truncation rate, which
	// is the number that says whether widen_max_factor or max_page_depth is
	// binding for real traffic.
	TruncatedTotal *prometheus.CounterVec
}

// JobMetrics covers spec §50's background job list.
type JobMetrics struct {
	Pending            *prometheus.GaugeVec
	Running            *prometheus.GaugeVec
	FailuresTotal      *prometheus.CounterVec
	RetriesTotal       *prometheus.CounterVec
	ProcessingDuration *prometheus.HistogramVec
}

// LifecycleMetrics covers what the per-tenant maintenance job did.
//
// The backlog gauge is the one an operator should alert on. Everything else
// here says what happened; that one says whether the sweep is keeping up, which
// is the question a corpus that is entirely due — every corpus written before
// Phase 10 — makes urgent.
type LifecycleMetrics struct {
	// TransitionsTotal counts transitions by kind: recalled, promoted,
	// expired, archived, hard_deleted. The kind label is the durable event
	// name, so a dashboard and an audit row use the same word.
	TransitionsTotal *prometheus.CounterVec
	// SweepDuration measures one pass over a tenant's due memories.
	SweepDuration *prometheus.HistogramVec
	// DueBacklog is what a run left behind: memories that were due and not
	// reached. Zero is the healthy steady state.
	DueBacklog *prometheus.GaugeVec
}

// There is deliberately no events_written counter here.
//
// The first version had one, and Phase 10's end-to-end run showed what it was:
// TransitionsTotal by another name. Every transition writes exactly one event,
// so the two counters were incremented in the same loop with the same value —
// and the name promised something it did not deliver, because the commonest
// event by far is a recall, which the sweep never sees. Recall volume is already
// visible as requests to the memory route. Two metrics with identical values are
// worse than one, and a name that overstates its coverage is worse than both.

// DiscoveryMetrics covers what relationship discovery did.
//
// The outcome label is what an operator reads first, and the one worth alerting
// on is `missing`: a subject hard-deleted between its write and its discovery is
// ordinary at a trickle and means something else at a rate.
//
// There is deliberately no dropped counter. Rust has one
// (`dropped_discovery_count`) because its queue is a bounded channel that
// discards work; here the queue is the disk, so the number would be a constant
// zero dressed as a measurement.
type DiscoveryMetrics struct {
	// SubjectsTotal counts memories considered, by outcome: linked, unlinked,
	// skipped, missing.
	SubjectsTotal *prometheus.CounterVec
	// EdgesCreatedTotal counts relationships written.
	EdgesCreatedTotal *prometheus.CounterVec
	// CandidatesConsidered is how wide the union of the sources got, per
	// subject. It is the histogram that says whether MaxCandidates is binding.
	CandidatesConsidered *prometheus.HistogramVec
	// RunDuration measures one job, which may carry many subjects.
	RunDuration *prometheus.HistogramVec
}

// MigrationMetrics covers which migration is running, how far it has got, what
// failed, and how much is left where that is knowable.
type MigrationMetrics struct {
	Running          *prometheus.GaugeVec
	Progress         *prometheus.GaugeVec
	FailuresTotal    *prometheus.CounterVec
	RecordsRemaining *prometheus.GaugeVec
}

// Label names, fixed here so a dashboard written against one subsystem reads
// the same as one written against another.
const (
	labelTenant    = "tenant"
	labelKeyspace  = "op"
	labelComponent = "component"
	labelResult    = "result"
	labelMode      = "mode"
	labelKind      = "kind"
	labelOutcome   = "outcome"
	labelMigration = "migration"
	labelRoute     = "route"
	labelMethod    = "method"
	labelStatus    = "status"
)

// Latency buckets. Remem's operations span microseconds (a point read) to
// seconds (a cold hybrid query), so the range is wide and exponential.
var latencyBuckets = prometheus.ExponentialBuckets(0.0001, 3, 12) // 100µs … ~53s

// NewMetrics builds and registers every collector on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	// Process and runtime health, which every operator expects to find here.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		API: APIMetrics{
			RequestsTotal: counter("api_requests_total",
				"HTTP requests served, by tenant, route, method and status.",
				labelTenant, labelRoute, labelMethod, labelStatus),
			Duration: histogram("api_request_duration_seconds",
				"HTTP request duration, by route and method.",
				latencyBuckets, labelRoute, labelMethod),
			InFlight: gauge("api_requests_in_flight",
				"HTTP requests currently being served, by surface.", labelComponent),
		},
		Storage: StorageMetrics{
			WritesTotal: counter("storage_writes_total",
				"Durable writes, by tenant and keyspace.", labelTenant, labelKeyspace),
			ReadsTotal: counter("storage_reads_total",
				"Point reads, by tenant and keyspace.", labelTenant, labelKeyspace),
			ScanDuration: histogram("storage_scan_duration_seconds",
				"Range scan duration.", latencyBuckets, labelTenant, labelKeyspace),
			SizeBytes: gauge("storage_size_bytes",
				"On-disk size of a storage component.", labelComponent),
			CompactionsTotal: counter("storage_compactions_total",
				"Compactions run by the storage engine.", labelComponent),
			CacheAccessesTotal: counter("storage_cache_accesses_total",
				"Block cache accesses, by result.", labelComponent, labelResult),
		},
		Search: SearchMetrics{
			Duration: histogram("search_duration_seconds",
				"Search duration, by search_type (semantic, keyword, hybrid). A continuation page is not a search and is not observed.",
				latencyBuckets, labelTenant, labelMode),
			CandidatesConsidered: histogram("search_candidates_considered",
				"Candidates examined before ranking, by search_type.",
				prometheus.ExponentialBuckets(1, 4, 10), labelTenant, labelMode),
			TruncatedTotal: counter("search_truncated_total",
				"Searches that reported truncated: true, by search_type.", labelTenant, labelMode),
		},
		Jobs: JobMetrics{
			Pending: gauge("jobs_pending",
				"Jobs waiting to run.", labelTenant, labelKind),
			Running: gauge("jobs_running",
				"Jobs currently leased by a worker.", labelTenant, labelKind),
			FailuresTotal: counter("jobs_failures_total",
				"Job executions that failed.", labelTenant, labelKind),
			RetriesTotal: counter("jobs_retries_total",
				"Job executions retried after a failure.", labelTenant, labelKind),
			ProcessingDuration: histogram("jobs_processing_duration_seconds",
				"Time from lease to completion.", latencyBuckets, labelTenant, labelKind),
		},
		Lifecycle: LifecycleMetrics{
			TransitionsTotal: counter("lifecycle_transitions_total",
				"Lifecycle transitions applied, by kind.", labelTenant, labelKind),
			// A sweep is minutes at the top end, not milliseconds, so it gets
			// its own buckets rather than the shared latency ones — which top
			// out where a full run budget begins.
			SweepDuration: histogram("lifecycle_sweep_duration_seconds",
				"Time for one pass over a tenant's due memories.",
				prometheus.ExponentialBuckets(0.01, 4, 10), labelTenant),
			DueBacklog: gauge("lifecycle_due_backlog",
				"Memories that were due when a run ended and were not reached.", labelTenant),
		},
		Discovery: DiscoveryMetrics{
			SubjectsTotal: counter("discovery_subjects_total",
				"Memories considered for relationship discovery, by outcome.",
				labelTenant, labelOutcome),
			EdgesCreatedTotal: counter("discovery_edges_created_total",
				"Relationships written by discovery.", labelTenant),
			// Bounded by MaxCandidates, so the buckets stop where the bound
			// does: a histogram whose top bucket can never fill says nothing.
			CandidatesConsidered: histogram("discovery_candidates_considered",
				"Candidates gathered per subject, across every source.",
				prometheus.LinearBuckets(0, 8, 9), labelTenant),
			RunDuration: histogram("discovery_run_duration_seconds",
				"Time for one discovery job, which may carry many subjects.",
				latencyBuckets, labelTenant),
		},
		Migrations: MigrationMetrics{
			Running: gauge("migration_running",
				"1 while a migration is executing, 0 otherwise.", labelMigration),
			Progress: gauge("migration_progress_ratio",
				"Fraction of the migration completed, 0 to 1.", labelMigration),
			FailuresTotal: counter("migration_failures_total",
				"Migration attempts that failed.", labelMigration),
			RecordsRemaining: gauge("migration_records_remaining",
				"Records left to migrate, where the count is knowable.", labelMigration),
		},
	}

	m.engine = &engineCollector{storage: &m.Storage}
	m.collectors = []prometheus.Collector{
		m.API.RequestsTotal, m.API.Duration, m.API.InFlight,
		m.Storage.WritesTotal, m.Storage.ReadsTotal, m.Storage.ScanDuration,
		m.engine, // SizeBytes, CompactionsTotal and CacheAccessesTotal, sampled at scrape time
		m.Search.Duration, m.Search.CandidatesConsidered, m.Search.TruncatedTotal,
		m.Jobs.Pending, m.Jobs.Running, m.Jobs.FailuresTotal,
		m.Jobs.RetriesTotal, m.Jobs.ProcessingDuration,
		m.Lifecycle.TransitionsTotal, m.Lifecycle.SweepDuration,
		m.Lifecycle.DueBacklog,
		m.Discovery.SubjectsTotal, m.Discovery.EdgesCreatedTotal,
		m.Discovery.CandidatesConsidered, m.Discovery.RunDuration,
		m.Migrations.Running, m.Migrations.Progress,
		m.Migrations.FailuresTotal, m.Migrations.RecordsRemaining,
	}
	reg.MustRegister(m.collectors...)
	return m
}

// Registry exposes the registry, for a subsystem that must register a
// collector it owns — the Pebble adapter's engine statistics, for one.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		// A scrape must never take the server down with it.
		ErrorHandling: promhttp.ContinueOnError,
	})
}

func counter(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
}

func gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
}

func histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets}, labels)
}

// TenantLabel is the label every per-tenant series carries. It is named here
// because [Metrics.HandlerFor] filters on it, and a filter that guessed the
// label name would silently pass everything the day the label was renamed.
const TenantLabel = "tenant"

// HandlerFor serves the metrics, showing only the series whose tenant label
// allowed admits. A series with no tenant label — a process or engine metric —
// is always shown: it belongs to the deployment rather than to a tenant.
//
// It exists because a scrape is inherently cross-tenant: the labels on the
// per-tenant series are a list of every tenant the server has served. A
// deployment that hands a credential an explicit tenant scope must not have
// that scope widened by the monitoring endpoint, and the endpoint is the one
// surface where the scope cannot come from the request — a scrape names no
// tenant at all.
//
// The filter is a func over a plain string rather than a set of tenant ids,
// because this package deliberately does not know internal/tenant exists: the
// tenant reaches it as a label, through [WithTenant], and nothing here should
// start deciding what a tenant is.
func (m *Metrics) HandlerFor(allowed func(tenant string) bool) http.Handler {
	if allowed == nil {
		return m.Handler()
	}
	return promhttp.HandlerFor(scopedGatherer{inner: m.reg, allowed: allowed},
		promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// scopedGatherer filters what the registry reports rather than what it holds.
//
// Filtering at the gatherer keeps every encoding decision — the text format,
// the negotiation, the error handling — in promhttp where it already lives, and
// means a series is never omitted from the process's own accounting merely
// because the last scrape was not allowed to see it.
type scopedGatherer struct {
	inner   prometheus.Gatherer
	allowed func(string) bool
}

func (g scopedGatherer) Gather() ([]*dto.MetricFamily, error) {
	families, err := g.inner.Gather()
	if err != nil {
		return nil, err
	}
	out := make([]*dto.MetricFamily, 0, len(families))
	for _, fam := range families {
		kept := make([]*dto.Metric, 0, len(fam.Metric))
		for _, met := range fam.Metric {
			if g.admits(met) {
				kept = append(kept, met)
			}
		}
		if len(kept) == 0 {
			// A family with no visible series is dropped rather than exported
			// empty: an empty family is a statement that the metric exists and
			// has no values, which is not what happened.
			continue
		}
		// A fresh family rather than a copy of the one the registry returned:
		// a protobuf message carries internal state that must not be copied,
		// and the encoder reads only these four fields.
		out = append(out, &dto.MetricFamily{
			Name:   fam.Name,
			Help:   fam.Help,
			Type:   fam.Type,
			Unit:   fam.Unit,
			Metric: kept,
		})
	}
	return out, nil
}

func (g scopedGatherer) admits(met *dto.Metric) bool {
	for _, pair := range met.Label {
		if pair.GetName() == TenantLabel {
			return g.allowed(pair.GetValue())
		}
	}
	return true
}
