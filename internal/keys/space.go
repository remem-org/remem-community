// Package keys is Remem's key encoding: a durable format, versioned as
// version.KeyEncoding, whose byte layout is as much a compatibility surface as
// any file format.
//
// # Layout
//
//	key := <tlen:uvarint><tenant> <nslen:uvarint><namespace> <space:1> <remainder>
//
// The tenant comes first, and that is the load-bearing decision. Everything one
// tenant owns is therefore one contiguous range: erasing a departing customer,
// exporting them, and splitting them onto their own node in Phase 14 are each a
// single range operation rather than one per space that somebody has to
// remember to keep in step. It also makes plan §II.2's own promise — "tenant
// ranges are contiguous" — literally true, and [TenantRange] implementable.
//
// Both identifiers are length-prefixed, and that is not a formatting
// preference. Without it "acme" is a byte prefix of "acmecorp", and a prefix
// scan over one silently returns the other's rows: a tenant-isolation defect
// that no amount of care at a higher layer can undo.
//
// The system space is the one space with no tenant and no namespace. It does
// not get a special layout — it uses an empty tenant and an empty namespace, so
// the parser has no branch and system rows sort ahead of all user data.
//
// # Invariant 1
//
// There is no builder for a user-space key that does not take a tenant. The
// unchecked builders assume a scope already validated at the request boundary;
// [RecordChecked] is the validating form for the paths that have not proved it
// yet. An empty tenant is refused rather than defaulted.
package keys

// Space is the one-byte discriminator that says what kind of row a key
// addresses. The values are a durable format: a space byte is never reused for
// a different meaning, and a retired one stays retired.
type Space byte

const (
	// SpaceSystem holds format versions, the tenant directory and migration
	// state. Canonical and untenanted.
	SpaceSystem Space = 0x01
	// SpaceRecord holds canonical record bodies.
	SpaceRecord Space = 0x02
	// SpaceVector holds canonical embeddings, stamped with model id and dim.
	SpaceVector Space = 0x03
	// SpaceEdgeOut holds canonical out-edges.
	SpaceEdgeOut Space = 0x04
	// SpaceEdgeIn is the in-edge index, derived from out-edges.
	SpaceEdgeIn Space = 0x05
	// SpaceAttrRow holds packed indexed fields, derived from record bodies.
	SpaceAttrRow Space = 0x06
	// SpaceAttrIndex is the ordered index over attribute slots, derived from
	// attribute rows. This is the space [PutFloat64] and friends exist for.
	SpaceAttrIndex Space = 0x07
	// SpaceText holds inverted-index postings, derived from record bodies.
	SpaceText Space = 0x08
	// SpaceVectorIndex holds HNSW node records, derived from canonical
	// vectors and the only asynchronously maintained index (plan §II.4).
	SpaceVectorIndex Space = 0x09
	// SpaceJob holds the canonical job queue.
	SpaceJob Space = 0x0A
	// SpaceEvent holds the canonical recall and lifecycle audit stream.
	SpaceEvent Space = 0x0B
	// SpaceSession holds MCP sessions. Canonical but expendable.
	SpaceSession Space = 0x0C
	// SpaceJobPause holds an operator's suppression of one recurring job type
	// in one tenant. Canonical: nothing derives it and nothing rebuilds it —
	// a pause that could be reconstructed from elsewhere would be a pause
	// somebody set that nothing recorded.
	SpaceJobPause Space = 0x0D
	// SpaceJobRun holds one row per execution attempt: what a job did, when,
	// and how many records it touched. Canonical for the same reason the job
	// row is — a lost attempt is an execution nothing can explain afterwards —
	// and bounded by jobs.retention rather than kept for ever.
	SpaceJobRun Space = 0x0E
)

var spaceNames = map[Space]string{
	SpaceSystem:      "system",
	SpaceRecord:      "record",
	SpaceVector:      "vector",
	SpaceEdgeOut:     "edge_out",
	SpaceEdgeIn:      "edge_in",
	SpaceAttrRow:     "attr_row",
	SpaceAttrIndex:   "attr_index",
	SpaceText:        "text",
	SpaceVectorIndex: "vector_index",
	SpaceJob:         "job",
	SpaceEvent:       "event",
	SpaceSession:     "session",
	SpaceJobPause:    "job_pause",
	SpaceJobRun:      "job_run",
}

// String returns the stable snake_case name used in logs, metric labels and
// `remem-admin inspect`. The names do not change once shipped.
func (s Space) String() string {
	if n, ok := spaceNames[s]; ok {
		return n
	}
	return "unknown"
}

// Class says whether a space holds canonical data or an index derived from it.
//
// It is a property of the space rather than of whichever package writes it,
// because Invariant 3 — every derived index has a tested rebuild path — is
// checked by walking [AllSpaces]. A space cannot be added without joining that
// walk, which is the point: a derived index nobody registered a rebuild for is
// found by the guard, not by an operator with a damaged index and no repair.
type Class uint8

const (
	// Canonical data is derived from nothing. Nothing rebuilds it, which is
	// what makes it canonical, and a snapshot carries it.
	Canonical Class = iota + 1
	// Derived data is a function of canonical data and can be deleted and
	// rebuilt from it without loss.
	Derived
)

func (c Class) String() string {
	switch c {
	case Canonical:
		return "canonical"
	case Derived:
		return "derived"
	}
	return "unknown"
}

// derivations names, for each derived space, the spaces its rows are computed
// from. A space absent from this map is canonical. Plan §II.4 is the source.
var derivations = map[Space][]Space{
	SpaceEdgeIn:      {SpaceEdgeOut},
	SpaceAttrRow:     {SpaceRecord},
	SpaceAttrIndex:   {SpaceAttrRow},
	SpaceText:        {SpaceRecord},
	SpaceVectorIndex: {SpaceVector},
}

// Class reports whether the space is canonical or derived. An undeclared space
// has no class.
func (s Space) Class() Class {
	if _, known := spaceNames[s]; !known {
		return 0
	}
	if _, derived := derivations[s]; derived {
		return Derived
	}
	return Canonical
}

// DerivedFrom returns the spaces a derived space is computed from, or nothing
// for a canonical one.
func (s Space) DerivedFrom() []Space {
	return append([]Space(nil), derivations[s]...)
}

// AllSpaces returns every space, in byte order. A new space must be added here
// as well as above, which is what TestSpacesDoNotCollide checks.
func AllSpaces() []Space {
	return []Space{
		SpaceSystem, SpaceRecord, SpaceVector, SpaceEdgeOut, SpaceEdgeIn,
		SpaceAttrRow, SpaceAttrIndex, SpaceText, SpaceVectorIndex,
		SpaceJob, SpaceEvent, SpaceSession, SpaceJobPause, SpaceJobRun,
	}
}

// RecordType distinguishes the kinds of record sharing the record space. It is
// one byte ahead of the id so that a scan can be confined to one kind.
type RecordType byte

const (
	// RecordMemory is a memory: the only record type the product has.
	RecordMemory RecordType = 0x01
)

func (r RecordType) String() string {
	if r == RecordMemory {
		return "memory"
	}
	return "unknown"
}

// RelType identifies a relationship kind. Two bytes, sitting between the two
// endpoints of an edge key so that a traversal can filter by relationship
// without decoding the far endpoint.
type RelType uint16

// JobState is the leading byte of a job key, so that the queue scan for
// pending work never walks running or completed rows.
type JobState byte

const (
	// JobPending is queued and due at the encoded time. It holds both of the
	// logical states that are waiting to run — pending and retry — because a
	// retry *is* a pending job with a later due time, and a fourth partition
	// would only make the claim scan read two ranges and merge them.
	JobPending JobState = 0x01
	// JobRunning is leased by a worker, and its encoded time is the lease
	// expiry, so reclaiming lapsed leases is the same bounded forward scan
	// that claiming due work is.
	JobRunning JobState = 0x02
	// JobDone is terminal — completed, failed or cancelled — retained for
	// audit until reaped, and encoded at the time it finished so that the
	// reaper walks the oldest first.
	JobDone JobState = 0x03
)

var jobStateNames = map[JobState]string{
	JobPending: "pending",
	JobRunning: "running",
	JobDone:    "done",
}

// String returns the stable name of the partition, used in logs and in the
// admin API. The names do not change once shipped.
func (s JobState) String() string {
	if n, ok := jobStateNames[s]; ok {
		return n
	}
	return "unknown"
}

// AllJobStates returns every partition, in byte order. A new one must be added
// here as well as above.
func AllJobStates() []JobState { return []JobState{JobPending, JobRunning, JobDone} }
