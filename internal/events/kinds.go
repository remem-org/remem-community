package events

// The event kinds: what can happen to a memory, named. Split from events.go in
// Phase 13 to keep that file under the project's size limit; nothing here
// changed.

// Kind is the durable name of a transition.
//
// A name is never reused for a different meaning and a retired kind stays
// retired, for the reason a job type does: it is written into every row of that
// kind and read back by tools outside this binary.
type Kind string

const (
	// Recalled is a memory addressed by id. Search does not produce one —
	// search discovers memories rather than addressing them (baseline §3) —
	// and neither does budgeted recall, which is a search by another name.
	Recalled Kind = "recalled"
	// Promoted is a memory that earned a longer-lived policy.
	Promoted Kind = "promoted"
	// Expired is a memory whose TTL elapsed without enough recalls to promote
	// it. It is followed by an archive, and the two are separate because "your
	// memory ran out of time" and "your memory was retired" are different
	// answers to the same question.
	Expired Kind = "expired"
	// Archived is a memory retired from retrieval. Its record survives.
	Archived Kind = "archived"
	// Restored is an archived memory brought back. Nothing in Phase 10 writes
	// one; it is declared because un-archiving is the inverse of a transition
	// that ships, and a stream that records only the losses is not an audit.
	Restored Kind = "restored"
	// Updated is a memory whose content or fields a caller changed. Its
	// Before/After name the fields that moved and never their values: storing
	// the previous content would double the corpus for every edited memory and
	// put memory content in a second durable place, and the field names answer
	// "why did this change" without either cost.
	Updated Kind = "updated"
	// HardDeleted is the record destroyed. It is the one row that survives the
	// deletion of its own subject's stream, so that "why did my memory
	// disappear" has an answer.
	HardDeleted Kind = "hard_deleted"
)

// kindNames is the declared set. A kind not in it is refused at write time.
//
// There is no `consolidated`: consolidation does not ship in Phase 10 (REM-113),
// and a declared kind nothing writes is the same defect as a declared attribute
// slot nothing populates — it looks like a capability and is an empty set.
var kindNames = map[Kind]bool{
	Recalled: true, Promoted: true, Expired: true,
	Archived: true, Restored: true, Updated: true, HardDeleted: true,
}

// AllKinds returns every kind, in the order above. A new one is added here as
// well as to the constants, which is what the table-driven transition test
// checks.
func AllKinds() []Kind {
	return []Kind{Recalled, Promoted, Expired, Archived, Restored, Updated, HardDeleted}
}

// Valid reports whether k is a kind this binary writes.
func (k Kind) Valid() bool { return kindNames[k] }

func (k Kind) String() string { return string(k) }
