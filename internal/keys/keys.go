package keys

import (
	"encoding/binary"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// idLen is the width of every identifier in a key.
const idLen = 16

// --- builders ---------------------------------------------------------------
//
// Every builder sizes its slice exactly and appends into it once, so a key
// costs one allocation (spec §56). TestBuildersAllocateOnce holds that: it is
// easy to lose by adding a component and forgetting the size calculation, and
// keys are built on every write and every index entry of every write.

// Record addresses a canonical record body.
func Record(t tenant.ID, ns tenant.Namespace, typ RecordType, rid id.ID) []byte {
	k := begin(t, ns, SpaceRecord, 1+idLen)
	k = append(k, byte(typ))
	return append(k, rid[:]...)
}

// RecordChecked is [Record] with the scope validated.
//
// It is the form for any path that has not already proved its scope — an
// import, an admin command, a decoded cursor. Refusing an empty tenant here is
// Invariant 1 made structural: there is no way to spell a user-space key that
// belongs to nobody.
func RecordChecked(t tenant.ID, ns tenant.Namespace, typ RecordType, rid id.ID) ([]byte, error) {
	if err := checkScope(t, ns); err != nil {
		return nil, err
	}
	return Record(t, ns, typ, rid), nil
}

// Vector addresses a canonical embedding.
func Vector(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return append(begin(t, ns, SpaceVector, idLen), rid[:]...)
}

// EdgeOut addresses a canonical out-edge.
func EdgeOut(t tenant.ID, ns tenant.Namespace, from id.ID, rel RelType, to id.ID) []byte {
	k := begin(t, ns, SpaceEdgeOut, idLen+2+idLen)
	k = append(k, from[:]...)
	k = binary.BigEndian.AppendUint16(k, uint16(rel))
	return append(k, to[:]...)
}

// EdgeIn addresses the derived in-edge. The endpoints are reversed so that
// "what points at this record" is a prefix scan rather than a full walk.
func EdgeIn(t tenant.ID, ns tenant.Namespace, to id.ID, rel RelType, from id.ID) []byte {
	k := begin(t, ns, SpaceEdgeIn, idLen+2+idLen)
	k = append(k, to[:]...)
	k = binary.BigEndian.AppendUint16(k, uint16(rel))
	return append(k, from[:]...)
}

// AttrRow addresses a record's packed indexed fields.
func AttrRow(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return append(begin(t, ns, SpaceAttrRow, idLen), rid[:]...)
}

// AttrIndex addresses one entry in the ordered index over attribute slot.
//
// value must already be order-preserving — the output of [PutFloat64],
// [PutInt64] and friends — because the whole purpose of this space is that
// byte order is value order. The record id trails the value so that two
// records sharing a value stay distinct rows.
func AttrIndex(t tenant.ID, ns tenant.Namespace, slot uint16, value []byte, rid id.ID) []byte {
	k := begin(t, ns, SpaceAttrIndex, 2+len(value)+idLen)
	k = binary.BigEndian.AppendUint16(k, slot)
	k = append(k, value...)
	return append(k, rid[:]...)
}

// Text addresses one posting: this term occurs in this record.
//
// The term is length-prefixed so that a scan of "cat" cannot run into "cats".
func Text(t tenant.ID, ns tenant.Namespace, term string, rid id.ID) []byte {
	k := begin(t, ns, SpaceText, uvarintLen(len(term))+len(term)+idLen)
	k = binary.AppendUvarint(k, uint64(len(term)))
	k = append(k, term...)
	return append(k, rid[:]...)
}

// VectorIndex addresses one HNSW node record.
func VectorIndex(t tenant.ID, ns tenant.Namespace, node uint64) []byte {
	return binary.BigEndian.AppendUint64(begin(t, ns, SpaceVectorIndex, 8), node)
}

// Job addresses one job.
//
// State leads so that the scan for due work never walks running or completed
// rows; due time is big-endian so that byte order is time order.
func Job(t tenant.ID, ns tenant.Namespace, state JobState, dueUnixMs uint64, jid id.ID) []byte {
	k := begin(t, ns, SpaceJob, 1+8+idLen)
	k = append(k, byte(state))
	k = binary.BigEndian.AppendUint64(k, dueUnixMs)
	return append(k, jid[:]...)
}

// JobPause addresses an operator's suppression of one recurring job type.
//
// The type name is the whole remainder, so nothing follows it and it needs no
// length prefix: a pause is addressed by name and read back one at a time or
// scanned whole, never as the prefix of something longer.
func JobPause(t tenant.ID, ns tenant.Namespace, typ string) []byte {
	return append(begin(t, ns, SpaceJobPause, len(typ)), typ...)
}

// JobRun addresses one execution attempt's history row.
//
// The finish time leads so that the range is ordered by when each attempt
// ended: the reaper walks the oldest first and a history page reads the newest
// backwards, which are the only two things that read this space. The run id
// trails it so two attempts finishing in the same millisecond stay distinct
// rows rather than one overwriting the other.
func JobRun(t tenant.ID, ns tenant.Namespace, finishedUnixMs uint64, runID id.ID) []byte {
	k := begin(t, ns, SpaceJobRun, 8+idLen)
	k = binary.BigEndian.AppendUint64(k, finishedUnixMs)
	return append(k, runID[:]...)
}

// Event addresses one entry in the audit stream for a subject.
//
// seq breaks ties within a millisecond, so two events recorded in the same
// millisecond keep the order they happened in rather than one overwriting the
// other.
func Event(t tenant.ID, ns tenant.Namespace, subject id.ID, tsUnixMs uint64, seq uint32) []byte {
	k := begin(t, ns, SpaceEvent, idLen+8+4)
	k = append(k, subject[:]...)
	k = binary.BigEndian.AppendUint64(k, tsUnixMs)
	return binary.BigEndian.AppendUint32(k, seq)
}

// Session addresses one MCP session.
func Session(t tenant.ID, ns tenant.Namespace, sid id.ID) []byte {
	return append(begin(t, ns, SpaceSession, idLen), sid[:]...)
}

// System addresses an untenanted system row: "/format/<name>",
// "/tenants/<id>", "/migration/<id>".
//
// It is the only builder that takes no tenant, and it uses the same layout as
// every other key with both identifiers empty — so it needs no special case in
// the parser, and system rows sort ahead of all user data.
func System(path string) []byte {
	k := begin("", "", SpaceSystem, len(path))
	return append(k, path...)
}

// --- ranges -----------------------------------------------------------------

// TenantRange is every key one tenant owns, across every namespace and every
// space, as one half-open range.
func TenantRange(t tenant.ID) (lower, upper []byte) {
	p := make([]byte, 0, uvarintLen(len(t))+len(t))
	p = binary.AppendUvarint(p, uint64(len(t)))
	p = append(p, t...)
	return PrefixRange(p)
}

// NamespaceRange is every key one namespace of one tenant owns.
func NamespaceRange(t tenant.ID, ns tenant.Namespace) (lower, upper []byte) {
	return PrefixRange(scopePrefix(t, ns, 0))
}

// SpaceRange is every key of one space, within one namespace of one tenant.
// This is the workhorse: nearly every scan in Remem is one of these.
func SpaceRange(t tenant.ID, ns tenant.Namespace, s Space) (lower, upper []byte) {
	return PrefixRange(append(scopePrefix(t, ns, 1), byte(s)))
}

// SystemRange is the whole system space.
func SystemRange() (lower, upper []byte) {
	return SpaceRange("", "", SpaceSystem)
}

// TextTermRange is every posting for one term: the scan behind a keyword
// lookup.
func TextTermRange(t tenant.ID, ns tenant.Namespace, term string) (lower, upper []byte) {
	p := begin(t, ns, SpaceText, uvarintLen(len(term))+len(term))
	p = binary.AppendUvarint(p, uint64(len(term)))
	return PrefixRange(append(p, term...))
}

// AttrSlotRange is every entry of one attribute slot, in value order.
func AttrSlotRange(t tenant.ID, ns tenant.Namespace, slot uint16) (lower, upper []byte) {
	return PrefixRange(binary.BigEndian.AppendUint16(begin(t, ns, SpaceAttrIndex, 2), slot))
}

// EdgesFromRange is every out-edge of one record.
func EdgesFromRange(t tenant.ID, ns tenant.Namespace, from id.ID) (lower, upper []byte) {
	return PrefixRange(append(begin(t, ns, SpaceEdgeOut, idLen), from[:]...))
}

// EdgesToRange is every in-edge of one record: "what points at this".
func EdgesToRange(t tenant.ID, ns tenant.Namespace, to id.ID) (lower, upper []byte) {
	return PrefixRange(append(begin(t, ns, SpaceEdgeIn, idLen), to[:]...))
}

// JobStateRange is every job in one partition of one tenant, in due order.
//
// It is the range the reaper walks and the range an administrative lookup by id
// falls back to. Claiming and reclaiming use [JobDueRange] instead, because
// both of them want the head of this range and not the whole of it.
func JobStateRange(t tenant.ID, ns tenant.Namespace, state JobState) (lower, upper []byte) {
	return PrefixRange(append(begin(t, ns, SpaceJob, 1), byte(state)))
}

// JobDueRange is every job in one partition due at or before dueUnixMs.
//
// This is the scan behind claiming ("pending and due now"), behind lease
// reclamation ("running and expired") and behind reaping ("finished before the
// retention"). Each is a forward scan that stops at the first row it does not
// want, which is the whole reason the partition byte leads the key and the due
// time follows it.
//
// The upper bound is exclusive and is built at dueUnixMs+1 with a zero id, so
// every job due *at* the bound is included however large its id — an upper
// bound built at the bound itself would silently drop half of them.
func JobDueRange(t tenant.ID, ns tenant.Namespace, state JobState, dueUnixMs uint64) (lower, upper []byte) {
	lower, _ = JobStateRange(t, ns, state)
	if dueUnixMs == ^uint64(0) {
		_, upper = JobStateRange(t, ns, state)
		return lower, upper
	}
	return lower, Job(t, ns, state, dueUnixMs+1, id.Zero)
}

// JobPauseRange is every pause one tenant holds.
func JobPauseRange(t tenant.ID, ns tenant.Namespace) (lower, upper []byte) {
	return SpaceRange(t, ns, SpaceJobPause)
}

// JobRunRange is every retained attempt of one tenant, oldest first.
func JobRunRange(t tenant.ID, ns tenant.Namespace) (lower, upper []byte) {
	return SpaceRange(t, ns, SpaceJobRun)
}

// JobRunBeforeRange is every retained attempt that finished at or before
// finishedUnixMs: the reaper's scan, bounded the way [JobDueRange] is and for
// the same reason.
func JobRunBeforeRange(t tenant.ID, ns tenant.Namespace, finishedUnixMs uint64) (lower, upper []byte) {
	lower, upper = JobRunRange(t, ns)
	if finishedUnixMs == ^uint64(0) {
		return lower, upper
	}
	return lower, JobRun(t, ns, finishedUnixMs+1, id.Zero)
}

// EventsForRange is every audit entry about one subject, oldest first.
func EventsForRange(t tenant.ID, ns tenant.Namespace, subject id.ID) (lower, upper []byte) {
	return PrefixRange(append(begin(t, ns, SpaceEvent, idLen), subject[:]...))
}

// PrefixRange converts a prefix into the half-open range that covers it.
//
// The upper bound is the prefix with its last non-0xFF byte incremented. A
// prefix that is all 0xFF has no successor, and the range then runs to the end
// of the keyspace — a nil upper, which storage.KV reads as unbounded.
func PrefixRange(prefix []byte) (lower, upper []byte) {
	lower = prefix
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		upper = make([]byte, i+1)
		copy(upper, prefix[:i+1])
		upper[i]++
		return lower, upper
	}
	return lower, nil
}

// --- parsing ----------------------------------------------------------------

// ParseRecord decodes a record key.
//
// A key that does not decode is [errs.Corruption], not a parse miss: these
// bytes came from the store, and a key the store holds that this binary cannot
// read means something wrote it that should not have. Returning a zero value
// instead would put a record under the wrong tenant.
func ParseRecord(k []byte) (tenant.ID, tenant.Namespace, RecordType, id.ID, error) {
	const op = "keys.ParseRecord"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", 0, id.Zero, err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceRecord {
		return "", "", 0, id.Zero, corrupt(op, "key is not in the record space")
	}
	rest = rest[1:]
	if len(rest) != 1+idLen {
		return "", "", 0, id.Zero, corrupt(op, fmt.Sprintf("record key body is %d bytes, want %d", len(rest), 1+idLen))
	}
	rid, err := id.FromBytes(rest[1:])
	if err != nil {
		return "", "", 0, id.Zero, corrupt(op, "record key holds a malformed id")
	}
	return t, ns, RecordType(rest[0]), rid, nil
}

// ParseText decodes a posting key back into its term and record.
//
// It exists because a rebuild and an audit both have to read the space as a
// whole rather than term by term: "does any term still list this record" and
// "are these postings the ones this record's content produces" are questions
// about every row, and neither can be asked through TextTermRange.
func ParseText(k []byte) (tenant.ID, tenant.Namespace, string, id.ID, error) {
	const op = "keys.ParseText"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", "", id.Zero, err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceText {
		return "", "", "", id.Zero, corrupt(op, "key is not in the text space")
	}
	term, rest, err := parseComponent(rest[1:], op, "term")
	if err != nil {
		return "", "", "", id.Zero, err
	}
	if len(rest) != idLen {
		return "", "", "", id.Zero, corrupt(op, fmt.Sprintf("text key ends with %d bytes, want a %d-byte record id", len(rest), idLen))
	}
	rid, err := id.FromBytes(rest)
	if err != nil {
		return "", "", "", id.Zero, corrupt(op, "text key holds a malformed record id")
	}
	return t, ns, term, rid, nil
}

// ParseJob decodes a job key back into its partition, due time and job id.
//
// The queue scans partitions rather than reading rows by name, so this is how
// every claim, reclamation and reap learns which job it is looking at. The id
// and the partition live only in the key: a second copy inside the value would
// be a second thing that can disagree with the first.
func ParseJob(k []byte) (tenant.ID, tenant.Namespace, JobState, uint64, id.ID, error) {
	const op = "keys.ParseJob"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", 0, 0, id.Zero, err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceJob {
		return "", "", 0, 0, id.Zero, corrupt(op, "key is not in the job space")
	}
	rest = rest[1:]
	if len(rest) != 1+8+idLen {
		return "", "", 0, 0, id.Zero, corrupt(op, fmt.Sprintf(
			"job key body is %d bytes, want %d", len(rest), 1+8+idLen))
	}
	state := JobState(rest[0])
	if _, known := jobStateNames[state]; !known {
		return "", "", 0, 0, id.Zero, corrupt(op, fmt.Sprintf("unknown job state %#x", byte(state)))
	}
	jid, err := id.FromBytes(rest[9:])
	if err != nil {
		return "", "", 0, 0, id.Zero, corrupt(op, "job key holds a malformed id")
	}
	return t, ns, state, binary.BigEndian.Uint64(rest[1:9]), jid, nil
}

// ParseJobPause decodes a pause key back into the type it suppresses.
//
// The space is scanned as a whole to answer "what is paused in this tenant",
// so the type name is read from the key rather than trusted from the value —
// the key is the address and a second copy inside the value would be a second
// thing that can disagree with it.
func ParseJobPause(k []byte) (tenant.ID, tenant.Namespace, string, error) {
	const op = "keys.ParseJobPause"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", "", err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceJobPause {
		return "", "", "", corrupt(op, "key is not in the job pause space")
	}
	rest = rest[1:]
	if len(rest) == 0 {
		return "", "", "", corrupt(op, "job pause key names no type")
	}
	return t, ns, string(rest), nil
}

// ParseJobRun decodes a history key back into its finish time and run id.
func ParseJobRun(k []byte) (tenant.ID, tenant.Namespace, uint64, id.ID, error) {
	const op = "keys.ParseJobRun"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", 0, id.Zero, err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceJobRun {
		return "", "", 0, id.Zero, corrupt(op, "key is not in the job run space")
	}
	rest = rest[1:]
	if len(rest) != 8+idLen {
		return "", "", 0, id.Zero, corrupt(op, fmt.Sprintf(
			"job run key body is %d bytes, want %d", len(rest), 8+idLen))
	}
	rid, err := id.FromBytes(rest[8:])
	if err != nil {
		return "", "", 0, id.Zero, corrupt(op, "job run key holds a malformed id")
	}
	return t, ns, binary.BigEndian.Uint64(rest[:8]), rid, nil
}

// ParseEvent decodes an audit key back into its subject, instant and sequence.
//
// The stream is walked rather than read row by row — a history is a prefix
// scan, a trim is a bounded forward scan, and a fold counts what is newer than
// a watermark — so every reader learns which event it is holding from the key.
// The subject, the instant and the sequence live only there: a second copy
// inside the value would be a second thing that can disagree with the first.
func ParseEvent(k []byte) (tenant.ID, tenant.Namespace, id.ID, uint64, uint32, error) {
	const op = "keys.ParseEvent"

	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", id.Zero, 0, 0, err
	}
	if len(rest) < 1 || Space(rest[0]) != SpaceEvent {
		return "", "", id.Zero, 0, 0, corrupt(op, "key is not in the event space")
	}
	rest = rest[1:]
	if len(rest) != idLen+8+4 {
		return "", "", id.Zero, 0, 0, corrupt(op, fmt.Sprintf(
			"event key body is %d bytes, want %d", len(rest), idLen+8+4))
	}
	subject, err := id.FromBytes(rest[:idLen])
	if err != nil {
		return "", "", id.Zero, 0, 0, corrupt(op, "event key holds a malformed subject id")
	}
	return t, ns, subject,
		binary.BigEndian.Uint64(rest[idLen : idLen+8]),
		binary.BigEndian.Uint32(rest[idLen+8:]), nil
}

// ParseSpace reports which space a key belongs to, without decoding its body.
// It is what a scan uses to assert it has not wandered out of its range.
func ParseSpace(k []byte) (tenant.ID, tenant.Namespace, Space, error) {
	const op = "keys.ParseSpace"
	t, ns, rest, err := parseScope(k, op)
	if err != nil {
		return "", "", 0, err
	}
	if len(rest) < 1 {
		return "", "", 0, corrupt(op, "key ends before its space byte")
	}
	s := Space(rest[0])
	if _, known := spaceNames[s]; !known {
		return "", "", 0, corrupt(op, fmt.Sprintf("unknown space byte %#x", byte(s)))
	}
	return t, ns, s, nil
}

// parseScope decodes the tenant and namespace prefix, returning the remainder.
func parseScope(k []byte, op string) (tenant.ID, tenant.Namespace, []byte, error) {
	t, rest, err := parseComponent(k, op, "tenant")
	if err != nil {
		return "", "", nil, err
	}
	ns, rest, err := parseComponent(rest, op, "namespace")
	if err != nil {
		return "", "", nil, err
	}
	return tenant.ID(t), tenant.Namespace(ns), rest, nil
}

func parseComponent(k []byte, op, what string) (string, []byte, error) {
	n, w := binary.Uvarint(k)
	if w <= 0 {
		return "", nil, corrupt(op, "key ends before its "+what+" length")
	}
	// binary.Uvarint accepts padded encodings — 0x80 0x00 is zero — and the
	// encoder never writes one. Accepting it would admit a second byte string
	// for the same key, which a range built by the encoder does not cover: the
	// row would exist and no scan would find it. FuzzKeyDecoders found this.
	if w != uvarintLen(int(n)) {
		return "", nil, corrupt(op, "key has a non-minimal "+what+" length prefix")
	}
	k = k[w:]
	if uint64(len(k)) < n {
		return "", nil, corrupt(op, "key ends inside its "+what)
	}
	return string(k[:n]), k[n:], nil
}

// --- internals --------------------------------------------------------------

// begin writes the scope prefix and the space byte into a slice sized for the
// whole key, so that appending the remainder never reallocates.
func begin(t tenant.ID, ns tenant.Namespace, s Space, remainder int) []byte {
	k := scopePrefix(t, ns, 1+remainder)
	return append(k, byte(s))
}

// scopePrefix writes <tlen><tenant><nslen><namespace> into a slice with room
// for extra more bytes.
func scopePrefix(t tenant.ID, ns tenant.Namespace, extra int) []byte {
	n := uvarintLen(len(t)) + len(t) + uvarintLen(len(ns)) + len(ns)
	k := make([]byte, 0, n+extra)
	k = binary.AppendUvarint(k, uint64(len(t)))
	k = append(k, t...)
	k = binary.AppendUvarint(k, uint64(len(ns)))
	return append(k, ns...)
}

// uvarintLen is the encoded width of n as a uvarint. Identifiers are capped at
// tenant.MaxIDLen, so this is 1 for every value in practice and 2 at the
// boundary; it is computed rather than assumed because the size calculation
// above has to be exact.
func uvarintLen(n int) int {
	w := 1
	for v := uint64(n); v >= 0x80; v >>= 7 {
		w++
	}
	return w
}

func checkScope(t tenant.ID, ns tenant.Namespace) error {
	if t == "" {
		return errs.E(errs.Invalid, "keys.checkScope",
			fmt.Errorf("a user-space key requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if !t.Valid() {
		return errs.E(errs.Invalid, "keys.checkScope", fmt.Errorf("tenant id %q is malformed", t))
	}
	if ns == "" {
		return errs.E(errs.Invalid, "keys.checkScope",
			fmt.Errorf("a user-space key requires a namespace; use tenant.DefaultNamespace"))
	}
	if !ns.Valid() {
		return errs.E(errs.Invalid, "keys.checkScope", fmt.Errorf("namespace %q is malformed", ns))
	}
	return nil
}

func corrupt(op, msg string) error {
	return errs.E(errs.Corruption, op, fmt.Errorf("%s", msg))
}
