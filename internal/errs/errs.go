// Package errs is Remem's error taxonomy.
//
// Spec §58 requires errors to preserve context, to distinguish the classes
// below, and — the part that matters most — forbids exposing Pebble or
// etcd/raft errors through a public API. Adapters translate; everything above
// them classifies with [Kind].
//
// Only internal/api/http turns a Kind into a status code. No package below the
// API layer knows that HTTP exists.
package errs

import (
	"errors"
	"fmt"
)

// Kind classifies an error. The zero value, Unclassified, means the error has
// not been through [E]; [KindOf] never returns it for a non-nil error.
type Kind uint8

const (
	// Unclassified is the zero value. It is a kind no error carries: an error
	// that has not been classified is reported as Storage, not as this.
	Unclassified Kind = iota

	// NotFound: the addressed record, tenant or key does not exist.
	NotFound
	// Conflict: a concurrent modification lost a write. Recovery means
	// re-reading and re-deciding, which only the caller can do, so this is not
	// retryable here.
	Conflict
	// Invalid: caller error — malformed input, a value outside its domain.
	Invalid
	// Unauthorized: no credential, or the credential did not verify.
	Unauthorized
	// Forbidden: authenticated, but not for this tenant or this operation.
	Forbidden
	// Storage: an adapter failed. Retryable; the data is intact.
	Storage
	// Corruption: canonical data is unreadable. Never retryable — retrying
	// cannot repair a byte. Derived indexes are rebuilt instead (Invariant 3).
	Corruption
	// Unavailable: shutting down, rebuilding, or shedding load.
	Unavailable
	// MigrationRequired: the on-disk format is older and must be migrated
	// forward before the operation can proceed.
	MigrationRequired
	// IncompatibleVersion: the on-disk or peer format is newer than this
	// binary understands. Downgrade is never automatic (spec §59).
	IncompatibleVersion
	// NotLeader: this node cannot serve the write; the caller retries against
	// the leader. Phase 15+.
	NotLeader
	// Transient is spec §58's "retryable distributed error": a consensus or
	// transport failure whose retry is safe. Phase 15+.
	//
	// The spec calls this kind "retryable"; it is named Transient here because
	// the package already exports [Retryable], the predicate that answers the
	// question for every kind.
	Transient
	// RateLimited: the caller has spent its request allowance. The identical
	// request succeeds later with no new decision, so it is retryable — by the
	// caller, after the wait the response names. Appended rather than placed
	// beside Unavailable, because a Kind's value is its position.
	RateLimited
)

var kindNames = [...]string{
	Unclassified:        "unclassified",
	NotFound:            "not_found",
	Conflict:            "conflict",
	Invalid:             "invalid",
	Unauthorized:        "unauthorized",
	Forbidden:           "forbidden",
	Storage:             "storage",
	Corruption:          "corruption",
	Unavailable:         "unavailable",
	MigrationRequired:   "migration_required",
	IncompatibleVersion: "incompatible_version",
	NotLeader:           "not_leader",
	Transient:           "transient",
	RateLimited:         "rate_limited",
}

// String returns the stable snake_case name of the kind. The names are used in
// logs and metric labels, so they do not change once shipped.
func (k Kind) String() string {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return kindNames[k]
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// retryableKinds is the whole of the retry policy. A kind is retryable only if
// repeating the identical operation could succeed without the caller making a
// new decision.
var retryableKinds = map[Kind]bool{
	Storage:     true,
	Unavailable: true,
	NotLeader:   true,
	Transient:   true,
	RateLimited: true,
}

// Error is a classified error carrying the operation that produced it.
type Error struct {
	Kind Kind
	Op   string // e.g. "record.Get", "pebble.Set"
	Err  error
}

func (e *Error) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Kind, e.Err)
}

// Unwrap exposes the cause so errors.Is and errors.As keep working through the
// classification.
func (e *Error) Unwrap() error { return e.Err }

// E classifies err as kind, recording the operation that produced it.
//
// E(kind, op, nil) is nil, so it composes with the usual `return E(...)` at the
// end of a function without a nil check at every call site.
func E(kind Kind, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Op: op, Err: err}
}

// KindOf reports the kind of err.
//
// It returns Unclassified for nil, the outermost classification for an error
// that has been through [E] (wrapped with %w any number of times), and Storage
// for anything else: an error that reached us unclassified came from an
// adapter or a dependency, which is an infrastructure failure until someone
// proves otherwise. It never panics.
func KindOf(err error) Kind {
	if err == nil {
		return Unclassified
	}
	var e *Error
	if errors.As(err, &e) && e.Kind != Unclassified {
		return e.Kind
	}
	return Storage
}

// Is reports whether err classifies as k.
func Is(err error, k Kind) bool {
	if err == nil {
		return false
	}
	return KindOf(err) == k
}

// Retryable reports whether repeating the identical operation could succeed.
// Corruption never qualifies; nor does any caller error.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	return retryableKinds[KindOf(err)]
}
