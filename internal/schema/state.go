package schema

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema/pb"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/txn"
	"google.golang.org/protobuf/proto"
)

// statePrefix is where migration state rows live, under the untenanted system
// space beside the format manifest and the tenant directory. A migration is
// about the whole directory, so its state has no tenant.
const statePrefix = "/migration/"

// State is where a migration has got to.
type State uint8

const (
	// StateUnspecified is the zero value and is never persisted.
	StateUnspecified State = iota
	// StatePending is planned but not started.
	StatePending
	// StateRunning is started and not finished. A row left in this state is
	// what a crash looks like, and it is what the runner resumes from.
	StateRunning
	// StateDone is terminal and successful.
	StateDone
	// StateFailed means the last attempt returned an error. It is retried on
	// the next open — a migration that gave up permanently after one bad disk
	// day would need an operator to know it existed.
	StateFailed
)

var stateNames = [...]string{
	StateUnspecified: "unspecified",
	StatePending:     "pending",
	StateRunning:     "running",
	StateDone:        "done",
	StateFailed:      "failed",
}

func (s State) String() string {
	if int(s) < len(stateNames) && stateNames[s] != "" {
		return stateNames[s]
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// MigrationState is the durable record of one migration (spec §20.3).
//
// It is what makes a migration resumable across a process restart, a node
// restart and — from Phase 16 — a cluster failover. The alternative is
// restarting a step from the beginning against a corpus it has already partly
// rewritten, which for a step that is not idempotent is the difference between
// correct and not.
type MigrationState struct {
	ID string

	// Source and Target are the subsystem versions the step moves between,
	// captured when it started. They are recorded rather than looked up so a
	// resumed run can tell it is resuming the same work: a binary upgraded
	// mid-migration would otherwise continue a step under new assumptions.
	Source map[string]uint32
	Target map[string]uint32

	State State

	// Cursor is where the last checkpoint left off, opaque to everything but
	// the step that wrote it. It is committed in the same transaction as the
	// work it describes, so it never points past unfinished work.
	Cursor    []byte
	Processed uint64

	StartedAt      time.Time
	LastProgressAt time.Time

	// Error is the failure that stopped the last attempt, kept so an operator
	// finds out why without the log line that carried it.
	Error string
}

// Equal reports whether two states record the same thing. Nil and empty maps
// and cursors compare equal, because protobuf does not distinguish them and a
// round trip must not appear to change the row.
func (s MigrationState) Equal(other MigrationState) bool {
	return s.ID == other.ID &&
		s.State == other.State &&
		s.Processed == other.Processed &&
		s.Error == other.Error &&
		s.StartedAt.Equal(other.StartedAt) &&
		s.LastProgressAt.Equal(other.LastProgressAt) &&
		string(s.Cursor) == string(other.Cursor) &&
		maps.Equal(nonEmpty(s.Source), nonEmpty(other.Source)) &&
		maps.Equal(nonEmpty(s.Target), nonEmpty(other.Target))
}

func nonEmpty(m map[string]uint32) map[string]uint32 {
	if m == nil {
		return map[string]uint32{}
	}
	return m
}

// StateKey is the storage key of one migration's state row.
//
// It is exported so a test can damage exactly that row and so `remem-admin`
// can name what it is inspecting.
func StateKey(id string) []byte { return keys.System(statePrefix + id) }

// StateRange is every migration state row, for a scan.
func StateRange() (lower, upper []byte) {
	return keys.PrefixRange(keys.System(statePrefix))
}

// ReadState reads one migration's state. The bool reports whether a row exists:
// a step that has never run has none, which is the first run rather than damage.
func ReadState(ctx context.Context, kv storage.KV, id string) (MigrationState, bool, error) {
	value, err := kv.Get(ctx, StateKey(id))
	if errs.Is(err, errs.NotFound) {
		return MigrationState{}, false, nil
	}
	if err != nil {
		return MigrationState{}, false, err
	}
	s, err := decodeState(id, value, "schema.ReadState")
	if err != nil {
		return MigrationState{}, false, err
	}
	return s, true, nil
}

// WriteState replaces a migration's state row.
//
// It is the out-of-band form, for the transitions the runner makes on its own
// behalf — started, failed, done. The cursor is never advanced through it; that
// is [StageState], and the difference is the whole reason resume is exact.
func WriteState(ctx context.Context, kv storage.KV, s MigrationState) error {
	value, err := encodeState(s, "schema.WriteState")
	if err != nil {
		return err
	}
	return kv.Set(ctx, StateKey(s.ID), value)
}

// StageState stages a migration's state into tx without committing it.
//
// This is the checkpoint path. Staging rather than writing is what puts the
// cursor in the same transaction as the work it describes, so there is no
// window in which the cursor claims a record is done and the record is not.
func StageState(tx txn.Tx, s MigrationState) error {
	value, err := encodeState(s, "schema.StageState")
	if err != nil {
		return err
	}
	tx.Set(StateKey(s.ID), value)
	return nil
}

// ListStates returns every migration state row, in id order. It is what
// `remem-admin inspect` reports and what the runner logs at start-up.
func ListStates(ctx context.Context, kv storage.KV) ([]MigrationState, error) {
	const op = "schema.ListStates"

	lower, upper := StateRange()
	it := kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	prefix := keys.System(statePrefix)
	var out []MigrationState
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) <= len(prefix) {
			return nil, errs.E(errs.Corruption, op, errors.New("a migration state row has no id"))
		}
		s, err := decodeState(string(k[len(prefix):]), it.Value(), op)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func encodeState(s MigrationState, op string) ([]byte, error) {
	if s.ID == "" {
		return nil, errs.E(errs.Invalid, op, errors.New("a migration state has no id"))
	}
	row := &pb.MigrationState{
		MigrationId:          s.ID,
		SourceFormat:         nonEmpty(s.Source),
		TargetFormat:         nonEmpty(s.Target),
		State:                stateToProto(s.State),
		Cursor:               s.Cursor,
		ProcessedCount:       s.Processed,
		StartedAtUnixMs:      unixMilli(s.StartedAt),
		LastProgressAtUnixMs: unixMilli(s.LastProgressAt),
		Error:                s.Error,
	}
	b, err := proto.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the state of migration %q: %w", s.ID, err))
	}
	return b, nil
}

// decodeState turns a stored value into a MigrationState, checking that it is
// the row that was asked for.
//
// The id in the body and the id in the key are two copies of one fact, and when
// they disagree something wrote a state row under the wrong key. Resuming from
// it would resume the wrong migration — from a cursor that means nothing to the
// step that reads it.
func decodeState(id string, value []byte, op string) (MigrationState, error) {
	var row pb.MigrationState
	if err := proto.Unmarshal(value, &row); err != nil {
		// Corruption, never an absent row: reading damage as "never started"
		// would restart a half-finished migration from the beginning, against a
		// corpus it has already partly rewritten.
		return MigrationState{}, errs.E(errs.Corruption, op,
			fmt.Errorf("the state row of migration %q will not parse", id))
	}
	if row.GetMigrationId() != id {
		return MigrationState{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"the state row stored under migration %q says it belongs to %q", id, row.GetMigrationId()))
	}
	return MigrationState{
		ID:             id,
		Source:         row.GetSourceFormat(),
		Target:         row.GetTargetFormat(),
		State:          stateFromProto(row.GetState()),
		Cursor:         row.GetCursor(),
		Processed:      row.GetProcessedCount(),
		StartedAt:      fromUnixMilli(row.GetStartedAtUnixMs()),
		LastProgressAt: fromUnixMilli(row.GetLastProgressAtUnixMs()),
		Error:          row.GetError(),
	}, nil
}

// unixMilli renders a time for storage. The zero time is stored as zero rather
// than as the epoch, so "never started" stays distinguishable from "started in
// 1970".
func unixMilli(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixMilli())
}

func fromUnixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}

func stateToProto(s State) pb.MigrationState_State {
	switch s {
	case StatePending:
		return pb.MigrationState_STATE_PENDING
	case StateRunning:
		return pb.MigrationState_STATE_RUNNING
	case StateDone:
		return pb.MigrationState_STATE_DONE
	case StateFailed:
		return pb.MigrationState_STATE_FAILED
	default:
		return pb.MigrationState_STATE_UNSPECIFIED
	}
}

func stateFromProto(s pb.MigrationState_State) State {
	switch s {
	case pb.MigrationState_STATE_PENDING:
		return StatePending
	case pb.MigrationState_STATE_RUNNING:
		return StateRunning
	case pb.MigrationState_STATE_DONE:
		return StateDone
	case pb.MigrationState_STATE_FAILED:
		return StateFailed
	default:
		return StateUnspecified
	}
}
