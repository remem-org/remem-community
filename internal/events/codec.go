package events

import (
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events/pb"
	"github.com/remem-org/remem-go/internal/keys"
	"google.golang.org/protobuf/proto"
)

// The event row is protobuf, unframed and not independently versioned.
//
// Unframed because the key's space byte already says what the value is; the
// record envelope exists so a body encoding can be replaced under a value
// several codecs share, and nothing else writes into the event space. Not
// independently versioned because protobuf's field-number evolution carries it
// forward — the footing the job row, the tenant directory and the session
// registry are already on.
//
// Deterministic marshalling, because the row holds two maps and Go's protobuf
// runtime randomises map field order unless told not to. Two nodes handed the
// same transition must write the same bytes (Invariants 8 and 9), which is what
// lets a replicated apply in Phase 14 be compared rather than merely trusted.
var deterministic = proto.MarshalOptions{Deterministic: true}

func marshal(e Event, op string) ([]byte, error) {
	row := &pb.Event{
		Kind:   string(e.Kind),
		Actor:  e.Actor,
		Reason: e.Reason,
		Before: e.Before,
		After:  e.After,
	}
	out, err := deterministic.Marshal(row)
	if err != nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("encoding the %s event: %w", e.Kind, err))
	}
	return out, nil
}

// unmarshal decodes a row read from key.
//
// A row that does not decode is [errs.Corruption], never a zero value. The
// stream is canonical: an event silently read as an empty one would report a
// memory as untouched at the exact moment the trail says otherwise.
//
// An *unknown kind* is not corruption. It is what a newer binary's event looks
// like to an older one during a rolling upgrade, and the honest thing is to
// return it verbatim so a history shows a transition it cannot name rather than
// hiding one it cannot recognise. Only [Store.Append] holds the declared set.
func unmarshal(key, value []byte) (Event, error) {
	const op = "events.unmarshal"

	t, ns, subject, ms, seq, err := keys.ParseEvent(key)
	if err != nil {
		return Event{}, err
	}

	var row pb.Event
	if err := proto.Unmarshal(value, &row); err != nil {
		return Event{}, errs.E(errs.Corruption, op, fmt.Errorf(
			"the event row at %x does not decode: %w", key, err))
	}

	return Event{
		Tenant:    t,
		Namespace: ns,
		Subject:   subject,
		At:        fromUnixMilli(ms),
		Seq:       seq,
		Kind:      Kind(row.GetKind()),
		Actor:     row.GetActor(),
		Reason:    row.GetReason(),
		Before:    row.GetBefore(),
		After:     row.GetAfter(),
	}, nil
}

func fromUnixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
