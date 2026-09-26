package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot/pb"
	"github.com/remem-org/remem-go/internal/snapshot/pbext"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"google.golang.org/protobuf/proto"
)

// importer holds the state one import run accumulates.
//
// The page buffer is the only thing here proportional to anything, and it is
// bounded by the exporter's page size: a records block and the vectors block
// that follows it describe the same page, so the records wait exactly that long
// before they are written.
type importer struct {
	dst    Destination
	opts   ImportOpts
	header *pb.Header
	report Report

	// page is the records block being assembled, in the order it was read.
	page []*pendingRecord
	// byID indexes page, so a companion or a vector finds its record without
	// scanning.
	byID map[id.ID]*pendingRecord

	// pendingTenants is the tenants block being assembled, waiting for the
	// companion that carries each one's schema version.
	pendingTenants []tenant.Meta

	// tenants are the scopes seen, so the orphan sweep knows where to look.
	tenants map[tenant.ID]struct{}

	// The last position at which everything before is durably committed and
	// nothing is buffered: a legal place to restart. A records block and the
	// vectors block that follows it describe one page, so resuming between
	// them would skip the records and then meet their vectors with nothing to
	// attach them to — the resume points are the page boundaries, not the
	// blocks.
	resumeBlocks uint32
	resumeOffset int64
	resumeStats  Stats
}

// mark records a legal resume point, with the work done to reach it.
func (im *importer) mark(blocks uint32, offset int64) {
	im.resumeBlocks, im.resumeOffset, im.resumeStats = blocks, offset, im.report.Stats
}

// pendingRecord is one record waiting for the rest of its page.
type pendingRecord struct {
	rec *record.Record
	// vector is what the snapshot carried, before the vector policy is applied.
	vector *vector.Vector
	// hasVector distinguishes "the snapshot carried none" from "it carried an
	// empty one", which are different facts about the corpus.
	hasVector bool
}

func (im *importer) now() time.Time {
	if im.opts.Now != nil {
		return im.opts.Now()
	}
	return time.Now().UTC()
}

// idle reports whether the importer is between pages, with nothing buffered.
func (im *importer) idle() bool {
	return len(im.page) == 0 && len(im.pendingTenants) == 0
}

// flushAndMark commits whatever is buffered and records this block as the place
// to restart: everything before it is now durable, and this block begins the
// next page.
func (im *importer) flushAndMark(ctx context.Context, blk *Block) error {
	if err := im.flush(ctx); err != nil {
		return err
	}
	im.mark(blk.Index, blk.Offset)
	return nil
}

func (im *importer) note(t tenant.ID) {
	if im.tenants == nil {
		im.tenants = map[tenant.ID]struct{}{}
	}
	im.tenants[t] = struct{}{}
}

// block dispatches one block to the section that owns it.
func (im *importer) block(ctx context.Context, blk *Block) error {
	const op = "snapshot.Import"

	switch blk.Section {
	case SectionRecords:
		// A new records block starts a new page, so whatever is buffered
		// belongs to the previous one and is complete.
		if err := im.flushAndMark(ctx, blk); err != nil {
			return err
		}
		return im.readRecords(blk)
	case SectionGoRecordExt:
		return im.readRecordExts(blk)
	case SectionVectors:
		return im.readVectors(blk)
	case SectionTenants:
		if err := im.flushAndMark(ctx, blk); err != nil {
			return err
		}
		return im.readTenants(blk)
	case SectionGoTenantExt:
		return im.readTenantExts(blk)
	case SectionEdges:
		if err := im.flushAndMark(ctx, blk); err != nil {
			return err
		}
		return im.applyEdges(ctx, blk)
	case SectionGoEdgeExt:
		return im.applyEdgeExts(ctx, blk)
	case SectionGoEvents:
		if err := im.flushAndMark(ctx, blk); err != nil {
			return err
		}
		return im.applyEvents(ctx, blk)
	case SectionSchema, SectionEvents:
		// Declared in the shared schema and written by no released binary. A
		// file that carries one was written by something this build does not
		// understand, and skipping it would be the silent data loss spec §59
		// exists to forbid.
		return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"this snapshot carries a %s block, which no released Remem writes. It is refused "+
				"rather than skipped: a section this binary cannot interpret is data the import "+
				"would silently leave behind", blk.Section))
	default:
		_, err := ParseSection(byte(blk.Section), op)
		return err
	}
}

// readTenants buffers the tenants block, which its companion completes.
func (im *importer) readTenants(blk *Block) error {
	const op = "snapshot.Import"

	return blk.Each(func(raw []byte) error {
		var m pb.Tenant
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("decoding a tenant: %w", err))
		}
		t, err := tenant.Parse(m.GetId())
		if err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"the snapshot names tenant %q, which is not a tenant id: %w", m.GetId(), err))
		}
		im.note(t)
		created := fromUnixMs(m.GetCreatedAtUnixMs())
		if created.IsZero() {
			created = im.now()
		}
		im.report.Stats.Tenants++
		im.pendingTenants = append(im.pendingTenants, tenant.Meta{
			ID:            t,
			DisplayName:   m.GetDisplayName(),
			SchemaVersion: tenant.DefaultSchemaVersion,
			CreatedAt:     created,
		})
		return nil
	})
}

// readTenantExts carries the per-tenant schema version across.
//
// It is a companion rather than a field on the shared message because
// snapshot.v1's Tenant has none: Rust's tenants are implicit in the key
// encoding and are not registered entities at all, so there was nothing for the
// shared schema to carry.
func (im *importer) readTenantExts(blk *Block) error {
	const op = "snapshot.Import"

	byID := make(map[string]*tenant.Meta, len(im.pendingTenants))
	for i := range im.pendingTenants {
		byID[im.pendingTenants[i].ID.String()] = &im.pendingTenants[i]
	}
	return blk.Each(func(raw []byte) error {
		var m pbext.TenantExt
		if err := proto.Unmarshal(raw, &m); err != nil {
			return errs.E(errs.Corruption, op, fmt.Errorf("decoding a tenant extension: %w", err))
		}
		meta, ok := byID[m.GetId()]
		if !ok {
			return errs.E(errs.Corruption, op, fmt.Errorf(
				"a tenant extension names %q, which is not in the tenants block it annotates",
				m.GetId()))
		}
		if v := m.GetSchemaVersion(); v != 0 {
			meta.SchemaVersion = v
		}
		return nil
	})
}

// flushTenants registers the buffered tenants.
//
// A tenant that already exists keeps its own metadata rather than taking the
// snapshot's. Which memories it owns is what an import is for; its display name
// is not worth overwriting on the way past, and its schema version least of all
// — that one describes the *destination's* records, not the file's.
func (im *importer) flushTenants(ctx context.Context) error {
	if len(im.pendingTenants) == 0 {
		return nil
	}
	pending := im.pendingTenants
	im.pendingTenants = nil

	for _, meta := range pending {
		err := im.dst.Tenants.Create(ctx, meta.ID, meta)
		if errs.Is(err, errs.Conflict) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// applyEvents restores the lifecycle audit stream verbatim.
func (im *importer) applyEvents(ctx context.Context, blk *Block) error {
	const op = "snapshot.Import"

	if im.dst.Events == nil {
		return errs.E(errs.Invalid, op, errors.New(
			"this snapshot carries a lifecycle audit stream and this import has no event store to "+
				"put it in; it is refused rather than dropped"))
	}
	msgs, err := SplitMessages(blk)
	if err != nil {
		return err
	}
	batch := im.opts.batchSize()
	for start := 0; start < len(msgs); start += batch {
		end := min(start+batch, len(msgs))
		if err := im.writeEvents(ctx, msgs[start:end], op); err != nil {
			return err
		}
	}
	return nil
}

func (im *importer) writeEvents(ctx context.Context, msgs [][]byte, op string) error {
	return txn.Do(ctx, im.dst.KV, func(tx txn.Tx) error {
		for _, raw := range msgs {
			var m pbext.Event
			if err := proto.Unmarshal(raw, &m); err != nil {
				return errs.E(errs.Corruption, op, fmt.Errorf("decoding an event: %w", err))
			}
			subject, err := id.FromBytes(m.GetSubject())
			if err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			t, err := tenant.Parse(m.GetTenant())
			if err != nil {
				return errs.E(errs.Corruption, op, err)
			}
			im.note(t)
			if err := im.dst.Events.Restore(ctx, tx, events.Event{
				Tenant: t, Namespace: tenant.DefaultNamespace, Subject: subject,
				At: fromUnixMs(m.GetAtUnixMs()), Seq: m.GetSeq(),
				Kind: events.Kind(m.GetKind()), Actor: m.GetActor(), Reason: m.GetReason(),
				Before: m.GetBefore(), After: m.GetAfter(),
			}); err != nil {
				return err
			}
			im.report.Stats.Events++
		}
		return nil
	})
}

func (im *importer) reject(into *[]Rejected, r Rejected) {
	if len(*into) < MaxRejected {
		*into = append(*into, r)
	} else {
		im.report.Truncated = true
	}
}
