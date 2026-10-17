package vector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// A memory's index entries are its passage rows when it has any, and otherwise
// its canonical vector, as passage 0 (openspec/changes/passage-vectors, design
// decision 4). A multi-passage memory's canonical vector is its document
// vector — the mean its passages already cover — and is deliberately not an
// entry: indexing it would let a vague whole-memory match outrank the specific
// passage that holds the answer.
//
// This file owns that rule and both key spaces it reads. The record indexer
// stages passage rows through [StagePassages], and every index reads entries
// through [Store.ScanEntries], so there is one definition of what an index
// holds and one writer of each row.

// StagePassages makes rows the complete passage set of rid in tx: rows are
// written as passages 0…n-1, and any stored passage numbered n or above is
// deleted. An empty set leaves the memory with none.
//
// A memory's passages are always the whole set, numbered with no gaps, which is
// what lets a transaction with no range delete remove the old ones: it deletes
// from the first number the new set does not use until the first that is not
// stored.
func StagePassages(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, rows []PassageRow) error {
	for i, row := range rows {
		value, err := EncodePassage(row)
		if err != nil {
			return err
		}
		tx.Set(keys.Passage(t, ns, rid, uint32(i)), value)
	}
	return deletePassagesFrom(tx, t, ns, rid, uint32(len(rows)))
}

// StageDeletePassages deletes every passage row of rid in tx.
func StageDeletePassages(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) error {
	return deletePassagesFrom(tx, t, ns, rid, 0)
}

func deletePassagesFrom(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, from uint32) error {
	for p := from; ; p++ {
		k := keys.Passage(t, ns, rid, p)
		if _, err := tx.Get(k); err != nil {
			if errs.Is(err, errs.NotFound) {
				return nil
			}
			return err
		}
		tx.Delete(k)
	}
}

// ScanEntries walks every index entry of a tenant's default namespace, in key
// order, with the model identity attached.
//
// It is a merge of the canonical vectors and the passage rows, both of which
// sort by record id. A memory with passage rows yields them and not its
// canonical vector; one without yields its canonical vector as passage 0. A
// passage row whose memory has no canonical vector is no entry at all: a hard
// delete removes the canonical vector in the record's own transaction, and that
// is what makes a memory unfindable at once, whatever else was left behind.
//
// The Vector handed to fn is borrowed from the iterator. A callback that keeps
// one calls Clone.
func (s *Store) ScanEntries(ctx context.Context, t tenant.ID, fn func(Key, *Vector) error) error {
	const op = "vector.Store.ScanEntries"
	if t == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a vector scan requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	ns := tenant.DefaultNamespace

	plo, phi := keys.SpaceRange(t, ns, keys.SpacePassage)
	pit := s.kv.NewIterator(plo, phi)
	defer func() { _ = pit.Close() }()
	pok := pit.First()

	// passageOf decodes the passage iterator's current key.
	passageOf := func() (id.ID, uint32, error) {
		_, _, rid, p, err := keys.ParsePassage(pit.Key())
		return rid, p, err
	}

	err := s.scan(ctx, t, ns, func(rid id.ID, canonical *Vector) error {
		// Skip passage rows of records before this one: their canonical vector
		// is gone, so they are not entries.
		for pok {
			prid, _, err := passageOf()
			if err != nil {
				return err
			}
			if bytes.Compare(prid[:], rid[:]) >= 0 {
				break
			}
			pok = pit.Next()
		}
		yielded := false
		for pok {
			prid, p, err := passageOf()
			if err != nil {
				return err
			}
			if prid != rid {
				break
			}
			row, err := DecodePassage(pit.Value())
			if err != nil {
				return errs.E(errs.KindOf(err), op, fmt.Errorf("passage %d of record %s: %w", p, rid, err))
			}
			if err := fn(Key{ID: rid, Passage: p}, row.Vector); err != nil {
				return err
			}
			yielded = true
			pok = pit.Next()
		}
		if yielded {
			return nil
		}
		return fn(Key{ID: rid}, canonical)
	})
	if err != nil {
		return err
	}
	return pit.Error()
}

// Scan walks every index entry of a tenant's default namespace, in key order,
// as plain coordinates. It is how a Store serves as a [Source].
//
// The values handed to fn are borrowed from the iterator and are valid only for
// the duration of the call. A callback that keeps one copies it.
func (s *Store) Scan(ctx context.Context, t tenant.ID, fn func(Key, []float32) error) error {
	return s.ScanEntries(ctx, t, func(k Key, v *Vector) error { return fn(k, v.Values) })
}

// PutEntries writes rid's entry set as canonical rows, outside any record
// transaction: the canonical vector, and passage rows when there is more than
// one entry. It is the path [Index.Insert] takes; a memory written through the
// record repository commits its rows with its record instead.
//
// A multi-passage memory written this way has no document vector to give, so
// its canonical vector is the normalised mean of its entries: a stand-in, never
// produced by the production write path, and never an index entry.
func (s *Store) PutEntries(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID,
	modelID string, entries []Entry) error {
	tx := txn.New(s.kv)
	defer tx.Close()
	if err := s.stageEntries(tx, t, ns, rid, modelID, entries, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeleteEntries removes rid's canonical vector and every passage row, outside
// any record transaction.
func (s *Store) DeleteEntries(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID) error {
	tx := txn.New(s.kv)
	defer tx.Close()
	s.StageDelete(tx, t, ns, rid)
	if err := StageDeletePassages(tx, t, ns, rid); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReplaceEntries atomically makes entries the complete entry set of one tenant
// and namespace: memories absent from it lose their rows, and every memory in
// it gets the set given. No reader observes any of it until the whole
// transaction commits.
//
// A rebuild reads through [Source], which yields coordinates and nothing else,
// so what a source cannot know is inherited from what is stored rather than
// erased — the rule [Store.Replace] states for model ids, extended to the two
// other things a source does not carry. A passage keeps its stored span, and a
// multi-passage memory keeps its stored canonical vector, which is its
// document vector and no function of its passages. Without that, an index
// rebuilding itself from its own store would wipe every span.
//
// modelID stamps every row written when the caller knows the model, and is
// left empty to inherit the stored one when it does not.
func (s *Store) ReplaceEntries(ctx context.Context, t tenant.ID, ns tenant.Namespace,
	modelID string, entries map[id.ID][]Entry) error {
	var existing []id.ID
	if err := s.scan(ctx, t, ns, func(rid id.ID, _ *Vector) error {
		existing = append(existing, rid)
		return nil
	}); err != nil {
		return err
	}

	tx := txn.New(s.kv)
	defer tx.Close()
	for _, rid := range existing {
		if _, kept := entries[rid]; kept {
			continue
		}
		s.StageDelete(tx, t, ns, rid)
		if err := StageDeletePassages(tx, t, ns, rid); err != nil {
			return err
		}
	}
	for rid, set := range entries {
		if err := s.stageEntries(tx, t, ns, rid, modelID, set, false); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// stageEntries stages rid's entry set. modelID empty inherits the stored one;
// overwriteCanonical decides whether a multi-passage memory's canonical vector
// is replaced by the mean of its entries or kept.
func (s *Store) stageEntries(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID,
	modelID string, entries []Entry, overwriteCanonical bool) error {
	const op = "vector.Store.stageEntries"
	if err := CheckEntries(entries); err != nil {
		return errs.E(errs.Invalid, op, fmt.Errorf("record %s: %w", rid, err))
	}

	canonical, err := s.read(tx, CanonicalKey(t, ns, rid))
	if err != nil {
		return err
	}
	inherit := func(stored *Vector) string {
		switch {
		case modelID != "":
			return modelID
		case stored != nil && stored.ModelID != "":
			return stored.ModelID
		case canonical != nil && canonical.ModelID != "":
			return canonical.ModelID
		}
		return UnknownModel
	}

	if len(entries) == 1 {
		v := entries[0].Vector
		if err := s.Stage(tx, t, ns, rid, &Vector{ModelID: inherit(canonical), Dim: len(v), Values: v}); err != nil {
			return err
		}
		return StageDeletePassages(tx, t, ns, rid)
	}

	rows := make([]PassageRow, len(entries))
	for i, e := range entries {
		var stored *PassageRow
		if b, err := tx.Get(keys.Passage(t, ns, rid, e.Passage)); err == nil {
			row, err := DecodePassage(b)
			if err != nil {
				return err
			}
			stored = &row
		} else if !errs.Is(err, errs.NotFound) {
			return err
		}
		row := PassageRow{Vector: &Vector{Dim: len(e.Vector), Values: e.Vector}}
		var storedVector *Vector
		if stored != nil {
			row.Start, row.End = stored.Start, stored.End
			storedVector = stored.Vector
		}
		row.Vector.ModelID = inherit(storedVector)
		rows[i] = row
	}
	if err := StagePassages(tx, t, ns, rid, rows); err != nil {
		return err
	}
	if canonical == nil || overwriteCanonical {
		mean := meanOf(entries)
		return s.Stage(tx, t, ns, rid, &Vector{ModelID: inherit(canonical), Dim: len(mean), Values: mean})
	}
	return nil
}

// read returns the stored vector at key through tx, or nil.
func (s *Store) read(tx txn.Tx, key []byte) (*Vector, error) {
	b, err := tx.Get(key)
	if errs.Is(err, errs.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return Decode(b)
}

// CheckEntries holds an entry set to its shape: at least one entry, numbered
// 0…n-1 in order, none empty, all one width.
func CheckEntries(entries []Entry) error {
	if len(entries) == 0 {
		return errors.New("an entry set needs at least one entry")
	}
	for i, e := range entries {
		if e.Passage != uint32(i) {
			return fmt.Errorf("entry %d is numbered %d; a memory's entries are numbered 0…n-1 in order", i, e.Passage)
		}
		if len(e.Vector) == 0 {
			return fmt.Errorf("entry %d is an empty vector, which cannot be indexed", i)
		}
		if len(e.Vector) != len(entries[0].Vector) {
			return fmt.Errorf("entry %d is %d wide and entry 0 is %d", i, len(e.Vector), len(entries[0].Vector))
		}
	}
	return nil
}

// meanOf is the normalised mean of a set's vectors.
func meanOf(entries []Entry) []float32 {
	out := make([]float32, len(entries[0].Vector))
	for _, e := range entries {
		for i, x := range e.Vector {
			out[i] += x
		}
	}
	var sum float64
	for _, x := range out {
		sum += float64(x) * float64(x)
	}
	if sum > 0 {
		inv := float32(1 / math.Sqrt(sum))
		for i := range out {
			out[i] *= inv
		}
	}
	return out
}
