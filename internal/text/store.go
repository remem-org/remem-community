// Package text is Remem's inverted index: the keyword half of search, and the
// tag filter that used to be a special case.
//
// # Everything here is derived
//
// Content and tags are canonical, and they live in the record body. Every byte
// in the text key space — postings, per-record term rows, per-tenant corpus
// statistics — is derived from those bodies and is rebuilt rather than repaired
// (Invariant 3).
//
// Unlike the vector index it is maintained *inside the record's own
// transaction* (spec §12), through [record.Indexer]. That removes the drift
// class Phase 7 had to design against: a memory and its postings land together
// or not at all, so there is no window in which a stored memory is unfindable
// by a word it contains. The one way this index can be incomplete is an
// interrupted rebuild, which is what the rebuilding marker in [Stats] reports.
//
// # Cost is proportional to matches
//
// This is the whole point of the phase (plan §II.10 row 1). Rust scans every
// record on every keyword query; here a term is a key prefix, and answering a
// query reads the postings of the terms asked for and nothing else. The
// per-document numbers BM25 needs — term frequency and document length — are in
// the posting itself, so scoring a candidate costs no second read.
package text

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// --- keys -------------------------------------------------------------------

// postingKey addresses one posting: this term occurs in this record.
func postingKey(t tenant.ID, ns tenant.Namespace, term string, rid id.ID) []byte {
	return keys.Text(t, ns, term, rid)
}

// docKey addresses a record's own term row.
//
// It is what makes an update able to withdraw the postings the previous
// version wrote without reading the old record body — the same problem
// attr.Indexer solves by reading the old attribute row, and the same reason:
// an index entry that is never withdrawn is not detectably wrong, it just
// returns a memory for a word that memory no longer contains.
func docKey(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return keys.Text(t, ns, docTerm, rid)
}

// statsKey addresses a tenant's corpus statistics.
//
// The record id is zero because there is one such row per tenant and
// namespace. It uses the same key shape as every other row in the space so the
// space needs no second parser.
func statsKey(t tenant.ID, ns tenant.Namespace) []byte {
	return keys.Text(t, ns, statsTerm, id.Zero)
}

// --- postings ---------------------------------------------------------------

// posting is one term's presence in one record.
//
// Both numbers are what BM25 needs and neither can be recovered from the key,
// so both are in the value: term frequency is what the score rises with, and
// document length is what it is normalised by. Holding the length here rather
// than fetching the record is what keeps a keyword query a scan of the matches.
type posting struct {
	TermFreq uint32
	DocLen   uint32
}

func encodePosting(p posting) []byte {
	b := make([]byte, 0, binary.MaxVarintLen32*2)
	b = binary.AppendUvarint(b, uint64(p.TermFreq))
	return binary.AppendUvarint(b, uint64(p.DocLen))
}

func decodePosting(b []byte) (posting, error) {
	const op = "text.decodePosting"

	tf, n := binary.Uvarint(b)
	if n <= 0 {
		return posting{}, errs.E(errs.Corruption, op, errors.New("a posting carries no term frequency"))
	}
	length, m := binary.Uvarint(b[n:])
	if m <= 0 {
		return posting{}, errs.E(errs.Corruption, op, errors.New("a posting carries no document length"))
	}
	return posting{TermFreq: uint32(tf), DocLen: uint32(length)}, nil
}

// --- the per-record term row -------------------------------------------------

// docRow is every term one record contributed, and how long it is.
type docRow struct {
	DocLen uint32
	Terms  []string
}

// encodeDocRow writes a term row.
//
// The terms are sorted and de-duplicated first, so the bytes are a function of
// the term *set* rather than of the order the tokeniser happened to emit them
// in. That is what lets a rebuilt row be compared with an incrementally written
// one byte for byte, and it is Invariants 8 and 9 for the apply path Phase 14
// will replicate: two replicas deriving the same row must store the same bytes.
func encodeDocRow(r docRow) []byte {
	terms := slices.Clone(r.Terms)
	slices.Sort(terms)
	terms = slices.Compact(terms)

	size := binary.MaxVarintLen32 * 2
	for _, term := range terms {
		size += binary.MaxVarintLen32 + len(term)
	}
	b := make([]byte, 0, size)
	b = binary.AppendUvarint(b, uint64(r.DocLen))
	b = binary.AppendUvarint(b, uint64(len(terms)))
	for _, term := range terms {
		b = binary.AppendUvarint(b, uint64(len(term)))
		b = append(b, term...)
	}
	return b
}

func decodeDocRow(b []byte) (docRow, error) {
	const op = "text.decodeDocRow"

	bad := func(what string) (docRow, error) {
		return docRow{}, errs.E(errs.Corruption, op, errors.New("a record's term row "+what))
	}

	length, n := binary.Uvarint(b)
	if n <= 0 {
		return bad("carries no document length")
	}
	b = b[n:]
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return bad("carries no term count")
	}
	b = b[n:]

	out := docRow{DocLen: uint32(length), Terms: make([]string, 0, count)}
	for range count {
		size, n := binary.Uvarint(b)
		if n <= 0 {
			return bad("ends inside a term length")
		}
		b = b[n:]
		if uint64(len(b)) < size {
			return bad("ends inside a term")
		}
		out.Terms = append(out.Terms, string(b[:size]))
		b = b[size:]
	}
	return out, nil
}

// --- corpus statistics -------------------------------------------------------

// Stats is one tenant's corpus, as BM25 needs to see it.
//
// The two numbers are running totals moved by every write, held exactly: they
// are staged into the record's own transaction and conditionally committed, so
// no increment is lost. The alternative — a blind read-modify-write — cannot
// lose a memory, only an increment, but a document count that drifts low is an
// inverse document frequency that is wrong for every query from then on, with
// nothing reporting it and no repair an operator has a reason to run.
type Stats struct {
	// Documents is how many records this tenant has in the index.
	Documents uint64
	// TotalLength is the sum of their lengths in terms. Divided by Documents
	// it is BM25's avgdl, and it is stored as a sum because a running mean
	// cannot be updated by a delta without the count anyway.
	TotalLength uint64
	// Rebuilding reports that a rebuild started and has not finished. A search
	// served while it is set is incomplete, which is what a result's
	// `truncated` flag means.
	Rebuilding bool
}

// AvgDocLen is BM25's avgdl, and 1 for an empty corpus.
//
// The floor is not cosmetic: avgdl divides the length-normalisation term, and
// a zero there turns every score into an infinity that sorts arbitrarily.
func (s Stats) AvgDocLen() float64 {
	if s.Documents == 0 || s.TotalLength == 0 {
		return 1
	}
	return float64(s.TotalLength) / float64(s.Documents)
}

func encodeStats(s Stats) []byte {
	b := make([]byte, 0, binary.MaxVarintLen64*2+1)
	b = binary.AppendUvarint(b, s.Documents)
	b = binary.AppendUvarint(b, s.TotalLength)
	if s.Rebuilding {
		return append(b, 1)
	}
	return append(b, 0)
}

func decodeStats(b []byte) (Stats, error) {
	const op = "text.decodeStats"

	bad := func(what string) (Stats, error) {
		return Stats{}, errs.E(errs.Corruption, op, errors.New("the corpus statistics row "+what))
	}

	docs, n := binary.Uvarint(b)
	if n <= 0 {
		return bad("carries no document count")
	}
	b = b[n:]
	total, n := binary.Uvarint(b)
	if n <= 0 {
		return bad("carries no total length")
	}
	b = b[n:]
	if len(b) < 1 {
		return bad("carries no rebuild marker")
	}
	return Stats{Documents: docs, TotalLength: total, Rebuilding: b[0] == 1}, nil
}
