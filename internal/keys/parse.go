package keys

import (
	"bytes"
	"encoding/binary"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The allocation-free readers over the key layout, and the length-prefixed
// component decoder that every parser here is built on. They sit apart from the
// builders and the full parsers in keys.go only to keep that file under the
// repository's size limit; the layout they read is the one keys.go writes.

// TextRecordID is the record a posting key names, read without decoding the key.
//
// lower is the lower bound [TextTermRange] returned for the term being scanned,
// which is the prefix every one of that term's postings begins with: [Text]
// writes that prefix and then the record's sixteen raw bytes and nothing else,
// so the record id is the tail. TestAPostingKeyIsItsTermRangeFollowedByTheRecordID
// is what keeps the two spellings in step.
//
// It exists beside [ParseText] rather than instead of it, because the two answer
// different questions. ParseText decodes a key nobody has told it anything about
// — which is what a rebuild and an audit need, reading the space as a whole — and
// to do that it must return the tenant, the namespace and the term, which is
// three string allocations. A keyword search already knows all three: it built
// the range from them. Spending three allocations per posting to be told them
// again was 99% of everything a keyword search allocated, on the one path whose
// entire cost is postings, so the scan asks this instead and keeps its
// allocations to the postings it keeps.
//
// A key inside the range that is not one of the term's postings is
// [errs.Corruption]: it cannot happen without something writing into the text
// space that this package does not know about, and scoring it as a posting of
// this term would be worse than refusing.
func TextRecordID(key, lower []byte) (id.ID, error) {
	const op = "keys.TextRecordID"

	if len(key) != len(lower)+idLen || !bytes.Equal(key[:len(lower)], lower) {
		return id.Zero, corrupt(op, "a posting inside one term's range belongs to another term")
	}
	var rid id.ID
	copy(rid[:], key[len(lower):])
	return rid, nil
}

// TenantOf reports which tenant owns a key, reading only the scope prefix.
//
// It is the storage layer's question about a key it is about to write: whose
// other writes must this one be ordered against. Pebble's store answers it for
// every written key and every condition key of every batch to pick the write
// stripes it locks, so it must stay cheap — it decodes one length-prefixed
// component and allocates nothing.
//
// A system-space key (see [System]) is untenanted and answers with the empty
// id, which is the same answer its own layout gives.
//
// ok is false for a key whose scope prefix does not decode. That is
// [errs.Corruption] to a parser, but not to a caller asking about ordering: a
// key nothing in Remem can have written cannot be the key another writer's
// condition guards. So the miss is a flag rather than an error, and costs no
// allocation either.
func TenantOf(key []byte) (tenant.ID, bool) {
	t, _, fault := component(key)
	if fault != componentOK {
		return "", false
	}
	return tenant.ID(t), true
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
	val, rest, fault := component(k)
	switch fault {
	case componentNoLength:
		return "", nil, corrupt(op, "key ends before its "+what+" length")
	case componentNonMinimal:
		return "", nil, corrupt(op, "key has a non-minimal "+what+" length prefix")
	case componentTruncated:
		return "", nil, corrupt(op, "key ends inside its "+what)
	}
	return string(val), rest, nil
}

// componentFault says why a length-prefixed component did not decode.
//
// It is a code and not an error because a caller that only wants to know
// whether the component is there should not pay for the words explaining why it
// is not: building one allocates, and [TenantOf] is asked about every key of
// every write.
type componentFault uint8

const (
	componentOK componentFault = iota
	componentNoLength
	componentNonMinimal
	componentTruncated
)

// component decodes one length-prefixed component, returning it as a view into
// k together with the remainder. The view aliases k and lives as long as k
// does; [parseComponent] copies it, and nothing else may keep it.
func component(k []byte) (val, rest []byte, fault componentFault) {
	n, w := binary.Uvarint(k)
	if w <= 0 {
		return nil, nil, componentNoLength
	}
	// binary.Uvarint accepts padded encodings — 0x80 0x00 is zero — and the
	// encoder never writes one. Accepting it would admit a second byte string
	// for the same key, which a range built by the encoder does not cover: the
	// row would exist and no scan would find it. FuzzKeyDecoders found this.
	if w != uvarintLen(int(n)) {
		return nil, nil, componentNonMinimal
	}
	k = k[w:]
	if uint64(len(k)) < n {
		return nil, nil, componentTruncated
	}
	return k[:n], k[n:], componentOK
}
