// Package codec frames the bytes stored in the record key space.
//
// Every value in that space is an envelope around a body:
//
//	byte 0..3  magic      "RMM1"
//	byte 4     envelope   format version
//	byte 5     codec      which encoding the body uses
//	byte 6..   body
//
// The envelope exists so the body encoding can be replaced without ambiguity,
// which is what spec §40.4 requires: no third-party serialisation library gets
// to define Remem's long-term storage format without an explicit version
// wrapper around it. Six bytes buys the ability to change that decision later.
//
// # Why the classification of a bad value matters
//
// A value that fails to parse is [errs.Corruption], not a miss. These bytes
// came out of the store; if they cannot be read, something wrote them that
// should not have, or the medium damaged them. Returning a zero-valued record
// instead would silently replace a memory with an empty one — the failure mode
// plan §II.4 names for canonical data, which is never discarded on a decode
// failure.
//
// A value whose envelope version or codec is *higher* than this binary knows is
// [errs.IncompatibleVersion], which is a different thing: the bytes are
// probably fine and this binary is simply too old to read them. Spec §59 is
// explicit that this is refused rather than guessed at.
package codec

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/version"
)

// Magic begins every record value.
const Magic = "RMM1"

// HeaderLen is the size of the envelope: magic, envelope version, codec.
const HeaderLen = len(Magic) + 2

// Body codecs. The numbers are a durable format; a retired one stays retired.
const (
	// CodecProtobuf is a record.v1.Record.
	CodecProtobuf uint8 = 1
)

// EnvelopeVersion reports the envelope version and body codec of a stored
// value without decoding the body.
//
// A migration walks every record to decide what to do with each one, and
// parsing a body it is about to rewrite is work it does not need. Six bytes is
// enough to decide.
func EnvelopeVersion(b []byte) (envelope, codec uint8, err error) {
	if err := checkHeader(b, "codec.EnvelopeVersion"); err != nil {
		return 0, 0, err
	}
	return b[4], b[5], nil
}

// checkHeader validates the magic, the envelope version and the record codec.
func checkHeader(b []byte, op string) error {
	return checkHeaderCodec(b, op, CodecProtobuf)
}

// checkHeaderCodec validates the magic, the envelope version and that the body
// is encoded with want.
func checkHeaderCodec(b []byte, op string, want uint8) error {
	if len(b) < HeaderLen {
		return errs.E(errs.Corruption, op,
			fmt.Errorf("a record value is at least %d bytes, this one is %d", HeaderLen, len(b)))
	}
	if string(b[:len(Magic)]) != Magic {
		return errs.E(errs.Corruption, op,
			fmt.Errorf("value does not begin with %q; it is not a record", Magic))
	}
	if got := uint32(b[4]); got > version.RecordEnvelope {
		return errs.E(errs.IncompatibleVersion, op,
			fmt.Errorf("record envelope version %d on disk, this binary writes %d; "+
				"upgrade rather than downgrade — a downgrade would rewrite data it cannot read",
				got, version.RecordEnvelope))
	}
	switch b[5] {
	case want:
		return nil
	case CodecProtobuf, CodecVectorProtobuf, CodecEdgeProtobuf:
		// A codec this binary knows, in a value it does not belong in. That is
		// a value read from the wrong key space, which is a bug in the reader
		// rather than damage on the medium.
		return errs.E(errs.Corruption, op,
			fmt.Errorf("this value is encoded with codec %d, but a codec %d value was expected here", b[5], want))
	default:
		return errs.E(errs.IncompatibleVersion, op,
			fmt.Errorf("body codec %d is not one this binary knows", b[5]))
	}
}

// header writes the envelope for the current version into dst.
func header(dst []byte) []byte { return headerFor(dst, CodecProtobuf) }

// headerFor writes the envelope naming the given body codec.
func headerFor(dst []byte, codec uint8) []byte {
	dst = append(dst, Magic...)
	dst = append(dst, byte(version.RecordEnvelope))
	return append(dst, codec)
}
