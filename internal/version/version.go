// Package version holds the binary version and the durable format versions,
// and decides whether a database this binary did not write may be opened.
//
// Invariant 11 requires the binary version to be distinct from the active
// storage and cluster compatibility versions: a rolling upgrade runs mixed
// binaries against one format, so the two cannot be the same number. Invariant
// 4 requires every durable format to be versioned independently, so a change
// to the attribute slot schema does not force a key-encoding migration.
//
// Four formats are versioned here plus the cluster compatibility version:
//
//	key encoding          internal/keys      the byte layout of every key
//	record envelope       internal/codec     the framing around a record body
//	snapshot format       internal/snapshot  the portable export (Invariant 12)
//	attribute slot schema internal/attr      which fields are indexed, and how
//
// The policy is spec §59's: lower or absent migrates forward, higher or
// unknown refuses, and a downgrade is never performed automatically.
package version

import (
	"errors"
	"fmt"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
)

// Binary is the build identity of this executable. It is set at link time:
//
//	go build -ldflags "-X github.com/remem-org/remem-go/internal/version.Binary=v1.2.3"
//
// It is deliberately a string and deliberately not compared to anything: no
// decision in Remem may be taken on the binary version (Invariant 11).
var Binary = "dev"

// The format versions this binary writes. Each is bumped independently, and
// only when the bytes on disk change meaning.
const (
	// KeyEncoding is the version of the key byte layout (Part II.2).
	KeyEncoding uint32 = 1
	// RecordEnvelope is the version of the record framing (Part II.3).
	RecordEnvelope uint32 = 1
	// SnapshotFormat is the version of the portable export (Part II.7). It is
	// the one format shared with Rust Remem, which writes it and Go reads it.
	SnapshotFormat uint32 = 1
	// AttrSchema is the version of the attribute slot registry.
	AttrSchema uint32 = 2
	// ClusterCompat is the active cluster compatibility version. Zero means no
	// cluster format is active; it stays zero until Phase 14.
	ClusterCompat uint32 = 0
	// TextIndex is the version of the rules that derive keyword postings from
	// text: tokenisation and Unicode normalisation. The postings key space is
	// written by those rules, so changing them changes the meaning of bytes on
	// disk, which is what a format version is for. Absent, in a directory
	// written before Phase 13, is Unicode 15 normalisation from
	// golang.org/x/text v0.25.0. Version 1 is v0.39.0, which normalises with
	// Unicode 17 tables on Go 1.27 and fixes GO-2026-5970.
	TextIndex uint32 = 1
)

// Versions is a set of durable format versions — either the ones this binary
// writes ([Current]) or the ones read from a database's format manifest.
//
// The zero value means "absent", which is how a database written before a
// format existed presents itself. Absent is older, so it migrates forward.
type Versions struct {
	KeyEncoding    uint32
	RecordEnvelope uint32
	SnapshotFormat uint32
	AttrSchema     uint32
	ClusterCompat  uint32
	TextIndex      uint32
}

// Current returns the versions this binary writes.
func Current() Versions {
	return Versions{
		KeyEncoding:    KeyEncoding,
		RecordEnvelope: RecordEnvelope,
		SnapshotFormat: SnapshotFormat,
		AttrSchema:     AttrSchema,
		ClusterCompat:  ClusterCompat,
		TextIndex:      TextIndex,
	}
}

// format names one versioned durable format: how it is spelled to an operator,
// how it is spelled in machine-readable output, and what this binary supports.
type format struct {
	name      string // for humans, in error messages
	key       string // for machines, in logs and manifests
	supported uint32
	onDisk    func(Versions) uint32
}

var formats = []format{
	{"key encoding", "key_encoding", KeyEncoding, func(v Versions) uint32 { return v.KeyEncoding }},
	{"record envelope", "record_envelope", RecordEnvelope, func(v Versions) uint32 { return v.RecordEnvelope }},
	{"snapshot format", "snapshot_format", SnapshotFormat, func(v Versions) uint32 { return v.SnapshotFormat }},
	{"attribute slot schema", "attr_schema", AttrSchema, func(v Versions) uint32 { return v.AttrSchema }},
	{"cluster compatibility", "cluster_compat", ClusterCompat, func(v Versions) uint32 { return v.ClusterCompat }},
	{"text index", "text_index", TextIndex, func(v Versions) uint32 { return v.TextIndex }},
}

// FormatNames returns the machine-readable name of every versioned durable
// format, in a stable order.
//
// It exists so that the format manifest on disk and the versions in this
// package cannot drift apart: internal/schema names its stored rows from this
// list rather than keeping a second copy of the same strings.
func FormatNames() []string {
	out := make([]string, 0, len(formats))
	for _, f := range formats {
		out = append(out, f.key)
	}
	return out
}

// SupportedVersion reports the version of the named format this binary writes,
// and whether the name is one it knows at all.
//
// An unknown name is not a lookup miss to shrug at: a manifest naming a format
// this binary has never heard of means files exist that it cannot interpret.
func SupportedVersion(name string) (uint32, bool) {
	for _, f := range formats {
		if f.key == name {
			return f.supported, true
		}
	}
	return 0, false
}

// Get returns the version of the named format in v.
func (v Versions) Get(name string) (uint32, bool) {
	for _, f := range formats {
		if f.key == name {
			return f.onDisk(v), true
		}
	}
	return 0, false
}

// Describe returns the human-readable name of a format, for error messages
// that an operator has to act on.
func Describe(name string) string {
	for _, f := range formats {
		if f.key == name {
			return f.name
		}
	}
	return name
}

// CheckOpen reports whether a database carrying onDisk may be opened by this
// binary.
//
// Newer on disk is refused, in every format independently: a newer writer may
// have written bytes this binary would misread, and silently mutating them is
// exactly what spec §59 forbids. Older or absent is accepted here and left to
// the migration runner — see [Versions.NeedsMigration].
func CheckOpen(onDisk Versions) error {
	var newer []string
	for _, f := range formats {
		if got := f.onDisk(onDisk); got > f.supported {
			newer = append(newer, fmt.Sprintf("%s is version %d on disk, this binary supports %d", f.name, got, f.supported))
		}
	}
	if len(newer) == 0 {
		return nil
	}
	msg := fmt.Sprintf(
		"this database was written by a newer Remem (%s). "+
			"Upgrade the server to a build that supports it; a downgrade is never performed automatically, "+
			"because it would mean rewriting data this binary cannot read (binary version %s)",
		strings.Join(newer, "; "), Binary)
	return errs.E(errs.IncompatibleVersion, "version.CheckOpen", errors.New(msg))
}

// NeedsMigration reports whether any format on disk is older than this binary
// writes, and therefore whether the migration runner has work to do.
//
// It says nothing about whether the database may be opened; that is
// [CheckOpen].
func (v Versions) NeedsMigration() bool {
	for _, f := range formats {
		if f.onDisk(v) < f.supported {
			return true
		}
	}
	return false
}

// String renders the versions as stable key=value pairs for logs and for
// `remem-admin inspect`.
func (v Versions) String() string {
	parts := make([]string, 0, len(formats))
	for _, f := range formats {
		parts = append(parts, fmt.Sprintf("%s=%d", f.key, f.onDisk(v)))
	}
	return strings.Join(parts, " ")
}
