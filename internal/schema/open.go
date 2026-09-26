package schema

import (
	"context"
	"fmt"
	"sort"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/version"
)

// Open is the gate every data directory passes through before anything reads
// or writes user data.
//
// The policy is spec §59's, and it is asymmetric on purpose:
//
//   - A directory with no manifest is stamped at current. Brand-new and
//     written-before-the-manifest-existed are the same case, and treating them
//     as one removes a branch that would otherwise be tested by nobody.
//   - A format this binary has never heard of refuses the open. It means files
//     exist that this binary cannot interpret, and continuing would mean
//     operating on a directory while ignoring part of it.
//   - A directory that requires a newer reader or writer refuses the open, and
//     the error names the format and both numbers. A downgrade is never
//     performed automatically, because it would mean rewriting data this binary
//     cannot read.
//   - An older directory opens. Moving it forward is [Runner]'s job;
//     [Registry.Plan] turns the gap into the steps that close it, and refuses
//     when nothing can.
//
// Open does not rewrite a manifest it can already use, so a restart is not a
// write to the one row that says whether the data is readable.
func Open(ctx context.Context, kv storage.KV, want version.Versions) (Format, error) {
	const op = "schema.Open"

	f, err := ReadFormat(ctx, kv)
	if err != nil {
		return Format{}, err
	}

	if len(f.Subsystems) == 0 {
		stamped := CurrentFormat(want)
		if err := WriteFormat(ctx, kv, stamped); err != nil {
			return Format{}, err
		}
		return stamped, nil
	}

	if err := check(f, want, op); err != nil {
		return Format{}, err
	}
	return f, nil
}

// check applies the compatibility policy to a manifest that already exists.
func check(f Format, want version.Versions, op string) error {
	var unknown, blocked []string

	for _, name := range sortedNames(f) {
		s := f.Subsystems[name]

		supported, known := version.SupportedVersion(name)
		if !known {
			unknown = append(unknown, fmt.Sprintf("%q (version %d)", name, s.Current))
			continue
		}
		// The binary's own supported version is what `want` carries; tests
		// inject it, and the composition root passes version.Current().
		if injected, ok := want.Get(name); ok {
			supported = injected
		}

		if s.MinReader > supported {
			blocked = append(blocked, fmt.Sprintf(
				"%s is at version %d and needs a reader supporting at least %d; this binary supports %d",
				version.Describe(name), s.Current, s.MinReader, supported))
			continue
		}
		if s.MinWriter > supported {
			blocked = append(blocked, fmt.Sprintf(
				"%s is at version %d and needs a writer supporting at least %d; this binary supports %d, "+
					"so it could read this directory but must not write to it",
				version.Describe(name), s.Current, s.MinWriter, supported))
		}
	}

	if len(unknown) > 0 {
		return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"this database records a storage format this binary has never heard of: %s. "+
				"That means it holds files this build cannot interpret, so it is refused rather than "+
				"partly understood. Upgrade the server to a build that knows this format (binary version %s)",
			join(unknown), version.Binary))
	}
	if len(blocked) > 0 {
		return errs.E(errs.IncompatibleVersion, op, fmt.Errorf(
			"this database was written by a newer Remem (%s). "+
				"Upgrade the server to a build that supports it; a downgrade is never performed "+
				"automatically, because it would mean rewriting data this binary cannot read "+
				"(binary version %s)",
			join(blocked), version.Binary))
	}
	return nil
}

func sortedNames(f Format) []string {
	names := make([]string, 0, len(f.Subsystems))
	for name := range f.Subsystems {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func join(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}
