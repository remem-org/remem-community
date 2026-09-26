package licenses

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

// Render writes the notices document: a table of what is linked, the curated
// section for what the image carries that is not a Go module, and the licence
// texts themselves.
//
// The texts are deduplicated by exact content, so the Apache-2.0 boilerplate
// appears once with the thirteen modules that use it listed against it, while
// two BSD-3-Clause files differing only in their copyright line stay separate —
// which is the point, since the copyright line is the part the licence requires
// to be reproduced.
//
// Nothing here reads a clock. A generated file with a timestamp in it changes
// on every run, and a document that always differs from the checked-in copy
// cannot be used as a drift check.
func Render(mods []Module, bundled string) string {
	var b strings.Builder
	b.WriteString(preamble)
	renderTable(&b, mods)
	b.WriteString("\n## 2. Components redistributed in the container image\n\n")
	b.WriteString(strings.TrimRight(bundled, "\n"))
	b.WriteString("\n\n")
	renderTexts(&b, mods)
	return b.String()
}

func renderTable(b *strings.Builder, mods []Module) {
	fmt.Fprintf(b, "%d modules are linked in.\n\n", len(mods))
	b.WriteString("| Module | Version | Licence | Files |\n|---|---|---|---|\n")
	for _, m := range mods {
		kinds := make([]string, 0, len(m.Files))
		paths := make([]string, 0, len(m.Files))
		seen := map[string]bool{}
		for _, f := range m.Files {
			k := kindOf(f)
			if !seen[k] {
				seen[k] = true
				kinds = append(kinds, k)
			}
			paths = append(paths, "`"+f.Path+"`")
		}
		if len(paths) == 0 {
			kinds, paths = []string{"**none found**"}, []string{"—"}
		}
		fmt.Fprintf(b, "| `%s` | %s | %s | %s |\n",
			m.Path, m.Version, strings.Join(kinds, ", "), strings.Join(paths, ", "))
	}
}

func renderTexts(b *strings.Builder, mods []Module) {
	b.WriteString("## 3. Licence texts\n\n" +
		"Reproduced verbatim. A text used by more than one module appears once, " +
		"with everything it covers listed above it.\n")

	type block struct {
		text  string
		kind  string
		users []string
		first int
	}
	blocks := map[string]*block{}
	var order []string
	n := 0
	for _, m := range mods {
		for _, f := range m.Files {
			sum := fmt.Sprintf("%x", sha256.Sum256([]byte(f.Text)))
			blk := blocks[sum]
			if blk == nil {
				blk = &block{text: f.Text, kind: kindOf(f), first: n}
				blocks[sum] = blk
				order = append(order, sum)
				n++
			}
			blk.users = append(blk.users, fmt.Sprintf("`%s` %s — `%s`", m.Path, m.Version, f.Path))
		}
	}
	sort.Slice(order, func(i, j int) bool { return blocks[order[i]].first < blocks[order[j]].first })

	for i, sum := range order {
		blk := blocks[sum]
		fmt.Fprintf(b, "\n### 3.%d %s\n\nApplies to:\n\n", i+1, blk.kind)
		for _, u := range blk.users {
			fmt.Fprintf(b, "- %s\n", u)
		}
		fence := fenceFor(blk.text)
		fmt.Fprintf(b, "\n%stext\n%s%s\n", fence, blk.text, fence)
	}
}

// kindOf names a licence file. A file called NOTICE is a notice whatever it
// contains — Apache-2.0 §4(d) is about the file, not about its wording — so the
// name decides before the text is read.
func kindOf(f File) string {
	base := f.Path
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if strings.HasPrefix(strings.ToUpper(base), "NOTICE") {
		return "Notice (Apache-2.0 §4(d))"
	}
	return Kind(f.Text)
}

// fenceFor returns a code fence the text cannot close from the inside.
func fenceFor(text string) string {
	fence := "```"
	for strings.Contains(text, fence) {
		fence += "`"
	}
	return fence
}

const preamble = `# Third-party notices

Remem's binaries and container images include third-party software. This file
reproduces the copyright notices and licence texts that software requires to be
distributed with it.

**It is generated. Do not edit it by hand** — run ` + "`make notices`" + `. The Go
section is read from ` + "`go list -deps`" + ` over the commands the image installs,
so it describes what the linker put in the binaries rather than what ` + "`go.mod`" + `
asks for. ` + "`TestThirdPartyNoticesAreCurrent`" + ` fails when this file and the
dependency graph disagree. The one hand-written part is section 2, which covers
what the image carries that is not a Go module; it lives in
` + "`internal/licenses/bundled.md`" + `.

This file must travel with every binary distribution: the container images, and
any released binary or archive. It is not required for running Remem as a hosted
service, and none of these licences has a network clause — the obligation
attaches at the moment a build is handed to someone else.

Nothing here restricts what Remem itself is licensed under. No dependency is
copyleft: every licence below is BSD, MIT, ISC or Apache-2.0.

## 1. Go modules compiled into the Remem binaries

`
