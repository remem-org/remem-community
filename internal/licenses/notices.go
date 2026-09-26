// Package licenses assembles the third-party notices a Remem distribution has
// to carry.
//
// Everything compiled into a Remem binary is under BSD, MIT, ISC or
// Apache-2.0, and all four say the same thing about a binary release: the
// copyright notice and the licence text must be reproduced in the materials
// that travel with it. Pebble states it plainly, and it is the representative
// case rather than a special one:
//
//	Redistributions in binary form must reproduce the above copyright
//	notice, this list of conditions and the following disclaimer in the
//	documentation and/or other materials provided with the distribution.
//
// A published container image is exactly such a distribution. Apache-2.0
// §4(d) goes one step further and requires a dependency's own NOTICE file to
// be carried forward, which three of the Prometheus modules have.
//
// The notices are generated rather than written. A hand-kept list is wrong the
// first time go.sum moves and nothing says so; what is generated is checked in,
// and TestThirdPartyNoticesAreCurrent fails when the two disagree. The
// dependency set comes from `go list -deps` rather than from go.mod, so the
// document describes what the linker actually put in the binary — go.mod names
// modules that no shipped binary imports, and listing those would be a claim
// about the distribution that is not true.
//
// Scope: this covers the Go modules. Everything else the image carries — the
// Go standard library, ONNX Runtime, the tokenizer's static archive, the model
// weights and the base image — cannot be read out of the module graph, so it is
// curated in bundled.md and rendered beside the generated part.
package licenses

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ShippedPatterns and ShippedTagSets are what a release distributes: the three
// commands the image installs, built with cgo and the onnx tag.
//
// Both tag sets are listed because an edition is a build tag, and a notices
// file that described only the community build would be silent about anything
// the business build links instead. They produce the same set today; the point
// is that a divergence would be caught rather than assumed away.
var (
	ShippedPatterns = []string{"./cmd/..."}
	ShippedTagSets  = []string{"onnx", "onnx business"}
)

// Module is one dependency of a shipped binary, with every licence file found
// between the packages that are linked and the module root.
type Module struct {
	Path    string
	Version string
	Files   []File
}

// File is one licence document, named relative to the module root. The
// relative path is part of the record rather than noise: Pebble carries three
// licences and they are not the same licence, so "pebble is BSD-3-Clause" is
// true of the module and false of two of its packages.
type File struct {
	Path string
	Text string
}

// licenceName matches the file and directory names a licence is kept under.
// The trailing separator class is what lets LICENSE.txt and LICENSE-MIT match
// while leaving LICENSING.md alone, and `licenses` is here as a plural because
// cockroachdb/crlib keeps a directory of them.
var licenceName = regexp.MustCompile(`(?i)^(licen[cs]es?|copying|notice|patents)([.\-_].*)?$`)

// listTemplate reports the module a package belongs to and both directories:
// the module root, and the package's own, which is what makes a licence
// governing one subtree of a module discoverable at all.
const listTemplate = "{{if .Module}}{{.Module.Path}}\t{{.Module.Version}}\t{{.Module.Dir}}\t{{.Dir}}{{end}}"

// Collect reads the modules linked into the given packages, under each of the
// given build-tag sets, and gathers their licence files.
func Collect(root string, tagSets []string, patterns []string) ([]Module, error) {
	byPath := map[string]*Module{}
	seen := map[string]bool{}
	for _, tags := range tagSets {
		lines, err := goList(root, tags, patterns)
		if err != nil {
			return nil, err
		}
		for _, line := range lines {
			f := strings.Split(line, "\t")
			if len(f) != 4 {
				continue
			}
			modPath, version, modDir, pkgDir := f[0], f[1], f[2], f[3]
			// No module is the standard library, which go list reports with
			// no module at all; no version is this module's own packages.
			// Remem's licence is not a notice about somebody else's work.
			if modPath == "" || version == "" || modDir == "" {
				continue
			}
			m := byPath[modPath]
			if m == nil {
				m = &Module{Path: modPath, Version: version}
				byPath[modPath] = m
			}
			files, err := licenceFilesBetween(modDir, pkgDir)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", modPath, err)
			}
			for _, lf := range files {
				key := modPath + "\x00" + lf.Path
				if seen[key] {
					continue
				}
				seen[key] = true
				m.Files = append(m.Files, lf)
			}
		}
	}

	out := make([]Module, 0, len(byPath))
	for _, m := range byPath {
		sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
		out = append(out, *m)
	}
	// Sorted case-insensitively: the module cache is case-sensitive and
	// RaduBerinde would otherwise sort before beorn7, which reads as a bug in
	// a document a human is meant to scan.
	sort.Slice(out, func(i, j int) bool {
		a, b := strings.ToLower(out[i].Path), strings.ToLower(out[j].Path)
		if a != b {
			return a < b
		}
		return out[i].Path < out[j].Path
	})
	return out, nil
}

func goList(root, tags string, patterns []string) ([]string, error) {
	args := append([]string{"list", "-deps", "-tags", tags, "-f", listTemplate}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	// CGO_ENABLED decides the dependency set and not only how it is built: the
	// ONNX binding, the tokenizer and Pebble's zstd are reachable only with
	// cgo on, and those three are in the image. Generating this file on a host
	// with cgo off would drop them silently.
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list -tags %q: %w: %s", tags, err, strings.TrimSpace(stderr.String()))
	}
	return strings.Split(strings.TrimRight(string(out), "\n"), "\n"), nil
}

// licenceFilesBetween returns the licence files in the module root and in every
// directory from there down to the package, which is where a licence covering
// one subtree of a module lives.
func licenceFilesBetween(modDir, pkgDir string) ([]File, error) {
	dirs := []string{modDir}
	if rel, err := filepath.Rel(modDir, pkgDir); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
		d := modDir
		for _, part := range strings.Split(rel, string(filepath.Separator)) {
			d = filepath.Join(d, part)
			dirs = append(dirs, d)
		}
	}

	var files []File
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			// A package directory that cannot be read is not a licence
			// question; the module root has already been scanned.
			continue
		}
		for _, e := range entries {
			if !licenceName.MatchString(e.Name()) {
				continue
			}
			found, err := readLicence(modDir, dir, e)
			if err != nil {
				return nil, err
			}
			files = append(files, found...)
		}
	}
	return files, nil
}

func readLicence(modDir, dir string, e os.DirEntry) ([]File, error) {
	full := filepath.Join(dir, e.Name())
	if !e.IsDir() {
		f, err := readFile(modDir, full)
		if err != nil {
			return nil, err
		}
		return []File{f}, nil
	}
	// A directory of licences, one file per licence: crlib keeps the Go
	// project's BSD grant for the code it copied that way.
	entries, err := os.ReadDir(full)
	if err != nil {
		return nil, err
	}
	var out []File
	for _, sub := range entries {
		if sub.IsDir() {
			continue
		}
		f, err := readFile(modDir, filepath.Join(full, sub.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func readFile(modDir, full string) (File, error) {
	b, err := os.ReadFile(full)
	if err != nil {
		return File{}, err
	}
	rel, err := filepath.Rel(modDir, full)
	if err != nil {
		rel = filepath.Base(full)
	}
	text := strings.ReplaceAll(string(b), "\r\n", "\n")
	return File{
		Path: filepath.ToSlash(rel),
		Text: strings.TrimRight(text, " \t\n") + "\n",
	}, nil
}

// Kind names the licence a text is, for the summary table. It is a reader's
// aid and nothing rests on it: the obligation is discharged by the verbatim
// text reproduced below the table, which is why an unrecognised licence is
// reported as unrecognised rather than guessed at.
func Kind(text string) string {
	switch {
	case strings.Contains(text, "Mozilla Public License"):
		return "MPL-2.0"
	case strings.Contains(text, "Apache License") && strings.Contains(text, "Version 2.0"):
		return "Apache-2.0"
	case strings.Contains(text, "Permission is hereby granted, free of charge"):
		return "MIT"
	case strings.Contains(text, "Permission to use, copy, modify, and/or distribute"):
		return "ISC"
	case strings.Contains(text, "Redistribution and use in source and binary"):
		if strings.Contains(text, "either the name") {
			return "BSD-3-Clause"
		}
		return "BSD-2-Clause"
	case strings.Contains(text, "Additional IP Rights Grant"):
		return "Patent grant"
	}
	return "unrecognised"
}
