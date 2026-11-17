// Package catalog is the pure-logic core of backup-stack's incremental
// backups and sync: a Catalog is the list of items a source produced in one
// run, and Diff compares two catalogs to find what an incremental run needs
// to capture (added/changed items) and what it should record as removed.
//
// Nothing here does I/O. A Fingerprint is an opaque, source-defined token
// that changes whenever an item's content changes — a content hash for a
// Kubernetes object, or a cheap size+mtime tag for a filesystem tree where
// hashing every file on every run would be too slow. Diff only ever compares
// Fingerprint strings for equality, so either choice works.
package catalog

import (
	"encoding/json"
	"sort"
)

// Item is one thing a source captured: a file under an ssh-path target, or
// one Kubernetes object under a k8s-namespace target.
type Item struct {
	// Path identifies the item within its target (a relative file path, or
	// "<kind>/<namespace>/<name>" for a Kubernetes object). Unique within a
	// Catalog.
	Path string `json:"path"`
	// Fingerprint changes whenever the item's content changes. See the
	// package doc for what it holds per source type.
	Fingerprint string `json:"fingerprint"`
	// Size is the item's content size in bytes, as stored by the sink.
	Size int64 `json:"size"`
}

// Catalog is everything a backup run captured.
type Catalog struct {
	Items []Item `json:"items"`
}

// byPath returns the catalog's items indexed by Path.
func (c Catalog) byPath() map[string]Item {
	m := make(map[string]Item, len(c.Items))
	for _, it := range c.Items {
		m[it.Path] = it
	}
	return m
}

// Marshal encodes the catalog as JSON, items sorted by Path so two catalogs
// with the same items always encode identically.
func (c Catalog) Marshal() ([]byte, error) {
	sorted := append([]Item{}, c.Items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	return json.Marshal(Catalog{Items: sorted})
}

// Unmarshal decodes a catalog written by Marshal.
func Unmarshal(b []byte) (Catalog, error) {
	var c Catalog
	err := json.Unmarshal(b, &c)
	return c, err
}

// Delta is what changed between a previous and a current Catalog: Added and
// Changed are the items an incremental run must (re)capture, and Deleted
// names items that no longer exist so a restore can remove them.
type Delta struct {
	Added   []Item
	Changed []Item
	Deleted []string
}

// Diff compares prev to cur. An item present in both with the same
// Fingerprint is unchanged and appears in neither list.
func Diff(prev, cur Catalog) Delta {
	prevByPath := prev.byPath()
	var d Delta
	seen := make(map[string]bool, len(cur.Items))
	for _, it := range cur.Items {
		seen[it.Path] = true
		old, existed := prevByPath[it.Path]
		switch {
		case !existed:
			d.Added = append(d.Added, it)
		case old.Fingerprint != it.Fingerprint:
			d.Changed = append(d.Changed, it)
		}
	}
	for _, old := range prev.Items {
		if !seen[old.Path] {
			d.Deleted = append(d.Deleted, old.Path)
		}
	}
	return d
}

// Apply replays a Delta onto base to produce the catalog it was computed
// against (base's items with Added appended, Changed replacing their old
// entry, and Deleted entries removed). Chaining Apply over a run of
// incremental Deltas, in the order they were taken, reconstructs the final
// catalog without needing every intermediate run's full item list.
func Apply(base Catalog, d Delta) Catalog {
	byPath := base.byPath()
	for _, it := range d.Added {
		byPath[it.Path] = it
	}
	for _, it := range d.Changed {
		byPath[it.Path] = it
	}
	for _, p := range d.Deleted {
		delete(byPath, p)
	}
	out := Catalog{Items: make([]Item, 0, len(byPath))}
	for _, it := range byPath {
		out.Items = append(out.Items, it)
	}
	sort.Slice(out.Items, func(i, j int) bool { return out.Items[i].Path < out.Items[j].Path })
	return out
}
