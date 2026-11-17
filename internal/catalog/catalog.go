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

// Catalog is everything a backup run captured. A full run's Catalog has
// every item that exists and an empty Deleted. An incremental run instead
// stores a "delta catalog": Items holds only what it newly captured (the
// added and changed items — a sink need only hold their content, not every
// unchanged item's), and Deleted names what no longer exists. Apply turns
// either shape into the full state it describes.
type Catalog struct {
	Items   []Item   `json:"items"`
	Deleted []string `json:"deleted,omitempty"`
}

// byPath returns the catalog's items indexed by Path.
func (c Catalog) byPath() map[string]Item {
	m := make(map[string]Item, len(c.Items))
	for _, it := range c.Items {
		m[it.Path] = it
	}
	return m
}

// Marshal encodes the catalog as JSON, items and deleted paths sorted so
// two catalogs with the same contents always encode identically.
func (c Catalog) Marshal() ([]byte, error) {
	sorted := append([]Item{}, c.Items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	deleted := append([]string{}, c.Deleted...)
	sort.Strings(deleted)
	return json.Marshal(Catalog{Items: sorted, Deleted: deleted})
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

// AsCatalog turns a Delta into the delta-shaped Catalog a run's sink stores:
// Items is Added and Changed combined (Apply treats both as an upsert, so
// nothing is lost by not keeping them separate), and Deleted carries over
// unchanged.
func (d Delta) AsCatalog() Catalog {
	items := make([]Item, 0, len(d.Added)+len(d.Changed))
	items = append(items, d.Added...)
	items = append(items, d.Changed...)
	return Catalog{Items: items, Deleted: d.Deleted}
}

// Apply overlays onto base to produce the full catalog it describes:
// base's items with overlay.Items upserted and overlay.Deleted's paths
// removed. overlay may be a full snapshot (Deleted empty, Items being
// everything) or a delta catalog (Items being only what changed). Chaining
// Apply over a run of incremental runs' delta catalogs, in the order they
// were taken, reconstructs the final state without needing every
// intermediate run to have stored every unchanged item.
func Apply(base, overlay Catalog) Catalog {
	byPath := base.byPath()
	for _, it := range overlay.Items {
		byPath[it.Path] = it
	}
	for _, p := range overlay.Deleted {
		delete(byPath, p)
	}
	out := Catalog{Items: make([]Item, 0, len(byPath))}
	for _, it := range byPath {
		out.Items = append(out.Items, it)
	}
	sort.Slice(out.Items, func(i, j int) bool { return out.Items[i].Path < out.Items[j].Path })
	return out
}
