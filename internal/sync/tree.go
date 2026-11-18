// Package sync one-way copies files between two endpoints — a local
// directory, a directory on a remote host reached over SSH, or a prefix of
// an S3-compatible bucket — for cross-location redundancy.
//
// Directory↔directory jobs (local or SSH, at most one side remote) use the
// system rsync when it is available on both ends, so only changed files
// cross the wire; everything else, and every job when rsync isn't there,
// uses the native Go copier in this package. Either way backup-stack
// evaluates the job's filter rules itself and, for rsync, passes the
// resulting explicit file list (--files-from), so the two paths select
// exactly the same files.
//
// Sync never deletes anything at the destination unless the job sets
// Delete (off by default) — delete propagation is the easiest way a sync
// tool can destroy the data it was supposed to protect. Even with Delete,
// only destination files the filters include are candidates.
package sync

import (
	"context"
	"io"
	"time"
)

// Entry is one file as a Tree lists it.
type Entry struct {
	Size int64
	// ModTime is zero when the tree has no meaningful modification time to
	// compare (S3 objects: their LastModified is the upload time).
	ModTime time.Time
}

// Tree is one side (source or destination) of a sync: a set of files keyed
// by "/"-separated path relative to the endpoint's root.
type Tree interface {
	// List returns every file under the root. A root that doesn't exist
	// yet lists empty (a fresh destination).
	List(ctx context.Context) (map[string]Entry, error)
	Open(ctx context.Context, rel string) (io.ReadCloser, error)
	// Write stores r (size bytes) at rel, creating parent directories, and
	// sets its modification time to e.ModTime where the tree supports it.
	Write(ctx context.Context, rel string, r io.Reader, e Entry) error
	Remove(ctx context.Context, rel string) error
	Close() error
}

// upToDate reports whether dst already matches src: same size, and the
// same modification time (to the second, since not every filesystem keeps
// sub-second precision) when both sides have one.
func upToDate(src, dst Entry) bool {
	if src.Size != dst.Size {
		return false
	}
	if src.ModTime.IsZero() || dst.ModTime.IsZero() {
		return true
	}
	return src.ModTime.Truncate(time.Second).Equal(dst.ModTime.Truncate(time.Second))
}
