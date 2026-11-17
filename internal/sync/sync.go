// Package sync one-way mirrors backup artifacts between two S3-compatible
// endpoints, for cross-location redundancy. It is deliberately simpler than
// backup-stack's incremental backups: it copies whatever the source has
// that the destination is missing or disagrees on, and never deletes
// anything from the destination — delete propagation is the single easiest
// way a sync tool could destroy data it was supposed to protect, so this
// package has no code path that can call Delete at all.
package sync

import (
	"context"
	"fmt"
	"io"
)

// ObjectInfo is one object's key and size, as reported by an ObjectStore's
// listing.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ObjectStore is the object-storage surface Syncer needs. Delete exists so
// a real implementation (an S3/MinIO bucket) satisfies the interface its
// other callers (backup-stack's retention pruning) need, but Syncer itself
// never calls it.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Delete(ctx context.Context, key string) error
}

// Syncer one-way mirrors Source's objects under Prefix onto Dest.
type Syncer struct {
	Source ObjectStore
	Dest   ObjectStore
	Prefix string
}

// Result is what one Run copied.
type Result struct {
	Copied []string
}

// Run copies every object under Prefix that Dest is missing or whose size
// disagrees with Source's. An object present on both sides with matching
// size is left alone. Nothing on Dest is ever deleted, even if Source no
// longer has it.
func (s *Syncer) Run(ctx context.Context) (Result, error) {
	srcList, err := s.Source.List(ctx, s.Prefix)
	if err != nil {
		return Result{}, fmt.Errorf("sync: list source: %w", err)
	}
	dstList, err := s.Dest.List(ctx, s.Prefix)
	if err != nil {
		return Result{}, fmt.Errorf("sync: list dest: %w", err)
	}
	dstByKey := make(map[string]ObjectInfo, len(dstList))
	for _, o := range dstList {
		dstByKey[o.Key] = o
	}

	var res Result
	for _, o := range srcList {
		if d, ok := dstByKey[o.Key]; ok && d.Size == o.Size {
			continue
		}
		r, err := s.Source.Get(ctx, o.Key)
		if err != nil {
			return res, fmt.Errorf("sync: get %s: %w", o.Key, err)
		}
		putErr := s.Dest.Put(ctx, o.Key, r, o.Size)
		closeErr := r.Close()
		if putErr != nil {
			return res, fmt.Errorf("sync: put %s: %w", o.Key, putErr)
		}
		if closeErr != nil {
			return res, fmt.Errorf("sync: close %s: %w", o.Key, closeErr)
		}
		res.Copied = append(res.Copied, o.Key)
	}
	return res, nil
}
