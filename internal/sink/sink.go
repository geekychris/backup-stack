// Package sink defines where a backup run's output goes. internal/sink/s3
// implements it over an S3-compatible (MinIO) bucket.
package sink

import (
	"context"
	"io"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// Sink stores one backup run's catalog and the content of the items it
// captured, keyed by runID (a globally unique id the store assigns, so a
// Sink needs no notion of target).
type Sink interface {
	WriteItem(ctx context.Context, runID, path string, r io.Reader) error
	OpenItem(ctx context.Context, runID, path string) (io.ReadCloser, error)
	WriteCatalog(ctx context.Context, runID string, cat catalog.Catalog) error
	OpenCatalog(ctx context.Context, runID string) (catalog.Catalog, error)
	// DeleteRun removes every object a run wrote (its catalog and items).
	DeleteRun(ctx context.Context, runID string) error
	// Prune deletes every run's data except the runIDs in keep. Used to
	// enforce a keep-last-N-fulls retention policy.
	Prune(ctx context.Context, keep map[string]bool) error
}
