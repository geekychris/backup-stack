// Package source defines what a backup target's Source looks like; the ssh
// and k8s subpackages implement it for the two target types backup-stack
// supports.
package source

import (
	"context"
	"io"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// Source captures one target's current state. Snapshot lists every item
// currently present; the backup runner diffs the result against the
// previous run's catalog to decide what an incremental run needs to
// (re)capture. Open streams one item's content, named by the Path a
// Snapshot item reported, for the runner to hand to a Sink.
type Source interface {
	Snapshot(ctx context.Context) (catalog.Catalog, error)
	Open(ctx context.Context, path string) (io.ReadCloser, error)
	Close() error
}
