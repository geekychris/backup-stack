package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/geekychris/backup-stack/internal/runstats"
	"github.com/geekychris/backup-stack/internal/store"
)

// Transfer methods a sync run records.
const (
	MethodRsync  = "rsync"
	MethodNative = "native"
)

// Runner executes sync jobs and records each run (with its stats) in a
// Store.
type Runner struct {
	Store *store.Store
	// RsyncPath is the local rsync binary ("" = always use the native
	// copier).
	RsyncPath string
	// Sinks resolves an s3 endpoint's "sink" name.
	Sinks SinkResolver
	// OpenTree connects an endpoint; defaults to the real dir/ssh/s3
	// trees. Injectable for tests.
	OpenTree func(ctx context.Context, e Endpoint) (Tree, error)
	// NewRunID defaults to a random id.
	NewRunID func() string
}

func (r *Runner) newRunID() string {
	if r.NewRunID != nil {
		return r.NewRunID()
	}
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return "srun_" + hex.EncodeToString(b)
}

// Run executes j once and returns its recorded run. A run that had any
// error — fatal or per-file — is recorded as failed, with the errors in
// its stats. store.ErrSyncInFlight means another run of j is in progress
// and nothing was started.
func (r *Runner) Run(ctx context.Context, j *store.SyncJob) (*store.SyncRun, error) {
	run := &store.SyncRun{ID: r.newRunID(), JobID: j.ID}
	if err := r.Store.StartSyncRun(ctx, run); err != nil {
		return nil, err
	}
	method, st, err := r.execute(ctx, j)
	state, msg := store.StateSucceeded, ""
	if err != nil {
		st.AddError(err)
		state, msg = store.StateFailed, err.Error()
	} else if st.Errors > 0 {
		state, msg = store.StateFailed, st.Summary()
	}
	// Recorded even if ctx was canceled, so the row never stays "running".
	fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if ferr := r.Store.FinishSyncRun(fctx, run.ID, method, state, st, msg); ferr != nil {
		return nil, ferr
	}
	return r.Store.GetSyncRun(fctx, run.ID)
}

func (r *Runner) open(ctx context.Context, e Endpoint) (Tree, error) {
	if r.OpenTree != nil {
		return r.OpenTree(ctx, e)
	}
	return openTree(ctx, e, r.Sinks)
}

func (r *Runner) execute(ctx context.Context, j *store.SyncJob) (string, runstats.Stats, error) {
	var st runstats.Stats
	if err := ValidateJob(j.SourceConfig, j.DestConfig); err != nil {
		return MethodNative, st, err
	}
	if err := j.Filters.Validate(); err != nil {
		return MethodNative, st, err
	}
	srcEP, _ := ParseEndpoint(j.SourceConfig)
	dstEP, _ := ParseEndpoint(j.DestConfig)
	src, err := r.open(ctx, srcEP)
	if err != nil {
		return MethodNative, st, fmt.Errorf("source %s: %w", srcEP, err)
	}
	defer src.Close()
	dst, err := r.open(ctx, dstEP)
	if err != nil {
		return MethodNative, st, fmt.Errorf("destination %s: %w", dstEP, err)
	}
	defer dst.Close()
	opt := Options{Filters: j.Filters, Delete: j.Delete}

	if r.useRsync(ctx, srcEP, dstEP, src, dst) {
		st, err := r.viaRsync(ctx, srcEP, dstEP, src, dst, opt)
		return MethodRsync, st, err
	}
	st, err = Native(ctx, src, dst, opt)
	return MethodNative, st, err
}

// useRsync: both sides directories, at most one remote (rsync can't copy
// between two remote hosts — an ssh->ssh job streams through this process
// instead), rsync here, and rsync on the remote side too.
func (r *Runner) useRsync(ctx context.Context, srcEP, dstEP Endpoint, src, dst Tree) bool {
	if r.RsyncPath == "" || !srcEP.IsDir() || !dstEP.IsDir() {
		return false
	}
	if srcEP.Type == TypeSSH && dstEP.Type == TypeSSH {
		return false
	}
	for _, t := range []Tree{src, dst} {
		if rt, ok := t.(interface{ HasRsync(context.Context) bool }); ok && !rt.HasRsync(ctx) {
			return false
		}
	}
	return true
}

func rsyncSideFor(e Endpoint) rsyncSide {
	if e.Type == TypeSSH {
		c := e.SSH()
		return rsyncSide{SSH: &c}
	}
	return rsyncSide{Local: e.Path}
}

// viaRsync lists the source and applies the filters here, hands rsync the
// selected files, and (with Delete) removes extras natively afterwards —
// rsync's own --delete doesn't combine with an explicit file list.
func (r *Runner) viaRsync(ctx context.Context, srcEP, dstEP Endpoint, src, dst Tree, opt Options) (runstats.Stats, error) {
	srcList, err := src.List(ctx)
	if err != nil {
		return runstats.Stats{}, fmt.Errorf("list source: %w", err)
	}
	files := selected(srcList, opt.Filters)
	st, err := rsyncTransfer(ctx, r.RsyncPath, rsyncSideFor(srcEP), rsyncSideFor(dstEP), files)
	if err != nil {
		return st, err
	}
	if opt.Delete {
		dstList, err := dst.List(ctx)
		if err != nil {
			return st, fmt.Errorf("list destination: %w", err)
		}
		deleteExtra(ctx, dst, dstList, srcList, opt.Filters, &st)
	}
	return st, nil
}
