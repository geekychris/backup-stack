// Package backup orchestrates backup runs: capturing a Source's current
// state, writing it to a Sink, and recording the run in a Store.
//
// A full run's Sink catalog holds every item that exists. An incremental
// run's Sink catalog instead holds only the items it newly captured (added
// or changed since the target's previous run) plus the paths that were
// deleted — a tar-incremental-style delta, not a content-addressed dedup
// store. Reconstructing the state as of any run means replaying its chain
// of runs, starting from the nearest preceding full run, with
// catalog.Apply; ChainCatalog does that.
package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
)

// ErrNoFullRun is returned by RunIncremental when a target has never had a
// successful full run: an incremental chain must start from one.
var ErrNoFullRun = errors.New("backup: target has no prior successful full run")

// Runner orchestrates backup runs, recording them in a Store.
type Runner struct {
	Store *store.Store
	// NewRunID generates a run's ID; defaults to a random id. Overridable
	// for deterministic tests.
	NewRunID func() string
}

func (r *Runner) newRunID() string {
	if r.NewRunID != nil {
		return r.NewRunID()
	}
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return "run_" + hex.EncodeToString(b)
}

// RunFull captures everything src currently reports: every item's content
// goes to sk, and sk's catalog for this run is the complete snapshot.
func (r *Runner) RunFull(ctx context.Context, targetID string, src source.Source, sk sink.Sink) (*store.Run, error) {
	snap, err := src.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: snapshot: %w", err)
	}
	run := &store.Run{ID: r.newRunID(), TargetID: targetID, Kind: store.KindFull, State: store.StateRunning}
	if err := r.Store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("backup: create run: %w", err)
	}
	if err := writeItems(ctx, run.ID, src, sk, snap.Items); err != nil {
		_ = r.Store.FinishRun(ctx, run.ID, store.StateFailed, 0, err.Error())
		return nil, err
	}
	if err := sk.WriteCatalog(ctx, run.ID, snap); err != nil {
		_ = r.Store.FinishRun(ctx, run.ID, store.StateFailed, 0, err.Error())
		return nil, fmt.Errorf("backup: write catalog: %w", err)
	}
	if err := r.Store.FinishRun(ctx, run.ID, store.StateSucceeded, len(snap.Items), ""); err != nil {
		return nil, err
	}
	return r.Store.GetRun(ctx, run.ID)
}

// RunIncremental captures only what changed since the target's most recent
// run (full or incremental): it diffs src's current Snapshot against the
// reconstructed state as of that run, writes just the added/changed items'
// content, and records sk's catalog for this run as that delta. It fails
// with ErrNoFullRun if the target has no successful full run yet.
func (r *Runner) RunIncremental(ctx context.Context, targetID string, src source.Source, sk sink.Sink) (*store.Run, error) {
	prev, err := r.Store.LatestRun(ctx, targetID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrNoFullRun
		}
		return nil, err
	}
	prevState, _, err := r.ChainCatalog(ctx, targetID, prev.ID, sk)
	if err != nil {
		return nil, err
	}

	snap, err := src.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: snapshot: %w", err)
	}
	delta := catalog.Diff(prevState, snap)
	deltaCat := delta.AsCatalog()

	run := &store.Run{ID: r.newRunID(), TargetID: targetID, Kind: store.KindIncremental, State: store.StateRunning}
	if err := r.Store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("backup: create run: %w", err)
	}
	if err := writeItems(ctx, run.ID, src, sk, deltaCat.Items); err != nil {
		_ = r.Store.FinishRun(ctx, run.ID, store.StateFailed, 0, err.Error())
		return nil, err
	}
	if err := sk.WriteCatalog(ctx, run.ID, deltaCat); err != nil {
		_ = r.Store.FinishRun(ctx, run.ID, store.StateFailed, 0, err.Error())
		return nil, fmt.Errorf("backup: write catalog: %w", err)
	}
	if err := r.Store.FinishRun(ctx, run.ID, store.StateSucceeded, len(deltaCat.Items), ""); err != nil {
		return nil, err
	}
	return r.Store.GetRun(ctx, run.ID)
}

func writeItems(ctx context.Context, runID string, src source.Source, sk sink.Sink, items []catalog.Item) error {
	for _, it := range items {
		rc, err := src.Open(ctx, it.Path)
		if err != nil {
			return fmt.Errorf("backup: open %s: %w", it.Path, err)
		}
		writeErr := sk.WriteItem(ctx, runID, it.Path, rc)
		closeErr := rc.Close()
		if writeErr != nil {
			return fmt.Errorf("backup: write %s: %w", it.Path, writeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("backup: close %s: %w", it.Path, closeErr)
		}
	}
	return nil
}

// chain returns a target's runs from the nearest successful full run at or
// before uptoRunID through uptoRunID itself, in chronological order. It
// drops any run in between that failed (so a prior failure never breaks a
// later chain), but a failed run named by uptoRunID itself is still
// returned as the chain's last element so callers can see its error.
func (r *Runner) chain(ctx context.Context, targetID, uptoRunID string) ([]*store.Run, error) {
	runs, err := r.Store.ListRuns(ctx, targetID) // most recent first
	if err != nil {
		return nil, err
	}
	idx := -1
	for i, rn := range runs {
		if rn.ID == uptoRunID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("backup: run %s not found for target %s", uptoRunID, targetID)
	}
	fullIdx := -1
	for i := idx; i < len(runs); i++ {
		if runs[i].Kind == store.KindFull && runs[i].State == store.StateSucceeded {
			fullIdx = i
			break
		}
	}
	if fullIdx == -1 {
		return nil, ErrNoFullRun
	}
	descending := runs[idx : fullIdx+1]
	chain := make([]*store.Run, 0, len(descending))
	for i := len(descending) - 1; i >= 0; i-- {
		rn := descending[i]
		if rn.State != store.StateSucceeded && rn.ID != uptoRunID {
			continue
		}
		chain = append(chain, rn)
	}
	return chain, nil
}

// ChainCatalog reconstructs the full-state catalog as of runID by replaying
// its chain's delta catalogs in order, and also returns, for every
// surviving item, the ID of the run whose Sink holds its content — the
// information a restore needs to fetch each item from the right run.
func (r *Runner) ChainCatalog(ctx context.Context, targetID, runID string, sk sink.Sink) (catalog.Catalog, map[string]string, error) {
	chain, err := r.chain(ctx, targetID, runID)
	if err != nil {
		return catalog.Catalog{}, nil, err
	}
	var state catalog.Catalog
	owner := map[string]string{}
	for _, rn := range chain {
		c, err := sk.OpenCatalog(ctx, rn.ID)
		if err != nil {
			return catalog.Catalog{}, nil, fmt.Errorf("backup: open catalog %s: %w", rn.ID, err)
		}
		state = catalog.Apply(state, c)
		for _, it := range c.Items {
			owner[it.Path] = rn.ID
		}
		for _, p := range c.Deleted {
			delete(owner, p)
		}
	}
	return state, owner, nil
}
