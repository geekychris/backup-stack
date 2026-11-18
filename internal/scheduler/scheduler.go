// Package scheduler periodically checks a Store for targets and sync jobs
// whose cron schedule says they are due and dispatches a run for each,
// bounding how many run at once (backups and syncs share the bound).
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/geekychris/backup-stack/internal/store"
)

// RunFunc executes one backup for a target, returning when the run is
// complete. It is expected to record the run in the Store itself (as
// internal/backup.Runner's RunFull/RunIncremental do), so a restart sees an
// accurate in-flight state even if this process's own bookkeeping is lost.
type RunFunc func(ctx context.Context, target *store.Target) error

// SyncFunc executes one run of a sync job (internal/sync.Runner.Run, which
// records it in the Store).
type SyncFunc func(ctx context.Context, job *store.SyncJob) error

// Scheduler polls a Store for due targets and dispatches RunFunc for each.
// Beyond the Store's own in-flight check (a target with a running Run row
// is never due), Scheduler also guards against dispatching the same target
// twice while its previous dispatch from this Scheduler hasn't returned yet
// — closing the race window between deciding a target is due and RunFunc
// getting around to recording that in the Store.
type Scheduler struct {
	Store *store.Store
	Run   RunFunc
	// Sync runs due sync jobs; nil leaves sync jobs unscheduled.
	Sync SyncFunc
	// PollInterval is how often RunLoop checks for due targets. Default 30s.
	PollInterval time.Duration
	// Concurrency bounds how many RunFunc calls run at once. Default 4.
	Concurrency int
	// Now is injectable for tests; defaults to time.Now.
	Now    func() time.Time
	Logger *slog.Logger

	once     sync.Once
	mu       sync.Mutex
	inFlight map[string]bool
	sem      chan struct{}
	wg       sync.WaitGroup
}

func (s *Scheduler) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Scheduler) init() {
	s.once.Do(func() {
		n := s.Concurrency
		if n <= 0 {
			n = 4
		}
		s.inFlight = map[string]bool{}
		s.sem = make(chan struct{}, n)
	})
}

// RunLoop polls on PollInterval until ctx is done.
func (s *Scheduler) RunLoop(ctx context.Context) {
	interval := s.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	s.Poll(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Poll(ctx)
		}
	}
}

// Poll runs one due-check-and-dispatch pass: targets already dispatched by
// an earlier Poll and still running are skipped.
func (s *Scheduler) Poll(ctx context.Context) {
	s.init()
	due, err := s.Store.DueTargets(ctx, s.now())
	if err != nil {
		if s.Logger != nil {
			s.Logger.Warn("scheduler: due targets", "err", err)
		}
		return
	}
	for _, t := range due {
		t := t
		s.dispatch(ctx, "target:"+t.ID, func(ctx context.Context) error { return s.Run(ctx, t) })
	}
	if s.Sync == nil {
		return
	}
	jobs, err := s.Store.DueSyncJobs(ctx, s.now())
	if err != nil {
		if s.Logger != nil {
			s.Logger.Warn("scheduler: due sync jobs", "err", err)
		}
		return
	}
	for _, j := range jobs {
		j := j
		s.dispatch(ctx, "sync:"+j.ID, func(ctx context.Context) error { return s.Sync(ctx, j) })
	}
}

// dispatch marks t in-flight synchronously (so the very next Poll call,
// even one made before this target's goroutine is scheduled, sees it as
// busy) and starts a goroutine that releases the mark and the concurrency
// slot when RunFunc returns, or if ctx ends before a slot frees up. The
// goroutine is tracked by s.wg so Wait can observe when every dispatched
// run has actually finished, not just when RunLoop stopped polling.
func (s *Scheduler) dispatch(ctx context.Context, key string, run func(context.Context) error) {
	s.mu.Lock()
	if s.inFlight[key] {
		s.mu.Unlock()
		return
	}
	s.inFlight[key] = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.inFlight, key)
			s.mu.Unlock()
		}()
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-s.sem }()
		if err := run(ctx); err != nil && s.Logger != nil {
			s.Logger.Warn("scheduler: run", "what", key, "err", err)
		}
	}()
}

// Wait blocks until every run dispatch has returned, including ones still
// in flight when RunLoop's ctx was canceled. Callers that need shutdown to
// be bounded (e.g. cmd/backup-stack's serve) should wrap this in their own
// timeout, since a RunFunc that ignores ctx cancellation could otherwise
// make Wait block indefinitely.
func (s *Scheduler) Wait() {
	s.init()
	s.wg.Wait()
}
