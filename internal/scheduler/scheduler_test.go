package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/geekychris/backup-stack/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func dueTarget(t *testing.T, st *store.Store, id string) *store.Target {
	t.Helper()
	tgt := &store.Target{ID: id, Name: id, Type: store.TypeSSHPath, Config: []byte(`{}`), Schedule: "@every 1m", Enabled: true}
	if err := st.CreateTarget(context.Background(), tgt); err != nil {
		t.Fatal(err)
	}
	return tgt
}

func TestSchedulerFiresOnce(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	dueTarget(t, st, "t1")

	var calls int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{}, 1)

	sched := &Scheduler{
		Store: st,
		Now:   func() time.Time { return time.Now() },
		Run: func(ctx context.Context, tgt *store.Target) error {
			atomic.AddInt32(&calls, 1)
			started <- struct{}{}
			<-release
			done <- struct{}{}
			return nil
		},
	}

	sched.Poll(ctx)
	<-started // the first dispatch has definitely entered Run

	// Further ticks while that run is still in flight must not re-dispatch.
	sched.Poll(ctx)
	sched.Poll(ctx)
	sched.Poll(ctx)

	close(release)
	<-done

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("Run was called %d times across 4 polls while in flight, want 1", got)
	}
}

func TestSchedulerSkipsInFlight(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	dueTarget(t, st, "t1")
	// Simulate a run already in progress (e.g. from before a restart), with
	// no Scheduler instance having dispatched it.
	if err := st.CreateRun(ctx, &store.Run{ID: "r1", TargetID: "t1", Kind: store.KindFull, State: store.StateRunning}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	sched := &Scheduler{
		Store: st,
		Now:   func() time.Time { return time.Now() },
		Run: func(ctx context.Context, tgt *store.Target) error {
			atomic.AddInt32(&calls, 1)
			return nil
		},
	}
	sched.Poll(ctx)
	// Give any (incorrectly) dispatched goroutine a chance to run.
	time.Sleep(20 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("Run was called %d times for an in-flight target, want 0", got)
	}
}

func TestSchedulerConcurrencyCap(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	const n, extra = 3, 2
	for i := 0; i < n+extra; i++ {
		dueTarget(t, st, "t"+string(rune('a'+i)))
	}

	var cur, max int32
	started := make(chan struct{}, n+extra)
	release := make(chan struct{})

	sched := &Scheduler{
		Store:       st,
		Concurrency: n,
		Now:         func() time.Time { return time.Now() },
		Run: func(ctx context.Context, tgt *store.Target) error {
			c := atomic.AddInt32(&cur, 1)
			for {
				m := atomic.LoadInt32(&max)
				if c <= m || atomic.CompareAndSwapInt32(&max, m, c) {
					break
				}
			}
			started <- struct{}{}
			<-release
			atomic.AddInt32(&cur, -1)
			return nil
		},
	}

	sched.Poll(ctx)
	for i := 0; i < n; i++ {
		<-started // wait until exactly n calls are concurrently running
	}
	if got := atomic.LoadInt32(&max); got != int32(n) {
		t.Fatalf("max concurrent = %d, want %d", got, n)
	}

	close(release)
	for i := 0; i < extra; i++ {
		<-started // drain the remaining targets as slots free up
	}
	if got := atomic.LoadInt32(&max); got > int32(n) {
		t.Fatalf("max concurrent exceeded cap: %d > %d", got, n)
	}
}

// TestSchedulerWaitBlocksUntilDispatchedRunsFinish proves Wait doesn't
// return just because RunLoop stopped polling: it must also block until
// every goroutine dispatch started has actually returned. This is what
// cmd/backup-stack's serve relies on to avoid closing the Store while a
// scheduler-dispatched backup is still writing to it.
func TestSchedulerWaitBlocksUntilDispatchedRunsFinish(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	dueTarget(t, st, "t1")

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	sched := &Scheduler{
		Store: st,
		Now:   func() time.Time { return time.Now() },
		Run: func(ctx context.Context, tgt *store.Target) error {
			started <- struct{}{}
			<-release
			return nil
		},
	}

	sched.Poll(ctx)
	<-started

	waitReturned := make(chan struct{})
	go func() {
		sched.Wait()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
		t.Fatal("Wait returned while the dispatched run was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-waitReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return within 2s of the dispatched run finishing")
	}
}
