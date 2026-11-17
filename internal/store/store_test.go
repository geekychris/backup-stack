package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestTargetCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	tgt := &Target{ID: "t1", Name: "spark-etc", Type: TypeSSHPath, Config: []byte(`{"host":"spark","path":"/etc"}`), SinkRef: "primary", Schedule: "@daily", Enabled: true}
	if err := s.CreateTarget(ctx, tgt); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := s.GetTarget(ctx, "t1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "spark-etc" || got.Type != TypeSSHPath || string(got.Config) != `{"host":"spark","path":"/etc"}` {
		t.Fatalf("got %+v", got)
	}

	list, err := s.ListTargets(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}

	got.Schedule = "@hourly"
	got.Enabled = false
	if err := s.UpdateTarget(ctx, got); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, err := s.GetTarget(ctx, "t1")
	if err != nil || got2.Schedule != "@hourly" || got2.Enabled {
		t.Fatalf("after update: %+v, %v", got2, err)
	}

	// Delete must cascade to runs.
	run := &Run{ID: "r1", TargetID: "t1", Kind: KindFull, State: StateSucceeded, StartedAt: time.Now()}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.DeleteTarget(ctx, "t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetTarget(ctx, "t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if _, err := s.GetRun(ctx, "r1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the run to be cascade-deleted, got %v", err)
	}
}

func TestDueTargets(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)

	mustCreate := func(id, schedule string, lastRunAgo time.Duration, inFlight bool) {
		t.Helper()
		tgt := &Target{ID: id, Name: id, Type: TypeSSHPath, Config: []byte(`{}`), Schedule: schedule, Enabled: true}
		if err := s.CreateTarget(ctx, tgt); err != nil {
			t.Fatal(err)
		}
		if lastRunAgo >= 0 {
			state := StateSucceeded
			if inFlight {
				state = StateRunning
			}
			run := &Run{ID: id + "-run", TargetID: id, Kind: KindFull, State: state, StartedAt: now.Add(-lastRunAgo)}
			if err := s.CreateRun(ctx, run); err != nil {
				t.Fatal(err)
			}
		}
	}

	// due: never run before, hourly schedule.
	mustCreate("never-run", "@hourly", -1, false)
	// due: ran 2 hours ago, hourly schedule -> next fire is 1h after last run, which is before now.
	mustCreate("overdue", "@hourly", 2*time.Hour, false)
	// not due: ran 10 minutes ago, hourly schedule -> next fire is 50 minutes from now.
	mustCreate("recent", "@hourly", 10*time.Minute, false)
	// not due: disabled.
	disabled := &Target{ID: "disabled", Name: "disabled", Type: TypeSSHPath, Config: []byte(`{}`), Schedule: "@hourly", Enabled: false}
	if err := s.CreateTarget(ctx, disabled); err != nil {
		t.Fatal(err)
	}
	// not due: no schedule at all (manual only).
	manual := &Target{ID: "manual", Name: "manual", Type: TypeSSHPath, Config: []byte(`{}`), Schedule: "", Enabled: true}
	if err := s.CreateTarget(ctx, manual); err != nil {
		t.Fatal(err)
	}
	// excluded: overdue by schedule, but has an in-flight run.
	mustCreate("in-flight", "@hourly", 2*time.Hour, true)

	due, err := s.DueTargets(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := map[string]bool{}
	for _, t := range due {
		gotIDs[t.ID] = true
	}
	want := map[string]bool{"never-run": true, "overdue": true}
	if len(gotIDs) != len(want) {
		t.Fatalf("due = %v, want %v", gotIDs, want)
	}
	for id := range want {
		if !gotIDs[id] {
			t.Fatalf("expected %q to be due; due = %v", id, gotIDs)
		}
	}
}

func TestStartRunRejectsConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	tgt := &Target{ID: "t1", Name: "t1", Type: TypeSSHPath, Config: []byte(`{}`)}
	if err := s.CreateTarget(ctx, tgt); err != nil {
		t.Fatal(err)
	}

	first := &Run{ID: "r1", TargetID: "t1", Kind: KindFull, State: StateRunning}
	if err := s.StartRun(ctx, first); err != nil {
		t.Fatalf("first StartRun: %v", err)
	}

	second := &Run{ID: "r2", TargetID: "t1", Kind: KindFull, State: StateRunning}
	if err := s.StartRun(ctx, second); !errors.Is(err, ErrRunInFlight) {
		t.Fatalf("second StartRun = %v, want ErrRunInFlight", err)
	}
	if _, err := s.GetRun(ctx, "r2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected run should not have been inserted, got %v", err)
	}

	// Once the first run finishes, a new StartRun must succeed again.
	if err := s.FinishRun(ctx, "r1", StateSucceeded, 0, ""); err != nil {
		t.Fatal(err)
	}
	third := &Run{ID: "r3", TargetID: "t1", Kind: KindIncremental, State: StateRunning}
	if err := s.StartRun(ctx, third); err != nil {
		t.Fatalf("StartRun after prior run finished: %v", err)
	}
}

func TestReconcileStaleRuns(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	tgt := &Target{ID: "t1", Name: "t1", Type: TypeSSHPath, Config: []byte(`{}`), Enabled: true}
	if err := s.CreateTarget(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(ctx, &Run{ID: "stale", TargetID: "t1", Kind: KindFull, State: StateRunning}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(ctx, &Run{ID: "done", TargetID: "t1", Kind: KindFull, State: StateSucceeded}); err != nil {
		t.Fatal(err)
	}

	n, err := s.ReconcileStaleRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ReconcileStaleRuns reported %d rows, want 1", n)
	}

	stale, err := s.GetRun(ctx, "stale")
	if err != nil {
		t.Fatal(err)
	}
	if stale.State != StateFailed || stale.Error == "" || stale.FinishedAt == nil {
		t.Fatalf("stale run after reconcile = %+v", stale)
	}
	done, err := s.GetRun(ctx, "done")
	if err != nil {
		t.Fatal(err)
	}
	if done.State != StateSucceeded {
		t.Fatalf("already-finished run was touched by reconcile: %+v", done)
	}

	// A target whose only run is now "failed" (not "running") must become
	// due again, proving reconciliation actually unblocks its schedule.
	tgt.Schedule = "@hourly"
	if err := s.UpdateTarget(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	due, err := s.DueTargets(ctx, stale.FinishedAt.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ID != "t1" {
		t.Fatalf("due = %+v, want [t1]", due)
	}
}
