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
