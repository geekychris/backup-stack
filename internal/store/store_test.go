package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/geekychris/backup-stack/internal/filter"
	"github.com/geekychris/backup-stack/internal/runstats"
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
	if err := s.FinishRun(ctx, "r1", StateSucceeded, 0, runstats.Stats{}, ""); err != nil {
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

func TestRunStatsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	start := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return start.Add(4 * time.Second) }
	if err := s.CreateTarget(ctx, &Target{ID: "t1", Name: "t1", Type: TypeSSHPath, Config: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartRun(ctx, &Run{ID: "r1", TargetID: "t1", Kind: KindFull, State: StateRunning, StartedAt: start}); err != nil {
		t.Fatal(err)
	}
	st := runstats.Stats{Bytes: 4000, Files: 3}
	st.AddErrorMsg("one")
	if err := s.FinishRun(ctx, "r1", StateFailed, 3, st, st.Summary()); err != nil {
		t.Fatal(err)
	}
	r, err := s.GetRun(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.Bytes != 4000 || r.Stats.Files != 3 || r.Stats.Errors != 1 || len(r.Stats.ErrorSamples) != 1 {
		t.Fatalf("stats = %+v", r.Stats)
	}
	if r.DurationMs != 4000 || r.RateBps != 1000 {
		t.Fatalf("duration=%d rate=%v", r.DurationMs, r.RateBps)
	}
}

func TestSyncJobFiltersAndRuns(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	j := &SyncJob{ID: "j1", Name: "photos", SourceConfig: []byte(`{"type":"dir","path":"/a"}`), DestConfig: []byte(`{"type":"dir","path":"/b"}`),
		Filters: filter.Rules{{Kind: filter.Exclude, Pattern: "*.tmp"}}, Delete: true, Enabled: true, Schedule: "@every 1m"}
	if err := s.CreateSyncJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSyncJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Filters) != 1 || got.Filters[0].Pattern != "*.tmp" || !got.Delete {
		t.Fatalf("job = %+v", got)
	}

	if err := s.StartSyncRun(ctx, &SyncRun{ID: "s1", JobID: "j1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartSyncRun(ctx, &SyncRun{ID: "s2", JobID: "j1"}); !errors.Is(err, ErrSyncInFlight) {
		t.Fatalf("second start: %v, want ErrSyncInFlight", err)
	}
	if due, _ := s.DueSyncJobs(ctx, time.Now().Add(time.Hour)); len(due) != 0 {
		t.Fatalf("in-flight job reported due: %+v", due)
	}
	if err := s.FinishSyncRun(ctx, "s1", "rsync", StateSucceeded, runstats.Stats{Bytes: 10, Files: 1, Skipped: 2}, ""); err != nil {
		t.Fatal(err)
	}
	runs, err := s.ListSyncRuns(ctx, "j1")
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %+v, %v", runs, err)
	}
	if runs[0].Method != "rsync" || runs[0].State != StateSucceeded || runs[0].Stats.Skipped != 2 {
		t.Fatalf("run = %+v", runs[0])
	}
	got, _ = s.GetSyncJob(ctx, "j1")
	if got.LastRunState != StateSucceeded || got.LastRunAt == nil {
		t.Fatalf("job last-run not updated: %+v", got)
	}
}

func TestListHistoryMergesBackupAndSync(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if err := s.CreateTarget(ctx, &Target{ID: "t1", Name: "etc", Type: TypeSSHPath, Config: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSyncJob(ctx, &SyncJob{ID: "j1", Name: "mirror"}); err != nil {
		t.Fatal(err)
	}
	fin := base.Add(time.Hour + time.Second)
	if err := s.CreateRun(ctx, &Run{ID: "r1", TargetID: "t1", Kind: KindFull, State: StateSucceeded, StartedAt: base.Add(time.Hour), FinishedAt: &fin}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartSyncRun(ctx, &SyncRun{ID: "s1", JobID: "j1", StartedAt: base.Add(2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(ctx, &Run{ID: "old", TargetID: "t1", Kind: KindFull, State: StateSucceeded, StartedAt: base.Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}

	all, err := s.ListHistory(ctx, HistoryQuery{Since: base})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].RunID != "s1" || all[0].Source != SourceSync || all[0].OwnerName != "mirror" ||
		all[1].RunID != "r1" || all[1].OwnerName != "etc" || all[1].DurationMs != 1000 {
		t.Fatalf("history = %+v %+v", all[0], all[1])
	}
	only, err := s.ListHistory(ctx, HistoryQuery{OwnerID: "t1"})
	if err != nil || len(only) != 2 {
		t.Fatalf("owner filter: %d entries, %v", len(only), err)
	}
	syncs, _ := s.ListHistory(ctx, HistoryQuery{Source: SourceSync})
	if len(syncs) != 1 {
		t.Fatalf("source filter: %d", len(syncs))
	}
}

// A database created before run history existed gains the new columns.
func TestOpenMigratesOldSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE targets (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, type TEXT NOT NULL, config TEXT NOT NULL,
		sink_ref TEXT NOT NULL DEFAULT '', schedule TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
	CREATE TABLE runs (id TEXT PRIMARY KEY, target_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, item_count INTEGER NOT NULL DEFAULT 0,
		error TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL, finished_at TEXT);
	CREATE TABLE sync_jobs (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, source_config TEXT NOT NULL, dest_config TEXT NOT NULL,
		schedule TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1, last_run_at TEXT, last_run_state TEXT NOT NULL DEFAULT '',
		last_run_error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
	INSERT INTO targets VALUES ('t1','etc','ssh-path','{}','','',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
	INSERT INTO runs (id,target_id,kind,state,started_at) VALUES ('r1','t1','full','succeeded','2026-01-01T00:00:00Z');
	INSERT INTO sync_jobs (id,name,source_config,dest_config,created_at,updated_at) VALUES ('j1','m','{}','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.GetRun(ctx, "r1")
	if err != nil || r.Stats.Bytes != 0 {
		t.Fatalf("old run: %+v, %v", r, err)
	}
	j, err := s.GetSyncJob(ctx, "j1")
	if err != nil || j.Delete || len(j.Filters) != 0 {
		t.Fatalf("old job: %+v, %v", j, err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
