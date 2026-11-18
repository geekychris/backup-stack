// Package store is backup-stack's metadata database: targets, the runs
// backing them up, and sync jobs. It is SQLite (modernc.org/sqlite, pure Go,
// no cgo) so the whole service is one self-contained binary.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // sqlite driver

	"github.com/robfig/cron/v3"

	"github.com/geekychris/backup-stack/internal/filter"
	"github.com/geekychris/backup-stack/internal/runstats"
)

//go:embed schema.sql
var schema string

// ErrNotFound is returned for a missing target, run or sync job.
var ErrNotFound = errors.New("not found")

// Target kinds.
const (
	TypeSSHPath      = "ssh-path"
	TypeK8sNamespace = "k8s-namespace"
)

// Run kinds.
const (
	KindFull        = "full"
	KindIncremental = "incremental"
)

// Run and sync-job states.
const (
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
)

// Target is one thing backup-stack knows how to back up.
type Target struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	Config    json.RawMessage `json:"config"`
	SinkRef   string          `json:"sinkRef"`
	Schedule  string          `json:"schedule,omitempty"`
	Enabled   bool            `json:"enabled"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// Run is one backup run of a Target.
type Run struct {
	ID         string     `json:"id"`
	TargetID   string     `json:"targetId"`
	Kind       string     `json:"kind"`
	State      string     `json:"state"`
	ItemCount  int        `json:"itemCount"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	// Stats is what the run moved: Files/Bytes are the items it wrote to
	// the sink (for an incremental, only the changed ones).
	Stats runstats.Stats `json:"stats"`
	// DurationMs and RateBps are derived (finished-started, bytes/duration);
	// a run still in progress has neither.
	DurationMs int64   `json:"durationMs"`
	RateBps    float64 `json:"rateBps"`
}

// SyncJob one-way copies a source endpoint's files onto a destination
// endpoint. SourceConfig and DestConfig are internal/sync endpoint configs
// ({"type":"s3"|"dir"|"ssh", ...}); Filters select which files move; Delete
// (off by default) also removes destination files the source no longer has.
type SyncJob struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	SourceConfig json.RawMessage `json:"sourceConfig"`
	DestConfig   json.RawMessage `json:"destConfig"`
	Filters      filter.Rules    `json:"filters"`
	Delete       bool            `json:"delete"`
	Schedule     string          `json:"schedule,omitempty"`
	Enabled      bool            `json:"enabled"`
	LastRunAt    *time.Time      `json:"lastRunAt,omitempty"`
	LastRunState string          `json:"lastRunState,omitempty"`
	LastRunError string          `json:"lastRunError,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
}

// SyncRun is one execution of a SyncJob.
type SyncRun struct {
	ID    string `json:"id"`
	JobID string `json:"jobId"`
	// Method is how the files moved: "rsync" or "native".
	Method     string         `json:"method"`
	State      string         `json:"state"`
	Error      string         `json:"error,omitempty"`
	StartedAt  time.Time      `json:"startedAt"`
	FinishedAt *time.Time     `json:"finishedAt,omitempty"`
	Stats      runstats.Stats `json:"stats"`
	DurationMs int64          `json:"durationMs"`
	RateBps    float64        `json:"rateBps"`
}

// Store is the SQLite-backed metadata store.
type Store struct {
	db *sql.DB
	// Now is injectable for tests; defaults to time.Now().UTC().
	Now func() time.Time
}

var memSeq int

// Open opens (and migrates) a SQLite database at path. path may be
// ":memory:" for an ephemeral store (tests): each call gets its own,
// uniquely named in-memory database so concurrent tests never collide.
func Open(ctx context.Context, path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)"
	if path == ":memory:" {
		memSeq++
		dsn = fmt.Sprintf("file:backupstack%d?mode=memory&cache=shared&_pragma=foreign_keys(on)", memSeq)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	if err := s.addMissingColumns(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// columnMigrations are columns added to tables after their first release:
// CREATE TABLE IF NOT EXISTS leaves an existing table alone, so Open adds
// any of these a database lacks.
var columnMigrations = []struct{ table, column, ddl string }{
	{"runs", "bytes", "INTEGER NOT NULL DEFAULT 0"},
	{"runs", "files", "INTEGER NOT NULL DEFAULT 0"},
	{"runs", "skipped", "INTEGER NOT NULL DEFAULT 0"},
	{"runs", "error_count", "INTEGER NOT NULL DEFAULT 0"},
	{"runs", "error_samples", "TEXT NOT NULL DEFAULT '[]'"},
	{"sync_jobs", "filters", "TEXT NOT NULL DEFAULT '[]'"},
	{"sync_jobs", "delete_extra", "INTEGER NOT NULL DEFAULT 0"},
}

func (s *Store) addMissingColumns(ctx context.Context) error {
	have := map[string]map[string]bool{}
	for _, m := range columnMigrations {
		cols, ok := have[m.table]
		if !ok {
			cols = map[string]bool{}
			rows, err := s.db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, m.table)
			if err != nil {
				return fmt.Errorf("store: migrate: %s columns: %w", m.table, err)
			}
			for rows.Next() {
				var n string
				if err := rows.Scan(&n); err != nil {
					rows.Close()
					return err
				}
				cols[n] = true
			}
			rows.Close()
			have[m.table] = cols
		}
		if cols[m.column] {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+m.table+` ADD COLUMN `+m.column+` `+m.ddl); err != nil {
			return fmt.Errorf("store: migrate: add %s.%s: %w", m.table, m.column, err)
		}
		cols[m.column] = true
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks the database answers (GET /healthz).
func (s *Store) Ping(ctx context.Context) error {
	var one int
	return s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// tsLayout is fixed-width (unlike RFC3339Nano, which trims trailing zeros)
// so stored timestamps sort and compare correctly as text.
const tsLayout = "2006-01-02T15:04:05.000000000Z07:00"

func ts(t time.Time) string { return t.UTC().Format(tsLayout) }

func tsp(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}

func parseTSP(v sql.NullString) *time.Time {
	if !v.Valid || v.String == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, v.String)
	if err != nil {
		return nil
	}
	return &t
}

func parseTS(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// ---- targets ----

// CreateTarget inserts a target. ID and Name must be unique.
func (s *Store) CreateTarget(ctx context.Context, t *Target) error {
	now := s.now()
	t.CreatedAt, t.UpdatedAt = now, now
	if t.Config == nil {
		t.Config = json.RawMessage("{}")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO targets
		(id, name, type, config, sink_ref, schedule, enabled, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		t.ID, t.Name, t.Type, string(t.Config), t.SinkRef, t.Schedule, boolInt(t.Enabled), ts(t.CreatedAt), ts(t.UpdatedAt))
	return err
}

const targetCols = `id, name, type, config, sink_ref, schedule, enabled, created_at, updated_at`

func scanTarget(row interface{ Scan(...any) error }) (*Target, error) {
	var t Target
	var config string
	var enabled int
	var created, updated string
	if err := row.Scan(&t.ID, &t.Name, &t.Type, &config, &t.SinkRef, &t.Schedule, &enabled, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.Config = json.RawMessage(config)
	t.Enabled = enabled != 0
	t.CreatedAt, t.UpdatedAt = parseTS(created), parseTS(updated)
	return &t, nil
}

// GetTarget returns a target by ID, or ErrNotFound.
func (s *Store) GetTarget(ctx context.Context, id string) (*Target, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+targetCols+` FROM targets WHERE id = ?`, id)
	return scanTarget(row)
}

// ListTargets returns every target, ordered by name.
func (s *Store) ListTargets(ctx context.Context) ([]*Target, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+targetCols+` FROM targets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Target
	for rows.Next() {
		t, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTarget replaces a target's mutable fields by ID.
func (s *Store) UpdateTarget(ctx context.Context, t *Target) error {
	t.UpdatedAt = s.now()
	res, err := s.db.ExecContext(ctx, `UPDATE targets SET name=?, type=?, config=?, sink_ref=?, schedule=?, enabled=?, updated_at=? WHERE id=?`,
		t.Name, t.Type, string(t.Config), t.SinkRef, t.Schedule, boolInt(t.Enabled), ts(t.UpdatedAt), t.ID)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

// DeleteTarget removes a target and, via ON DELETE CASCADE, its runs.
func (s *Store) DeleteTarget(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM targets WHERE id=?`, id)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

func checkAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---- runs ----

// CreateRun inserts a run.
func (s *Store) CreateRun(ctx context.Context, r *Run) error {
	if r.StartedAt.IsZero() {
		r.StartedAt = s.now()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO runs
		(id, target_id, kind, state, item_count, error, started_at, finished_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		r.ID, r.TargetID, r.Kind, r.State, r.ItemCount, r.Error, ts(r.StartedAt), tsp(r.FinishedAt))
	return err
}

// ErrRunInFlight is returned by StartRun when the target already has a run
// in state "running". Two concurrent starts for the same target (e.g. the
// Scheduler dispatching one while a manual "run now" is also requested)
// would otherwise both diff against the same prior catalog and write
// overlapping delta catalogs, corrupting what a later restore reconstructs.
var ErrRunInFlight = errors.New("target already has a run in progress")

// StartRun atomically checks that r.TargetID has no in-flight run and
// inserts r (which must already be in StateRunning), inside one
// transaction, so two callers racing to start a run for the same target can
// never both pass the check: whichever commits first wins, and the other
// gets ErrRunInFlight.
func (s *Store) StartRun(ctx context.Context, r *Run) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: start run: begin: %w", err)
	}
	defer tx.Rollback()

	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM runs WHERE target_id=? AND state=?`, r.TargetID, StateRunning).Scan(&n); err != nil {
		return fmt.Errorf("store: start run: check in-flight: %w", err)
	}
	if n > 0 {
		return ErrRunInFlight
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = s.now()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs
		(id, target_id, kind, state, item_count, error, started_at, finished_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		r.ID, r.TargetID, r.Kind, r.State, r.ItemCount, r.Error, ts(r.StartedAt), tsp(r.FinishedAt)); err != nil {
		return fmt.Errorf("store: start run: insert: %w", err)
	}
	return tx.Commit()
}

const runCols = `id, target_id, kind, state, item_count, error, started_at, finished_at, bytes, files, skipped, error_count, error_samples`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	var started, samples string
	var finished sql.NullString
	if err := row.Scan(&r.ID, &r.TargetID, &r.Kind, &r.State, &r.ItemCount, &r.Error, &started, &finished,
		&r.Stats.Bytes, &r.Stats.Files, &r.Stats.Skipped, &r.Stats.Errors, &samples); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	r.StartedAt = parseTS(started)
	r.FinishedAt = parseTSP(finished)
	r.Stats.ErrorSamples = parseSamples(samples)
	r.DurationMs, r.RateBps = derived(r.StartedAt, r.FinishedAt, r.Stats.Bytes)
	return &r, nil
}

func parseSamples(s string) []string {
	var out []string
	_ = json.Unmarshal([]byte(s), &out)
	return out
}

func samplesJSON(s []string) string {
	if len(s) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(s)
	return string(b)
}

// derived computes a finished run's duration and rate.
func derived(start time.Time, finished *time.Time, bytes int64) (int64, float64) {
	if finished == nil {
		return 0, 0
	}
	d := finished.Sub(start)
	if d < 0 {
		d = 0
	}
	return d.Milliseconds(), runstats.Rate(bytes, d)
}

// GetRun returns a run by ID, or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, id string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id)
	return scanRun(row)
}

// ListRuns returns a target's runs, most recent first.
func (s *Store) ListRuns(ctx context.Context, targetID string) ([]*Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runCols+` FROM runs WHERE target_id=? ORDER BY started_at DESC`, targetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestRun returns a target's most recent run (any kind, including ones
// still running), or ErrNotFound if it has none.
func (s *Store) LatestRun(ctx context.Context, targetID string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE target_id=? ORDER BY started_at DESC LIMIT 1`, targetID)
	return scanRun(row)
}

// LatestRunOfKind returns a target's most recent run of a given kind that
// has finished successfully, or ErrNotFound. Used to find the last full run
// an incremental chain must build on.
func (s *Store) LatestRunOfKind(ctx context.Context, targetID, kind string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE target_id=? AND kind=? AND state=? ORDER BY started_at DESC LIMIT 1`,
		targetID, kind, StateSucceeded)
	return scanRun(row)
}

// FinishRun records a run's outcome and what it moved.
func (s *Store) FinishRun(ctx context.Context, id, state string, itemCount int, st runstats.Stats, errMsg string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET state=?, item_count=?, error=?, finished_at=?,
		bytes=?, files=?, skipped=?, error_count=?, error_samples=? WHERE id=?`,
		state, itemCount, errMsg, ts(s.now()), st.Bytes, st.Files, st.Skipped, st.Errors, samplesJSON(st.ErrorSamples), id)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

// ReconcileStaleRuns marks every run still in state "running" as failed.
// Call this once, at process startup before dispatching any new runs: a
// "running" row left behind by an unclean shutdown or crash has no process
// actually working on it, yet DueTargets/hasInFlightRun treat any running
// row as blocking that target from ever being scheduled again — without
// this, such a row would stall the target's schedule forever.
// Sync runs get the same treatment, for the same reason (DueSyncJobs).
func (s *Store) ReconcileStaleRuns(ctx context.Context) (int, error) {
	const msg = "interrupted: process restarted before run finished"
	total := 0
	for _, table := range []string{"runs", "sync_runs"} {
		res, err := s.db.ExecContext(ctx, `UPDATE `+table+` SET state=?, error=?, finished_at=? WHERE state=?`,
			StateFailed, msg, ts(s.now()), StateRunning)
		if err != nil {
			return 0, fmt.Errorf("store: reconcile stale runs: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("store: reconcile stale runs: %w", err)
		}
		total += int(n)
	}
	return total, nil
}

// hasInFlightRun reports whether a target has a run in state "running".
func (s *Store) hasInFlightRun(ctx context.Context, targetID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM runs WHERE target_id=? AND state=?`, targetID, StateRunning).Scan(&n)
	return n > 0, err
}

// DueTargets returns the enabled targets whose cron Schedule's next fire
// time, computed from their most recent run (or never, for a target with
// none), is at or before now. A target with a run still in state "running"
// is never due, regardless of its schedule.
func (s *Store) DueTargets(ctx context.Context, now time.Time) ([]*Target, error) {
	targets, err := s.ListTargets(ctx)
	if err != nil {
		return nil, err
	}
	var due []*Target
	for _, t := range targets {
		if !t.Enabled || t.Schedule == "" {
			continue
		}
		sched, err := cron.ParseStandard(t.Schedule)
		if err != nil {
			continue
		}
		inFlight, err := s.hasInFlightRun(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		if inFlight {
			continue
		}
		last, err := s.LatestRun(ctx, t.ID)
		from := time.Time{}
		if err == nil {
			from = last.StartedAt
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if !sched.Next(from).After(now) {
			due = append(due, t)
		}
	}
	return due, nil
}

// ---- sync jobs ----

// CreateSyncJob inserts a sync job. ID and Name must be unique.
func (s *Store) CreateSyncJob(ctx context.Context, j *SyncJob) error {
	now := s.now()
	j.CreatedAt, j.UpdatedAt = now, now
	if j.SourceConfig == nil {
		j.SourceConfig = json.RawMessage("{}")
	}
	if j.DestConfig == nil {
		j.DestConfig = json.RawMessage("{}")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO sync_jobs
		(id, name, source_config, dest_config, schedule, enabled, last_run_at, last_run_state, last_run_error, created_at, updated_at, filters, delete_extra)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Name, string(j.SourceConfig), string(j.DestConfig), j.Schedule, boolInt(j.Enabled),
		tsp(j.LastRunAt), j.LastRunState, j.LastRunError, ts(j.CreatedAt), ts(j.UpdatedAt), filtersJSON(j.Filters), boolInt(j.Delete))
	return err
}

func filtersJSON(f filter.Rules) string {
	if len(f) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(f)
	return string(b)
}

const syncJobCols = `id, name, source_config, dest_config, schedule, enabled, last_run_at, last_run_state, last_run_error, created_at, updated_at, filters, delete_extra`

func scanSyncJob(row interface{ Scan(...any) error }) (*SyncJob, error) {
	var j SyncJob
	var src, dst, filters string
	var enabled, del int
	var lastAt sql.NullString
	var created, updated string
	if err := row.Scan(&j.ID, &j.Name, &src, &dst, &j.Schedule, &enabled, &lastAt, &j.LastRunState, &j.LastRunError, &created, &updated, &filters, &del); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	j.SourceConfig, j.DestConfig = json.RawMessage(src), json.RawMessage(dst)
	_ = json.Unmarshal([]byte(filters), &j.Filters)
	j.Delete = del != 0
	j.Enabled = enabled != 0
	j.LastRunAt = parseTSP(lastAt)
	j.CreatedAt, j.UpdatedAt = parseTS(created), parseTS(updated)
	return &j, nil
}

// GetSyncJob returns a sync job by ID, or ErrNotFound.
func (s *Store) GetSyncJob(ctx context.Context, id string) (*SyncJob, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+syncJobCols+` FROM sync_jobs WHERE id = ?`, id)
	return scanSyncJob(row)
}

// ListSyncJobs returns every sync job, ordered by name.
func (s *Store) ListSyncJobs(ctx context.Context) ([]*SyncJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+syncJobCols+` FROM sync_jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SyncJob
	for rows.Next() {
		j, err := scanSyncJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// UpdateSyncJob replaces a sync job's mutable fields by ID.
func (s *Store) UpdateSyncJob(ctx context.Context, j *SyncJob) error {
	j.UpdatedAt = s.now()
	res, err := s.db.ExecContext(ctx, `UPDATE sync_jobs SET name=?, source_config=?, dest_config=?, schedule=?, enabled=?, updated_at=?, filters=?, delete_extra=? WHERE id=?`,
		j.Name, string(j.SourceConfig), string(j.DestConfig), j.Schedule, boolInt(j.Enabled), ts(j.UpdatedAt), filtersJSON(j.Filters), boolInt(j.Delete), j.ID)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

// DeleteSyncJob removes a sync job.
func (s *Store) DeleteSyncJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM sync_jobs WHERE id=?`, id)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

// DueSyncJobs returns the enabled sync jobs whose cron Schedule's next fire
// time, computed from LastRunAt (or never), is at or before now. A job with
// a sync run still in state "running" is never due.
func (s *Store) DueSyncJobs(ctx context.Context, now time.Time) ([]*SyncJob, error) {
	jobs, err := s.ListSyncJobs(ctx)
	if err != nil {
		return nil, err
	}
	var due []*SyncJob
	for _, j := range jobs {
		if !j.Enabled || j.Schedule == "" {
			continue
		}
		sched, err := cron.ParseStandard(j.Schedule)
		if err != nil {
			continue
		}
		var inFlight int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM sync_runs WHERE job_id=? AND state=?`, j.ID, StateRunning).Scan(&inFlight); err != nil {
			return nil, err
		}
		if inFlight > 0 {
			continue
		}
		from := time.Time{}
		if j.LastRunAt != nil {
			from = *j.LastRunAt
		}
		if !sched.Next(from).After(now) {
			due = append(due, j)
		}
	}
	return due, nil
}

// ---- sync runs ----

// ErrSyncInFlight is returned by StartSyncRun when the job already has a
// run in state "running".
var ErrSyncInFlight = errors.New("sync job already has a run in progress")

// StartSyncRun atomically checks that r.JobID has no in-flight run and
// inserts r in state "running".
func (s *Store) StartSyncRun(ctx context.Context, r *SyncRun) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: start sync run: begin: %w", err)
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM sync_runs WHERE job_id=? AND state=?`, r.JobID, StateRunning).Scan(&n); err != nil {
		return fmt.Errorf("store: start sync run: check in-flight: %w", err)
	}
	if n > 0 {
		return ErrSyncInFlight
	}
	r.State = StateRunning
	if r.StartedAt.IsZero() {
		r.StartedAt = s.now()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sync_runs (id, job_id, method, state, started_at) VALUES (?,?,?,?,?)`,
		r.ID, r.JobID, r.Method, r.State, ts(r.StartedAt)); err != nil {
		return fmt.Errorf("store: start sync run: insert: %w", err)
	}
	return tx.Commit()
}

// FinishSyncRun records a sync run's outcome, and the job's last-run
// summary with it.
func (s *Store) FinishSyncRun(ctx context.Context, id, method, state string, st runstats.Stats, errMsg string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := ts(s.now())
	res, err := tx.ExecContext(ctx, `UPDATE sync_runs SET method=?, state=?, error=?, finished_at=?,
		bytes=?, files=?, skipped=?, deleted=?, error_count=?, error_samples=? WHERE id=?`,
		method, state, errMsg, now, st.Bytes, st.Files, st.Skipped, st.Deleted, st.Errors, samplesJSON(st.ErrorSamples), id)
	if err != nil {
		return err
	}
	if err := checkAffected(res); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sync_jobs SET last_run_at=?, last_run_state=?, last_run_error=?
		WHERE id=(SELECT job_id FROM sync_runs WHERE id=?)`, now, state, errMsg, id); err != nil {
		return err
	}
	return tx.Commit()
}

const syncRunCols = `id, job_id, method, state, error, started_at, finished_at, bytes, files, skipped, deleted, error_count, error_samples`

func scanSyncRun(row interface{ Scan(...any) error }) (*SyncRun, error) {
	var r SyncRun
	var started, samples string
	var finished sql.NullString
	if err := row.Scan(&r.ID, &r.JobID, &r.Method, &r.State, &r.Error, &started, &finished,
		&r.Stats.Bytes, &r.Stats.Files, &r.Stats.Skipped, &r.Stats.Deleted, &r.Stats.Errors, &samples); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	r.StartedAt = parseTS(started)
	r.FinishedAt = parseTSP(finished)
	r.Stats.ErrorSamples = parseSamples(samples)
	r.DurationMs, r.RateBps = derived(r.StartedAt, r.FinishedAt, r.Stats.Bytes)
	return &r, nil
}

// GetSyncRun returns a sync run by ID, or ErrNotFound.
func (s *Store) GetSyncRun(ctx context.Context, id string) (*SyncRun, error) {
	return scanSyncRun(s.db.QueryRowContext(ctx, `SELECT `+syncRunCols+` FROM sync_runs WHERE id=?`, id))
}

// ListSyncRuns returns a job's runs, most recent first.
func (s *Store) ListSyncRuns(ctx context.Context, jobID string) ([]*SyncRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+syncRunCols+` FROM sync_runs WHERE job_id=? ORDER BY started_at DESC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SyncRun
	for rows.Next() {
		r, err := scanSyncRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- history ----

// History sources.
const (
	SourceBackup = "backup"
	SourceSync   = "sync"
)

// HistoryEntry is one backup run or sync run, in the shape the history
// table and the graphs use.
type HistoryEntry struct {
	// Source is "backup" (a target's run) or "sync" (a sync job's run).
	Source string `json:"source"`
	// OwnerID / OwnerName are the target's or sync job's.
	OwnerID   string `json:"ownerId"`
	OwnerName string `json:"ownerName"`
	RunID     string `json:"runId"`
	// Kind is full/incremental for a backup run, rsync/native for a sync run.
	Kind       string         `json:"kind"`
	State      string         `json:"state"`
	Error      string         `json:"error,omitempty"`
	StartedAt  time.Time      `json:"startedAt"`
	FinishedAt *time.Time     `json:"finishedAt,omitempty"`
	Stats      runstats.Stats `json:"stats"`
	DurationMs int64          `json:"durationMs"`
	RateBps    float64        `json:"rateBps"`
}

// Point converts e for runstats.Aggregate.
func (e *HistoryEntry) Point() runstats.Point {
	return runstats.Point{
		Start: e.StartedAt, Duration: time.Duration(e.DurationMs) * time.Millisecond,
		Failed: e.State == StateFailed, Stats: e.Stats,
	}
}

// HistoryQuery narrows ListHistory. Zero values mean "any".
type HistoryQuery struct {
	Source  string // "backup" or "sync"
	OwnerID string
	Since   time.Time
	Until   time.Time
	Limit   int // default (and max) 5000
}

// ListHistory returns backup and sync runs together, most recent first.
func (s *Store) ListHistory(ctx context.Context, q HistoryQuery) ([]*HistoryEntry, error) {
	var where []string
	var args []any
	if q.Source != "" {
		where = append(where, "source = ?")
		args = append(args, q.Source)
	}
	if q.OwnerID != "" {
		where = append(where, "owner_id = ?")
		args = append(args, q.OwnerID)
	}
	if !q.Since.IsZero() {
		where = append(where, "started_at >= ?")
		args = append(args, ts(q.Since))
	}
	if !q.Until.IsZero() {
		where = append(where, "started_at <= ?")
		args = append(args, ts(q.Until))
	}
	limit := q.Limit
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	query := `SELECT source, owner_id, owner_name, run_id, kind, state, error, started_at, finished_at,
		bytes, files, skipped, deleted, error_count, error_samples FROM (
		  SELECT 'backup' AS source, r.target_id AS owner_id, t.name AS owner_name, r.id AS run_id, r.kind AS kind,
		         r.state AS state, r.error AS error, r.started_at AS started_at, r.finished_at AS finished_at,
		         r.bytes AS bytes, r.files AS files, r.skipped AS skipped, 0 AS deleted,
		         r.error_count AS error_count, r.error_samples AS error_samples
		    FROM runs r JOIN targets t ON t.id = r.target_id
		  UNION ALL
		  SELECT 'sync', s.job_id, j.name, s.id, s.method, s.state, s.error, s.started_at, s.finished_at,
		         s.bytes, s.files, s.skipped, s.deleted, s.error_count, s.error_samples
		    FROM sync_runs s JOIN sync_jobs j ON j.id = s.job_id
		)`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY started_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: history: %w", err)
	}
	defer rows.Close()
	var out []*HistoryEntry
	for rows.Next() {
		var e HistoryEntry
		var started, samples string
		var finished sql.NullString
		if err := rows.Scan(&e.Source, &e.OwnerID, &e.OwnerName, &e.RunID, &e.Kind, &e.State, &e.Error, &started, &finished,
			&e.Stats.Bytes, &e.Stats.Files, &e.Stats.Skipped, &e.Stats.Deleted, &e.Stats.Errors, &samples); err != nil {
			return nil, err
		}
		e.StartedAt = parseTS(started)
		e.FinishedAt = parseTSP(finished)
		e.Stats.ErrorSamples = parseSamples(samples)
		e.DurationMs, e.RateBps = derived(e.StartedAt, e.FinishedAt, e.Stats.Bytes)
		out = append(out, &e)
	}
	return out, rows.Err()
}
