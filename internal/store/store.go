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
	"time"

	_ "modernc.org/sqlite" // sqlite driver

	"github.com/robfig/cron/v3"
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
}

// SyncJob one-way mirrors a prefix of backup artifacts between two
// S3-compatible endpoints.
type SyncJob struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	SourceConfig json.RawMessage `json:"sourceConfig"`
	DestConfig   json.RawMessage `json:"destConfig"`
	Schedule     string          `json:"schedule,omitempty"`
	Enabled      bool            `json:"enabled"`
	LastRunAt    *time.Time      `json:"lastRunAt,omitempty"`
	LastRunState string          `json:"lastRunState,omitempty"`
	LastRunError string          `json:"lastRunError,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
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
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

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

const runCols = `id, target_id, kind, state, item_count, error, started_at, finished_at`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	var started string
	var finished sql.NullString
	if err := row.Scan(&r.ID, &r.TargetID, &r.Kind, &r.State, &r.ItemCount, &r.Error, &started, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	r.StartedAt = parseTS(started)
	r.FinishedAt = parseTSP(finished)
	return &r, nil
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

// FinishRun records a run's outcome.
func (s *Store) FinishRun(ctx context.Context, id, state string, itemCount int, errMsg string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE runs SET state=?, item_count=?, error=?, finished_at=? WHERE id=?`,
		state, itemCount, errMsg, ts(s.now()), id)
	if err != nil {
		return err
	}
	return checkAffected(res)
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
	_, err := s.db.ExecContext(ctx, `INSERT INTO sync_jobs
		(id, name, source_config, dest_config, schedule, enabled, last_run_at, last_run_state, last_run_error, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Name, string(j.SourceConfig), string(j.DestConfig), j.Schedule, boolInt(j.Enabled),
		tsp(j.LastRunAt), j.LastRunState, j.LastRunError, ts(j.CreatedAt), ts(j.UpdatedAt))
	return err
}

const syncJobCols = `id, name, source_config, dest_config, schedule, enabled, last_run_at, last_run_state, last_run_error, created_at, updated_at`

func scanSyncJob(row interface{ Scan(...any) error }) (*SyncJob, error) {
	var j SyncJob
	var src, dst string
	var enabled int
	var lastAt sql.NullString
	var created, updated string
	if err := row.Scan(&j.ID, &j.Name, &src, &dst, &j.Schedule, &enabled, &lastAt, &j.LastRunState, &j.LastRunError, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	j.SourceConfig, j.DestConfig = json.RawMessage(src), json.RawMessage(dst)
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
	res, err := s.db.ExecContext(ctx, `UPDATE sync_jobs SET name=?, source_config=?, dest_config=?, schedule=?, enabled=?, updated_at=? WHERE id=?`,
		j.Name, string(j.SourceConfig), string(j.DestConfig), j.Schedule, boolInt(j.Enabled), ts(j.UpdatedAt), j.ID)
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

// FinishSyncJob records a sync job's outcome.
func (s *Store) FinishSyncJob(ctx context.Context, id, state, errMsg string) error {
	now := s.now()
	res, err := s.db.ExecContext(ctx, `UPDATE sync_jobs SET last_run_at=?, last_run_state=?, last_run_error=?, updated_at=? WHERE id=?`,
		ts(now), state, errMsg, ts(now), id)
	if err != nil {
		return err
	}
	return checkAffected(res)
}

// DueSyncJobs returns the enabled sync jobs whose cron Schedule's next fire
// time, computed from LastRunAt (or never), is at or before now.
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
