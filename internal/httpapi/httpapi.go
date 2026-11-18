// Package httpapi is backup-stack's JSON API: target and sync-job CRUD,
// triggering a run, run history, and stats aggregated over time. Every
// /v1 route requires the bearer token; GET /healthz doesn't.
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/geekychris/backup-stack/internal/backup"
	"github.com/geekychris/backup-stack/internal/cloudstackinfra"
	"github.com/geekychris/backup-stack/internal/filter"
	"github.com/geekychris/backup-stack/internal/runstats"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
	syncpkg "github.com/geekychris/backup-stack/internal/sync"
)

// SyncRunner runs a sync job once, recording the run (internal/sync.Runner).
type SyncRunner interface {
	Run(ctx context.Context, j *store.SyncJob) (*store.SyncRun, error)
}

// Deps are a Server's dependencies. BuildSource and ResolveSink are
// injected so the API layer never hard-codes how a Target's Type/Config
// becomes a running Source, or how its SinkRef becomes a Sink — cmd/backup-
// stack wires the real ssh/k8s/S3 factories; tests inject fakes.
type Deps struct {
	Store  *store.Store
	Runner *backup.Runner
	// BuildSource constructs the Source for a target's Type and Config.
	BuildSource func(ctx context.Context, t *store.Target) (source.Source, error)
	// ResolveSink returns the Sink a target's SinkRef names.
	ResolveSink func(t *store.Target) (sink.Sink, error)
	// Sync runs sync jobs; nil disables POST /v1/sync-jobs/{id}:run.
	Sync SyncRunner
	// Infra discovers backup-target candidates from cloud-stack's infra
	// registry; nil disables GET /v1/discover.
	Infra *cloudstackinfra.Client
	// Token is the single bearer token every request must present.
	Token  string
	Logger *slog.Logger
}

// Server is backup-stack's JSON API.
type Server struct {
	d   Deps
	mux *chi.Mux
}

// New builds a Server.
func New(d Deps) *Server {
	s := &Server{d: d}
	r := chi.NewRouter()
	r.Get("/healthz", s.healthz)
	r.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		r.Route("/v1", func(r chi.Router) {
			r.Get("/targets", s.listTargets)
			r.Post("/targets", s.createTarget)
			r.Get("/targets/{id}", s.getTarget)
			r.Put("/targets/{id}", s.updateTarget)
			r.Delete("/targets/{id}", s.deleteTarget)
			r.Post("/targets/{id}:run", s.runTarget)
			r.Get("/targets/{id}/runs", s.listRuns)

			r.Get("/sync-jobs", s.listSyncJobs)
			r.Post("/sync-jobs", s.createSyncJob)
			r.Get("/sync-jobs/{id}", s.getSyncJob)
			r.Put("/sync-jobs/{id}", s.updateSyncJob)
			r.Delete("/sync-jobs/{id}", s.deleteSyncJob)
			r.Post("/sync-jobs/{id}:run", s.runSyncJob)
			r.Get("/sync-jobs/{id}/runs", s.listSyncRuns)

			r.Get("/history", s.history)
			r.Get("/stats", s.stats)

			r.Get("/discover", s.discover)
		})
	})
	s.mux = r
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// healthz is 200 while the store answers, 503 otherwise. No auth: it's the
// Kubernetes probe and cloud-stack's health check.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.d.Store.Ping(ctx); err != nil {
		http.Error(w, "store: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if len(h) <= len(prefix) || h[:len(prefix)] != prefix ||
			subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(s.d.Token)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeStoreErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func randomID(prefix string) string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

// ---- targets ----

type targetRequest struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Config   json.RawMessage `json:"config"`
	SinkRef  string          `json:"sinkRef"`
	Schedule string          `json:"schedule"`
	Enabled  *bool           `json:"enabled"`
}

func (s *Server) listTargets(w http.ResponseWriter, r *http.Request) {
	list, err := s.d.Store.ListTargets(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) createTarget(w http.ResponseWriter, r *http.Request) {
	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "name and type are required")
		return
	}
	if err := validateTargetConfig(req.Type, req.Config); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	t := &store.Target{
		ID: randomID("tgt"), Name: req.Name, Type: req.Type, Config: req.Config,
		SinkRef: req.SinkRef, Schedule: req.Schedule, Enabled: enabled,
	}
	if t.Config == nil {
		t.Config = json.RawMessage("{}")
	}
	if err := s.d.Store.CreateTarget(r.Context(), t); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) getTarget(w http.ResponseWriter, r *http.Request) {
	t, err := s.d.Store.GetTarget(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) updateTarget(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	existing, err := s.d.Store.GetTarget(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	var req targetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.Type != "" {
		existing.Type = req.Type
	}
	if req.Config != nil {
		existing.Config = req.Config
	}
	if err := validateTargetConfig(existing.Type, existing.Config); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	existing.SinkRef = req.SinkRef
	existing.Schedule = req.Schedule
	if req.Enabled != nil {
		existing.Enabled = *req.Enabled
	}
	if err := s.d.Store.UpdateTarget(r.Context(), existing); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) deleteTarget(w http.ResponseWriter, r *http.Request) {
	if err := s.d.Store.DeleteTarget(r.Context(), chi.URLParam(r, "id")); err != nil {
		writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// runTarget runs a target now: a full backup if it has never had a
// successful one, otherwise an incremental. It runs synchronously — at the
// scale this tool targets, a request thread blocking for a backup to
// finish is simpler than a job queue, and the caller's GET .../runs
// immediately reflects the outcome. If the target already has a run in
// progress (e.g. one the Scheduler just dispatched), Store.StartRun inside
// RunFull/RunIncremental rejects the second one with store.ErrRunInFlight,
// reported here as 409.
func (s *Server) runTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	t, err := s.d.Store.GetTarget(ctx, id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	src, err := s.d.BuildSource(ctx, t)
	if err != nil {
		writeError(w, http.StatusBadGateway, "build source: "+err.Error())
		return
	}
	defer src.Close()
	sk, err := s.d.ResolveSink(t)
	if err != nil {
		writeError(w, http.StatusBadGateway, "resolve sink: "+err.Error())
		return
	}

	_, fullErr := s.d.Store.LatestRunOfKind(ctx, t.ID, store.KindFull)
	var run *store.Run
	if errors.Is(fullErr, store.ErrNotFound) {
		run, err = s.d.Runner.RunFull(ctx, t.ID, src, sk)
	} else if fullErr != nil {
		writeStoreErr(w, fullErr)
		return
	} else {
		run, err = s.d.Runner.RunIncremental(ctx, t.ID, src, sk)
	}
	if err != nil {
		if errors.Is(err, store.ErrRunInFlight) {
			writeError(w, http.StatusConflict, "target already has a run in progress")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.d.Store.ListRuns(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

// validateTargetConfig checks the parts of a target's config the API can
// check without connecting: an ssh-path target's filter rules.
func validateTargetConfig(typ string, cfg json.RawMessage) error {
	if typ != store.TypeSSHPath || len(cfg) == 0 {
		return nil
	}
	var c struct {
		Filters filter.Rules `json:"filters"`
	}
	if err := json.Unmarshal(cfg, &c); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return c.Filters.Validate()
}

// ---- sync jobs ----

type syncJobRequest struct {
	Name         string          `json:"name"`
	SourceConfig json.RawMessage `json:"sourceConfig"`
	DestConfig   json.RawMessage `json:"destConfig"`
	Filters      *filter.Rules   `json:"filters"`
	Delete       *bool           `json:"delete"`
	Schedule     string          `json:"schedule"`
	Enabled      *bool           `json:"enabled"`
}

// syncJobView is a sync job with its derived kind ("dir->s3", ...).
type syncJobView struct {
	*store.SyncJob
	Kind string `json:"kind"`
}

func viewSyncJob(j *store.SyncJob) syncJobView {
	return syncJobView{SyncJob: j, Kind: syncpkg.JobKind(j.SourceConfig, j.DestConfig)}
}

func validateSyncJob(j *store.SyncJob) error {
	if err := syncpkg.ValidateJob(j.SourceConfig, j.DestConfig); err != nil {
		return err
	}
	return j.Filters.Validate()
}

func (s *Server) listSyncJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.d.Store.ListSyncJobs(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	out := make([]syncJobView, 0, len(list))
	for _, j := range list {
		out = append(out, viewSyncJob(j))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createSyncJob(w http.ResponseWriter, r *http.Request) {
	var req syncJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	j := &store.SyncJob{
		ID: randomID("sync"), Name: req.Name, SourceConfig: req.SourceConfig, DestConfig: req.DestConfig,
		Schedule: req.Schedule, Enabled: enabled,
	}
	if req.Filters != nil {
		j.Filters = *req.Filters
	}
	if req.Delete != nil {
		j.Delete = *req.Delete
	}
	if err := validateSyncJob(j); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.d.Store.CreateSyncJob(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, viewSyncJob(j))
}

func (s *Server) getSyncJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.d.Store.GetSyncJob(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewSyncJob(j))
}

func (s *Server) updateSyncJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	existing, err := s.d.Store.GetSyncJob(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	var req syncJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.SourceConfig != nil {
		existing.SourceConfig = req.SourceConfig
	}
	if req.DestConfig != nil {
		existing.DestConfig = req.DestConfig
	}
	if req.Filters != nil {
		existing.Filters = *req.Filters
	}
	if req.Delete != nil {
		existing.Delete = *req.Delete
	}
	existing.Schedule = req.Schedule
	if req.Enabled != nil {
		existing.Enabled = *req.Enabled
	}
	if err := validateSyncJob(existing); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.d.Store.UpdateSyncJob(r.Context(), existing); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewSyncJob(existing))
}

func (s *Server) deleteSyncJob(w http.ResponseWriter, r *http.Request) {
	if err := s.d.Store.DeleteSyncJob(r.Context(), chi.URLParam(r, "id")); err != nil {
		writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// runSyncJob runs a sync job now, synchronously (like runTarget); 409 if a
// run of it is already in progress.
func (s *Server) runSyncJob(w http.ResponseWriter, r *http.Request) {
	if s.d.Sync == nil {
		writeError(w, http.StatusNotImplemented, "sync runner not configured")
		return
	}
	j, err := s.d.Store.GetSyncJob(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	run, err := s.d.Sync.Run(r.Context(), j)
	if err != nil {
		if errors.Is(err, store.ErrSyncInFlight) {
			writeError(w, http.StatusConflict, "sync job already has a run in progress")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) listSyncRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.d.Store.ListSyncRuns(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

// ---- history and stats ----

// parseWindow reads ?since= (a duration back from now: "24h", "7d", "30d";
// or an RFC 3339 time; default 7d) and ?until= (RFC 3339; default now).
func parseWindow(r *http.Request, now time.Time) (time.Time, time.Time, error) {
	until := now
	if v := r.URL.Query().Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("until: %w", err)
		}
		until = t
	}
	v := r.URL.Query().Get("since")
	if v == "" {
		v = "7d"
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, until, nil
	}
	d, err := parseDuration(v)
	if err != nil || d <= 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("since: want a duration like 24h or 7d, or an RFC 3339 time, got %q", v)
	}
	return until.Add(-d), until, nil
}

// parseDuration is time.ParseDuration plus a "d" (day) unit.
func parseDuration(v string) (time.Duration, error) {
	if n, ok := strings.CutSuffix(v, "d"); ok {
		days, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(v)
}

func historyQuery(r *http.Request, since, until time.Time) store.HistoryQuery {
	q := r.URL.Query()
	return store.HistoryQuery{Source: q.Get("source"), OwnerID: q.Get("owner"), Since: since, Until: until}
}

// history lists backup and sync runs, most recent first. Query: source
// (backup|sync), owner (a target or sync job id), since, until, limit
// (default 200).
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	since, until, err := parseWindow(r, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	q := historyQuery(r, since, until)
	q.Limit = 200
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		q.Limit = n
	}
	list, err := s.d.Store.ListHistory(r.Context(), q)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if list == nil {
		list = []*store.HistoryEntry{}
	}
	writeJSON(w, http.StatusOK, list)
}

// OwnerTotals is one target's or sync job's totals over a stats window.
type OwnerTotals struct {
	Source    string `json:"source"`
	OwnerID   string `json:"ownerId"`
	OwnerName string `json:"ownerName"`
	runstats.Totals
}

// StatsResponse is GET /v1/stats.
type StatsResponse struct {
	Since         time.Time         `json:"since"`
	Until         time.Time         `json:"until"`
	BucketSeconds int64             `json:"bucketSeconds"`
	Buckets       []runstats.Bucket `json:"buckets"`
	Totals        runstats.Totals   `json:"totals"`
	Owners        []OwnerTotals     `json:"owners"`
}

// stats aggregates runs into time buckets for the graphs, plus totals
// overall and per target / sync job. Query: source, owner, since, until,
// bucket (a duration; default 1h for windows up to 2 days, else 1d).
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	since, until, err := parseWindow(r, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	width := time.Hour
	if until.Sub(since) > 48*time.Hour {
		width = 24 * time.Hour
	}
	if v := r.URL.Query().Get("bucket"); v != "" {
		d, err := parseDuration(v)
		if err != nil || d < time.Minute {
			writeError(w, http.StatusBadRequest, "bucket must be a duration of at least 1m")
			return
		}
		width = d
	}
	if until.Sub(since)/width > 2000 {
		writeError(w, http.StatusBadRequest, "too many buckets: widen bucket or narrow since")
		return
	}
	list, err := s.d.Store.ListHistory(r.Context(), historyQuery(r, since, until))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	points := make([]runstats.Point, 0, len(list))
	byOwner := map[string]*OwnerTotals{}
	ownerPoints := map[string][]runstats.Point{}
	for _, e := range list {
		p := e.Point()
		points = append(points, p)
		k := e.Source + "/" + e.OwnerID
		if _, ok := byOwner[k]; !ok {
			byOwner[k] = &OwnerTotals{Source: e.Source, OwnerID: e.OwnerID, OwnerName: e.OwnerName}
		}
		ownerPoints[k] = append(ownerPoints[k], p)
	}
	resp := StatsResponse{
		Since: since, Until: until, BucketSeconds: int64(width / time.Second),
		Buckets: runstats.Aggregate(points, width, since, until),
		Totals:  runstats.Total(points),
		Owners:  []OwnerTotals{},
	}
	for k, o := range byOwner {
		o.Totals = runstats.Total(ownerPoints[k])
		resp.Owners = append(resp.Owners, *o)
	}
	sort.Slice(resp.Owners, func(i, j int) bool {
		if resp.Owners[i].Source != resp.Owners[j].Source {
			return resp.Owners[i].Source < resp.Owners[j].Source
		}
		return resp.Owners[i].OwnerName < resp.Owners[j].OwnerName
	})
	writeJSON(w, http.StatusOK, resp)
}

// ---- discovery ----

func (s *Server) discover(w http.ResponseWriter, r *http.Request) {
	if s.d.Infra == nil {
		writeJSON(w, http.StatusOK, []cloudstackinfra.Candidate{})
		return
	}
	candidates, err := s.d.Infra.ListCandidates(r.Context())
	if errors.Is(err, cloudstackinfra.ErrNotConfigured) {
		writeJSON(w, http.StatusOK, []cloudstackinfra.Candidate{})
		return
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, candidates)
}
