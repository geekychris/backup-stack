// Package httpapi is backup-stack's JSON API: target and sync-job CRUD,
// triggering a run, and reading run history. Every route requires a bearer
// token.
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/geekychris/backup-stack/internal/backup"
	"github.com/geekychris/backup-stack/internal/cloudstackinfra"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
)

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

			r.Get("/discover", s.discover)
		})
	})
	s.mux = r
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
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

// ---- sync jobs ----

type syncJobRequest struct {
	Name         string          `json:"name"`
	SourceConfig json.RawMessage `json:"sourceConfig"`
	DestConfig   json.RawMessage `json:"destConfig"`
	Schedule     string          `json:"schedule"`
	Enabled      *bool           `json:"enabled"`
}

func (s *Server) listSyncJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.d.Store.ListSyncJobs(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
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
	if err := s.d.Store.CreateSyncJob(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) getSyncJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.d.Store.GetSyncJob(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
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
	existing.Schedule = req.Schedule
	if req.Enabled != nil {
		existing.Enabled = *req.Enabled
	}
	if err := s.d.Store.UpdateSyncJob(r.Context(), existing); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) deleteSyncJob(w http.ResponseWriter, r *http.Request) {
	if err := s.d.Store.DeleteSyncJob(r.Context(), chi.URLParam(r, "id")); err != nil {
		writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
