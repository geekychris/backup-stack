// Package webui is backup-stack's self-contained admin UI: plain
// server-rendered HTML with a little vanilla JS to call the JSON API (no
// npm toolchain — this is a pure-Go repository). It covers setup (creating
// targets and sync jobs, running cloud-stack discovery) and monitoring
// (target and run status at a glance).
package webui

import (
	"embed"
	"html/template"
	"net/http"

	"github.com/geekychris/backup-stack/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

var tmpl = template.Must(template.New("webui").Funcs(template.FuncMap{
	"statusBadge": statusBadge,
}).ParseFS(templateFS, "templates/*.html"))

// TargetView is one target row the targets-list template renders.
type TargetView struct {
	Target  *store.Target
	LastRun *store.Run // nil if the target has never run
}

// statusBadge is the short text the targets-list template shows for a
// target's most recent run.
func statusBadge(v TargetView) string {
	if v.LastRun == nil {
		return "never run"
	}
	switch v.LastRun.State {
	case store.StateRunning:
		return "running"
	case store.StateSucceeded:
		return "succeeded (" + v.LastRun.Kind + ")"
	case store.StateFailed:
		return "failed: " + v.LastRun.Error
	default:
		return v.LastRun.State
	}
}

// SyncJobView is one sync job row the sync-jobs-list template renders.
type SyncJobView struct {
	Job *store.SyncJob
}

// IndexData is what the index page template renders.
type IndexData struct {
	Targets    []TargetView
	SyncJobs   []SyncJobView
	CloudStack bool // whether cloud-stack discovery is configured
}

// Handler serves backup-stack's admin UI.
type Handler struct {
	Store *store.Store
	// CloudStackConfigured controls whether the page offers the "discover
	// from cloud-stack" action.
	CloudStackConfigured bool
}

// ServeHTTP implements http.Handler: the single index page at "/" (and
// "/index.html").
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
	targets, err := h.Store.ListTargets(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := IndexData{CloudStack: h.CloudStackConfigured}
	for _, t := range targets {
		tv := TargetView{Target: t}
		if last, err := h.Store.LatestRun(ctx, t.ID); err == nil {
			tv.LastRun = last
		}
		data.Targets = append(data.Targets, tv)
	}
	jobs, err := h.Store.ListSyncJobs(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, j := range jobs {
		data.SyncJobs = append(data.SyncJobs, SyncJobView{Job: j})
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
