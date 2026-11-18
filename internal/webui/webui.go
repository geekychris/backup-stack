// Package webui is backup-stack's self-contained admin UI: plain
// server-rendered HTML with a little vanilla JS to call the JSON API (no
// npm toolchain — this is a pure-Go repository). It covers setup (creating
// targets and sync jobs, running cloud-stack discovery) and monitoring
// (status at a glance, run history, and inline-SVG graphs of bytes, files,
// errors and transfer rate over time).
//
// Every URL the page uses is relative ("v1/targets", not "/v1/targets"),
// so it works unchanged at the root of its own port (cloud-stack's app
// proxy) or under a path prefix: a proxy that strips a prefix and sends
// X-Forwarded-Prefix gets a matching <base href>.
package webui

import (
	"embed"
	"html/template"
	"net/http"
	"regexp"
	"strings"

	"github.com/geekychris/backup-stack/internal/store"
	syncpkg "github.com/geekychris/backup-stack/internal/sync"
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
	Job  *store.SyncJob
	Kind string // "dir->s3", ...
	From string // short source description
	To   string // short destination description
}

func newSyncJobView(j *store.SyncJob) SyncJobView {
	v := SyncJobView{Job: j, Kind: syncpkg.JobKind(j.SourceConfig, j.DestConfig)}
	if e, err := syncpkg.ParseEndpoint(j.SourceConfig); err == nil {
		v.From = e.String()
	} else {
		v.From = "invalid: " + err.Error()
	}
	if e, err := syncpkg.ParseEndpoint(j.DestConfig); err == nil {
		v.To = e.String()
	} else {
		v.To = "invalid: " + err.Error()
	}
	return v
}

// IndexData is what the index page template renders.
type IndexData struct {
	// BaseHref is the <base href> for a page served under a path prefix
	// (X-Forwarded-Prefix), "" otherwise.
	BaseHref   string
	Targets    []TargetView
	SyncJobs   []SyncJobView
	CloudStack bool // whether cloud-stack discovery is configured
}

var safePrefix = regexp.MustCompile(`^(/[A-Za-z0-9._~-]+)+$`)

// baseHref turns X-Forwarded-Prefix into a <base href> ("/apps/backup" ->
// "/apps/backup/"); anything that isn't a plain path is ignored.
func baseHref(r *http.Request) string {
	p := strings.TrimRight(strings.TrimSpace(r.Header.Get("X-Forwarded-Prefix")), "/")
	if p == "" || !safePrefix.MatchString(p) {
		return ""
	}
	return p + "/"
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
	data := IndexData{CloudStack: h.CloudStackConfigured, BaseHref: baseHref(r)}
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
		data.SyncJobs = append(data.SyncJobs, newSyncJobView(j))
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
