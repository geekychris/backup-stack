package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/robfig/cron/v3"

	"github.com/geekychris/backup-stack/internal/clusters"
	"github.com/geekychris/backup-stack/internal/filter"
	k8ssrc "github.com/geekychris/backup-stack/internal/source/k8s"
	"github.com/geekychris/backup-stack/internal/store"
)

// SinkInfo describes a destination for the UI, without its credentials.
type SinkInfo struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix,omitempty"`
}

// targetView is a target with what it copies, in words: where (Cluster is
// a cluster's name for a k8s target, the host for an ssh one), which
// namespaces, and a one-line What and a longer Copies for the list.
type targetView struct {
	*store.Target
	Cluster    string     `json:"cluster"`
	Namespaces []string   `json:"namespaces,omitempty"`
	What       string     `json:"what"`
	Copies     string     `json:"copies"`
	LastRun    *store.Run `json:"lastRun,omitempty"`
}

type k8sConfig struct {
	KubeconfigPath string   `json:"kubeconfigPath"`
	Namespaces     []string `json:"namespaces"`
	ExcludeKinds   []string `json:"excludeKinds"`
}

type sshConfig struct {
	Addr    string       `json:"addr"`
	User    string       `json:"user"`
	Path    string       `json:"path"`
	Filters filter.Rules `json:"filters"`
}

func (s *Server) clusterName(kubeconfigPath string) string {
	if s.d.Clusters != nil {
		return s.d.Clusters.NameFor(kubeconfigPath)
	}
	if kubeconfigPath == "" {
		return "local"
	}
	return kubeconfigPath
}

func (s *Server) viewTarget(t *store.Target) targetView {
	v := targetView{Target: t}
	switch t.Type {
	case store.TypeK8sNamespace:
		var c k8sConfig
		_ = json.Unmarshal(t.Config, &c)
		v.Cluster = s.clusterName(c.KubeconfigPath)
		v.Namespaces = c.Namespaces
		v.What = "Kubernetes objects"
		ex := map[string]bool{}
		for _, k := range c.ExcludeKinds {
			ex[k] = true
		}
		n := 0
		for _, k := range k8ssrc.KindNames() {
			if !ex[k] {
				n++
			}
		}
		v.Copies = fmt.Sprintf("The manifests to recreate it (%d kinds: ConfigMaps, Secrets, Services, Deployments, StatefulSets, PVC claims, …). Not the files on its volumes, nor what its databases hold.", n)
		if len(c.ExcludeKinds) > 0 {
			v.Copies += " Leaves out " + strings.Join(c.ExcludeKinds, ", ") + "."
		}
	case store.TypeSSHPath:
		var c sshConfig
		_ = json.Unmarshal(t.Config, &c)
		host := c.Addr
		if h, _, ok := strings.Cut(c.Addr, ":"); ok {
			host = h
		}
		v.Cluster = host
		v.What = "Files over SSH"
		user := c.User
		if user != "" {
			user += "@"
		}
		v.Copies = "Every file under " + user + c.Addr + ":" + c.Path
		if n := len(c.Filters); n > 0 {
			v.Copies += fmt.Sprintf(", as %s choose", plural(n, "filter rule"))
		}
		v.Copies += "."
	default:
		v.What = t.Type
	}
	return v
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	if n == 0 {
		return "no " + word + "s"
	}
	return fmt.Sprintf("%d %ss", n, word)
}

var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// validateTargetConfig checks what can be checked without connecting: a
// k8s target names namespaces, known kinds and a known cluster; an ssh
// target has a host, a path and valid filter rules.
func (s *Server) validateTargetConfig(typ string, cfg json.RawMessage) error {
	if len(cfg) == 0 {
		cfg = json.RawMessage("{}")
	}
	switch typ {
	case store.TypeK8sNamespace:
		var c k8sConfig
		if err := json.Unmarshal(cfg, &c); err != nil {
			return fmt.Errorf("config: %w", err)
		}
		if len(c.Namespaces) == 0 {
			return fmt.Errorf("config: pick at least one namespace")
		}
		for _, n := range c.Namespaces {
			if !dnsLabel.MatchString(n) {
				return fmt.Errorf("config: %q is not a namespace name", n)
			}
		}
		known := map[string]bool{}
		for _, k := range k8ssrc.KindNames() {
			known[k] = true
		}
		for _, k := range c.ExcludeKinds {
			if !known[k] {
				return fmt.Errorf("config: excludeKinds: unknown kind %q (one of %s)", k, strings.Join(k8ssrc.KindNames(), ", "))
			}
		}
		if s.d.Clusters != nil && !s.d.Clusters.Known(c.KubeconfigPath) {
			return fmt.Errorf("config: kubeconfigPath %q is not one of the clusters (GET v1/clusters)", c.KubeconfigPath)
		}
	case store.TypeSSHPath:
		var c sshConfig
		if err := json.Unmarshal(cfg, &c); err != nil {
			return fmt.Errorf("config: %w", err)
		}
		if c.Addr == "" || c.Path == "" {
			return fmt.Errorf("config: an ssh-path backup needs addr (host[:port]) and path")
		}
		return c.Filters.Validate()
	default:
		return fmt.Errorf("unknown type %q (ssh-path or k8s-namespace)", typ)
	}
	return nil
}

func validateSchedule(expr string) error {
	if expr == "" {
		return nil
	}
	if _, err := cron.ParseStandard(expr); err != nil {
		return fmt.Errorf("schedule %q: %v", expr, err)
	}
	return nil
}

// previewSchedule says when a schedule would run next:
// GET v1/schedule?expr=… → {valid, error?, next: [3 times]}.
func (s *Server) previewSchedule(w http.ResponseWriter, r *http.Request) {
	expr := strings.TrimSpace(r.URL.Query().Get("expr"))
	out := map[string]any{"valid": true, "next": []time.Time{}}
	if expr == "" {
		out["manual"] = true
		writeJSON(w, http.StatusOK, out)
		return
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	next := []time.Time{}
	t := time.Now()
	for i := 0; i < 3; i++ {
		t = sched.Next(t)
		next = append(next, t)
	}
	out["next"] = next
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listSinks(w http.ResponseWriter, _ *http.Request) {
	out := append([]SinkInfo{}, s.d.Sinks...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listClusters(w http.ResponseWriter, _ *http.Request) {
	kinds := make([]map[string]string, 0, len(k8ssrc.Kinds))
	for _, k := range k8ssrc.Kinds {
		kinds = append(kinds, map[string]string{"name": k.Name, "about": k.About})
	}
	list := []clusters.Cluster{}
	if s.d.Clusters != nil {
		list = s.d.Clusters.List()
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": list, "kinds": kinds})
}

// coverage is a target that covers a namespace.
type coverage struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	What    string `json:"what"`
	Enabled bool   `json:"enabled"`
}

// clusterNamespaces is a cluster's namespaces with what is in each and
// which targets already back it up.
func (s *Server) clusterNamespaces(w http.ResponseWriter, r *http.Request) {
	if s.d.Clusters == nil || s.d.KubeClient == nil {
		writeError(w, http.StatusNotFound, "no clusters configured")
		return
	}
	c, ok := s.d.Clusters.Find(chi.URLParam(r, "name"))
	if !ok {
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	cli, err := s.d.KubeClient(c.KubeconfigPath)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	inv, err := clusters.Inventory(r.Context(), cli, r.URL.Query().Get("namespace"))
	if err != nil {
		writeError(w, http.StatusBadGateway, c.Name+": "+err.Error())
		return
	}
	targets, err := s.d.Store.ListTargets(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	covered := map[string][]coverage{}
	for _, t := range targets {
		if t.Type != store.TypeK8sNamespace {
			continue
		}
		v := s.viewTarget(t)
		if v.Cluster != c.Name {
			continue
		}
		for _, n := range v.Namespaces {
			covered[n] = append(covered[n], coverage{ID: t.ID, Name: t.Name, What: v.What, Enabled: t.Enabled})
		}
	}
	type nsView struct {
		clusters.Namespace
		Covered []coverage `json:"covered"`
	}
	out := make([]nsView, 0, len(inv))
	for _, n := range inv {
		cv := covered[n.Name]
		if cv == nil {
			cv = []coverage{}
		}
		out = append(out, nsView{Namespace: n, Covered: cv})
	}
	writeJSON(w, http.StatusOK, map[string]any{"cluster": c, "namespaces": out})
}
