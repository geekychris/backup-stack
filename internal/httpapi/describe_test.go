package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/geekychris/backup-stack/internal/clusters"
	"github.com/geekychris/backup-stack/internal/store"
)

// newClusterServer is a server with two clusters: spark (in-cluster) and
// wopr (a kubeconfig file), both answered by fake clients.
func newClusterServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wopr"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	wopr := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "wiki"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "saaspack"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "wiki-config", Namespace: "wiki"}},
	)
	s := newTestServer(t)
	s.d.Clusters = &clusters.Registry{LocalName: "spark", KubeDir: dir, InCluster: true}
	s.d.KubeClient = func(path string) (kubernetes.Interface, error) {
		if path == filepath.Join(dir, "wopr") {
			return wopr, nil
		}
		return fake.NewSimpleClientset(), nil
	}
	s.d.Sinks = []SinkInfo{{Name: "primary", Endpoint: "http://wopr.local:9010", Bucket: "lab-backups"}}
	return s, filepath.Join(dir, "wopr")
}

func TestTargetsSayWhatTheyCopy(t *testing.T) {
	s, wopr := newClusterServer(t)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{
		"name": "wopr/wiki objects", "type": store.TypeK8sNamespace, "schedule": "15 3 * * *",
		"config": map[string]any{"kubeconfigPath": wopr, "namespaces": []string{"wiki"}, "excludeKinds": []string{"Secret"}},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d %s", rec.Code, rec.Body)
	}
	var v targetView
	_ = json.Unmarshal(rec.Body.Bytes(), &v)
	if v.Cluster != "wopr" || v.What != "Kubernetes objects" || strings.Join(v.Namespaces, ",") != "wiki" ||
		!strings.Contains(v.Copies, "Not the files on its volumes") || !strings.Contains(v.Copies, "Leaves out Secret") || !strings.Contains(v.Copies, "(12 kinds") {
		t.Fatalf("view = %+v", v)
	}

	rec = doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{
		"name": "wopr etc", "type": store.TypeSSHPath,
		"config": map[string]any{"addr": "wopr.local:22", "user": "chris", "path": "/etc", "filters": []map[string]string{{"kind": "exclude", "pattern": "*.tmp"}}},
	})
	_ = json.Unmarshal(rec.Body.Bytes(), &v)
	if v.Cluster != "wopr.local" || v.What != "Files over SSH" || v.Copies != "Every file under chris@wopr.local:22:/etc, as 1 filter rule choose." {
		t.Fatalf("ssh view = %+v", v)
	}
}

func TestDuplicateNameIsAConflictInWords(t *testing.T) {
	s, _ := newClusterServer(t)
	body := map[string]any{"name": "same", "type": store.TypeSSHPath, "config": map[string]any{"addr": "h:22", "path": "/x"}}
	_ = doJSON(t, s, http.MethodPost, "/v1/targets", testToken, body)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, body)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `a backup named \"same\" already exists`) {
		t.Fatalf("duplicate = %d %s", rec.Code, rec.Body)
	}
}

func TestTargetValidation(t *testing.T) {
	s, wopr := newClusterServer(t)
	for _, c := range []struct {
		cfg  map[string]any
		want string
	}{
		{map[string]any{"kubeconfigPath": wopr}, "pick at least one namespace"},
		{map[string]any{"kubeconfigPath": wopr, "namespaces": []string{"Wiki!"}}, "not a namespace name"},
		{map[string]any{"kubeconfigPath": wopr, "namespaces": []string{"wiki"}, "excludeKinds": []string{"Pod"}}, "unknown kind"},
		{map[string]any{"kubeconfigPath": "/tmp/elsewhere", "namespaces": []string{"wiki"}}, "not one of the clusters"},
	} {
		rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{"name": "x", "type": store.TypeK8sNamespace, "config": c.cfg})
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%v: %d %s, want 400 %q", c.cfg, rec.Code, rec.Body, c.want)
		}
	}
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{"name": "x", "type": store.TypeSSHPath,
		"config": map[string]any{"addr": "h:22", "path": "/x"}, "schedule": "every day"})
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "schedule") {
		t.Errorf("bad schedule: %d %s", rec.Code, rec.Body)
	}
}

func TestUpdateKeepsWhatItLeavesOut(t *testing.T) {
	s, wopr := newClusterServer(t)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{
		"name": "t", "type": store.TypeK8sNamespace, "schedule": "@daily", "sinkRef": "primary",
		"config": map[string]any{"kubeconfigPath": wopr, "namespaces": []string{"wiki"}},
	})
	var v targetView
	_ = json.Unmarshal(rec.Body.Bytes(), &v)
	rec = doJSON(t, s, http.MethodPut, "/v1/targets/"+v.ID, testToken, map[string]any{"enabled": false})
	if rec.Code != http.StatusOK {
		t.Fatalf("update = %d %s", rec.Code, rec.Body)
	}
	got, _ := s.d.Store.GetTarget(context.Background(), v.ID)
	if got.Schedule != "@daily" || got.SinkRef != "primary" || got.Enabled {
		t.Fatalf("after disabling: %+v (schedule and sink must stay)", got)
	}
	rec = doJSON(t, s, http.MethodPut, "/v1/targets/"+v.ID, testToken, map[string]any{"schedule": ""})
	got, _ = s.d.Store.GetTarget(context.Background(), v.ID)
	if rec.Code != http.StatusOK || got.Schedule != "" {
		t.Fatalf("clearing the schedule: %d %+v", rec.Code, got)
	}
}

func TestClustersNamespacesSinksSchedule(t *testing.T) {
	s, wopr := newClusterServer(t)
	rec := doJSON(t, s, http.MethodGet, "/v1/clusters", testToken, nil)
	var cl struct {
		Clusters []clusters.Cluster `json:"clusters"`
		Kinds    []map[string]string
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &cl)
	if len(cl.Clusters) != 2 || cl.Clusters[0].Name != "spark" || cl.Clusters[1].Name != "wopr" || len(cl.Kinds) < 10 || cl.Kinds[0]["about"] == "" {
		t.Fatalf("clusters = %s", rec.Body)
	}

	_ = doJSON(t, s, http.MethodPost, "/v1/targets", testToken, map[string]any{
		"name": "wopr/wiki objects", "type": store.TypeK8sNamespace,
		"config": map[string]any{"kubeconfigPath": wopr, "namespaces": []string{"wiki"}},
	})
	rec = doJSON(t, s, http.MethodGet, "/v1/clusters/wopr/namespaces", testToken, nil)
	var ns struct {
		Namespaces []struct {
			Name    string
			Objects map[string]int
			Covered []coverage
		}
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &ns)
	if rec.Code != 200 || len(ns.Namespaces) != 2 || ns.Namespaces[1].Name != "wiki" || ns.Namespaces[1].Objects["ConfigMap"] != 1 ||
		len(ns.Namespaces[1].Covered) != 1 || len(ns.Namespaces[0].Covered) != 0 {
		t.Fatalf("namespaces = %d %s", rec.Code, rec.Body)
	}
	if rec := doJSON(t, s, http.MethodGet, "/v1/clusters/mars/namespaces", testToken, nil); rec.Code != 404 {
		t.Fatalf("unknown cluster = %d", rec.Code)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/sinks", testToken, nil)
	if !strings.Contains(rec.Body.String(), `"bucket":"lab-backups"`) || strings.Contains(rec.Body.String(), "ecret") {
		t.Fatalf("sinks = %s", rec.Body)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/schedule?expr="+strings.ReplaceAll("15 3 * * *", " ", "+"), testToken, nil)
	var sc struct {
		Valid bool
		Next  []string
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &sc)
	if !sc.Valid || len(sc.Next) != 3 || !strings.Contains(sc.Next[0], "T03:15:00") {
		t.Fatalf("schedule = %s", rec.Body)
	}
	rec = doJSON(t, s, http.MethodGet, "/v1/schedule?expr=nonsense", testToken, nil)
	if !strings.Contains(rec.Body.String(), `"valid":false`) {
		t.Fatalf("bad schedule = %s", rec.Body)
	}
}
