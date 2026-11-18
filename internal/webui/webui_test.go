package webui

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/geekychris/backup-stack/internal/store"
)

func TestRenderTargetsPage(t *testing.T) {
	views := []TargetView{
		{
			Target:  &store.Target{ID: "t1", Name: "spark-etc", Type: store.TypeSSHPath, Schedule: "@daily"},
			LastRun: &store.Run{ID: "r1", Kind: store.KindFull, State: store.StateSucceeded},
		},
		{
			Target: &store.Target{ID: "t2", Name: "wopr-ns", Type: store.TypeK8sNamespace},
		},
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "targets-list", views); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "spark-etc") {
		t.Fatalf("output missing target name:\n%s", out)
	}
	if !strings.Contains(out, "succeeded (full)") {
		t.Fatalf("output missing last-run status badge text:\n%s", out)
	}
	if !strings.Contains(out, "wopr-ns") || !strings.Contains(out, "never run") {
		t.Fatalf("output missing never-run target or its badge text:\n%s", out)
	}
}

func TestHandlerServesIndex(t *testing.T) {
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.CreateTarget(context.Background(), &store.Target{ID: "t1", Name: "spark-etc", Type: store.TypeSSHPath, Config: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}

	h := &Handler{Store: st}
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "spark-etc") {
		t.Fatalf("index page missing target name:\n%s", rec.Body.String())
	}
}

func newIndexStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctx := context.Background()
	if err := st.CreateSyncJob(ctx, &store.SyncJob{ID: "j1", Name: "photos-offsite",
		SourceConfig: []byte(`{"type":"ssh","addr":"wopr.local:22","user":"chris","path":"/srv/photos"}`),
		DestConfig:   []byte(`{"type":"s3","sink":"primary","prefix":"photos"}`)}); err != nil {
		t.Fatal(err)
	}
	return st
}

// The page must only use relative URLs so it works behind a path prefix;
// an absolute "/v1" from the browser would escape the prefix.
func TestIndexUsesRelativeURLs(t *testing.T) {
	h := &Handler{Store: newIndexStore(t)}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	body := rec.Body.String()
	for _, bad := range []string{`'/v1`, `"/v1`, "`/v1", `href="/`, `src="/`, `action="/`} {
		if strings.Contains(body, bad) {
			t.Errorf("index page contains absolute URL %s", bad)
		}
	}
	if !strings.Contains(body, `'v1/stats?'`) {
		t.Error("index page does not call the stats API relatively")
	}
	if strings.Contains(body, "<base") {
		t.Error("base href without X-Forwarded-Prefix")
	}
	if !strings.Contains(body, "photos-offsite") || !strings.Contains(body, "ssh-&gt;s3") {
		t.Errorf("sync job row missing name/kind")
	}
}

func TestIndexUnderForwardedPrefix(t *testing.T) {
	h := &Handler{Store: newIndexStore(t)}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-Prefix", "/apps/backup/")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), `<base href="/apps/backup/">`) {
		t.Fatalf("no base href for prefix:\n%.400s", rec.Body.String())
	}
	for _, evil := range []string{`/x"><script>`, "//evil.example", "javascript:alert(1)"} {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-Prefix", evil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), "<base") {
			t.Errorf("prefix %q produced a base href", evil)
		}
	}
}
