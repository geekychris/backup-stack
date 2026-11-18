package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/geekychris/backup-stack/internal/backup"
	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
	syncpkg "github.com/geekychris/backup-stack/internal/sync"
)

// fakeSource/fakeSink are minimal no-op implementations so runTarget has
// something to call without needing a real ssh/k8s host or S3 bucket.
type fakeSource struct{}

func (fakeSource) Snapshot(context.Context) (catalog.Catalog, error) { return catalog.Catalog{}, nil }
func (fakeSource) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (fakeSource) Close() error { return nil }

type fakeSink struct{ catalogs map[string]catalog.Catalog }

func (s *fakeSink) WriteItem(context.Context, string, string, io.Reader) error { return nil }
func (s *fakeSink) OpenItem(context.Context, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (s *fakeSink) WriteCatalog(_ context.Context, runID string, cat catalog.Catalog) error {
	s.catalogs[runID] = cat
	return nil
}
func (s *fakeSink) OpenCatalog(_ context.Context, runID string) (catalog.Catalog, error) {
	return s.catalogs[runID], nil
}
func (s *fakeSink) DeleteRun(context.Context, string) error      { return nil }
func (s *fakeSink) Prune(context.Context, map[string]bool) error { return nil }

const testToken = "test-token"

func newTestServer(t *testing.T) *Server {
	t.Helper()
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	sk := &fakeSink{catalogs: map[string]catalog.Catalog{}}
	return New(Deps{
		Store:  st,
		Runner: &backup.Runner{Store: st},
		BuildSource: func(context.Context, *store.Target) (source.Source, error) {
			return fakeSource{}, nil
		},
		ResolveSink: func(*store.Target) (sink.Sink, error) {
			return sk, nil
		},
		Sync:  &syncpkg.Runner{Store: st},
		Token: testToken,
	})
}

func doJSON(t *testing.T, s *Server, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestCreateAndListTargets(t *testing.T) {
	s := newTestServer(t)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, targetRequest{
		Name: "spark-etc", Type: store.TypeSSHPath, Config: json.RawMessage(`{"addr":"spark:22","path":"/etc"}`),
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var created store.Target
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Name != "spark-etc" || created.ID == "" {
		t.Fatalf("created = %+v", created)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/targets", testToken, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	var list []store.Target
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("list = %+v", list)
	}
}

func TestRunNow(t *testing.T) {
	s := newTestServer(t)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, targetRequest{
		Name: "t1", Type: store.TypeSSHPath, Config: json.RawMessage(`{"addr":"h:22","path":"/x"}`),
	})
	var created store.Target
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	rec = doJSON(t, s, http.MethodPost, "/v1/targets/"+created.ID+":run", testToken, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("run status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var run store.Run
	if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.Kind != store.KindFull || run.State != store.StateSucceeded {
		t.Fatalf("run = %+v", run)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/targets/"+created.ID+"/runs", testToken, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list runs status = %d", rec.Code)
	}
	var runs []store.Run
	if err := json.Unmarshal(rec.Body.Bytes(), &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestRunNowConflictWhenInFlight(t *testing.T) {
	s := newTestServer(t)
	rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, targetRequest{
		Name: "t1", Type: store.TypeSSHPath, Config: json.RawMessage(`{"addr":"h:22","path":"/x"}`),
	})
	var created store.Target
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	if err := s.d.Store.CreateRun(context.Background(), &store.Run{
		ID: "already-running", TargetID: created.ID, Kind: store.KindFull, State: store.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}

	rec = doJSON(t, s, http.MethodPost, "/v1/targets/"+created.ID+":run", testToken, nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("run status = %d, body = %s, want 409", rec.Code, rec.Body.String())
	}
}

func TestAuthRequired(t *testing.T) {
	s := newTestServer(t)
	if rec := doJSON(t, s, http.MethodGet, "/v1/targets", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d", rec.Code)
	}
	if rec := doJSON(t, s, http.MethodGet, "/v1/targets", "wrong-token", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d", rec.Code)
	}
	if rec := doJSON(t, s, http.MethodGet, "/v1/targets", testToken, nil); rec.Code != http.StatusOK {
		t.Fatalf("correct token: status = %d", rec.Code)
	}
}

func TestHealthzNoAuth(t *testing.T) {
	s := newTestServer(t)
	if rec := doJSON(t, s, http.MethodGet, "/healthz", "", nil); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
	s.d.Store.Close()
	if rec := doJSON(t, s, http.MethodGet, "/healthz", "", nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz with closed store = %d, want 503", rec.Code)
	}
}

func TestSyncJobValidation(t *testing.T) {
	s := newTestServer(t)
	bad := []map[string]any{
		{"name": "x", "sourceConfig": json.RawMessage(`{"type":"dir","path":"/a"}`), "destConfig": json.RawMessage(`{"type":"dir","path":"/a"}`)},
		{"name": "x", "sourceConfig": json.RawMessage(`{"type":"dir","path":"rel"}`), "destConfig": json.RawMessage(`{"type":"dir","path":"/b"}`)},
		{"name": "x", "sourceConfig": json.RawMessage(`{"type":"dir","path":"/a"}`), "destConfig": json.RawMessage(`{"type":"dir","path":"/b"}`),
			"filters": []map[string]string{{"kind": "maybe", "pattern": "*"}}},
	}
	for i, b := range bad {
		if rec := doJSON(t, s, http.MethodPost, "/v1/sync-jobs", testToken, b); rec.Code != http.StatusBadRequest {
			t.Errorf("case %d: status %d, body %s", i, rec.Code, rec.Body.String())
		}
	}
	if rec := doJSON(t, s, http.MethodPost, "/v1/targets", testToken, targetRequest{
		Name: "t", Type: store.TypeSSHPath, Config: json.RawMessage(`{"addr":"h:22","path":"/x","filters":[{"kind":"exclude","pattern":""}]}`),
	}); rec.Code != http.StatusBadRequest {
		t.Errorf("target with empty filter pattern: %d", rec.Code)
	}
}

func TestSyncJobRunHistoryAndStats(t *testing.T) {
	s := newTestServer(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	for name, content := range map[string]string{"a.txt": "12345", "b.tmp": "x", "sub/c.txt": "123"} {
		p := filepath.Join(srcDir, name)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rec := doJSON(t, s, http.MethodPost, "/v1/sync-jobs", testToken, map[string]any{
		"name":         "local-mirror",
		"sourceConfig": json.RawMessage(fmt.Sprintf(`{"type":"dir","path":%q}`, srcDir)),
		"destConfig":   json.RawMessage(fmt.Sprintf(`{"type":"dir","path":%q}`, dstDir)),
		"filters":      []map[string]string{{"kind": "exclude", "pattern": "*.tmp"}},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	var job struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		Delete bool   `json:"delete"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	if job.Kind != "dir->dir" || job.Delete {
		t.Fatalf("job = %+v (delete must default off)", job)
	}

	rec = doJSON(t, s, http.MethodPost, "/v1/sync-jobs/"+job.ID+":run", testToken, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("run = %d %s", rec.Code, rec.Body.String())
	}
	var run store.SyncRun
	_ = json.Unmarshal(rec.Body.Bytes(), &run)
	if run.State != store.StateSucceeded || run.Stats.Files != 2 || run.Stats.Bytes != 8 {
		t.Fatalf("run = %+v", run)
	}

	// A backup run shows up in the same history.
	rec = doJSON(t, s, http.MethodPost, "/v1/targets", testToken, targetRequest{Name: "t1", Type: store.TypeSSHPath, Config: json.RawMessage(`{"addr":"h:22","path":"/x"}`)})
	var tgt store.Target
	_ = json.Unmarshal(rec.Body.Bytes(), &tgt)
	if rec := doJSON(t, s, http.MethodPost, "/v1/targets/"+tgt.ID+":run", testToken, nil); rec.Code != http.StatusCreated {
		t.Fatalf("backup run = %d", rec.Code)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/history?since=1h", testToken, nil)
	var hist []store.HistoryEntry
	_ = json.Unmarshal(rec.Body.Bytes(), &hist)
	if rec.Code != http.StatusOK || len(hist) != 2 {
		t.Fatalf("history = %d %s", rec.Code, rec.Body.String())
	}
	rec = doJSON(t, s, http.MethodGet, "/v1/history?source=sync", testToken, nil)
	_ = json.Unmarshal(rec.Body.Bytes(), &hist)
	if len(hist) != 1 || hist[0].OwnerName != "local-mirror" || hist[0].Kind != "native" {
		t.Fatalf("sync history = %+v", hist)
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/stats?since=24h", testToken, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats = %d %s", rec.Code, rec.Body.String())
	}
	var stats StatsResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &stats)
	if stats.BucketSeconds != 3600 || len(stats.Buckets) < 24 || stats.Totals.Runs != 2 || stats.Totals.Bytes != 8 || len(stats.Owners) != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	last := stats.Buckets[len(stats.Buckets)-1]
	if last.Runs != 2 || !last.Start.Equal(time.Now().UTC().Truncate(time.Hour)) {
		t.Fatalf("current bucket = %+v", last)
	}
	rec = doJSON(t, s, http.MethodGet, "/v1/stats?since=30d&owner="+job.ID, testToken, nil)
	_ = json.Unmarshal(rec.Body.Bytes(), &stats)
	if stats.BucketSeconds != 86400 || stats.Totals.Runs != 1 {
		t.Fatalf("owner stats = %+v", stats)
	}
	for _, q := range []string{"since=bogus", "bucket=1s", "since=365d&bucket=1m"} {
		if rec := doJSON(t, s, http.MethodGet, "/v1/stats?"+q, testToken, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("stats?%s = %d, want 400", q, rec.Code)
		}
	}

	rec = doJSON(t, s, http.MethodGet, "/v1/sync-jobs/"+job.ID+"/runs", testToken, nil)
	var runs []store.SyncRun
	_ = json.Unmarshal(rec.Body.Bytes(), &runs)
	if len(runs) != 1 {
		t.Fatalf("runs = %s", rec.Body.String())
	}
}
