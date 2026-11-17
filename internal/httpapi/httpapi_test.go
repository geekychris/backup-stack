package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/geekychris/backup-stack/internal/backup"
	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
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
		Name: "spark-etc", Type: store.TypeSSHPath, Config: json.RawMessage(`{"host":"spark","path":"/etc"}`),
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
		Name: "t1", Type: store.TypeSSHPath, Config: json.RawMessage(`{}`),
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
		Name: "t1", Type: store.TypeSSHPath, Config: json.RawMessage(`{}`),
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
