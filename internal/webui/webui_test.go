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
