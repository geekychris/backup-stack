package cloudstackinfra

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
)

const fixedInfraBody = `{
  "configured": true,
  "inventory": {
    "hosts": [
      {"name": "spark", "address": "spark.local", "purpose": "control", "ssh": "spark.local", "kubernetes": {"distro": "microk8s", "api": "https://spark.local:16443"}},
      {"name": "unifi-gateway", "address": "192.168.1.1", "purpose": "router"}
    ]
  }
}`

func TestListServices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/infra" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fixedInfraBody))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok")
	got, err := c.ListCandidates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i].Host != got[j].Host {
			return got[i].Host < got[j].Host
		}
		return got[i].Type < got[j].Type
	})
	want := []Candidate{
		{Type: "k8s-namespace", Host: "spark", Address: "spark.local", K8sAPI: "https://spark.local:16443", Purpose: "control"},
		{Type: "ssh-path", Host: "spark", Address: "spark.local", SSH: "spark.local", Purpose: "control"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestListServicesNotConfigured(t *testing.T) {
	c := New("", "")
	if _, err := c.ListCandidates(context.Background()); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("got %v, want ErrNotConfigured", err)
	}
}
