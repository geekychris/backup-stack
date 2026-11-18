package k8s

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/geekychris/backup-stack/internal/catalog"
)

func seededClient() *fake.Clientset {
	cli := fake.NewSimpleClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "ns1"},
			Data:       map[string]string{"k": "v"},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sec", Namespace: "ns1"},
			Data:       map[string][]byte{"password": []byte("hunter2")},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "dep", Namespace: "ns1"},
			Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
		},
	)
	return cli
}

func int32Ptr(i int32) *int32 { return &i }

func TestK8sSourceSnapshot(t *testing.T) {
	cli := seededClient()
	src := New(Config{Client: cli, Namespaces: []string{"ns1"}})

	cat, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Items) != 3 {
		t.Fatalf("got %d items, want 3: %+v", len(cat.Items), cat.Items)
	}
	for _, it := range cat.Items {
		if it.Fingerprint == "" {
			t.Fatalf("item %s has no fingerprint", it.Path)
		}
	}

	cat2, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d := catalog.Diff(cat, cat2); len(d.Added) != 0 || len(d.Changed) != 0 || len(d.Deleted) != 0 {
		t.Fatalf("re-running unchanged should yield no diff, got %+v", d)
	}

	// Mutate the ConfigMap's data; only it should now look changed.
	cm, err := cli.CoreV1().ConfigMaps("ns1").Get(context.Background(), "cfg", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.Data["k"] = "changed"
	if _, err := cli.CoreV1().ConfigMaps("ns1").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cat3, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	d := catalog.Diff(cat, cat3)
	if len(d.Changed) != 1 || d.Changed[0].Path != "ConfigMap/ns1/cfg" {
		t.Fatalf("changed = %+v, want exactly ConfigMap/ns1/cfg", d.Changed)
	}
	if len(d.Added) != 0 || len(d.Deleted) != 0 {
		t.Fatalf("expected no added/deleted, got added=%+v deleted=%v", d.Added, d.Deleted)
	}
}

func TestK8sSourceExcludeKinds(t *testing.T) {
	cli := seededClient()
	src := New(Config{Client: cli, Namespaces: []string{"ns1"}, ExcludeKinds: []string{"Secret"}})

	cat, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range cat.Items {
		if it.Path == "Secret/ns1/sec" {
			t.Fatalf("Secret item present despite ExcludeKinds: %+v", cat.Items)
		}
	}
	if len(cat.Items) != 2 {
		t.Fatalf("got %d items, want 2 (ConfigMap+Deployment): %+v", len(cat.Items), cat.Items)
	}
}

func TestK8sSourceEveryKindCleanedForRestore(t *testing.T) {
	ctrl := true
	cli := fake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "wiki", UID: "u1", ResourceVersion: "9",
			Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{}"}},
			Spec: corev1.ServiceSpec{ClusterIP: "10.152.183.191", ClusterIPs: []string{"10.152.183.191"}, Ports: []corev1.ServicePort{{Port: 33060}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "wikidb-pv-claim", Namespace: "wiki"},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "wikidb-pv-volume"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "mysql", Namespace: "wiki"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "wiki"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "wiki"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tok", Namespace: "wiki"}, Type: corev1.SecretTypeServiceAccountToken},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "wiki",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "x", Controller: &ctrl}}}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "other"}},
	)
	src := New(Config{Client: cli, Namespaces: []string{"wiki"}})
	cat, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, it := range cat.Items {
		paths = append(paths, it.Path)
	}
	want := []string{"PersistentVolumeClaim/wiki/wikidb-pv-claim", "Service/wiki/db", "StatefulSet/wiki/mysql"}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("paths = %v, want %v (defaults, tokens and owned objects left out)", paths, want)
	}

	rc, err := src.Open(context.Background(), "Service/wiki/db")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	meta := m["metadata"].(map[string]any)
	spec := m["spec"].(map[string]any)
	if m["kind"] != "Service" || m["apiVersion"] != "v1" || meta["uid"] != nil || meta["resourceVersion"] != nil ||
		meta["annotations"] != nil || spec["clusterIP"] != nil || spec["clusterIPs"] != nil || m["status"] != nil {
		t.Fatalf("service not cleaned for restore: %s", b)
	}
	rc, _ = src.Open(context.Background(), "PersistentVolumeClaim/wiki/wikidb-pv-claim")
	b, _ = io.ReadAll(rc)
	if strings.Contains(string(b), "wikidb-pv-volume") || strings.Contains(string(b), "Bound") {
		t.Fatalf("claim keeps its binding: %s", b)
	}
}
