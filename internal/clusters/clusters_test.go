package clusters

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRegistry(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"wopr", "mp2", "..data"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.Mkdir(filepath.Join(dir, "..2026_11_18"), 0o700)
	r := &Registry{LocalName: "spark", KubeDir: dir, InCluster: true}
	got := r.List()
	if len(got) != 3 || got[0].Name != "spark" || !got[0].Local || got[1].Name != "mp2" || got[2].Name != "wopr" {
		t.Fatalf("List = %+v", got)
	}
	if r.NameFor("") != "spark" || r.NameFor(filepath.Join(dir, "wopr")) != "wopr" || r.NameFor("/elsewhere/k") != "/elsewhere/k" {
		t.Fatalf("NameFor wrong")
	}
	if !r.Known("") || !r.Known(filepath.Join(dir, "mp2")) || r.Known("/elsewhere/k") {
		t.Fatalf("Known wrong")
	}
	if c, ok := r.Find("wopr"); !ok || c.KubeconfigPath != filepath.Join(dir, "wopr") {
		t.Fatalf("Find = %+v %v", c, ok)
	}
}

func TestInventory_WikiNamespace(t *testing.T) {
	ctrl := true
	pod := func(name, rs, hash string, claims []string, containers ...corev1.Container) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "wiki", Labels: map[string]string{"pod-template-hash": hash},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: rs, Controller: &ctrl}}},
			Spec: corev1.PodSpec{Containers: containers}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
		for _, c := range claims {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: c, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c}}})
		}
		return p
	}
	class := "manual"
	pvc := func(name string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "wiki"},
			Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &class, Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}}},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	}
	cli := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "wiki"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "empty"}},
		pod("mediawiki-1", "mediawiki-56b", "56b", []string{"wiki-pv-claim"}, corev1.Container{Name: "mediawiki", Image: "supreetraut/mediawiki-1.34"}),
		pod("db-1", "db-6cb", "6cb", []string{"wikidb-pv-claim"}, corev1.Container{Name: "db", Image: "supreetraut/mariadb-wiki"}),
		pod("ri-1", "redisinsight-6b", "6b", nil, corev1.Container{Name: "ri", Image: "redislabs/redisinsight:latest"}),
		pod("cache-1", "cache-7", "7", nil, corev1.Container{Name: "c", Image: "example/kv", Ports: []corev1.ContainerPort{{ContainerPort: 6379}}}),
		pvc("wiki-pv-claim"), pvc("wikidb-pv-claim"), pvc("postgres-volume-claim"),
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "wiki-config", Namespace: "wiki"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "wiki"}},
	)
	inv, err := Inventory(context.Background(), cli, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(inv) != 2 || inv[0].Name != "empty" || inv[1].Name != "wiki" {
		t.Fatalf("namespaces = %+v", inv)
	}
	w := inv[1]
	if w.Pods != 4 || w.Objects["ConfigMap"] != 1 || w.Objects["PersistentVolumeClaim"] != 3 {
		t.Errorf("counts: pods %d objects %v", w.Pods, w.Objects)
	}
	if len(w.Databases) != 2 || w.Databases[0].Engine != "redis" || w.Databases[0].Workload != "Deployment/cache" ||
		w.Databases[1].Engine != "mariadb" || w.Databases[1].Workload != "Deployment/db" || w.Databases[1].Volumes[0] != "wikidb-pv-claim" {
		t.Errorf("databases = %+v (redisinsight is a tool, not a database)", w.Databases)
	}
	if len(w.Volumes) != 3 {
		t.Fatalf("volumes = %+v", w.Volumes)
	}
	if v := w.Volumes[0]; v.Claim != "postgres-volume-claim" || len(v.UsedBy) != 0 {
		t.Errorf("unused claim = %+v", v)
	}
	if v := w.Volumes[1]; v.Claim != "wiki-pv-claim" || v.UsedBy[0] != "Deployment/mediawiki" || v.Database != "" || v.Size != "10Gi" || v.StorageClass != "manual" {
		t.Errorf("wiki files claim = %+v", v)
	}
	if v := w.Volumes[2]; v.Claim != "wikidb-pv-claim" || v.Database != "mariadb" {
		t.Errorf("db claim = %+v", v)
	}
	if len(inv[0].Volumes) != 0 || inv[0].Objects == nil {
		t.Errorf("empty namespace = %+v", inv[0])
	}
}
