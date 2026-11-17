// Package k8s is the k8s-namespace Source: it backs up the namespaced API
// objects (manifests, via client-go) in one or more namespaces of a
// microk8s cluster, reached directly through its API server (no agent to
// install — every k8s host in cloud-stack's infra inventory already
// exposes kubernetes.api).
//
// This captures what is restorable without a CSI snapshot controller:
// object manifests, not PersistentVolume byte data.
package k8s

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// Config configures a Source.
type Config struct {
	Client kubernetes.Interface
	// Namespaces to back up.
	Namespaces []string
	// ExcludeKinds omits a kind ("ConfigMap", "Secret", "Deployment") from
	// the backup — the only kinds Snapshot/Open ever capture. Secrets are
	// included by default: a backup that can't restore them isn't useful
	// for disaster recovery, even though that means secret material is at
	// rest wherever the Sink stores it.
	ExcludeKinds []string
}

// Source backs up namespaced Kubernetes objects.
type Source struct {
	cfg      Config
	excluded map[string]bool
}

// New builds a Source.
func New(cfg Config) *Source {
	excluded := make(map[string]bool, len(cfg.ExcludeKinds))
	for _, k := range cfg.ExcludeKinds {
		excluded[k] = true
	}
	return &Source{cfg: cfg, excluded: excluded}
}

// Close is a no-op; the client-go clientset owns no resource of ours to
// release.
func (s *Source) Close() error { return nil }

func itemPath(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

func hashJSON(v any) (fingerprint string, size int64, err error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), int64(len(b)), nil
}

// canonicalConfigMap, canonicalSecret and canonicalDeployment project an
// object onto the fields that matter for change detection, dropping
// server-assigned bookkeeping (resourceVersion, uid, managedFields,
// status, ...) so a List that only refreshed those doesn't look changed.
func canonicalConfigMap(cm *corev1.ConfigMap) any {
	return struct {
		Labels, Annotations map[string]string
		Data                map[string]string
		BinaryData          map[string][]byte
	}{cm.Labels, cm.Annotations, cm.Data, cm.BinaryData}
}

func canonicalSecret(sec *corev1.Secret) any {
	return struct {
		Labels, Annotations map[string]string
		Type                corev1.SecretType
		Data                map[string][]byte
		StringData          map[string]string
	}{sec.Labels, sec.Annotations, sec.Type, sec.Data, sec.StringData}
}

func canonicalDeployment(d *appsv1.Deployment) any {
	return struct {
		Labels, Annotations map[string]string
		Spec                appsv1.DeploymentSpec
	}{d.Labels, d.Annotations, d.Spec}
}

// Snapshot lists every ConfigMap, Secret and Deployment (except any kind in
// ExcludeKinds) across cfg.Namespaces.
func (s *Source) Snapshot(ctx context.Context) (catalog.Catalog, error) {
	var cat catalog.Catalog
	for _, ns := range s.cfg.Namespaces {
		if !s.excluded["ConfigMap"] {
			list, err := s.cfg.Client.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return catalog.Catalog{}, fmt.Errorf("source/k8s: list configmaps in %s: %w", ns, err)
			}
			for i := range list.Items {
				cm := &list.Items[i]
				fp, size, err := hashJSON(canonicalConfigMap(cm))
				if err != nil {
					return catalog.Catalog{}, err
				}
				cat.Items = append(cat.Items, catalog.Item{Path: itemPath("ConfigMap", ns, cm.Name), Fingerprint: fp, Size: size})
			}
		}
		if !s.excluded["Secret"] {
			list, err := s.cfg.Client.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return catalog.Catalog{}, fmt.Errorf("source/k8s: list secrets in %s: %w", ns, err)
			}
			for i := range list.Items {
				sec := &list.Items[i]
				fp, size, err := hashJSON(canonicalSecret(sec))
				if err != nil {
					return catalog.Catalog{}, err
				}
				cat.Items = append(cat.Items, catalog.Item{Path: itemPath("Secret", ns, sec.Name), Fingerprint: fp, Size: size})
			}
		}
		if !s.excluded["Deployment"] {
			list, err := s.cfg.Client.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return catalog.Catalog{}, fmt.Errorf("source/k8s: list deployments in %s: %w", ns, err)
			}
			for i := range list.Items {
				d := &list.Items[i]
				fp, size, err := hashJSON(canonicalDeployment(d))
				if err != nil {
					return catalog.Catalog{}, err
				}
				cat.Items = append(cat.Items, catalog.Item{Path: itemPath("Deployment", ns, d.Name), Fingerprint: fp, Size: size})
			}
		}
	}
	return cat, nil
}

// Open fetches and JSON-encodes the single object named by path (as
// reported by Snapshot: "<kind>/<namespace>/<name>"), with its Kind and
// APIVersion filled in so the result is directly restorable.
func (s *Source) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	parts := strings.SplitN(path, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("source/k8s: malformed item path %q", path)
	}
	kind, ns, name := parts[0], parts[1], parts[2]
	var obj any
	switch kind {
	case "ConfigMap":
		cm, err := s.cfg.Client.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		cm.TypeMeta = metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"}
		obj = cm
	case "Secret":
		sec, err := s.cfg.Client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		sec.TypeMeta = metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"}
		obj = sec
	case "Deployment":
		d, err := s.cfg.Client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		d.TypeMeta = metav1.TypeMeta{Kind: "Deployment", APIVersion: "apps/v1"}
		obj = d
	default:
		return nil, fmt.Errorf("source/k8s: unsupported kind %q", kind)
	}
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
