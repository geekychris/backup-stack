// Package k8s is the k8s-namespace Source: it backs up the namespaced API
// objects (manifests, via client-go) of a namespace of a cluster, reached
// directly through its API server (no agent to install — every k8s host in
// cloud-stack's infra inventory already exposes kubernetes.api).
//
// It captures what it takes to recreate the namespace's objects: every kind
// in Kinds, cleaned of what the API server assigns (uid, resourceVersion,
// status, a Service's clusterIP, …) so the saved JSON can be applied again.
// It does not capture PersistentVolume data or databases' contents: a PVC is
// saved as a claim, not as the files on it.
package k8s

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// Config configures a Source.
type Config struct {
	Client kubernetes.Interface
	// Namespaces to back up (usually one per target).
	Namespaces []string
	// ExcludeKinds omits kinds (names from Kinds, e.g. "Secret") from the
	// backup. Secrets are included by default: a backup that can't restore
	// them isn't useful for disaster recovery, even though that means
	// secret material is at rest wherever the Sink stores it.
	ExcludeKinds []string
}

// Kind is one object kind a Source captures.
type Kind struct {
	Name       string `json:"name"`
	APIVersion string `json:"apiVersion"`
	About      string `json:"about"` // one line for the UI
	list       func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error)
	get        func(ctx context.Context, c kubernetes.Interface, ns, name string) (any, error)
}

// listOf adapts a typed List call to a slice of objects.
func listOf[T any](items []T, err error) ([]any, error) {
	if err != nil {
		return nil, err
	}
	out := make([]any, len(items))
	for i := range items {
		out[i] = &items[i]
	}
	return out, nil
}

var lo = metav1.ListOptions{}
var gopt = metav1.GetOptions{}

// Kinds are the kinds a Source captures, in the order it lists them.
var Kinds = []Kind{
	{Name: "ConfigMap", APIVersion: "v1", About: "configuration files and settings",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.CoreV1().ConfigMaps(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.CoreV1().ConfigMaps(ns).Get(ctx, n, gopt)
		}},
	{Name: "Secret", APIVersion: "v1", About: "passwords, keys and certificates (stored as they are, base64)",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.CoreV1().Secrets(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.CoreV1().Secrets(ns).Get(ctx, n, gopt)
		}},
	{Name: "Service", APIVersion: "v1", About: "network names and ports",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.CoreV1().Services(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.CoreV1().Services(ns).Get(ctx, n, gopt)
		}},
	{Name: "ServiceAccount", APIVersion: "v1", About: "identities pods run as",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.CoreV1().ServiceAccounts(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.CoreV1().ServiceAccounts(ns).Get(ctx, n, gopt)
		}},
	{Name: "PersistentVolumeClaim", APIVersion: "v1", About: "volume claims (size and class, not the files on them)",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.CoreV1().PersistentVolumeClaims(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, n, gopt)
		}},
	{Name: "Deployment", APIVersion: "apps/v1", About: "stateless workloads",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.AppsV1().Deployments(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.AppsV1().Deployments(ns).Get(ctx, n, gopt)
		}},
	{Name: "StatefulSet", APIVersion: "apps/v1", About: "workloads with stable storage (databases, brokers)",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.AppsV1().StatefulSets(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.AppsV1().StatefulSets(ns).Get(ctx, n, gopt)
		}},
	{Name: "DaemonSet", APIVersion: "apps/v1", About: "one pod per node",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.AppsV1().DaemonSets(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.AppsV1().DaemonSets(ns).Get(ctx, n, gopt)
		}},
	{Name: "CronJob", APIVersion: "batch/v1", About: "scheduled jobs",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.BatchV1().CronJobs(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.BatchV1().CronJobs(ns).Get(ctx, n, gopt)
		}},
	{Name: "Ingress", APIVersion: "networking.k8s.io/v1", About: "HTTP routes from outside",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.NetworkingV1().Ingresses(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.NetworkingV1().Ingresses(ns).Get(ctx, n, gopt)
		}},
	{Name: "NetworkPolicy", APIVersion: "networking.k8s.io/v1", About: "which pods may talk to which",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.NetworkingV1().NetworkPolicies(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.NetworkingV1().NetworkPolicies(ns).Get(ctx, n, gopt)
		}},
	{Name: "Role", APIVersion: "rbac.authorization.k8s.io/v1", About: "permissions inside the namespace",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.RbacV1().Roles(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.RbacV1().Roles(ns).Get(ctx, n, gopt)
		}},
	{Name: "RoleBinding", APIVersion: "rbac.authorization.k8s.io/v1", About: "who has those permissions",
		list: func(ctx context.Context, c kubernetes.Interface, ns string) ([]any, error) {
			l, err := c.RbacV1().RoleBindings(ns).List(ctx, lo)
			if err != nil {
				return nil, err
			}
			return listOf(l.Items, nil)
		},
		get: func(ctx context.Context, c kubernetes.Interface, ns, n string) (any, error) {
			return c.RbacV1().RoleBindings(ns).Get(ctx, n, gopt)
		}},
}

// KindNames are the names of Kinds.
func KindNames() []string {
	out := make([]string, len(Kinds))
	for i, k := range Kinds {
		out[i] = k.Name
	}
	return out
}

func kindByName(name string) (Kind, bool) {
	for _, k := range Kinds {
		if k.Name == name {
			return k, true
		}
	}
	return Kind{}, false
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

// clean turns an object into the map that is saved and fingerprinted: kind
// and apiVersion set, and what the API server assigns or derives removed
// (uid, resourceVersion, timestamps, managedFields, status, a Service's
// cluster IPs, a claim's bound volume), so the JSON can be applied to a
// cluster again and a List that only refreshed bookkeeping doesn't look
// changed. It reports false for objects that should not be saved on their
// own: ones a controller owns and recreates (a CronJob's Jobs, …) and
// ones Kubernetes makes in every namespace.
func clean(k Kind, obj any) (map[string]any, bool, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, false, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false, err
	}
	meta, _ := m["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	if refs, ok := meta["ownerReferences"].([]any); ok {
		for _, r := range refs {
			if rm, ok := r.(map[string]any); ok && rm["controller"] == true {
				return nil, false, nil
			}
		}
	}
	name, _ := meta["name"].(string)
	switch {
	case k.Name == "ConfigMap" && (name == "kube-root-ca.crt" || name == "istio-ca-root-cert"):
		return nil, false, nil
	case k.Name == "ServiceAccount" && name == "default":
		return nil, false, nil
	case k.Name == "Secret" && m["type"] == "kubernetes.io/service-account-token":
		return nil, false, nil
	}
	for _, f := range []string{"uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "selfLink", "deletionTimestamp", "deletionGracePeriodSeconds"} {
		delete(meta, f)
	}
	if ann, ok := meta["annotations"].(map[string]any); ok {
		for a := range ann {
			if a == "kubectl.kubernetes.io/last-applied-configuration" || a == "deployment.kubernetes.io/revision" || strings.HasPrefix(a, "pv.kubernetes.io/") ||
				strings.HasPrefix(a, "volume.beta.kubernetes.io/") || strings.HasPrefix(a, "volume.kubernetes.io/") {
				delete(ann, a)
			}
		}
		if len(ann) == 0 {
			delete(meta, "annotations")
		}
	}
	delete(m, "status")
	if spec, ok := m["spec"].(map[string]any); ok {
		switch k.Name {
		case "Service":
			delete(spec, "clusterIP")
			delete(spec, "clusterIPs")
		case "PersistentVolumeClaim":
			delete(spec, "volumeName")
		}
	}
	m["metadata"] = meta
	m["kind"] = k.Name
	m["apiVersion"] = k.APIVersion
	return m, true, nil
}

// Snapshot lists every object of every kind in Kinds (except those in
// ExcludeKinds) across cfg.Namespaces.
func (s *Source) Snapshot(ctx context.Context) (catalog.Catalog, error) {
	var cat catalog.Catalog
	for _, ns := range s.cfg.Namespaces {
		for _, k := range Kinds {
			if s.excluded[k.Name] {
				continue
			}
			objs, err := k.list(ctx, s.cfg.Client, ns)
			if err != nil {
				return catalog.Catalog{}, fmt.Errorf("source/k8s: list %ss in %s: %w", strings.ToLower(k.Name), ns, err)
			}
			for _, o := range objs {
				m, keep, err := clean(k, o)
				if err != nil {
					return catalog.Catalog{}, err
				}
				if !keep {
					continue
				}
				b, err := json.Marshal(m) // map keys marshal sorted: a stable fingerprint
				if err != nil {
					return catalog.Catalog{}, err
				}
				sum := sha256.Sum256(b)
				name, _ := m["metadata"].(map[string]any)["name"].(string)
				cat.Items = append(cat.Items, catalog.Item{Path: itemPath(k.Name, ns, name), Fingerprint: hex.EncodeToString(sum[:]), Size: int64(len(b))})
			}
		}
	}
	sort.SliceStable(cat.Items, func(i, j int) bool { return cat.Items[i].Path < cat.Items[j].Path })
	return cat, nil
}

// Open fetches the single object named by path (as reported by Snapshot:
// "<kind>/<namespace>/<name>") and returns it cleaned and indented, ready
// for kubectl apply.
func (s *Source) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	parts := strings.SplitN(path, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("source/k8s: malformed item path %q", path)
	}
	k, ok := kindByName(parts[0])
	if !ok {
		return nil, fmt.Errorf("source/k8s: unsupported kind %q", parts[0])
	}
	obj, err := k.get(ctx, s.cfg.Client, parts[1], parts[2])
	if err != nil {
		return nil, err
	}
	m, _, err := clean(k, obj)
	if err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// CountObjects counts, per namespace and kind, the objects a Source would
// save from ns ("" = every namespace).
func CountObjects(ctx context.Context, c kubernetes.Interface, ns string) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	for _, k := range Kinds {
		objs, err := k.list(ctx, c, ns)
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", strings.ToLower(k.Name), err)
		}
		for _, o := range objs {
			m, keep, err := clean(k, o)
			if err != nil || !keep {
				continue
			}
			on, _ := m["metadata"].(map[string]any)["namespace"].(string)
			if out[on] == nil {
				out[on] = map[string]int{}
			}
			out[on][k.Name]++
		}
	}
	return out, nil
}
