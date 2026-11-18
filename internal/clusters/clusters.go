// Package clusters knows the Kubernetes clusters backup-stack can reach
// (its own, in-cluster, and one per kubeconfig file in the mounted
// backup-stack-kubeconfigs Secret) and takes an inventory of a cluster's
// namespaces for the UI: how many objects of each kind, the volume claims
// and which workloads use them, and the databases its pods run.
package clusters

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	k8ssrc "github.com/geekychris/backup-stack/internal/source/k8s"
)

// Cluster is one cluster a k8s target can name.
type Cluster struct {
	Name           string `json:"name"`
	KubeconfigPath string `json:"kubeconfigPath"` // "" = in-cluster
	Local          bool   `json:"local"`
}

// Registry lists the clusters: the in-cluster one (when backup-stack runs
// in Kubernetes) under LocalName, and a file in KubeDir per other cluster,
// named after the file.
type Registry struct {
	LocalName string // e.g. "spark"; default "local"
	KubeDir   string // e.g. /etc/backup-stack/kube
	InCluster bool
}

func (r *Registry) localName() string {
	if r.LocalName != "" {
		return r.LocalName
	}
	return "local"
}

// List returns the clusters, the local one first.
func (r *Registry) List() []Cluster {
	var out []Cluster
	if r.InCluster {
		out = append(out, Cluster{Name: r.localName(), Local: true})
	}
	if r.KubeDir != "" {
		entries, _ := os.ReadDir(r.KubeDir)
		var names []string
		for _, e := range entries {
			// A mounted Secret holds ..data and ..<timestamp> next to the keys.
			if strings.HasPrefix(e.Name(), ".") || e.IsDir() {
				continue
			}
			names = append(names, e.Name())
		}
		sort.Strings(names)
		for _, n := range names {
			out = append(out, Cluster{Name: n, KubeconfigPath: filepath.Join(r.KubeDir, n)})
		}
	}
	return out
}

// Find returns the cluster named name.
func (r *Registry) Find(name string) (Cluster, bool) {
	for _, c := range r.List() {
		if c.Name == name {
			return c, true
		}
	}
	return Cluster{}, false
}

// NameFor names the cluster a target's kubeconfigPath reaches: the local
// one for "", a file's name for a path in KubeDir, else the path itself.
func (r *Registry) NameFor(kubeconfigPath string) string {
	if kubeconfigPath == "" {
		return r.localName()
	}
	if r.KubeDir != "" && filepath.Dir(kubeconfigPath) == filepath.Clean(r.KubeDir) {
		return filepath.Base(kubeconfigPath)
	}
	return kubeconfigPath
}

// Known reports whether kubeconfigPath is one of List's clusters.
func (r *Registry) Known(kubeconfigPath string) bool {
	for _, c := range r.List() {
		if c.KubeconfigPath == kubeconfigPath {
			return true
		}
	}
	return false
}

// Volume is a PersistentVolumeClaim and what uses it.
type Volume struct {
	Claim        string   `json:"claim"`
	Size         string   `json:"size,omitempty"`
	StorageClass string   `json:"storageClass,omitempty"`
	Phase        string   `json:"phase,omitempty"`
	UsedBy       []string `json:"usedBy,omitempty"`   // workloads (Deployment/x, StatefulSet/y, Pod/z)
	Database     string   `json:"database,omitempty"` // engine, when a database pod mounts it
}

// Database is a database server a namespace's pods run, recognised by
// image (or, failing that, by its well-known port).
type Database struct {
	Engine    string   `json:"engine"` // postgres, mysql, mariadb, mongodb, redis
	Workload  string   `json:"workload"`
	Pod       string   `json:"pod"`
	Container string   `json:"container"`
	Image     string   `json:"image"`
	Volumes   []string `json:"volumes,omitempty"` // claims it mounts
}

// Namespace is one namespace's inventory.
type Namespace struct {
	Name      string         `json:"name"`
	Objects   map[string]int `json:"objects"` // kind → count of objects a k8s-namespace backup would save
	Pods      int            `json:"pods"`
	Volumes   []Volume       `json:"volumes"`
	Databases []Database     `json:"databases"`
}

var engines = []struct {
	engine string
	image  *regexp.Regexp
	port   int32
}{
	{"postgres", regexp.MustCompile(`(?i)(postgres|postgis|timescale|pgvector|cloudnative-pg)`), 5432},
	{"mariadb", regexp.MustCompile(`(?i)mariadb`), 0},
	{"mysql", regexp.MustCompile(`(?i)(mysql|percona)`), 3306},
	{"mongodb", regexp.MustCompile(`(?i)mongo`), 27017},
	{"redis", regexp.MustCompile(`(?i)(redis|valkey|keydb)`), 6379},
}

// tools are images that name a database but are not one.
var tools = regexp.MustCompile(`(?i)(redisinsight|redis-commander|phpmyadmin|adminer|pgadmin|mongo-express|exporter|operator)`)

// engineOf names the database a container runs, or "".
func engineOf(c corev1.Container) string {
	if tools.MatchString(c.Image) {
		return ""
	}
	for _, e := range engines {
		if e.image.MatchString(c.Image) {
			return e.engine
		}
	}
	for _, e := range engines {
		for _, p := range c.Ports {
			if e.port != 0 && p.ContainerPort == e.port {
				return e.engine
			}
		}
	}
	return ""
}

func controllerOf(p *corev1.Pod) string {
	for _, o := range p.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			continue
		}
		if o.Kind == "ReplicaSet" {
			if h := p.Labels["pod-template-hash"]; h != "" && strings.HasSuffix(o.Name, "-"+h) {
				return "Deployment/" + strings.TrimSuffix(o.Name, "-"+h)
			}
		}
		return o.Kind + "/" + o.Name
	}
	return "Pod/" + p.Name
}

// Inventory takes stock of every namespace of a cluster (or just ns).
func Inventory(ctx context.Context, cli kubernetes.Interface, ns string) ([]Namespace, error) {
	var names []string
	if ns != "" {
		names = []string{ns}
	} else {
		l, err := cli.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for _, n := range l.Items {
			names = append(names, n.Name)
		}
		sort.Strings(names)
	}
	counts, err := k8ssrc.CountObjects(ctx, cli, ns)
	if err != nil {
		return nil, err
	}
	pods, err := cli.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pvcs, err := cli.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	by := map[string]*Namespace{}
	out := make([]Namespace, len(names))
	for i, n := range names {
		out[i] = Namespace{Name: n, Objects: counts[n], Volumes: []Volume{}, Databases: []Database{}}
		if out[i].Objects == nil {
			out[i].Objects = map[string]int{}
		}
		by[n] = &out[i]
	}
	// claim → workloads using it, and → the engine of a database mounting it
	users := map[string]map[string]bool{}
	dbOn := map[string]string{}
	seenDB := map[string]bool{}
	for i := range pods.Items {
		p := &pods.Items[i]
		n := by[p.Namespace]
		if n == nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		n.Pods++
		wl := controllerOf(p)
		var claims []string
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				key := p.Namespace + "/" + v.PersistentVolumeClaim.ClaimName
				if users[key] == nil {
					users[key] = map[string]bool{}
				}
				users[key][wl] = true
				claims = append(claims, v.PersistentVolumeClaim.ClaimName)
			}
		}
		for _, c := range p.Spec.Containers {
			e := engineOf(c)
			if e == "" || seenDB[p.Namespace+"/"+wl+"/"+c.Name] {
				continue
			}
			seenDB[p.Namespace+"/"+wl+"/"+c.Name] = true
			n.Databases = append(n.Databases, Database{Engine: e, Workload: wl, Pod: p.Name, Container: c.Name, Image: c.Image, Volumes: claims})
			for _, cl := range claims {
				dbOn[p.Namespace+"/"+cl] = e
			}
		}
	}
	for _, c := range pvcs.Items {
		n := by[c.Namespace]
		if n == nil {
			continue
		}
		v := Volume{Claim: c.Name, Phase: string(c.Status.Phase), Database: dbOn[c.Namespace+"/"+c.Name]}
		if q, ok := c.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			v.Size = q.String()
		}
		if c.Spec.StorageClassName != nil {
			v.StorageClass = *c.Spec.StorageClassName
		}
		for u := range users[c.Namespace+"/"+c.Name] {
			v.UsedBy = append(v.UsedBy, u)
		}
		sort.Strings(v.UsedBy)
		n.Volumes = append(n.Volumes, v)
	}
	for i := range out {
		sort.Slice(out[i].Volumes, func(a, b int) bool { return out[i].Volumes[a].Claim < out[i].Volumes[b].Claim })
		sort.Slice(out[i].Databases, func(a, b int) bool { return out[i].Databases[a].Workload < out[i].Databases[b].Workload })
	}
	return out, nil
}
