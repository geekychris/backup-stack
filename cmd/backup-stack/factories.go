package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	k8ssrc "github.com/geekychris/backup-stack/internal/source/k8s"
	sshsrc "github.com/geekychris/backup-stack/internal/source/ssh"

	s3sink "github.com/geekychris/backup-stack/internal/sink/s3"
)

// newSSHSource builds an ssh-path Source, authenticating with the private
// key at cfg.PrivateKeyPath and verifying the host key against
// cfg.KnownHostsPath (or skipping verification if cfg explicitly opts out —
// never silently).
//
// A blank privateKeyPath / knownHostsPath falls back to BACKUP_STACK_SSH_KEY
// / BACKUP_STACK_SSH_KNOWN_HOSTS (the mounted backup-stack-ssh Secret in
// Kubernetes).
func newSSHSource(cfg sshTargetConfig) (*sshsrc.Source, error) {
	if err := cfg.Filters.Validate(); err != nil {
		return nil, err
	}
	conn := cfg.Config.WithDefaults()
	if err := conn.Validate(); err != nil {
		return nil, err
	}
	cc, err := conn.ClientConfig()
	if err != nil {
		return nil, err
	}
	return sshsrc.New(sshsrc.Config{
		Addr:         conn.Addr,
		User:         conn.User,
		ClientConfig: cc,
		Path:         conn.Path,
		Filters:      cfg.Filters,
	}), nil
}

// newK8sSource builds a k8s-namespace Source from a kubeconfig path (or,
// if empty, the in-cluster config, then the default kubeconfig chain).
func newK8sSource(cfg k8sTargetConfig) (*k8ssrc.Source, error) {
	rc, err := newRestConfig(cfg.KubeconfigPath)
	if err != nil {
		return nil, err
	}
	cli, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	return k8ssrc.New(k8ssrc.Config{Client: cli, Namespaces: cfg.Namespaces, ExcludeKinds: cfg.ExcludeKinds}), nil
}

func newRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("KUBECONFIG")
	}
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		return rest.InClusterConfig()
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

// newS3Sink connects a Sink from an s3Config.
func newS3Sink(ctx context.Context, cfg s3Config) (*s3sink.Sink, error) {
	return s3sink.New(ctx, s3sink.Config{
		Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, Prefix: cfg.Prefix,
		UseSSL: cfg.UseSSL, CreateBucket: cfg.CreateBucket,
	})
}

// writeFileAll writes r to dest, creating any missing parent directories.
func writeFileAll(dest string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}
