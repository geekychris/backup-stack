package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	k8ssrc "github.com/geekychris/backup-stack/internal/source/k8s"
	sshsrc "github.com/geekychris/backup-stack/internal/source/ssh"

	s3sink "github.com/geekychris/backup-stack/internal/sink/s3"
	syncpkg "github.com/geekychris/backup-stack/internal/sync"

	"github.com/geekychris/backup-stack/internal/store"
)

// newSSHSource builds an ssh-path Source, authenticating with the private
// key at cfg.PrivateKeyPath and verifying the host key against
// cfg.KnownHostsPath (or skipping verification if cfg explicitly opts out —
// never silently).
func newSSHSource(cfg sshTargetConfig) (*sshsrc.Source, error) {
	keyBytes, err := os.ReadFile(cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("ssh private key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("ssh private key: %w", err)
	}
	var hostKeyCB ssh.HostKeyCallback
	switch {
	case cfg.InsecureSkipHostKeyCheck:
		hostKeyCB = ssh.InsecureIgnoreHostKey()
	case cfg.KnownHostsPath != "":
		cb, err := knownhosts.New(cfg.KnownHostsPath)
		if err != nil {
			return nil, fmt.Errorf("ssh known_hosts: %w", err)
		}
		hostKeyCB = cb
	default:
		return nil, errors.New("set knownHostsPath (the host's known_hosts entry) or insecureSkipHostKeyCheck")
	}
	return sshsrc.New(sshsrc.Config{
		Addr: cfg.Addr,
		User: cfg.User,
		ClientConfig: &ssh.ClientConfig{
			User:            cfg.User,
			Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
			HostKeyCallback: hostKeyCB,
			Timeout:         15 * time.Second,
		},
		Path: cfg.Path,
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

// runSyncJob one-way mirrors j's source bucket onto its dest bucket.
func runSyncJob(ctx context.Context, j *store.SyncJob) (syncpkg.Result, error) {
	var srcCfg, dstCfg syncpkg.S3Config
	if err := json.Unmarshal(j.SourceConfig, &srcCfg); err != nil {
		return syncpkg.Result{}, fmt.Errorf("sourceConfig: %w", err)
	}
	if err := json.Unmarshal(j.DestConfig, &dstCfg); err != nil {
		return syncpkg.Result{}, fmt.Errorf("destConfig: %w", err)
	}
	srcStore, err := syncpkg.NewS3Store(ctx, srcCfg)
	if err != nil {
		return syncpkg.Result{}, fmt.Errorf("source bucket: %w", err)
	}
	dstStore, err := syncpkg.NewS3Store(ctx, dstCfg)
	if err != nil {
		return syncpkg.Result{}, fmt.Errorf("dest bucket: %w", err)
	}
	syncer := &syncpkg.Syncer{Source: srcStore, Dest: dstStore}
	return syncer.Run(ctx)
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
