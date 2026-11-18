package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/geekychris/backup-stack/internal/sshconn"
)

// Endpoint types.
const (
	TypeS3  = "s3"
	TypeDir = "dir" // a directory local to backup-stack (in Kubernetes: a mounted volume)
	TypeSSH = "ssh" // a directory on a remote host over SSH
)

// Endpoint is one side of a sync job, as stored in a job's sourceConfig /
// destConfig:
//
//	{"type": "dir", "path": "/data/photos"}
//	{"type": "ssh", "addr": "wopr.local:22", "user": "chris", "path": "/srv/photos",
//	 "privateKeyPath": "...", "knownHostsPath": "..."}   (key/known_hosts default to BACKUP_STACK_SSH_*)
//	{"type": "s3", "sink": "primary", "prefix": "mirror/photos"}   (a configured sink's endpoint and credentials)
//	{"type": "s3", "endpoint": "http://host:9000", "bucket": "b", "accessKey": "...", "secretKey": "...", "prefix": "p"}
//
// A config with no "type" is S3 — the shape sync jobs had before
// directories were supported.
type Endpoint struct {
	Type string `json:"type"`
	// Path is the directory for dir and ssh endpoints.
	Path string `json:"path,omitempty"`

	// ssh (the ssh-path backup target's connection fields).
	Addr                     string `json:"addr,omitempty"`
	User                     string `json:"user,omitempty"`
	PrivateKeyPath           string `json:"privateKeyPath,omitempty"`
	KnownHostsPath           string `json:"knownHostsPath,omitempty"`
	InsecureSkipHostKeyCheck bool   `json:"insecureSkipHostKeyCheck,omitempty"`

	// s3
	Sink         string `json:"sink,omitempty"`
	Endpoint     string `json:"endpoint,omitempty"`
	Region       string `json:"region,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	AccessKey    string `json:"accessKey,omitempty"`
	SecretKey    string `json:"secretKey,omitempty"`
	Prefix       string `json:"prefix,omitempty"`
	UseSSL       bool   `json:"useSSL,omitempty"`
	CreateBucket bool   `json:"createBucket,omitempty"`
}

// ParseEndpoint decodes and validates one endpoint config.
func ParseEndpoint(raw json.RawMessage) (Endpoint, error) {
	var e Endpoint
	if len(raw) == 0 {
		return e, errors.New("endpoint config is empty")
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, err
	}
	if e.Type == "" {
		e.Type = TypeS3
	}
	return e, e.Validate()
}

// Validate checks the fields e's type needs.
func (e Endpoint) Validate() error {
	switch e.Type {
	case TypeDir:
		if !path.IsAbs(e.Path) {
			return fmt.Errorf("dir endpoint: path must be absolute, got %q", e.Path)
		}
	case TypeSSH:
		if err := e.SSH().Validate(); err != nil {
			return err
		}
		if !path.IsAbs(e.Path) {
			return fmt.Errorf("ssh endpoint: path must be absolute, got %q", e.Path)
		}
	case TypeS3:
		if e.Sink == "" && e.Bucket == "" {
			return errors.New("s3 endpoint: set sink (a configured sink's name) or bucket")
		}
	default:
		return fmt.Errorf("unknown endpoint type %q (want dir, ssh or s3)", e.Type)
	}
	return nil
}

// IsDir reports whether e is a directory (local or remote).
func (e Endpoint) IsDir() bool { return e.Type == TypeDir || e.Type == TypeSSH }

// SSH is e's connection config, defaults applied.
func (e Endpoint) SSH() sshconn.Config {
	return sshconn.Config{
		Addr: e.Addr, User: e.User, Path: e.Path,
		PrivateKeyPath: e.PrivateKeyPath, KnownHostsPath: e.KnownHostsPath,
		InsecureSkipHostKeyCheck: e.InsecureSkipHostKeyCheck,
	}.WithDefaults()
}

// String is a short human-readable location: "dir:/data", "ssh:chris@wopr:/srv",
// "s3:primary/prefix".
func (e Endpoint) String() string {
	switch e.Type {
	case TypeDir:
		return "dir:" + e.Path
	case TypeSSH:
		return "ssh:" + e.User + "@" + e.Addr + ":" + e.Path
	default:
		where := e.Sink
		if where == "" {
			where = e.Bucket
		}
		if e.Prefix != "" {
			where += "/" + strings.Trim(e.Prefix, "/")
		}
		return "s3:" + where
	}
}

// same reports whether two endpoints name the same place (a job copying
// onto itself).
func same(a, b Endpoint) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TypeDir:
		return path.Clean(a.Path) == path.Clean(b.Path)
	case TypeSSH:
		return a.SSH().Addr == b.SSH().Addr && path.Clean(a.Path) == path.Clean(b.Path)
	default:
		return a.Sink == b.Sink && a.Endpoint == b.Endpoint && a.Bucket == b.Bucket &&
			strings.Trim(a.Prefix, "/") == strings.Trim(b.Prefix, "/")
	}
}

// JobKind is a job's kind, "<source type>-><dest type>" (dir->s3, ssh->dir, ...).
func JobKind(src, dst json.RawMessage) string {
	t := func(raw json.RawMessage) string {
		var e struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.Type == "" {
			return TypeS3
		}
		return e.Type
	}
	return t(src) + "->" + t(dst)
}

// ValidateJob checks a job's two endpoints and that they differ.
func ValidateJob(src, dst json.RawMessage) error {
	s, err := ParseEndpoint(src)
	if err != nil {
		return fmt.Errorf("sourceConfig: %w", err)
	}
	d, err := ParseEndpoint(dst)
	if err != nil {
		return fmt.Errorf("destConfig: %w", err)
	}
	if same(s, d) {
		return fmt.Errorf("source and destination are the same place (%s)", s)
	}
	return nil
}

// SinkResolver returns a configured sink's S3 settings by name.
type SinkResolver func(name string) (S3Config, string, error)

// openTree connects e.
func openTree(ctx context.Context, e Endpoint, sinks SinkResolver) (Tree, error) {
	switch e.Type {
	case TypeDir:
		return &LocalTree{Root: e.Path}, nil
	case TypeSSH:
		return NewSSHTree(e.SSH()), nil
	default:
		cfg := S3Config{
			Endpoint: e.Endpoint, Region: e.Region, Bucket: e.Bucket, AccessKey: e.AccessKey,
			SecretKey: e.SecretKey, UseSSL: e.UseSSL, CreateBucket: e.CreateBucket,
		}
		prefix := e.Prefix
		if e.Sink != "" {
			if sinks == nil {
				return nil, fmt.Errorf("s3 endpoint names sink %q but no sinks are configured", e.Sink)
			}
			sc, sinkPrefix, err := sinks(e.Sink)
			if err != nil {
				return nil, err
			}
			cfg = sc
			if e.Bucket != "" {
				cfg.Bucket = e.Bucket
			}
			prefix = path.Join(strings.Trim(sinkPrefix, "/"), strings.Trim(e.Prefix, "/"))
			if prefix == "." {
				prefix = ""
			}
		}
		st, err := NewS3Store(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return &S3Tree{Store: st, Prefix: prefix}, nil
	}
}
