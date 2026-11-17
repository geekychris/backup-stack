// Package s3 is a Sink backed by an S3-compatible (MinIO) bucket.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/sink"
)

// Config configures a Sink.
type Config struct {
	// Endpoint is host[:port] or a URL (whose scheme decides TLS).
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	// Prefix is prepended to every key (e.g. "backup-stack/").
	Prefix string
	UseSSL bool
	// CreateBucket creates the bucket when missing.
	CreateBucket bool
}

// Sink stores backup runs in S3 or MinIO under
// "<prefix><runID>/catalog.json" and "<prefix><runID>/items/<path>".
type Sink struct {
	c      *minio.Client
	bucket string
	prefix string
}

var _ sink.Sink = (*Sink)(nil)

// New connects to the configured bucket.
func New(ctx context.Context, cfg Config) (*Sink, error) {
	endpoint, secure := cfg.Endpoint, cfg.UseSSL
	if u, err := url.Parse(cfg.Endpoint); err == nil && u.Scheme != "" && u.Host != "" {
		endpoint, secure = u.Host, u.Scheme == "https"
	}
	c, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: secure, Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("sink/s3: client: %w", err)
	}
	ok, err := c.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("sink/s3: bucket %q: %w", cfg.Bucket, err)
	}
	if !ok {
		if !cfg.CreateBucket {
			return nil, fmt.Errorf("sink/s3: bucket %q does not exist", cfg.Bucket)
		}
		if err := c.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, fmt.Errorf("sink/s3: create bucket %q: %w", cfg.Bucket, err)
		}
	}
	prefix := strings.TrimPrefix(cfg.Prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Sink{c: c, bucket: cfg.Bucket, prefix: prefix}, nil
}

func (s *Sink) itemKey(runID, path string) string { return s.prefix + runID + "/items/" + path }
func (s *Sink) catalogKey(runID string) string    { return s.prefix + runID + "/catalog.json" }
func (s *Sink) runPrefix(runID string) string     { return s.prefix + runID + "/" }

// WriteItem implements sink.Sink.
func (s *Sink) WriteItem(ctx context.Context, runID, path string, r io.Reader) error {
	_, err := s.c.PutObject(ctx, s.bucket, s.itemKey(runID, path), r, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("sink/s3: write item %s/%s: %w", runID, path, err)
	}
	return nil
}

// OpenItem implements sink.Sink.
func (s *Sink) OpenItem(ctx context.Context, runID, path string) (io.ReadCloser, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, s.itemKey(runID, path), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("sink/s3: open item %s/%s: %w", runID, path, err)
	}
	return obj, nil
}

// WriteCatalog implements sink.Sink.
func (s *Sink) WriteCatalog(ctx context.Context, runID string, cat catalog.Catalog) error {
	b, err := cat.Marshal()
	if err != nil {
		return err
	}
	_, err = s.c.PutObject(ctx, s.bucket, s.catalogKey(runID), bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("sink/s3: write catalog %s: %w", runID, err)
	}
	return nil
}

// OpenCatalog implements sink.Sink.
func (s *Sink) OpenCatalog(ctx context.Context, runID string) (catalog.Catalog, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, s.catalogKey(runID), minio.GetObjectOptions{})
	if err != nil {
		return catalog.Catalog{}, fmt.Errorf("sink/s3: open catalog %s: %w", runID, err)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		return catalog.Catalog{}, err
	}
	return catalog.Unmarshal(b)
}

// DeleteRun implements sink.Sink.
func (s *Sink) DeleteRun(ctx context.Context, runID string) error {
	return s.deletePrefix(ctx, s.runPrefix(runID))
}

func (s *Sink) deletePrefix(ctx context.Context, prefix string) error {
	for obj := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if err := s.c.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("sink/s3: delete %s: %w", obj.Key, err)
		}
	}
	return nil
}

// Prune implements sink.Sink: it deletes every run's data except the
// runIDs named in keep.
func (s *Sink) Prune(ctx context.Context, keep map[string]bool) error {
	toDelete := map[string]bool{}
	for obj := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		rest := strings.TrimPrefix(obj.Key, s.prefix)
		runID, _, ok := strings.Cut(rest, "/")
		if !ok || keep[runID] {
			continue
		}
		toDelete[runID] = true
	}
	for runID := range toDelete {
		if err := s.DeleteRun(ctx, runID); err != nil {
			return err
		}
	}
	return nil
}
