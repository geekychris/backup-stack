package sync

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectInfo is one object's key and size, as an ObjectStore lists it.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ObjectStore is the object-storage surface an S3Tree needs; S3Store
// implements it over minio-go, tests over a map.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Delete(ctx context.Context, key string) error
}

// S3Config configures an S3Store, one side of a sync (source or dest).
type S3Config struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	UseSSL       bool   `json:"useSSL"`
	CreateBucket bool   `json:"createBucket"`
}

// S3Store is an ObjectStore backed by an S3-compatible (MinIO) bucket.
type S3Store struct {
	c      *minio.Client
	bucket string
}

var _ ObjectStore = (*S3Store)(nil)

// NewS3Store connects to the configured bucket.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	endpoint, secure := cfg.Endpoint, cfg.UseSSL
	if u, err := url.Parse(cfg.Endpoint); err == nil && u.Scheme != "" && u.Host != "" {
		endpoint, secure = u.Host, u.Scheme == "https"
	}
	c, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: secure, Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("sync/s3: client: %w", err)
	}
	ok, err := c.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("sync/s3: bucket %q: %w", cfg.Bucket, err)
	}
	if !ok {
		if !cfg.CreateBucket {
			return nil, fmt.Errorf("sync/s3: bucket %q does not exist", cfg.Bucket)
		}
		if err := c.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, fmt.Errorf("sync/s3: create bucket %q: %w", cfg.Bucket, err)
		}
	}
	return &S3Store{c: c, bucket: cfg.Bucket}, nil
}

// List implements ObjectStore.
func (s *S3Store) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for obj := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, ObjectInfo{Key: obj.Key, Size: obj.Size})
	}
	return out, nil
}

// Get implements ObjectStore.
func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("sync/s3: get %s: %w", key, err)
	}
	return obj, nil
}

// Put implements ObjectStore.
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.c.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("sync/s3: put %s: %w", key, err)
	}
	return nil
}

// Delete implements ObjectStore. Sync only calls it for a job with Delete
// set.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	if err := s.c.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("sync/s3: delete %s: %w", key, err)
	}
	return nil
}

// S3Tree is a key prefix in a bucket, as a Tree.
type S3Tree struct {
	Store  ObjectStore
	Prefix string
}

var _ Tree = (*S3Tree)(nil)

func (t *S3Tree) dirPrefix() string {
	p := strings.Trim(t.Prefix, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// List implements Tree. S3 objects carry no comparable modification time,
// so a changed object is detected by size only.
func (t *S3Tree) List(ctx context.Context) (map[string]Entry, error) {
	objs, err := t.Store.List(ctx, t.dirPrefix())
	if err != nil {
		return nil, err
	}
	out := make(map[string]Entry, len(objs))
	p := t.dirPrefix()
	for _, o := range objs {
		rel := strings.TrimPrefix(o.Key, p)
		if rel == "" || strings.HasSuffix(rel, "/") {
			continue
		}
		out[rel] = Entry{Size: o.Size}
	}
	return out, nil
}

// Open implements Tree.
func (t *S3Tree) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	return t.Store.Get(ctx, t.dirPrefix()+rel)
}

// Write implements Tree.
func (t *S3Tree) Write(ctx context.Context, rel string, r io.Reader, e Entry) error {
	return t.Store.Put(ctx, t.dirPrefix()+rel, r, e.Size)
}

// Remove implements Tree.
func (t *S3Tree) Remove(ctx context.Context, rel string) error {
	return t.Store.Delete(ctx, t.dirPrefix()+rel)
}

// Close implements Tree.
func (t *S3Tree) Close() error { return nil }
