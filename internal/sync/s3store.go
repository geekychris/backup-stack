package sync

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config configures an S3Store, one side of a sync (source or dest).
type S3Config struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	UseSSL       bool
	CreateBucket bool
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

// Delete implements ObjectStore. Syncer itself never calls this; it exists
// so S3Store satisfies the interface other callers (retention pruning) use.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	if err := s.c.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("sync/s3: delete %s: %w", key, err)
	}
	return nil
}
