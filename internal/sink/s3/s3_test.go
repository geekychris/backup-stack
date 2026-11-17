package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// TestS3SinkWriteAndRead exercises a real MinIO (or other S3-compatible)
// endpoint. It is skipped unless BACKUP_TEST_S3_ENDPOINT is set, so the
// default `go test ./...` run needs no real MinIO.
func TestS3SinkWriteAndRead(t *testing.T) {
	endpoint := os.Getenv("BACKUP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("BACKUP_TEST_S3_ENDPOINT not set")
	}
	bucket := os.Getenv("BACKUP_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "backup-stack-test"
	}
	ctx := context.Background()
	sk, err := New(ctx, Config{
		Endpoint:     endpoint,
		Bucket:       bucket,
		AccessKey:    os.Getenv("BACKUP_TEST_S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("BACKUP_TEST_S3_SECRET_KEY"),
		Prefix:       "s3sink-test/",
		CreateBucket: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	runID := "run-1"
	want := catalog.Catalog{Items: []catalog.Item{{Path: "a.txt", Fingerprint: "1:2", Size: 5}}}
	if err := sk.WriteCatalog(ctx, runID, want); err != nil {
		t.Fatalf("WriteCatalog: %v", err)
	}
	got, err := sk.OpenCatalog(ctx, runID)
	if err != nil {
		t.Fatalf("OpenCatalog: %v", err)
	}
	wb, _ := want.Marshal()
	gb, _ := got.Marshal()
	if !bytes.Equal(wb, gb) {
		t.Fatalf("catalog round trip mismatch: got %s want %s", gb, wb)
	}

	if err := sk.WriteItem(ctx, runID, "a.txt", bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("WriteItem: %v", err)
	}
	r, err := sk.OpenItem(ctx, runID, "a.txt")
	if err != nil {
		t.Fatalf("OpenItem: %v", err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello" {
		t.Fatalf("got %q", content)
	}

	if err := sk.DeleteRun(ctx, runID); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}
	if _, err := sk.OpenCatalog(ctx, runID); err == nil {
		t.Fatal("expected OpenCatalog to fail after DeleteRun")
	}
}
