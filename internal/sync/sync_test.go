package sync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
)

// fakeStore is an in-memory ObjectStore; DeleteCalls records every Delete
// call so a test can assert it was never made.
type fakeStore struct {
	objects     map[string][]byte
	DeleteCalls []string
}

func newFakeStore(objs map[string]string) *fakeStore {
	m := make(map[string][]byte, len(objs))
	for k, v := range objs {
		m[k] = []byte(v)
	}
	return &fakeStore{objects: m}
}

func (f *fakeStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for k, v := range f.objects {
		out = append(out, ObjectInfo{Key: k, Size: int64(len(v))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	_ = prefix
	return out, nil
}

func (f *fakeStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	b, ok := f.objects[key]
	if !ok {
		return nil, fmt.Errorf("fakeStore: no object %q", key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.objects[key] = b
	return nil
}

func (f *fakeStore) Delete(_ context.Context, key string) error {
	f.DeleteCalls = append(f.DeleteCalls, key)
	delete(f.objects, key)
	return nil
}

func keys(infos []string) []string {
	out := append([]string{}, infos...)
	sort.Strings(out)
	return out
}

func TestSyncCopiesMissingAndDiffering(t *testing.T) {
	src := newFakeStore(map[string]string{
		"a": "same-content",      // present on dest, same size: untouched
		"b": "missing-from-dest", // missing on dest: copied
		"c": "longer-now-value",  // present on dest, different size: copied
	})
	dst := newFakeStore(map[string]string{
		"a": "same-content",
		"c": "short",
		"d": "dest-only", // not on source: must survive (see TestSyncNeverDeletes)
	})
	s := &Syncer{Source: src, Dest: dst}
	res, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := keys(res.Copied); !equalStrs(got, []string{"b", "c"}) {
		t.Fatalf("copied = %v, want [b c]", got)
	}
	if string(dst.objects["b"]) != "missing-from-dest" || string(dst.objects["c"]) != "longer-now-value" {
		t.Fatalf("dest contents wrong: b=%q c=%q", dst.objects["b"], dst.objects["c"])
	}
	if string(dst.objects["a"]) != "same-content" {
		t.Fatalf("untouched key a changed: %q", dst.objects["a"])
	}
}

func TestSyncNeverDeletes(t *testing.T) {
	src := newFakeStore(map[string]string{"a": "x"})
	dst := newFakeStore(map[string]string{"a": "x", "only-on-dest": "y"})
	s := &Syncer{Source: src, Dest: dst}
	if _, err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dst.DeleteCalls) != 0 {
		t.Fatalf("Syncer.Run called Delete: %v", dst.DeleteCalls)
	}
	if _, ok := dst.objects["only-on-dest"]; !ok {
		t.Fatal("destination-only object was removed; sync must be one-way and additive only")
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
