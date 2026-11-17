package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/store"
)

// fakeSource is an in-memory source.Source: its file set is whatever the
// test last Set/Delete'd, and Snapshot/Open always reflect that.
type fakeSource struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newFakeSource() *fakeSource { return &fakeSource{files: map[string][]byte{}} }

func (f *fakeSource) Set(path string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = content
}

func (f *fakeSource) Delete(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, path)
}

func (f *fakeSource) Snapshot(context.Context) (catalog.Catalog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var cat catalog.Catalog
	for p, b := range f.files {
		sum := sha256.Sum256(b)
		cat.Items = append(cat.Items, catalog.Item{Path: p, Fingerprint: hex.EncodeToString(sum[:]), Size: int64(len(b))})
	}
	return cat, nil
}

func (f *fakeSource) Open(_ context.Context, path string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.files[path]
	if !ok {
		return nil, fmt.Errorf("fakeSource: no file %q", path)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeSource) Close() error { return nil }

// fakeSink is an in-memory sink.Sink.
type fakeSink struct {
	mu       sync.Mutex
	items    map[string]map[string][]byte
	catalogs map[string]catalog.Catalog
}

func newFakeSink() *fakeSink {
	return &fakeSink{items: map[string]map[string][]byte{}, catalogs: map[string]catalog.Catalog{}}
}

func (s *fakeSink) WriteItem(_ context.Context, runID, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items[runID] == nil {
		s.items[runID] = map[string][]byte{}
	}
	s.items[runID][path] = b
	return nil
}

func (s *fakeSink) OpenItem(_ context.Context, runID, path string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.items[runID][path]
	if !ok {
		return nil, fmt.Errorf("fakeSink: no item %s/%s", runID, path)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *fakeSink) WriteCatalog(_ context.Context, runID string, cat catalog.Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalogs[runID] = cat
	return nil
}

func (s *fakeSink) OpenCatalog(_ context.Context, runID string) (catalog.Catalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.catalogs[runID]
	if !ok {
		return catalog.Catalog{}, fmt.Errorf("fakeSink: no catalog %s", runID)
	}
	return c, nil
}

func (s *fakeSink) DeleteRun(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, runID)
	delete(s.catalogs, runID)
	return nil
}

func (s *fakeSink) Prune(context.Context, map[string]bool) error { return nil }

func newTestRunner(t *testing.T) (*Runner, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seq := 0
	return &Runner{Store: st, NewRunID: func() string {
		seq++
		return fmt.Sprintf("run-%d", seq)
	}}, st
}

func createTarget(t *testing.T, st *store.Store, id string) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateTarget(ctx, &store.Target{ID: id, Name: id, Type: store.TypeSSHPath, Config: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestRunFull(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)
	createTarget(t, st, "t1")

	src := newFakeSource()
	src.Set("a.txt", []byte("aaa"))
	src.Set("b.txt", []byte("bbb"))
	sk := newFakeSink()

	run, err := r.RunFull(ctx, "t1", src, sk)
	if err != nil {
		t.Fatal(err)
	}
	if run.Kind != store.KindFull || run.State != store.StateSucceeded || run.ItemCount != 2 {
		t.Fatalf("run = %+v", run)
	}
	if len(sk.items[run.ID]) != 2 {
		t.Fatalf("sink wrote %d items, want 2", len(sk.items[run.ID]))
	}
	cat := sk.catalogs[run.ID]
	if len(cat.Items) != 2 || len(cat.Deleted) != 0 {
		t.Fatalf("catalog = %+v", cat)
	}
}

func TestRunFullRejectsWhenAlreadyInFlight(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)
	createTarget(t, st, "t1")
	if err := st.CreateRun(ctx, &store.Run{ID: "already-running", TargetID: "t1", Kind: store.KindFull, State: store.StateRunning}); err != nil {
		t.Fatal(err)
	}

	src := newFakeSource()
	src.Set("a.txt", []byte("aaa"))
	sk := newFakeSink()

	if _, err := r.RunFull(ctx, "t1", src, sk); !errors.Is(err, store.ErrRunInFlight) {
		t.Fatalf("RunFull while a run is in flight = %v, want ErrRunInFlight", err)
	}
}

func TestRunIncrementalWithNoPriorFull(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)
	createTarget(t, st, "t1")

	src := newFakeSource()
	sk := newFakeSink()
	if _, err := r.RunIncremental(ctx, "t1", src, sk); !errors.Is(err, ErrNoFullRun) {
		t.Fatalf("got %v, want ErrNoFullRun", err)
	}
}

func finalState(paths []string) []string {
	sort.Strings(paths)
	return paths
}

func itemPaths(items []catalog.Item) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.Path)
	}
	return finalState(out)
}

func TestRunIncrementalChain(t *testing.T) {
	ctx := context.Background()
	r, st := newTestRunner(t)
	createTarget(t, st, "t1")

	src := newFakeSource()
	src.Set("a.txt", []byte("a-v1"))
	src.Set("b.txt", []byte("b-v1"))
	sk := newFakeSink()

	full, err := r.RunFull(ctx, "t1", src, sk)
	if err != nil {
		t.Fatal(err)
	}

	// Incremental 1: change a.txt.
	src.Set("a.txt", []byte("a-v2"))
	inc1, err := r.RunIncremental(ctx, "t1", src, sk)
	if err != nil {
		t.Fatal(err)
	}
	if inc1.Kind != store.KindIncremental || inc1.ItemCount != 1 {
		t.Fatalf("inc1 = %+v", inc1)
	}
	if got := itemPaths(sk.catalogs[inc1.ID].Items); !equalStrs(got, []string{"a.txt"}) {
		t.Fatalf("inc1 catalog items = %v, want [a.txt]", got)
	}

	// Incremental 2: add c.txt, delete b.txt.
	src.Set("c.txt", []byte("c-v1"))
	src.Delete("b.txt")
	inc2, err := r.RunIncremental(ctx, "t1", src, sk)
	if err != nil {
		t.Fatal(err)
	}
	if got := itemPaths(sk.catalogs[inc2.ID].Items); !equalStrs(got, []string{"c.txt"}) {
		t.Fatalf("inc2 catalog items = %v, want [c.txt]", got)
	}
	if got := sk.catalogs[inc2.ID].Deleted; !equalStrs(got, []string{"b.txt"}) {
		t.Fatalf("inc2 catalog deleted = %v, want [b.txt]", got)
	}

	// Replaying full + inc1 + inc2, in order, must reconstruct exactly
	// {a.txt (v2 content), c.txt}, with b.txt gone.
	state, owner, err := r.ChainCatalog(ctx, "t1", inc2.ID, sk)
	if err != nil {
		t.Fatal(err)
	}
	if got := itemPaths(state.Items); !equalStrs(got, []string{"a.txt", "c.txt"}) {
		t.Fatalf("reconstructed state = %v, want [a.txt c.txt]", got)
	}
	_ = full // the full run's ID isn't asserted on directly; its presence in the chain is what matters.

	// The restored content for each surviving path must come from the run
	// that actually wrote it: a.txt's latest content from inc1, c.txt's
	// from inc2.
	assertContent := func(path, wantRunID, wantContent string) {
		t.Helper()
		if owner[path] != wantRunID {
			t.Fatalf("owner[%q] = %q, want %q", path, owner[path], wantRunID)
		}
		r, err := sk.OpenItem(ctx, owner[path], path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, _ := io.ReadAll(r)
		if string(got) != wantContent {
			t.Fatalf("content of %q = %q, want %q", path, got, wantContent)
		}
	}
	assertContent("a.txt", inc1.ID, "a-v2")
	assertContent("c.txt", inc2.ID, "c-v1")
}

func equalStrs(a, b []string) bool {
	a, b = finalState(append([]string{}, a...)), finalState(append([]string{}, b...))
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
