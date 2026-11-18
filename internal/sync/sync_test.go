package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/geekychris/backup-stack/internal/filter"
	"github.com/geekychris/backup-stack/internal/sshconn"
	"github.com/geekychris/backup-stack/internal/store"
)

// ---- helpers ----

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func readTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		b, _ := os.ReadFile(p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// fakeObjects is an in-memory ObjectStore; DeleteCalls records every Delete.
type fakeObjects struct {
	mu          stdsync.Mutex
	objects     map[string][]byte
	DeleteCalls []string
}

func newFakeObjects(objs map[string]string) *fakeObjects {
	m := map[string][]byte{}
	for k, v := range objs {
		m[k] = []byte(v)
	}
	return &fakeObjects{objects: m}
}

func (f *fakeObjects) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ObjectInfo
	for k, v := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(v))})
		}
	}
	return out, nil
}

func (f *fakeObjects) Get(_ context.Context, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok {
		return nil, fmt.Errorf("no object %q", key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeObjects) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = b
	return nil
}

func (f *fakeObjects) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DeleteCalls = append(f.DeleteCalls, key)
	delete(f.objects, key)
	return nil
}

// ---- native directory sync ----

func TestNativeDirSync(t *testing.T) {
	ctx := context.Background()
	srcDir, dstDir := t.TempDir(), filepath.Join(t.TempDir(), "dst")
	writeFiles(t, srcDir, map[string]string{
		"a.txt":                 "aaa",
		"sub/b.txt":             "bbbb",
		"sub/skip.tmp":          "tmp",
		"node_modules/x/i.js":   "js",
		"deep/node_modules/y.c": "c",
	})
	rules := filter.Rules{
		{Kind: filter.Exclude, Pattern: "**/*.tmp"},
		{Kind: filter.Exclude, Pattern: "**/node_modules/**"},
	}
	src, dst := &LocalTree{Root: srcDir}, &LocalTree{Root: dstDir}
	st, err := Native(ctx, src, dst, Options{Filters: rules})
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 2 || st.Bytes != 7 || st.Errors != 0 || st.Skipped != 0 {
		t.Fatalf("first run stats = %+v", st)
	}
	got := readTree(t, dstDir)
	if strings.Join(sortedKeys(got), ",") != "a.txt,sub/b.txt" || got["sub/b.txt"] != "bbbb" {
		t.Fatalf("dest = %v", got)
	}
	// mtime carried over, so a second run has nothing to do.
	si, _ := os.Stat(filepath.Join(srcDir, "a.txt"))
	di, _ := os.Stat(filepath.Join(dstDir, "a.txt"))
	if !si.ModTime().Truncate(time.Second).Equal(di.ModTime().Truncate(time.Second)) {
		t.Fatalf("mtime not preserved: %v vs %v", si.ModTime(), di.ModTime())
	}
	st, err = Native(ctx, src, dst, Options{Filters: rules})
	if err != nil || st.Files != 0 || st.Skipped != 2 {
		t.Fatalf("second run stats = %+v, %v", st, err)
	}

	// A changed file is copied again.
	writeFiles(t, srcDir, map[string]string{"a.txt": "changed!"})
	st, _ = Native(ctx, src, dst, Options{Filters: rules})
	if st.Files != 1 || st.Bytes != 8 || readTree(t, dstDir)["a.txt"] != "changed!" {
		t.Fatalf("after change: %+v", st)
	}
}

func TestNativeNeverDeletesByDefault(t *testing.T) {
	ctx := context.Background()
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFiles(t, srcDir, map[string]string{"a": "x"})
	writeFiles(t, dstDir, map[string]string{"a": "x", "only-on-dest": "y", "keep.log": "z"})
	st, err := Native(ctx, &LocalTree{Root: srcDir}, &LocalTree{Root: dstDir}, Options{})
	if err != nil || st.Deleted != 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	if _, ok := readTree(t, dstDir)["only-on-dest"]; !ok {
		t.Fatal("destination-only file removed without Delete")
	}

	// With Delete: extras go, but never ones the filters exclude.
	st, err = Native(ctx, &LocalTree{Root: srcDir}, &LocalTree{Root: dstDir}, Options{
		Delete: true, Filters: filter.Rules{{Kind: filter.Exclude, Pattern: "*.log"}},
	})
	if err != nil || st.Deleted != 1 {
		t.Fatalf("delete stats %+v, %v", st, err)
	}
	got := readTree(t, dstDir)
	if _, ok := got["only-on-dest"]; ok {
		t.Fatal("extra not deleted")
	}
	if _, ok := got["keep.log"]; !ok {
		t.Fatal("excluded file deleted")
	}
}

func TestNativeCountsPerFileErrors(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	writeFiles(t, srcDir, map[string]string{"ok.txt": "1", "bad.txt": "2"})
	dst := &failingTree{LocalTree: LocalTree{Root: t.TempDir()}, fail: "bad.txt"}
	st, err := Native(ctx, &LocalTree{Root: srcDir}, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 1 || st.Errors != 1 || len(st.ErrorSamples) != 1 || !strings.Contains(st.ErrorSamples[0], "bad.txt") {
		t.Fatalf("stats = %+v", st)
	}
}

type failingTree struct {
	LocalTree
	fail string
}

func (f *failingTree) Write(ctx context.Context, rel string, r io.Reader, e Entry) error {
	if rel == f.fail {
		return fmt.Errorf("disk full")
	}
	return f.LocalTree.Write(ctx, rel, r, e)
}

func TestLocalTreeRejectsEscapes(t *testing.T) {
	tr := &LocalTree{Root: t.TempDir()}
	if err := tr.Write(context.Background(), "../evil", strings.NewReader("x"), Entry{}); err == nil {
		t.Fatal("write outside root allowed")
	}
}

// ---- S3 ----

func TestDirToS3AndBack(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	writeFiles(t, srcDir, map[string]string{"a.txt": "aaa", "d/b.txt": "bb", "x.tmp": "t"})
	objs := newFakeObjects(map[string]string{"mirror/stale": "old", "other/z": "z"})
	s3 := &S3Tree{Store: objs, Prefix: "/mirror/"}
	st, err := Native(ctx, &LocalTree{Root: srcDir}, s3, Options{Filters: filter.Rules{{Kind: filter.Exclude, Pattern: "*.tmp"}}})
	if err != nil || st.Files != 2 || st.Bytes != 5 {
		t.Fatalf("dir->s3: %+v, %v", st, err)
	}
	if string(objs.objects["mirror/d/b.txt"]) != "bb" || len(objs.DeleteCalls) != 0 {
		t.Fatalf("objects = %v, deletes = %v", sortedKeys(objs.objects), objs.DeleteCalls)
	}
	// Same size again: skipped.
	if st, _ := Native(ctx, &LocalTree{Root: srcDir}, s3, Options{}); st.Files != 1 || st.Skipped != 2 {
		// x.tmp is now unfiltered and new; a.txt and d/b.txt are skipped.
		t.Fatalf("second dir->s3: %+v", st)
	}

	back := t.TempDir()
	st, err = Native(ctx, s3, &LocalTree{Root: back}, Options{})
	if err != nil || st.Files != 4 {
		t.Fatalf("s3->dir: %+v, %v", st, err)
	}
	got := readTree(t, back)
	if got["d/b.txt"] != "bb" || got["stale"] != "old" {
		t.Fatalf("restored = %v", got)
	}
	if _, ok := got["../other/z"]; ok {
		t.Fatal("object outside the prefix copied")
	}
}

func TestS3ToS3NeverDeletes(t *testing.T) {
	src := newFakeObjects(map[string]string{"a": "x", "b": "longer"})
	dst := newFakeObjects(map[string]string{"a": "x", "b": "s", "only-on-dest": "y"})
	st, err := Native(context.Background(), &S3Tree{Store: src}, &S3Tree{Store: dst}, Options{})
	if err != nil || st.Files != 1 || st.Skipped != 1 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	if len(dst.DeleteCalls) != 0 {
		t.Fatalf("Delete called: %v", dst.DeleteCalls)
	}
	if string(dst.objects["b"]) != "longer" {
		t.Fatal("changed object not copied")
	}
}

// ---- endpoints ----

func TestEndpointsAndKinds(t *testing.T) {
	legacy := json.RawMessage(`{"endpoint":"http://wopr.local:9010","bucket":"lab-backups"}`)
	if e, err := ParseEndpoint(legacy); err != nil || e.Type != TypeS3 {
		t.Fatalf("legacy config: %+v, %v", e, err)
	}
	if k := JobKind(json.RawMessage(`{"type":"ssh"}`), legacy); k != "ssh->s3" {
		t.Fatalf("kind = %q", k)
	}
	bad := []string{`{"type":"dir","path":"rel"}`, `{"type":"ssh","addr":"h","path":"/x"}`, `{"type":"s3"}`, `{"type":"ftp"}`}
	for _, b := range bad {
		if _, err := ParseEndpoint(json.RawMessage(b)); err == nil {
			t.Errorf("%s: expected error", b)
		}
	}
	if err := ValidateJob(json.RawMessage(`{"type":"dir","path":"/a/"}`), json.RawMessage(`{"type":"dir","path":"/a"}`)); err == nil {
		t.Fatal("same source and dest accepted")
	}
	if err := ValidateJob(json.RawMessage(`{"type":"dir","path":"/a"}`), json.RawMessage(`{"type":"ssh","addr":"wopr.local","user":"u","path":"/a"}`)); err != nil {
		t.Fatal(err)
	}
}

// ---- rsync ----

func TestBuildRsyncArgs(t *testing.T) {
	remote := &sshconn.Config{Addr: "wopr.local:2222", User: "chris", Path: "/srv/photos",
		PrivateKeyPath: "/etc/backup-stack/ssh/id_ed25519", KnownHostsPath: "/etc/backup-stack/ssh/known_hosts"}

	args, err := buildRsyncArgs(rsyncSide{Local: "/data/a"}, rsyncSide{Local: "/data/b/"}, "/tmp/list")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(args, " "); got != "-rlpt --stats --files-from=/tmp/list /data/a/ /data/b/" {
		t.Fatalf("local args = %q", got)
	}

	args, _ = buildRsyncArgs(rsyncSide{Local: "/data/a"}, rsyncSide{SSH: remote}, "/tmp/list")
	if args[len(args)-1] != "chris@wopr.local:/srv/photos/" || args[len(args)-2] != "/data/a/" {
		t.Fatalf("push args = %q", args)
	}
	shell := args[4]
	if args[3] != "-e" {
		t.Fatalf("args = %q", args)
	}
	for _, want := range []string{"ssh -p 2222", "-i /etc/backup-stack/ssh/id_ed25519", "UserKnownHostsFile=/etc/backup-stack/ssh/known_hosts", "StrictHostKeyChecking=yes", "BatchMode=yes"} {
		if !strings.Contains(shell, want) {
			t.Errorf("-e %q missing %q", shell, want)
		}
	}

	args, _ = buildRsyncArgs(rsyncSide{SSH: remote}, rsyncSide{Local: "/restore"}, "/tmp/list")
	if args[len(args)-2] != "chris@wopr.local:/srv/photos/" || args[len(args)-1] != "/restore/" {
		t.Fatalf("pull args = %q", args)
	}
	if _, err := buildRsyncArgs(rsyncSide{SSH: remote}, rsyncSide{SSH: remote}, "/tmp/list"); err == nil {
		t.Fatal("remote->remote accepted")
	}

	insecure := sshconn.Config{Addr: "h", User: "u", Path: "/p dir", InsecureSkipHostKeyCheck: true}.WithDefaults()
	if s := insecure.RsyncShell(); !strings.Contains(s, "StrictHostKeyChecking=no") || !strings.Contains(s, "-p 22") {
		t.Fatalf("insecure shell = %q", s)
	}
}

func TestParseRsyncStats(t *testing.T) {
	gnu := `
Number of files: 12 (reg: 10, dir: 2)
Number of created files: 3 (reg: 3)
Number of deleted files: 0
Number of regular files transferred: 3
Total file size: 1,234,567 bytes
Total transferred file size: 12,345 bytes
Literal data: 12,345 bytes
rsync: [sender] send_files failed to open "/src/locked": Permission denied (13)
rsync error: some files/attrs were not transferred (see previous errors) (code 23) at main.c(1338) [sender=3.2.7]
`
	st := parseRsyncStats(gnu)
	if st.Files != 3 || st.Bytes != 12345 || st.Errors != 2 || len(st.ErrorSamples) != 2 {
		t.Fatalf("gnu = %+v", st)
	}
	open := "Number of files: 3\nNumber of files transferred: 1\nTotal file size: 3 B\nTotal transferred file size: 3 B\n"
	if st := parseRsyncStats(open); st.Files != 1 || st.Bytes != 3 || st.Errors != 0 {
		t.Fatalf("openrsync = %+v", st)
	}
}

func requireRsync(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed")
	}
	return p
}

func newRunner(t *testing.T, rsyncPath string) (*Runner, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &Runner{Store: st, RsyncPath: rsyncPath}, st
}

func dirJob(t *testing.T, st *store.Store, id, src, dst string, rules filter.Rules, del bool) *store.SyncJob {
	t.Helper()
	j := &store.SyncJob{ID: id, Name: id,
		SourceConfig: json.RawMessage(fmt.Sprintf(`{"type":"dir","path":%q}`, src)),
		DestConfig:   json.RawMessage(fmt.Sprintf(`{"type":"dir","path":%q}`, dst)),
		Filters:      rules, Delete: del, Enabled: true}
	if err := st.CreateSyncJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestRunnerRsyncDirToDir(t *testing.T) {
	rsync := requireRsync(t)
	ctx := context.Background()
	r, st := newRunner(t, rsync)
	srcDir, dstDir := t.TempDir(), filepath.Join(t.TempDir(), "out")
	writeFiles(t, srcDir, map[string]string{"a.txt": "hello", "sub/b.txt": "bb", "sub/c.tmp": "tmp", "cache/big": "zzzz"})
	j := dirJob(t, st, "j1", srcDir, dstDir, filter.Rules{
		{Kind: filter.Exclude, Pattern: "**/*.tmp"},
		{Kind: filter.Exclude, Pattern: "cache/**"},
	}, false)

	run, err := r.Run(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if run.Method != MethodRsync || run.State != store.StateSucceeded {
		t.Fatalf("run = %+v", run)
	}
	if run.Stats.Files != 2 || run.Stats.Bytes != 7 {
		t.Fatalf("stats = %+v", run.Stats)
	}
	got := readTree(t, dstDir)
	if strings.Join(sortedKeys(got), ",") != "a.txt,sub/b.txt" {
		t.Fatalf("dest = %v", got)
	}

	// Second run: nothing to transfer.
	run, err = r.Run(ctx, j)
	if err != nil || run.Stats.Files != 0 || run.Stats.Skipped != 2 {
		t.Fatalf("second run = %+v, %v", run, err)
	}

	// Delete on: an extra (included) file goes, an excluded one stays.
	writeFiles(t, dstDir, map[string]string{"extra.txt": "x", "keep.tmp": "y"})
	j.Delete = true
	run, err = r.Run(ctx, j)
	if err != nil || run.Stats.Deleted != 1 {
		t.Fatalf("delete run = %+v, %v", run, err)
	}
	got = readTree(t, dstDir)
	if _, ok := got["extra.txt"]; ok {
		t.Fatal("extra not deleted")
	}
	if _, ok := got["keep.tmp"]; !ok {
		t.Fatal("excluded file deleted")
	}
}

func TestRunnerFallsBackToNativeAndRecordsFailures(t *testing.T) {
	ctx := context.Background()
	r, st := newRunner(t, "") // no rsync
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFiles(t, srcDir, map[string]string{"a": "1"})
	j := dirJob(t, st, "j1", srcDir, dstDir, nil, false)
	run, err := r.Run(ctx, j)
	if err != nil || run.Method != MethodNative || run.State != store.StateSucceeded || run.Stats.Files != 1 {
		t.Fatalf("native run = %+v, %v", run, err)
	}

	bad := &store.SyncJob{ID: "bad", Name: "bad", SourceConfig: json.RawMessage(`{"type":"dir","path":"/x"}`), DestConfig: json.RawMessage(`{"type":"dir","path":"/x"}`)}
	if err := st.CreateSyncJob(ctx, bad); err != nil {
		t.Fatal(err)
	}
	run, err = r.Run(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != store.StateFailed || run.Stats.Errors != 1 || run.Error == "" {
		t.Fatalf("bad job run = %+v", run)
	}
	job, _ := st.GetSyncJob(ctx, "bad")
	if job.LastRunState != store.StateFailed {
		t.Fatalf("job last state = %q", job.LastRunState)
	}
}
