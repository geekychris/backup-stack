package ssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	xssh "golang.org/x/crypto/ssh"

	"github.com/geekychris/backup-stack/internal/catalog"
)

// fakeServer is an in-process SSH server that answers every "exec" request
// with whatever fileListing currently holds, formatted as the find-based
// listing command this package's Source parses, or with the raw bytes
// fileContents[path] when the command is this package's "cat" command for
// that path.
type fakeServer struct {
	mu           sync.Mutex
	fileListing  []listEntry
	fileContents map[string][]byte
	// hang marks paths whose "cat" never completes on its own: handleExec
	// blocks until the client closes its side of the channel, simulating a
	// wedged remote command.
	hang         map[string]bool
	addr         string
	clientSigner xssh.Signer
	hostSigner   xssh.Signer
}

type listEntry struct {
	path  string
	size  int64
	mtime float64
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	_, clientPriv, _ := ed25519.GenerateKey(rand.Reader)
	clientSigner, _ := xssh.NewSignerFromKey(clientPriv)
	_, hostPriv, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := xssh.NewSignerFromKey(hostPriv)

	fs := &fakeServer{fileContents: map[string][]byte{}, hang: map[string]bool{}, clientSigner: clientSigner, hostSigner: hostSigner}

	cfg := &xssh.ServerConfig{PublicKeyCallback: func(_ xssh.ConnMetadata, k xssh.PublicKey) (*xssh.Permissions, error) {
		if string(k.Marshal()) == string(clientSigner.PublicKey().Marshal()) {
			return nil, nil
		}
		return nil, io.EOF
	}}
	cfg.AddHostKey(hostSigner)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	fs.addr = ln.Addr().String()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go fs.serve(c, cfg)
		}
	}()
	return fs
}

func (fs *fakeServer) serve(c net.Conn, cfg *xssh.ServerConfig) {
	_, chans, reqs, err := xssh.NewServerConn(c, cfg)
	if err != nil {
		return
	}
	go xssh.DiscardRequests(reqs)
	for nc := range chans {
		ch, in, err := nc.Accept()
		if err != nil {
			continue
		}
		go func() {
			for req := range in {
				if req.Type != "exec" {
					_ = req.Reply(false, nil)
					continue
				}
				var payload struct{ Command string }
				_ = xssh.Unmarshal(req.Payload, &payload)
				_ = req.Reply(true, nil)
				fs.handleExec(ch, payload.Command)
				_, _ = ch.SendRequest("exit-status", false, xssh.Marshal(struct{ Status uint32 }{0}))
				_ = ch.Close()
				return
			}
		}()
	}
}

func (fs *fakeServer) handleExec(ch xssh.Channel, cmd string) {
	if strings.HasPrefix(cmd, catCommandPrefix) {
		path := strings.TrimPrefix(cmd, catCommandPrefix)
		path = strings.Trim(path, "'")
		fs.mu.Lock()
		hang := fs.hang[path]
		content := fs.fileContents[path]
		fs.mu.Unlock()
		if hang {
			// Block until the client closes its side of the channel (e.g.
			// because ctx was canceled and the caller gave up waiting),
			// simulating a remote command that never exits on its own.
			_, _ = io.Copy(io.Discard, ch)
			return
		}
		_, _ = ch.Write(content)
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var b strings.Builder
	for _, e := range fs.fileListing {
		b.WriteString(e.path)
		b.WriteByte('\t')
		b.WriteString(strconv.FormatInt(e.size, 10))
		b.WriteByte('\t')
		b.WriteString(strconv.FormatFloat(e.mtime, 'f', 9, 64))
		b.WriteByte('\n')
	}
	_, _ = ch.Write([]byte(b.String()))
}

func (fs *fakeServer) setListing(entries []listEntry) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.fileListing = entries
}

func (fs *fakeServer) setHang(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.hang[path] = true
}

func newSource(t *testing.T, fs *fakeServer, path string) *Source {
	t.Helper()
	cfg := Config{
		Addr: fs.addr,
		User: "tester",
		ClientConfig: &xssh.ClientConfig{
			User:            "tester",
			Auth:            []xssh.AuthMethod{xssh.PublicKeys(fs.clientSigner)},
			HostKeyCallback: xssh.FixedHostKey(fs.hostSigner.PublicKey()),
		},
		Path: path,
	}
	return New(cfg)
}

func TestSSHSourceSnapshot(t *testing.T) {
	fs := newFakeServer(t)
	fs.setListing([]listEntry{
		{path: "a.txt", size: 3, mtime: 1000.0},
		{path: "dir/b.txt", size: 5, mtime: 2000.0},
	})
	src := newSource(t, fs, "/data")
	defer src.Close()

	cat, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Items) != 2 {
		t.Fatalf("got %d items, want 2: %+v", len(cat.Items), cat.Items)
	}

	// Re-snapshot with one file's mtime changed: only that file should look
	// changed when diffed against the first catalog.
	fs.setListing([]listEntry{
		{path: "a.txt", size: 3, mtime: 1000.0},
		{path: "dir/b.txt", size: 5, mtime: 9999.0},
	})
	cat2, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	d := catalog.Diff(cat, cat2)
	if len(d.Changed) != 1 || d.Changed[0].Path != "dir/b.txt" {
		t.Fatalf("changed = %+v, want exactly dir/b.txt", d.Changed)
	}
	if len(d.Added) != 0 || len(d.Deleted) != 0 {
		t.Fatalf("expected no added/deleted, got added=%+v deleted=%v", d.Added, d.Deleted)
	}
}

func TestSSHSourceOpen(t *testing.T) {
	fs := newFakeServer(t)
	fs.fileContents["/data/a.txt"] = []byte("hello world")
	src := newSource(t, fs, "/data")
	defer src.Close()

	r, err := src.Open(context.Background(), "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q", got)
	}
}

// TestSSHSourceOpenCloseAbortsOnContextCancel proves that Close on the
// reader returned by Open doesn't hang forever when the remote command
// never exits on its own (e.g. a wedged `cat`): canceling ctx must make
// Close return promptly, not block on the blocking ssh.Session.Wait call.
func TestSSHSourceOpenCloseAbortsOnContextCancel(t *testing.T) {
	fs := newFakeServer(t)
	fs.setHang("/data/wedged.txt")
	src := newSource(t, fs, "/data")
	defer src.Close()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := src.Open(ctx, "wedged.txt")
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	closeErr := make(chan error, 1)
	go func() { closeErr <- r.Close() }()
	select {
	case err := <-closeErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close() err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3s of ctx cancellation; it hung on the wedged remote command")
	}
}
