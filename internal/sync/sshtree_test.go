package sync

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/geekychris/backup-stack/internal/sshconn"
)

func TestSSHCommands(t *testing.T) {
	tr := NewSSHTree(sshconn.Config{Addr: "h:22", User: "u", Path: "/srv/it's"})
	if got := tr.listCommand(); !strings.Contains(got, `cd '/srv/it'\''s'`) || !strings.Contains(got, "-printf '%P\\t%s\\t%T@\\n'") {
		t.Fatalf("list = %q", got)
	}
	w := writeCommand("/srv/d/f.txt", Entry{ModTime: time.Unix(1700000000, 0)})
	want := "mkdir -p -- '/srv/d' && cat > '/srv/d/.backup-stack-f.txt.tmp' && mv -f -- '/srv/d/.backup-stack-f.txt.tmp' '/srv/d/f.txt' && touch -m -d @1700000000 -- '/srv/d/f.txt'"
	if w != want {
		t.Fatalf("write =\n%s\nwant\n%s", w, want)
	}
	for _, bad := range []string{"../x", "a/../../x", "/abs"} {
		if _, err := tr.abs(bad); err == nil {
			t.Errorf("abs(%q) accepted", bad)
		}
	}
	m := parseFindListing([]byte("a.txt\t3\t1700000000.5000000000\nd/b\t10\t1700000001.0\nbroken line\n"))
	if len(m) != 2 || m["a.txt"].Size != 3 || m["d/b"].ModTime.Unix() != 1700000001 {
		t.Fatalf("listing = %+v", m)
	}
}

// execServer is an in-process SSH server that runs each exec request with
// the local /bin/sh — enough to exercise SSHTree's real commands end to end
// where GNU find is available (Linux; the image backup-stack ships in).
func execServer(t *testing.T) func(ctx context.Context) (*ssh.Client, error) {
	t.Helper()
	_, hostPriv, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := ssh.NewSignerFromKey(hostPriv)
	_, clientPriv, _ := ed25519.GenerateKey(rand.Reader)
	clientSigner, _ := ssh.NewSignerFromKey(clientPriv)
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, k ssh.PublicKey) (*ssh.Permissions, error) {
		if string(k.Marshal()) == string(clientSigner.PublicKey().Marshal()) {
			return nil, nil
		}
		return nil, errors.New("unknown key")
	}}
	cfg.AddHostKey(hostSigner)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(c, cfg)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
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
							var p struct{ Command string }
							_ = ssh.Unmarshal(req.Payload, &p)
							_ = req.Reply(true, nil)
							cmd := exec.Command("/bin/sh", "-c", p.Command)
							cmd.Stdin, cmd.Stdout, cmd.Stderr = ch, ch, ch.Stderr()
							status := uint32(0)
							if err := cmd.Run(); err != nil {
								status = 1
								var ee *exec.ExitError
								if errors.As(err, &ee) {
									status = uint32(ee.ExitCode())
								}
							}
							_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
							_ = ch.Close()
							return
						}
					}()
				}
			}()
		}
	}()
	addr := ln.Addr().String()
	return func(ctx context.Context) (*ssh.Client, error) {
		return sshconn.Dial(ctx, addr, &ssh.ClientConfig{
			User: "tester", Auth: []ssh.AuthMethod{ssh.PublicKeys(clientSigner)},
			HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()),
		})
	}
}

func requireGNUFind(t *testing.T) {
	t.Helper()
	if err := exec.Command("find", t.TempDir(), "-maxdepth", "0", "-printf", "%P").Run(); err != nil {
		t.Skip("GNU find (-printf) not available; SSHTree's remote commands assume it")
	}
	if err := exec.Command("touch", "-m", "-d", "@1700000000", filepath.Join(t.TempDir(), "x")).Run(); err != nil {
		t.Skip("GNU touch (-d @epoch) not available")
	}
}

func TestSSHTreeEndToEnd(t *testing.T) {
	requireGNUFind(t)
	ctx := context.Background()
	dial := execServer(t)

	local := t.TempDir()
	writeFiles(t, local, map[string]string{"a.txt": "aaa", "d/b c.txt": "space", "x.tmp": "t"})
	remoteRoot := filepath.Join(t.TempDir(), "remote root")
	remote := NewSSHTree(sshconn.Config{Addr: "test", User: "tester", Path: remoteRoot})
	remote.dial = dial
	defer remote.Close()

	// local -> "remote" host, filtered.
	st, err := Native(ctx, &LocalTree{Root: local}, remote, Options{})
	if err != nil || st.Files != 3 || st.Errors != 0 {
		t.Fatalf("push: %+v, %v", st, err)
	}
	if got := readTree(t, remoteRoot); got["d/b c.txt"] != "space" {
		t.Fatalf("remote = %v", got)
	}
	// mtimes carried over: nothing to do the second time.
	if st, _ := Native(ctx, &LocalTree{Root: local}, remote, Options{}); st.Files != 0 || st.Skipped != 3 {
		t.Fatalf("second push: %+v", st)
	}

	// "remote" -> another "remote" directory (host to host, streamed).
	other := NewSSHTree(sshconn.Config{Addr: "test2", User: "tester", Path: filepath.Join(t.TempDir(), "o")})
	other.dial = dial
	defer other.Close()
	st, err = Native(ctx, remote, other, Options{})
	if err != nil || st.Files != 3 || st.Bytes != 9 {
		t.Fatalf("host->host: %+v, %v", st, err)
	}
	if !remote.HasRsync(ctx) && func() bool { _, err := exec.LookPath("rsync"); return err == nil }() {
		t.Fatal("HasRsync false although rsync is on PATH")
	}

	// Delete of an included extra.
	writeFiles(t, filepath.Join(remoteRoot), map[string]string{"extra": "e"})
	st, _ = Native(ctx, &LocalTree{Root: local}, remote, Options{Delete: true})
	if st.Deleted != 1 {
		t.Fatalf("delete: %+v", st)
	}
	r, err := remote.Open(ctx, "missing")
	if err == nil {
		_, _ = io.ReadAll(r)
		if r.Close() == nil {
			t.Fatal("reading a missing remote file reported no error")
		}
	}
}
