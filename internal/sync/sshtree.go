package sync

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	stdsync "sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/geekychris/backup-stack/internal/sshconn"
)

// SSHTree is a directory on a remote host, reached over SSH with only a
// POSIX shell and GNU coreutils/findutils on the other end (the same
// assumption the ssh-path backup source makes) — no agent, no sftp
// subsystem required. Files stream through the SSH session: `find -printf`
// lists, `cat` reads, `cat >` into a temporary file then `mv` writes.
type SSHTree struct {
	Cfg sshconn.Config

	mu     stdsync.Mutex
	client *ssh.Client
	// dial is injectable for tests; defaults to sshconn.Dial with
	// Cfg.ClientConfig().
	dial func(ctx context.Context) (*ssh.Client, error)
}

var _ Tree = (*SSHTree)(nil)

// NewSSHTree returns a tree that dials lazily on first use.
func NewSSHTree(cfg sshconn.Config) *SSHTree { return &SSHTree{Cfg: cfg} }

func (t *SSHTree) conn(ctx context.Context) (*ssh.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != nil {
		return t.client, nil
	}
	dial := t.dial
	if dial == nil {
		dial = func(ctx context.Context) (*ssh.Client, error) {
			cc, err := t.Cfg.ClientConfig()
			if err != nil {
				return nil, err
			}
			return sshconn.Dial(ctx, t.Cfg.Addr, cc)
		}
	}
	c, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	t.client = c
	return c, nil
}

// run executes cmd remotely, feeding it stdin, and returns its stdout. A
// non-zero exit returns an error carrying its stderr. ctx ending closes
// the session.
func (t *SSHTree) run(ctx context.Context, cmd string, stdin io.Reader) ([]byte, error) {
	c, err := t.conn(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := c.NewSession()
	if err != nil {
		return nil, fmt.Errorf("ssh session: %w", err)
	}
	defer sess.Close()
	var stdout, stderr bytes.Buffer
	sess.Stdout, sess.Stderr = &stdout, &stderr
	if stdin != nil {
		sess.Stdin = stdin
	}
	done := make(chan error, 1)
	go func() { done <- sess.Run(cmd) }()
	select {
	case <-ctx.Done():
		_ = sess.Close()
		<-done
		return nil, ctx.Err()
	case err := <-done:
		if err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return stdout.Bytes(), fmt.Errorf("%w: %s", err, msg)
			}
			return stdout.Bytes(), err
		}
		return stdout.Bytes(), nil
	}
}

func (t *SSHTree) abs(rel string) (string, error) {
	clean := path.Clean("/" + rel)[1:]
	if clean == "" || clean != strings.TrimPrefix(rel, "./") || strings.HasPrefix(rel, "/") {
		return "", errors.New("path escapes the sync root: " + rel)
	}
	return path.Join(t.Cfg.Path, clean), nil
}

func (t *SSHTree) listCommand() string {
	q := sshconn.QuoteSingle(t.Cfg.Path)
	return "if [ -d " + q + " ]; then cd " + q + " && find . -type f -printf '%P\\t%s\\t%T@\\n'; fi"
}

// List implements Tree.
func (t *SSHTree) List(ctx context.Context) (map[string]Entry, error) {
	out, err := t.run(ctx, t.listCommand(), nil)
	if err != nil {
		return nil, fmt.Errorf("list %s:%s: %w", t.Cfg.Addr, t.Cfg.Path, err)
	}
	return parseFindListing(out), nil
}

// parseFindListing parses `find -printf '%P\t%s\t%T@\n'` output.
func parseFindListing(out []byte) map[string]Entry {
	m := map[string]Entry{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		f := strings.SplitN(sc.Text(), "\t", 3)
		if len(f) != 3 || f[0] == "" {
			continue
		}
		size, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		var mt time.Time
		if secs, err := strconv.ParseFloat(f[2], 64); err == nil {
			whole, frac := math.Modf(secs)
			mt = time.Unix(int64(whole), int64(frac*1e9))
		}
		m[f[0]] = Entry{Size: size, ModTime: mt}
	}
	return m
}

// Open implements Tree.
func (t *SSHTree) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	p, err := t.abs(rel)
	if err != nil {
		return nil, err
	}
	c, err := t.conn(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := c.NewSession()
	if err != nil {
		return nil, fmt.Errorf("ssh session: %w", err)
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		return nil, err
	}
	var stderr bytes.Buffer
	sess.Stderr = &stderr
	if err := sess.Start("cat -- " + sshconn.QuoteSingle(p)); err != nil {
		sess.Close()
		return nil, err
	}
	return &remoteReader{ctx: ctx, sess: sess, r: stdout, stderr: &stderr}, nil
}

// remoteReader streams a remote cat's stdout; Close reports the command's
// failure (a file that vanished, a permission error).
type remoteReader struct {
	ctx    context.Context
	sess   *ssh.Session
	r      io.Reader
	stderr *bytes.Buffer
}

func (r *remoteReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (r *remoteReader) Close() error {
	done := make(chan error, 1)
	go func() {
		// Drain so the remote side can exit even if the caller stopped early.
		_, _ = io.Copy(io.Discard, r.r)
		done <- r.sess.Wait()
	}()
	var err error
	select {
	case <-r.ctx.Done():
		_ = r.sess.Close()
		<-done
		return r.ctx.Err()
	case err = <-done:
	}
	_ = r.sess.Close()
	if err != nil {
		if msg := strings.TrimSpace(r.stderr.String()); msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}
	}
	return err
}

// writeCommand is the remote command Write runs, its content on stdin.
func writeCommand(p string, e Entry) string {
	dir, base := path.Split(p)
	tmp := path.Join(dir, ".backup-stack-"+base+".tmp")
	cmd := "mkdir -p -- " + sshconn.QuoteSingle(path.Clean(dir)) +
		" && cat > " + sshconn.QuoteSingle(tmp) +
		" && mv -f -- " + sshconn.QuoteSingle(tmp) + " " + sshconn.QuoteSingle(p)
	if !e.ModTime.IsZero() {
		cmd += " && touch -m -d @" + strconv.FormatInt(e.ModTime.Unix(), 10) + " -- " + sshconn.QuoteSingle(p)
	}
	return cmd
}

// Write implements Tree.
func (t *SSHTree) Write(ctx context.Context, rel string, r io.Reader, e Entry) error {
	p, err := t.abs(rel)
	if err != nil {
		return err
	}
	_, err = t.run(ctx, writeCommand(p, e), r)
	return err
}

// Remove implements Tree.
func (t *SSHTree) Remove(ctx context.Context, rel string) error {
	p, err := t.abs(rel)
	if err != nil {
		return err
	}
	_, err = t.run(ctx, "rm -f -- "+sshconn.QuoteSingle(p), nil)
	return err
}

// HasRsync reports whether the remote host has an rsync binary.
func (t *SSHTree) HasRsync(ctx context.Context) bool {
	_, err := t.run(ctx, "command -v rsync >/dev/null 2>&1", nil)
	return err == nil
}

// Close implements Tree.
func (t *SSHTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client == nil {
		return nil
	}
	err := t.client.Close()
	t.client = nil
	return err
}
