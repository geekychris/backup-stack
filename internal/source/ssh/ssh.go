// Package ssh is the ssh-path Source: it backs up a directory tree on a
// remote host reached over SSH, using only a POSIX shell and coreutils (no
// agent to install — every host in cloud-stack's infra inventory already
// exposes ssh:).
package ssh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/geekychris/backup-stack/internal/catalog"
	"github.com/geekychris/backup-stack/internal/filter"
)

// catCommandPrefix begins the remote command Open runs to stream one file's
// content; a test fake server matches on it to tell a cat from a listing.
const catCommandPrefix = "cat '" // "cat " + quoteSingle(path)

// Config configures a Source.
type Config struct {
	// Addr is host:port of the SSH server.
	Addr string
	// User is the remote username.
	User string
	// ClientConfig carries auth and host-key verification.
	ClientConfig *ssh.ClientConfig
	// Path is the root directory on the remote host to back up.
	Path string
	// DialTimeout bounds the TCP+SSH handshake. Default 15s.
	DialTimeout time.Duration
	// Filters select which files (by path relative to Path) are backed up;
	// a file they exclude is simply not part of the target's state.
	Filters filter.Rules
}

// Source backs up a directory tree on a remote host over SSH.
type Source struct {
	cfg Config

	mu     sync.Mutex
	client *ssh.Client
}

// New builds a Source. It dials lazily, on the first Snapshot or Open call,
// and reuses the connection across calls until Close.
func New(cfg Config) *Source {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 15 * time.Second
	}
	return &Source{cfg: cfg}
}

func (s *Source) dial() (*ssh.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return s.client, nil
	}
	d := net.Dialer{Timeout: s.cfg.DialTimeout}
	conn, err := d.Dial("tcp", s.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("source/ssh: dial %s: %w", s.cfg.Addr, err)
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, s.cfg.Addr, s.cfg.ClientConfig)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("source/ssh: handshake %s: %w", s.cfg.Addr, err)
	}
	s.client = ssh.NewClient(c, chans, reqs)
	return s.client, nil
}

// Close closes the underlying SSH connection, if one was opened.
func (s *Source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return nil
	}
	err := s.client.Close()
	s.client = nil
	return err
}

func quoteSingle(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func (s *Source) listCommand() string {
	return fmt.Sprintf("cd %s && find . -mindepth 1 -type f -printf '%%P\\t%%s\\t%%T@\\n'", quoteSingle(s.cfg.Path))
}

// runWithContext runs cmd on sess and returns its stdout, aborting (by
// closing sess) if ctx is done first. The goroutine it starts always
// terminates: sess.Output returns once the session is closed, from either
// path.
func runWithContext(ctx context.Context, sess *ssh.Session, cmd string) ([]byte, error) {
	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := sess.Output(cmd)
		done <- result{out, err}
	}()
	select {
	case <-ctx.Done():
		_ = sess.Close()
		<-done
		return nil, ctx.Err()
	case r := <-done:
		return r.out, r.err
	}
}

// Snapshot lists every regular file under cfg.Path. Item.Fingerprint is the
// remote file's "<size>:<mtime>" tag (from `find -printf`), so an unchanged
// file keeps the same Fingerprint across runs and a changed size or mtime
// makes it look changed, without hashing file content over the network.
func (s *Source) Snapshot(ctx context.Context) (catalog.Catalog, error) {
	client, err := s.dial()
	if err != nil {
		return catalog.Catalog{}, err
	}
	sess, err := client.NewSession()
	if err != nil {
		return catalog.Catalog{}, fmt.Errorf("source/ssh: session: %w", err)
	}
	defer sess.Close()

	out, err := runWithContext(ctx, sess, s.listCommand())
	if err != nil {
		return catalog.Catalog{}, fmt.Errorf("source/ssh: list %s: %w", s.cfg.Path, err)
	}

	var cat catalog.Catalog
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 {
			continue
		}
		if !s.cfg.Filters.Included(fields[0]) {
			continue
		}
		size, _ := strconv.ParseInt(fields[1], 10, 64)
		cat.Items = append(cat.Items, catalog.Item{
			Path:        fields[0],
			Size:        size,
			Fingerprint: fields[1] + ":" + fields[2],
		})
	}
	return cat, nil
}

// Open streams the content of the file at p (relative to cfg.Path, as
// reported by Snapshot).
func (s *Source) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	client, err := s.dial()
	if err != nil {
		return nil, err
	}
	sess, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("source/ssh: session: %w", err)
	}
	full := path.Join(s.cfg.Path, p)
	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		return nil, err
	}
	// "cat " + a single-quoted path, so a quote in a file name can't break
	// out of it.
	if err := sess.Start("cat " + quoteSingle(full)); err != nil {
		sess.Close()
		return nil, fmt.Errorf("source/ssh: open %s: %w", p, err)
	}
	return &sessionReader{ctx: ctx, sess: sess, r: stdout}, nil
}

// sessionReader adapts a running ssh.Session's stdout into an io.ReadCloser;
// Close waits for the remote command to finish and releases the session.
type sessionReader struct {
	ctx  context.Context
	sess *ssh.Session
	r    io.Reader
}

func (r *sessionReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Close waits for the remote command to finish, same as runWithContext does
// for Snapshot: if ctx ends first, it closes the session to force the
// pending Wait to return rather than blocking forever on a wedged remote
// command. The remote command's own exit status is intentionally ignored
// (as before this fix) — only Close's own error is returned.
func (r *sessionReader) Close() error {
	waitDone := make(chan struct{})
	go func() {
		_ = r.sess.Wait()
		close(waitDone)
	}()
	select {
	case <-r.ctx.Done():
		_ = r.sess.Close()
		<-waitDone
		return r.ctx.Err()
	case <-waitDone:
		return r.sess.Close()
	}
}
