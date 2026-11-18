// Package sshconn is the SSH connection settings an ssh-path backup target
// and an ssh sync endpoint share ({addr, user, path, privateKeyPath,
// knownHostsPath}), turned into either a Go SSH client config (the native
// paths) or the `ssh` command line rsync's -e runs.
package sshconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Env vars giving the key and known_hosts used when a config leaves them
// blank — in the Kubernetes deployment, the backup-stack-ssh Secret mounted
// at /etc/backup-stack/ssh.
const (
	EnvDefaultKey        = "BACKUP_STACK_SSH_KEY"
	EnvDefaultKnownHosts = "BACKUP_STACK_SSH_KNOWN_HOSTS"
)

// Config is one remote host directory reached over SSH.
type Config struct {
	// Addr is host:port (":22" is added when the port is missing).
	Addr string `json:"addr"`
	User string `json:"user"`
	// Path is the directory on the remote host.
	Path           string `json:"path"`
	PrivateKeyPath string `json:"privateKeyPath,omitempty"`
	KnownHostsPath string `json:"knownHostsPath,omitempty"`
	// InsecureSkipHostKeyCheck disables host key verification; it must be
	// set explicitly, never implied by a missing known_hosts.
	InsecureSkipHostKeyCheck bool `json:"insecureSkipHostKeyCheck,omitempty"`
}

// WithDefaults fills a blank key / known_hosts path from the environment
// and adds the default port.
func (c Config) WithDefaults() Config {
	if c.PrivateKeyPath == "" {
		c.PrivateKeyPath = os.Getenv(EnvDefaultKey)
	}
	if c.KnownHostsPath == "" && !c.InsecureSkipHostKeyCheck {
		c.KnownHostsPath = os.Getenv(EnvDefaultKnownHosts)
	}
	if c.Addr != "" {
		if _, _, err := net.SplitHostPort(c.Addr); err != nil {
			c.Addr = net.JoinHostPort(c.Addr, "22")
		}
	}
	return c
}

// Validate checks the fields every use needs.
func (c Config) Validate() error {
	switch {
	case c.Addr == "":
		return errors.New("ssh: addr is required")
	case c.User == "":
		return errors.New("ssh: user is required")
	case c.Path == "":
		return errors.New("ssh: path is required")
	}
	return nil
}

// ClientConfig builds the Go SSH client config: public-key auth with
// PrivateKeyPath, host key checked against KnownHostsPath.
func (c Config) ClientConfig() (*ssh.ClientConfig, error) {
	if c.PrivateKeyPath == "" {
		return nil, fmt.Errorf("ssh: no privateKeyPath (and %s is unset)", EnvDefaultKey)
	}
	keyBytes, err := os.ReadFile(c.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("ssh private key: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("ssh private key: %w", err)
	}
	var hostKeyCB ssh.HostKeyCallback
	switch {
	case c.InsecureSkipHostKeyCheck:
		hostKeyCB = ssh.InsecureIgnoreHostKey()
	case c.KnownHostsPath != "":
		cb, err := knownhosts.New(c.KnownHostsPath)
		if err != nil {
			return nil, fmt.Errorf("ssh known_hosts: %w", err)
		}
		hostKeyCB = cb
	default:
		return nil, errors.New("ssh: set knownHostsPath (the host's known_hosts entry) or insecureSkipHostKeyCheck")
	}
	return &ssh.ClientConfig{
		User:            c.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCB,
		Timeout:         15 * time.Second,
	}, nil
}

// Dial connects with cc.
func Dial(ctx context.Context, addr string, cc *ssh.ClientConfig) (*ssh.Client, error) {
	d := net.Dialer{Timeout: 15 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ssh: dial %s: %w", addr, err)
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, cc)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh: handshake %s: %w", addr, err)
	}
	return ssh.NewClient(c, chans, reqs), nil
}

// RsyncShell is the remote shell command rsync's -e runs to reach this
// host: the same key and known_hosts as the native path, batch mode (never
// a password prompt), strict host key checking unless explicitly disabled.
func (c Config) RsyncShell() string {
	_, port, err := net.SplitHostPort(c.Addr)
	if err != nil {
		port = "22"
	}
	args := []string{"ssh", "-p", port, "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"}
	if c.PrivateKeyPath != "" {
		args = append(args, "-i", c.PrivateKeyPath, "-o", "IdentitiesOnly=yes")
	}
	if c.InsecureSkipHostKeyCheck {
		args = append(args, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null")
	} else {
		args = append(args, "-o", "StrictHostKeyChecking=yes")
		if c.KnownHostsPath != "" {
			args = append(args, "-o", "UserKnownHostsFile="+c.KnownHostsPath)
		}
	}
	for i, a := range args {
		args[i] = rsyncQuote(a)
	}
	return strings.Join(args, " ")
}

// RsyncRemote is rsync's user@host:path/ spec for this directory (trailing
// slash: the directory's contents, not the directory itself).
func (c Config) RsyncRemote() string {
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		host = c.Addr
	}
	if strings.Contains(host, ":") { // IPv6 literal
		host = "[" + host + "]"
	}
	p := c.Path
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return c.User + "@" + host + ":" + p
}

// rsyncQuote quotes an -e argument containing whitespace; rsync splits the
// -e string itself and honours single quotes.
func rsyncQuote(s string) string {
	if !strings.ContainsAny(s, " \t'\"") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// QuoteSingle quotes s for a POSIX shell.
func QuoteSingle(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
