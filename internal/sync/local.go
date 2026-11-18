package sync

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// LocalTree is a directory on the machine (or pod) backup-stack runs on.
type LocalTree struct {
	Root string
}

var _ Tree = (*LocalTree)(nil)

func (t *LocalTree) path(rel string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes the sync root: " + rel)
	}
	return filepath.Join(t.Root, clean), nil
}

// List implements Tree. Only regular files are listed; symlinks are not
// followed.
func (t *LocalTree) List(ctx context.Context) (map[string]Entry, error) {
	out := map[string]Entry{}
	if _, err := os.Stat(t.Root); errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	err := filepath.WalkDir(t.Root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(t.Root, p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = Entry{Size: info.Size(), ModTime: info.ModTime()}
		return nil
	})
	return out, err
}

// Open implements Tree.
func (t *LocalTree) Open(_ context.Context, rel string) (io.ReadCloser, error) {
	p, err := t.path(rel)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

// Write implements Tree: it writes to a temporary file beside the target
// and renames it into place, so a reader never sees a half-written file,
// then sets the source's modification time.
func (t *LocalTree) Write(_ context.Context, rel string, r io.Reader, e Entry) error {
	p, err := t.path(rel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".backup-stack-*")
	if err != nil {
		return err
	}
	_, cerr := io.Copy(tmp, r)
	if err := tmp.Close(); cerr == nil {
		cerr = err
	}
	if cerr == nil {
		cerr = os.Chmod(tmp.Name(), 0o644)
	}
	if cerr == nil {
		cerr = os.Rename(tmp.Name(), p)
	}
	if cerr != nil {
		os.Remove(tmp.Name())
		return cerr
	}
	if !e.ModTime.IsZero() {
		return os.Chtimes(p, e.ModTime, e.ModTime)
	}
	return nil
}

// Remove implements Tree.
func (t *LocalTree) Remove(_ context.Context, rel string) error {
	p, err := t.path(rel)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

// Close implements Tree.
func (t *LocalTree) Close() error { return nil }
