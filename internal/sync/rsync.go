package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/geekychris/backup-stack/internal/runstats"
	"github.com/geekychris/backup-stack/internal/sshconn"
)

// rsyncSide is one end of an rsync transfer: a local directory, or a
// remote one over SSH.
type rsyncSide struct {
	Local string          // local directory, when Remote is nil
	SSH   *sshconn.Config // remote directory
}

func (s rsyncSide) spec() string {
	if s.SSH != nil {
		return s.SSH.RsyncRemote()
	}
	p := s.Local
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

// buildRsyncArgs renders one transfer: archive mode (minus owner/group,
// which a non-root receiver can't set anyway) with stats, the explicit
// file list backup-stack's filters selected (--files-from, so rsync's own
// filter language never comes into it), and -e for the remote side. rsync
// can't copy between two remote hosts, so at most one side is remote.
func buildRsyncArgs(src, dst rsyncSide, filesFrom string) ([]string, error) {
	if src.SSH != nil && dst.SSH != nil {
		return nil, errors.New("rsync: both sides remote")
	}
	args := []string{"-rlpt", "--stats", "--files-from=" + filesFrom}
	switch {
	case src.SSH != nil:
		args = append(args, "-e", src.SSH.RsyncShell())
	case dst.SSH != nil:
		args = append(args, "-e", dst.SSH.RsyncShell())
	}
	return append(args, src.spec(), dst.spec()), nil
}

// rsyncTransfer runs rsync for files (already filtered, relative to the
// source root) and returns what it reports. A non-zero exit is counted as
// an error with rsync's own messages as samples, not returned, so the run
// still records how much did transfer.
func rsyncTransfer(ctx context.Context, rsyncPath string, src, dst rsyncSide, files []string) (runstats.Stats, error) {
	var st runstats.Stats
	if len(files) == 0 {
		return st, nil
	}
	list, err := os.CreateTemp("", "backup-stack-files-*")
	if err != nil {
		return st, fmt.Errorf("rsync: file list: %w", err)
	}
	defer os.Remove(list.Name())
	_, werr := list.WriteString(strings.Join(files, "\n") + "\n")
	if cerr := list.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return st, fmt.Errorf("rsync: file list: %w", werr)
	}
	args, err := buildRsyncArgs(src, dst, list.Name())
	if err != nil {
		return st, err
	}
	if dst.SSH == nil {
		if err := os.MkdirAll(dst.Local, 0o755); err != nil {
			return st, err
		}
	}
	cmd := exec.CommandContext(ctx, rsyncPath, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	runErr := cmd.Run()
	if ctx.Err() != nil {
		return st, ctx.Err()
	}
	st = parseRsyncStats(out.String())
	if runErr != nil && st.Errors == 0 {
		st.AddErrorMsg("rsync: " + runErr.Error() + lastLine(out.String()))
	}
	st.Skipped = len(files) - st.Files
	if st.Skipped < 0 {
		st.Skipped = 0
	}
	return st, nil
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return ": " + s
}

var (
	// GNU rsync 3.x says "regular files", rsync 2.6.9 / openrsync just "files".
	reFilesTransferred = regexp.MustCompile(`(?m)^Number of (?:regular )?files transferred:\s*([0-9,.]+)`)
	// "1,234 bytes" (GNU rsync 3.x), "1234 B" (openrsync), or a bare number.
	reBytesTransferred = regexp.MustCompile(`(?m)^Total transferred file size:\s*([0-9,.]+)`)
	reDeleted          = regexp.MustCompile(`(?m)^Number of deleted files:\s*([0-9,.]+)`)
)

// parseRsyncStats extracts Stats from rsync --stats output (stdout and
// stderr together). "rsync: ..." and "rsync error: ..." lines each count as
// one error, so a partial failure isn't reported as a clean run.
func parseRsyncStats(output string) runstats.Stats {
	var st runstats.Stats
	if m := reFilesTransferred.FindStringSubmatch(output); m != nil {
		st.Files = int(parseRsyncNumber(m[1]))
	}
	if m := reBytesTransferred.FindStringSubmatch(output); m != nil {
		st.Bytes = parseRsyncNumber(m[1])
	}
	if m := reDeleted.FindStringSubmatch(output); m != nil {
		st.Deleted = int(parseRsyncNumber(m[1]))
	}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "rsync:") || strings.HasPrefix(line, "rsync error:") ||
			strings.HasPrefix(line, "rsync warning:") || strings.HasPrefix(line, "openrsync:") {
			st.AddErrorMsg(line)
		}
	}
	return st
}

// parseRsyncNumber parses "1,234" (or locale-grouped "1.234"); unparseable
// input is zero.
func parseRsyncNumber(s string) int64 {
	s = strings.NewReplacer(",", "", ".", "").Replace(s)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
