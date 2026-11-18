package sync

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/geekychris/backup-stack/internal/filter"
	"github.com/geekychris/backup-stack/internal/runstats"
)

// Options are a job's per-run settings.
type Options struct {
	Filters filter.Rules
	// Delete removes destination files (that the filters include) which
	// the source no longer has. Off by default.
	Delete bool
}

// selected returns the source files the filters include, sorted.
func selected(srcList map[string]Entry, rules filter.Rules) []string {
	out := make([]string, 0, len(srcList))
	for rel := range srcList {
		if rules.Included(rel) {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

// Native copies every included source file the destination is missing or
// disagrees on, file by file through this process. A per-file error is
// counted (with a sample message) and the run carries on; only failing to
// list either side aborts it.
func Native(ctx context.Context, src, dst Tree, opt Options) (runstats.Stats, error) {
	var st runstats.Stats
	srcList, err := src.List(ctx)
	if err != nil {
		return st, fmt.Errorf("list source: %w", err)
	}
	dstList, err := dst.List(ctx)
	if err != nil {
		return st, fmt.Errorf("list destination: %w", err)
	}
	files := selected(srcList, opt.Filters)
	for _, rel := range files {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		e := srcList[rel]
		if d, ok := dstList[rel]; ok && upToDate(e, d) {
			st.Skipped++
			continue
		}
		n, err := copyOne(ctx, src, dst, rel, e)
		if err != nil {
			st.AddError(fmt.Errorf("%s: %w", rel, err))
			continue
		}
		st.Files++
		st.Bytes += n
	}
	if opt.Delete {
		deleteExtra(ctx, dst, dstList, srcList, opt.Filters, &st)
	}
	return st, nil
}

func copyOne(ctx context.Context, src, dst Tree, rel string, e Entry) (int64, error) {
	r, err := src.Open(ctx, rel)
	if err != nil {
		return 0, err
	}
	cr := &countingReader{r: r}
	werr := dst.Write(ctx, rel, cr, e)
	cerr := r.Close()
	if werr != nil {
		return cr.n, werr
	}
	if cerr != nil {
		return cr.n, cerr
	}
	return cr.n, nil
}

// deleteExtra removes destination files the source lacks. Files the
// filters exclude are never touched: an exclude rule means "not this job's
// business", not "delete it".
func deleteExtra(ctx context.Context, dst Tree, dstList, srcList map[string]Entry, rules filter.Rules, st *runstats.Stats) {
	var extra []string
	for rel := range dstList {
		if _, ok := srcList[rel]; ok || !rules.Included(rel) {
			continue
		}
		extra = append(extra, rel)
	}
	sort.Strings(extra)
	for _, rel := range extra {
		if ctx.Err() != nil {
			return
		}
		if err := dst.Remove(ctx, rel); err != nil {
			st.AddError(fmt.Errorf("delete %s: %w", rel, err))
			continue
		}
		st.Deleted++
	}
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
