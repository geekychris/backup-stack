// Package runstats is what every backup run and sync run records about
// itself — bytes and files moved, errors (a count plus a few sample
// messages), duration and transfer rate — and the aggregation of those
// records into evenly spaced time buckets for the admin UI's graphs.
package runstats

import (
	"fmt"
	"sort"
	"time"
)

// MaxErrorSamples bounds how many error messages a run keeps; the count
// keeps going past it.
const MaxErrorSamples = 10

// Stats are the counters one run reports.
type Stats struct {
	// Bytes is the content transferred (not the size of what was skipped).
	Bytes int64 `json:"bytes"`
	// Files is the number of files/objects/items transferred.
	Files int `json:"files"`
	// Skipped is the number of files already up to date at the destination.
	Skipped int `json:"skipped"`
	// Deleted is the number of destination files removed (sync jobs with
	// delete enabled only).
	Deleted int `json:"deleted"`
	// Errors is the number of errors, including ones not sampled.
	Errors int `json:"errors"`
	// ErrorSamples holds up to MaxErrorSamples error messages.
	ErrorSamples []string `json:"errorSamples,omitempty"`
}

// AddError counts err and keeps its message if there is room.
func (s *Stats) AddError(err error) {
	if err == nil {
		return
	}
	s.AddErrorMsg(err.Error())
}

// AddErrorMsg counts one error with the given message.
func (s *Stats) AddErrorMsg(msg string) {
	s.Errors++
	if len(s.ErrorSamples) < MaxErrorSamples {
		s.ErrorSamples = append(s.ErrorSamples, msg)
	}
}

// Summary is a one-line description of a run's errors, or "" if it had none.
func (s Stats) Summary() string {
	switch {
	case s.Errors == 0:
		return ""
	case len(s.ErrorSamples) == 0:
		return fmt.Sprintf("%d errors", s.Errors)
	case s.Errors == 1:
		return s.ErrorSamples[0]
	default:
		return fmt.Sprintf("%d errors; first: %s", s.Errors, s.ErrorSamples[0])
	}
}

// Rate is bytes per second over d, or zero for a zero duration or no bytes.
func Rate(bytes int64, d time.Duration) float64 {
	if d <= 0 || bytes == 0 {
		return 0
	}
	return float64(bytes) / d.Seconds()
}

// Point is one finished (or still running) run, as the aggregation sees it.
type Point struct {
	Start    time.Time
	Duration time.Duration
	Failed   bool
	Stats    Stats
}

// Bucket is the aggregate of every run that started within one time window.
type Bucket struct {
	Start      time.Time `json:"start"`
	Runs       int       `json:"runs"`
	Failed     int       `json:"failed"`
	Bytes      int64     `json:"bytes"`
	Files      int       `json:"files"`
	Errors     int       `json:"errors"`
	DurationMs int64     `json:"durationMs"`
	// RateBps is the bucket's bytes over its runs' summed duration — the
	// average throughput while something was transferring.
	RateBps float64 `json:"rateBps"`
}

// Aggregate groups points into fixed-width, UTC-aligned buckets covering
// [from, to] — including empty buckets — so the result plots as an evenly
// spaced time series. A zero from starts at the earliest point; a zero to
// ends at the latest. Points outside the window are dropped.
func Aggregate(points []Point, width time.Duration, from, to time.Time) []Bucket {
	if width <= 0 {
		return nil
	}
	if from.IsZero() || to.IsZero() {
		if len(points) == 0 {
			return nil
		}
		sorted := append([]Point(nil), points...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start.Before(sorted[j].Start) })
		if from.IsZero() {
			from = sorted[0].Start
		}
		if to.IsZero() {
			to = sorted[len(sorted)-1].Start
		}
	}
	first := from.UTC().Truncate(width)
	last := to.UTC().Truncate(width)
	if last.Before(first) {
		return nil
	}
	n := int(last.Sub(first)/width) + 1
	out := make([]Bucket, n)
	for i := range out {
		out[i].Start = first.Add(time.Duration(i) * width)
	}
	for _, p := range points {
		if p.Start.Before(from) || p.Start.After(to) {
			continue
		}
		i := int(p.Start.UTC().Truncate(width).Sub(first) / width)
		if i < 0 || i >= n {
			continue
		}
		b := &out[i]
		b.Runs++
		if p.Failed {
			b.Failed++
		}
		b.Bytes += p.Stats.Bytes
		b.Files += p.Stats.Files
		b.Errors += p.Stats.Errors
		b.DurationMs += p.Duration.Milliseconds()
	}
	for i := range out {
		out[i].RateBps = Rate(out[i].Bytes, time.Duration(out[i].DurationMs)*time.Millisecond)
	}
	return out
}

// Totals sums every point: the "overall" row above the graphs.
type Totals struct {
	Runs       int     `json:"runs"`
	Failed     int     `json:"failed"`
	Bytes      int64   `json:"bytes"`
	Files      int     `json:"files"`
	Errors     int     `json:"errors"`
	DurationMs int64   `json:"durationMs"`
	RateBps    float64 `json:"rateBps"`
}

// Total sums points.
func Total(points []Point) Totals {
	var t Totals
	for _, p := range points {
		t.Runs++
		if p.Failed {
			t.Failed++
		}
		t.Bytes += p.Stats.Bytes
		t.Files += p.Stats.Files
		t.Errors += p.Stats.Errors
		t.DurationMs += p.Duration.Milliseconds()
	}
	t.RateBps = Rate(t.Bytes, time.Duration(t.DurationMs)*time.Millisecond)
	return t
}
