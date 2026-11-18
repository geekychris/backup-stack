package runstats

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestAddErrorCapsSamples(t *testing.T) {
	var s Stats
	for i := 0; i < MaxErrorSamples+5; i++ {
		s.AddError(fmt.Errorf("e%d", i))
	}
	s.AddError(nil)
	if s.Errors != MaxErrorSamples+5 || len(s.ErrorSamples) != MaxErrorSamples {
		t.Fatalf("errors=%d samples=%d", s.Errors, len(s.ErrorSamples))
	}
	if got := s.Summary(); got != "15 errors; first: e0" {
		t.Fatalf("summary = %q", got)
	}
	one := Stats{}
	one.AddError(errors.New("boom"))
	if one.Summary() != "boom" {
		t.Fatalf("summary = %q", one.Summary())
	}
	if (Stats{}).Summary() != "" {
		t.Fatal("clean run should have an empty summary")
	}
}

func TestRate(t *testing.T) {
	if r := Rate(1000, 2*time.Second); r != 500 {
		t.Fatalf("rate = %v", r)
	}
	if Rate(1000, 0) != 0 || Rate(0, time.Second) != 0 {
		t.Fatal("zero duration / zero bytes must give zero rate")
	}
}

func TestAggregateFillsGapsAndSums(t *testing.T) {
	base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	pts := []Point{
		{Start: base.Add(5 * time.Minute), Duration: 2 * time.Second, Stats: Stats{Bytes: 100, Files: 2}},
		{Start: base.Add(50 * time.Minute), Duration: 2 * time.Second, Stats: Stats{Bytes: 300, Files: 1, Errors: 1}, Failed: true},
		{Start: base.Add(3*time.Hour + time.Minute), Duration: time.Second, Stats: Stats{Bytes: 10}},
	}
	got := Aggregate(pts, time.Hour, time.Time{}, time.Time{})
	if len(got) != 4 {
		t.Fatalf("buckets = %d, want 4 (10:00, 11:00 empty, 12:00 empty, 13:00): %+v", len(got), got)
	}
	b0 := got[0]
	if !b0.Start.Equal(base) || b0.Runs != 2 || b0.Failed != 1 || b0.Bytes != 400 || b0.Files != 3 || b0.Errors != 1 || b0.DurationMs != 4000 {
		t.Fatalf("bucket 0 = %+v", b0)
	}
	if b0.RateBps != 100 {
		t.Fatalf("bucket 0 rate = %v, want 100 B/s", b0.RateBps)
	}
	if got[1].Runs != 0 || got[2].Runs != 0 || got[3].Bytes != 10 {
		t.Fatalf("gap/last buckets wrong: %+v", got[1:])
	}
}

func TestAggregateWindow(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	pts := []Point{
		{Start: base.Add(-48 * time.Hour), Stats: Stats{Bytes: 999}}, // before window: dropped
		{Start: base.Add(36 * time.Hour), Stats: Stats{Bytes: 5}},
	}
	day := 24 * time.Hour
	got := Aggregate(pts, day, base, base.Add(6*day))
	if len(got) != 7 {
		t.Fatalf("buckets = %d, want 7", len(got))
	}
	var sum int64
	for _, b := range got {
		sum += b.Bytes
	}
	if sum != 5 || got[1].Bytes != 5 {
		t.Fatalf("window sum = %d, buckets %+v", sum, got)
	}
	if Aggregate(nil, day, time.Time{}, time.Time{}) != nil {
		t.Fatal("no points and no window: nil")
	}
	if n := len(Aggregate(nil, day, base, base.Add(2*day))); n != 3 {
		t.Fatalf("empty window still plots: got %d buckets", n)
	}
}

func TestTotal(t *testing.T) {
	tot := Total([]Point{
		{Duration: time.Second, Stats: Stats{Bytes: 10, Files: 1}},
		{Duration: time.Second, Failed: true, Stats: Stats{Bytes: 30, Errors: 2}},
	})
	if tot.Runs != 2 || tot.Failed != 1 || tot.Bytes != 40 || tot.Errors != 2 || tot.RateBps != 20 {
		t.Fatalf("totals = %+v", tot)
	}
}
