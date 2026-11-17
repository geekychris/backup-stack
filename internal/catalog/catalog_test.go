package catalog

import (
	"reflect"
	"sort"
	"testing"
)

func item(path, fp string, size int64) Item {
	return Item{Path: path, Fingerprint: fp, Size: size}
}

func names(items []Item) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.Path)
	}
	sort.Strings(out)
	return out
}

func TestDiff(t *testing.T) {
	cases := []struct {
		name        string
		prev, cur   Catalog
		wantAdded   []string
		wantChanged []string
		wantDeleted []string
	}{
		{
			name: "no changes",
			prev: Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
			cur:  Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
		},
		{
			name:        "one changed",
			prev:        Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
			cur:         Catalog{Items: []Item{item("a", "1", 1), item("b", "3", 2)}},
			wantChanged: []string{"b"},
		},
		{
			name:      "one added",
			prev:      Catalog{Items: []Item{item("a", "1", 1)}},
			cur:       Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
			wantAdded: []string{"b"},
		},
		{
			name:        "one deleted",
			prev:        Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
			cur:         Catalog{Items: []Item{item("a", "1", 1)}},
			wantDeleted: []string{"b"},
		},
		{
			name:        "fully disjoint",
			prev:        Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}},
			cur:         Catalog{Items: []Item{item("c", "3", 3), item("d", "4", 4)}},
			wantAdded:   []string{"c", "d"},
			wantDeleted: []string{"a", "b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := Diff(tc.prev, tc.cur)
			if got := names(d.Added); !reflect.DeepEqual(got, sortedOrNil(tc.wantAdded)) {
				t.Errorf("added = %v, want %v", got, tc.wantAdded)
			}
			if got := names(d.Changed); !reflect.DeepEqual(got, sortedOrNil(tc.wantChanged)) {
				t.Errorf("changed = %v, want %v", got, tc.wantChanged)
			}
			sortedDeleted := append([]string{}, tc.wantDeleted...)
			sort.Strings(sortedDeleted)
			if got := append([]string{}, d.Deleted...); !reflect.DeepEqual(sortCopy(got), sortedOrNil(tc.wantDeleted)) {
				t.Errorf("deleted = %v, want %v", got, tc.wantDeleted)
			}
		})
	}
}

func sortCopy(s []string) []string {
	out := append([]string{}, s...)
	sort.Strings(out)
	return sortedOrNil(out)
}

// sortedOrNil normalizes an empty slice to nil so reflect.DeepEqual treats
// "no entries" the same whether the test wrote nil or []string{}.
func sortedOrNil(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := append([]string{}, s...)
	sort.Strings(out)
	return out
}

func TestCatalogMarshalRoundTrip(t *testing.T) {
	c := Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}}
	b, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, c) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, c)
	}
}

func TestApplyReplayAppliesDiffInOrder(t *testing.T) {
	base := Catalog{Items: []Item{item("a", "1", 1), item("b", "2", 2)}}
	d1 := Diff(base, Catalog{Items: []Item{item("a", "1", 1), item("b", "3", 2), item("c", "1", 1)}})
	step1 := Apply(base, d1)
	d2 := Diff(step1, Catalog{Items: []Item{item("a", "1", 1), item("c", "1", 1)}})
	step2 := Apply(step1, d2)

	want := []string{"a", "c"}
	if got := names(step2.Items); !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed catalog = %v, want %v", got, want)
	}
}
