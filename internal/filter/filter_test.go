package filter

import "testing"

func TestIncluded(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		rules Rules
		want  bool
	}{
		{
			name:  "zero rules defaults to include",
			path:  "any/file.txt",
			rules: nil,
			want:  true,
		},
		{
			name: "first match wins: exclude before include",
			path: "logs/app.log",
			rules: Rules{
				{Kind: Exclude, Pattern: "logs/*"},
				{Kind: Include, Pattern: "*"},
			},
			want: false,
		},
		{
			name: "first match wins: include before exclude",
			path: "logs/app.log",
			rules: Rules{
				{Kind: Include, Pattern: "logs/*"},
				{Kind: Exclude, Pattern: "*"},
			},
			want: true,
		},
		{
			name: "no rule matches defaults to include",
			path: "data/file.bin",
			rules: Rules{
				{Kind: Exclude, Pattern: "logs/*"},
			},
			want: true,
		},
		{
			name: "recursive glob matches nested path",
			path: "a/b/c/node_modules/pkg/index.js",
			rules: Rules{
				{Kind: Exclude, Pattern: "**/node_modules/**"},
			},
			want: false,
		},
		{
			name: "recursive glob does not match unrelated path",
			path: "a/b/c/vendor/pkg/index.js",
			rules: Rules{
				{Kind: Exclude, Pattern: "**/node_modules/**"},
			},
			want: true,
		},
		{
			name: "leading recursive glob matches from root",
			path: "node_modules/pkg/index.js",
			rules: Rules{
				{Kind: Exclude, Pattern: "**/node_modules/**"},
			},
			want: false,
		},
		{
			name: "trailing recursive glob matches everything under prefix",
			path: "build/x/y/z.o",
			rules: Rules{
				{Kind: Exclude, Pattern: "build/**"},
			},
			want: false,
		},
		{
			name: "single star does not cross segment boundary",
			path: "a/b.txt",
			rules: Rules{
				{Kind: Exclude, Pattern: "*.txt"},
			},
			want: true,
		},
		{
			name: "single star matches within segment",
			path: "b.txt",
			rules: Rules{
				{Kind: Exclude, Pattern: "*.txt"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.rules.Included(tc.path)
			if got != tc.want {
				t.Errorf("Rules(%+v).Included(%q) = %v, want %v", tc.rules, tc.path, got, tc.want)
			}
		})
	}
}

func TestMatchEdgeCases(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"**", "anything/at/all", true},
		{"**/*.log", "x.log", true},
		{"**/*.log", "a/b/x.log", true},
		{"**/*.log", "a/b/x.log.gz", false},
		{"logs/*", "logs/a/b", false},
		{"logs/**", "logs", false},
		{"*/cache/**", "u/cache/f", true},
		{"*/cache/**", "u/v/cache/f", false},
	}
	for _, c := range cases {
		if got := Match(c.pattern, c.name); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestParse(t *testing.T) {
	rs, err := Parse("exclude:*.tmp\n\n# comment\n+ keep/**\n- **/node_modules/**\ninclude: a/*")
	if err != nil {
		t.Fatal(err)
	}
	want := Rules{{Exclude, "*.tmp"}, {Include, "keep/**"}, {Exclude, "**/node_modules/**"}, {Include, "a/*"}}
	if len(rs) != len(want) {
		t.Fatalf("got %+v", rs)
	}
	for i := range want {
		if rs[i] != want[i] {
			t.Fatalf("rule %d = %+v, want %+v", i, rs[i], want[i])
		}
	}
	if _, err := Parse("bogus"); err == nil {
		t.Fatal("expected error for a line without kind")
	}
	if _, err := Parse("delete:*.tmp"); err == nil {
		t.Fatal("expected error for unknown kind")
	}
	if err := (Rules{{Include, " "}}).Validate(); err == nil {
		t.Fatal("expected error for empty pattern")
	}
}
