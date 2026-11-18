// Package filter is backup-stack's ordered include/exclude rule list. The
// same rules select which files a sync job copies and which files an
// ssh-path backup target captures, whichever transfer path (rsync or the
// native Go copier) ends up moving them: backup-stack always evaluates the
// rules itself and hands rsync an explicit file list, so the semantics never
// depend on rsync's own (subtly different) filter language.
package filter

import (
	"fmt"
	"strings"
)

// Kind distinguishes an include rule from an exclude rule.
type Kind string

const (
	Include Kind = "include"
	Exclude Kind = "exclude"
)

// Rule is one include/exclude glob rule. Rules are evaluated in order; the
// first rule whose Pattern matches a path decides whether that path is
// included. Pattern is matched against the path relative to the root being
// synced or backed up, using "/" as the separator regardless of OS. "*"
// matches any run of characters within one path segment, "?" matches one
// character, and "**" matches any run of characters across segment
// boundaries (including none).
type Rule struct {
	Kind    Kind   `json:"kind"`
	Pattern string `json:"pattern"`
}

// Rules is an ordered rule list.
type Rules []Rule

// Included reports whether relPath should be copied. With no rules,
// everything is included. Otherwise the first matching rule wins; a path
// matched by no rule is included.
func (rs Rules) Included(relPath string) bool {
	relPath = strings.TrimPrefix(relPath, "/")
	for _, rule := range rs {
		if Match(rule.Pattern, relPath) {
			return rule.Kind == Include
		}
	}
	return true
}

// Validate reports the first malformed rule: an unknown kind or an empty
// pattern.
func (rs Rules) Validate() error {
	for i, r := range rs {
		if r.Kind != Include && r.Kind != Exclude {
			return fmt.Errorf("filter %d: kind must be %q or %q, got %q", i, Include, Exclude, r.Kind)
		}
		if strings.TrimSpace(r.Pattern) == "" {
			return fmt.Errorf("filter %d: empty pattern", i)
		}
	}
	return nil
}

// Parse reads the one-rule-per-line text form the admin UI uses:
// "exclude:*.tmp", "include:logs/**", or "- *.tmp" / "+ logs/**" (rsync
// style). Blank lines and lines starting with "#" are ignored.
func Parse(text string) (Rules, error) {
	var out Rules
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var r Rule
		switch {
		case strings.HasPrefix(line, "+ "):
			r = Rule{Include, strings.TrimSpace(line[2:])}
		case strings.HasPrefix(line, "- "):
			r = Rule{Exclude, strings.TrimSpace(line[2:])}
		default:
			k, p, ok := strings.Cut(line, ":")
			if !ok {
				return nil, fmt.Errorf("filter line %d: want kind:pattern, got %q", n+1, line)
			}
			r = Rule{Kind(strings.TrimSpace(k)), strings.TrimSpace(p)}
		}
		out = append(out, r)
	}
	return out, out.Validate()
}

// Match matches pattern against name, both using "/" as the path
// separator. It supports "*" (within a segment), "?" (one character) and
// "**" (any number of characters, spanning segments).
func Match(pattern, name string) bool {
	for {
		if pattern == "" {
			return name == ""
		}
		if strings.HasPrefix(pattern, "**") {
			rest := strings.TrimPrefix(pattern[2:], "/")
			if rest == "" {
				return true
			}
			// Try the remaining pattern against every suffix of name that
			// starts at a segment boundary (including name itself).
			for i := 0; i <= len(name); i++ {
				if i != 0 && i < len(name) && name[i] != '/' {
					continue
				}
				if Match(rest, strings.TrimPrefix(name[i:], "/")) {
					return true
				}
			}
			return false
		}

		pSeg, pMore, pRest := cutSegment(pattern)
		nSeg, nMore, nRest := cutSegment(name)
		if !matchSegment([]rune(pSeg), []rune(nSeg)) {
			return false
		}
		if !pMore && !nMore {
			return true
		}
		if pMore != nMore {
			// "dir/**" also matches "dir" itself's contents only, not "dir".
			return false
		}
		pattern, name = pRest, nRest
	}
}

// cutSegment splits off the first "/"-delimited segment of s, reporting
// whether a "/" was found (i.e. more segments follow).
func cutSegment(s string) (seg string, more bool, rest string) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], true, s[i+1:]
	}
	return s, false, ""
}

// matchSegment matches one path segment against one pattern segment
// supporting "*" and "?".
func matchSegment(pattern, seg []rune) bool {
	for {
		if len(pattern) == 0 {
			return len(seg) == 0
		}
		switch pattern[0] {
		case '*':
			for i := 0; i <= len(seg); i++ {
				if matchSegment(pattern[1:], seg[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(seg) == 0 {
				return false
			}
		default:
			if len(seg) == 0 || pattern[0] != seg[0] {
				return false
			}
		}
		pattern, seg = pattern[1:], seg[1:]
	}
}
