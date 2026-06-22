package search

import (
	"sort"
	"strings"
)

type posting struct {
	path string
	line int // 1-based
}

// TrigramIndex is a line-level trigram inverted index over a set of files.
// Variant A: it retains line text, so queries touch no object storage.
type TrigramIndex struct {
	postings map[string][]posting
	lines    map[string][]string // path -> lines; line N is lines[path][N-1]
}

// trigrams returns the distinct lowercase 3-rune shingles of s (nil if len<3).
func trigrams(s string) []string {
	r := []rune(strings.ToLower(s))
	if len(r) < 3 {
		return nil
	}
	seen := make(map[string]struct{}, len(r))
	out := make([]string, 0, len(r)-2)
	for i := 0; i+3 <= len(r); i++ {
		t := string(r[i : i+3])
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// BuildTrigram indexes files (path -> content bytes).
func BuildTrigram(files map[string][]byte) *TrigramIndex {
	idx := &TrigramIndex{postings: map[string][]posting{}, lines: map[string][]string{}}
	for path, content := range files {
		ls := strings.Split(string(content), "\n")
		// lines retains original-case text for snippets; trigrams() lowercases for indexing/matching.
		idx.lines[path] = ls
		for i, line := range ls {
			for _, tg := range trigrams(line) {
				idx.postings[tg] = append(idx.postings[tg], posting{path: path, line: i + 1})
			}
		}
	}
	return idx
}

func (t *TrigramIndex) lineText(path string, line int) string {
	ls := t.lines[path]
	if line < 1 || line > len(ls) {
		return ""
	}
	return ls[line-1]
}

// candidates returns the lines that contain every trigram of query. For a
// query shorter than 3 runes (no trigrams) it returns all lines (full scan).
func (t *TrigramIndex) candidates(query string) map[posting]struct{} {
	tgs := trigrams(query)
	if len(tgs) == 0 {
		all := map[posting]struct{}{}
		for path, ls := range t.lines {
			for i := range ls {
				all[posting{path: path, line: i + 1}] = struct{}{}
			}
		}
		return all
	}
	sets := make([]map[posting]struct{}, 0, len(tgs))
	for _, tg := range tgs {
		s := make(map[posting]struct{})
		for _, p := range t.postings[tg] {
			s[p] = struct{}{}
		}
		if len(s) == 0 {
			return map[posting]struct{}{} // a trigram absent everywhere => no candidates
		}
		sets = append(sets, s)
	}
	sort.Slice(sets, func(i, j int) bool { return len(sets[i]) < len(sets[j]) }) // intersect from smallest
	result := map[posting]struct{}{}
	for p := range sets[0] {
		inAll := true
		for _, s := range sets[1:] {
			if _, ok := s[p]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			result[p] = struct{}{}
		}
	}
	return result
}

// Search returns up to k line-range Hits whose line contains query
// (case-insensitive). Trigram intersection is a fast prefilter; the substring
// check is the source of truth (removes trigram false positives).
func (t *TrigramIndex) Search(query string, k int) []Hit {
	if k <= 0 {
		k = 8
	}
	if query == "" {
		return nil
	}
	q := strings.ToLower(query)
	var matches []posting
	for p := range t.candidates(q) {
		if strings.Contains(strings.ToLower(t.lineText(p.path, p.line)), q) {
			matches = append(matches, p)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].path != matches[j].path {
			return matches[i].path < matches[j].path
		}
		return matches[i].line < matches[j].line
	})
	var hits []Hit
	for _, m := range matches {
		if len(hits) >= k {
			break
		}
		hits = append(hits, Hit{Path: m.path, LineStart: m.line, LineEnd: m.line, Snippet: t.lineText(m.path, m.line)})
	}
	return hits
}
