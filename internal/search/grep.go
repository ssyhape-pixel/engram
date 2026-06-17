package search

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// GrepSearch is the L2 recall stub: a case-insensitive substring scan over a
// working directory, returning each matching line as a single-line Hit. It is
// bound to one agent's materialized working tree.
type GrepSearch struct {
	root string
}

func NewGrep(root string) *GrepSearch { return &GrepSearch{root: root} }

func (g *GrepSearch) Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error) {
	if k <= 0 {
		k = 8
	}
	needle := strings.ToLower(query)
	var hits []Hit
	err := filepath.WalkDir(g.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// stop walking once k hits collected (from earlier files)
		if len(hits) >= k {
			return filepath.SkipAll
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("search: open %s: %w", path, err)
		}
		// defer runs when this per-file closure returns, not when Recall returns — fd is closed before the next file opens.
		defer f.Close()
		rel, _ := filepath.Rel(g.root, path)
		sc := bufio.NewScanner(f)
		line := 0
		for sc.Scan() {
			line++
			text := sc.Text()
			if strings.Contains(strings.ToLower(text), needle) {
				hits = append(hits, Hit{Path: rel, LineStart: line, LineEnd: line, Snippet: text})
				// stop scanning lines in this file
				if len(hits) >= k {
					break
				}
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("search: scan %s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return hits, nil
}

// Reindex is a no-op stub in L2.
func (g *GrepSearch) Reindex(ctx context.Context, agentID, from, to string) error { return nil }

var _ Search = (*GrepSearch)(nil)
