package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ssy/engram/internal/memstore"
)

func isTopHeading(line []byte) bool {
	return bytes.HasPrefix(bytes.TrimLeft(line, " "), []byte("# "))
}

// splitTopLevel splits markdown at top-level ('# ') headings into parts (newlines
// preserved). Content before the first top-level heading (preamble) becomes
// part[0] if it has non-whitespace content. '##'+ are NOT split points. Fewer
// than 2 parts ⇒ not splittable.
func splitTopLevel(content []byte) [][]byte {
	var parts [][]byte
	var cur []byte
	flush := func() {
		if len(bytes.TrimSpace(cur)) > 0 {
			parts = append(parts, cur)
		}
		cur = nil
	}
	for _, ln := range bytes.SplitAfter(content, []byte("\n")) {
		if len(ln) == 0 {
			continue
		}
		if isTopHeading(ln) {
			flush()
		}
		cur = append(cur, ln...)
	}
	flush()
	return parts
}

// isSplittable reports whether a file qualifies for defrag: a .md file larger
// than maxBytes with at least 2 top-level-heading parts (so splitting strictly
// reduces the largest file — guaranteeing convergence).
func isSplittable(path string, content []byte, maxBytes int) bool {
	return strings.HasSuffix(path, ".md") && len(content) > maxBytes && len(splitTopLevel(content)) >= 2
}

// dirHasSplittable walks a materialized dir and reports whether any file is
// splittable (used by the scan to decide whether to enqueue a defrag job).
func dirHasSplittable(dir string, maxBytes int) (bool, error) {
	found := false
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || found {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if !strings.HasSuffix(rel, ".md") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() <= int64(maxBytes) {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if isSplittable(rel, content, maxBytes) {
			found = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// Defrag splits an agent's oversized, splittable .md files at top-level headings
// into sibling <base>.NN.md files and commits the result (jobs=nil — no
// self-trigger). It commits only if something changed (idempotent). A CAS
// conflict returns ErrConflict. No lock is taken here (the caller holds the
// per-agent advisory lock).
func Defrag(ctx context.Context, store memstore.MemStore, agentID string, maxBytes int) error {
	head, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		return fmt.Errorf("maintenance: defrag resolve head %s: %w", agentID, err)
	}
	dir, err := os.MkdirTemp("", "engram-defrag-*")
	if err != nil {
		return fmt.Errorf("maintenance: defrag scratch: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := store.Materialize(ctx, agentID, head, dir); err != nil {
		return fmt.Errorf("maintenance: defrag materialize: %w", err)
	}

	type target struct {
		abs   string
		parts [][]byte
	}
	var targets []target
	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if isSplittable(rel, content, maxBytes) {
			targets = append(targets, target{abs: path, parts: splitTopLevel(content)})
		}
		return nil
	})
	if walkErr != nil {
		return fmt.Errorf("maintenance: defrag walk: %w", walkErr)
	}
	if len(targets) == 0 {
		return nil
	}
	for _, tg := range targets {
		base := strings.TrimSuffix(tg.abs, ".md")
		for i, p := range tg.parts {
			out := fmt.Sprintf("%s.%02d.md", base, i+1)
			if err := os.WriteFile(out, p, 0o644); err != nil {
				return fmt.Errorf("maintenance: defrag write %s: %w", out, err)
			}
		}
		if err := os.Remove(tg.abs); err != nil {
			return fmt.Errorf("maintenance: defrag remove %s: %w", tg.abs, err)
		}
	}

	if _, err := store.CommitWithCAS(ctx, agentID, head, dir, nil); err != nil {
		if errors.Is(err, memstore.ErrCASConflict) {
			return ErrConflict
		}
		return fmt.Errorf("maintenance: defrag commit: %w", err)
	}
	return nil
}
