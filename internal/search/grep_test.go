package search

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGrepRecallFindsLines(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, dir, "a.md", "first line\nhas needle here\nlast\n")
	writeFile(t, dir, "sub/b.md", "nothing\n")

	g := NewGrep(dir)
	hits, err := g.Recall(ctx, "a1", "needle", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("hits = %d want 1: %+v", len(hits), hits)
	}
	h := hits[0]
	if h.Path != "a.md" || h.LineStart != 2 || h.LineEnd != 2 {
		t.Fatalf("hit = %+v want a.md:2", h)
	}
}

func TestGrepRecallCaseInsensitiveAndK(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, dir, "f.md", "Alpha\nalpha\nALPHA\n")
	g := NewGrep(dir)
	hits, err := g.Recall(ctx, "a1", "alpha", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("k truncation failed: got %d want 2", len(hits))
	}
}

func TestGrepRecallNoMatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, dir, "f.md", "abc\n")
	g := NewGrep(dir)
	hits, err := g.Recall(ctx, "a1", "zzz", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("want no hits, got %+v", hits)
	}
}

func TestGrepReindexIsNoop(t *testing.T) {
	g := NewGrep(t.TempDir())
	if err := g.Reindex(context.Background(), "a1", "x", "y"); err != nil {
		t.Fatalf("reindex stub should be nil: %v", err)
	}
}
