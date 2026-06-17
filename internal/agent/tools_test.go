package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ssy/engram/internal/search"
)

func setupWorkdir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	mk := func(rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("system/about.md", "---\ndescription: who the agent is\n---\nresident\n")
	mk("notes/todo.md", "---\ndescription: a todo list\n---\nbuy milk\nfind needle\n")
	return dir
}

func newTools(dir string) *Toolset {
	return NewToolset(dir, "a1", search.NewGrep(dir))
}

func TestToolsetDefs(t *testing.T) {
	defs := newTools(t.TempDir()).Defs()
	names := map[string]bool{}
	for _, d := range defs {
		names[d.Name] = true
	}
	for _, want := range []string{"list", "read", "recall", "edit"} {
		if !names[want] {
			t.Fatalf("missing tool def %q (got %v)", want, names)
		}
	}
}

func TestListReturnsTreeIndexWithDescriptions(t *testing.T) {
	ts := newTools(setupWorkdir(t))
	res := ts.Dispatch(context.Background(), ToolCall{Name: "list"})
	if res.IsError {
		t.Fatalf("list error: %s", res.Content)
	}
	if !strings.Contains(res.Content, "system/about.md") || !strings.Contains(res.Content, "who the agent is") {
		t.Fatalf("list missing path/description:\n%s", res.Content)
	}
	if !strings.Contains(res.Content, "a todo list") {
		t.Fatalf("list missing second description:\n%s", res.Content)
	}
}

func TestReadFullAndRange(t *testing.T) {
	ts := newTools(setupWorkdir(t))
	full := ts.Dispatch(context.Background(), ToolCall{Name: "read", Input: map[string]any{"path": "notes/todo.md"}})
	if full.IsError || !strings.Contains(full.Content, "buy milk") {
		t.Fatalf("read full = %+v", full)
	}
	rng := ts.Dispatch(context.Background(), ToolCall{Name: "read", Input: map[string]any{"path": "notes/todo.md", "start": float64(4), "end": float64(4)}})
	if rng.IsError || strings.Contains(rng.Content, "buy milk") || !strings.Contains(rng.Content, "find needle") {
		t.Fatalf("read range = %+v", rng)
	}
}

func TestReadRejectsTraversal(t *testing.T) {
	ts := newTools(setupWorkdir(t))
	res := ts.Dispatch(context.Background(), ToolCall{Name: "read", Input: map[string]any{"path": "../../etc/passwd"}})
	if !res.IsError {
		t.Fatalf("expected traversal rejection, got %+v", res)
	}
}

func TestEditWritesFile(t *testing.T) {
	dir := setupWorkdir(t)
	ts := newTools(dir)
	res := ts.Dispatch(context.Background(), ToolCall{Name: "edit", Input: map[string]any{"path": "notes/new.md", "content": "fresh\n"}})
	if res.IsError {
		t.Fatalf("edit error: %s", res.Content)
	}
	got, err := os.ReadFile(filepath.Join(dir, "notes/new.md"))
	if err != nil || string(got) != "fresh\n" {
		t.Fatalf("edit did not write: %q %v", got, err)
	}
}

func TestEditRejectsTraversal(t *testing.T) {
	ts := newTools(setupWorkdir(t))
	res := ts.Dispatch(context.Background(), ToolCall{Name: "edit", Input: map[string]any{"path": "../escape.md", "content": "x"}})
	if !res.IsError {
		t.Fatalf("expected traversal rejection, got %+v", res)
	}
}

func TestRecallReturnsLineRanges(t *testing.T) {
	ts := newTools(setupWorkdir(t))
	res := ts.Dispatch(context.Background(), ToolCall{Name: "recall", Input: map[string]any{"query": "needle"}})
	if res.IsError {
		t.Fatalf("recall error: %s", res.Content)
	}
	if !strings.Contains(res.Content, "notes/todo.md") || !strings.Contains(res.Content, "find needle") {
		t.Fatalf("recall missing hit:\n%s", res.Content)
	}
}

func TestUnknownToolIsError(t *testing.T) {
	ts := newTools(t.TempDir())
	res := ts.Dispatch(context.Background(), ToolCall{Name: "frobnicate"})
	if !res.IsError {
		t.Fatal("unknown tool should be an error result")
	}
}
