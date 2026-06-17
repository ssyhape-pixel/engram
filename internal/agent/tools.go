package agent

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ssy/engram/internal/search"
)

// Toolset exposes the memory tools (list/read/recall/edit) bound to one
// session's working directory. Dispatch is stateless; the Session tracks dirty.
type Toolset struct {
	workdir string
	agentID string
	search  search.Search
}

func NewToolset(workdir, agentID string, s search.Search) *Toolset {
	return &Toolset{workdir: workdir, agentID: agentID, search: s}
}

// Defs advertises the tools to the model.
func (t *Toolset) Defs() []ToolDef {
	return []ToolDef{
		{Name: "list", Description: "List memory files with their descriptions (the tree index).", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}}},
		{Name: "read", Description: "Read a memory file, optionally a 1-based inclusive line range.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}, "start": map[string]any{"type": "integer"}, "end": map[string]any{"type": "integer"}}, "required": []any{"path"}}},
		{Name: "recall", Description: "Search memory for a query; returns matching line ranges.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []any{"query"}}},
		{Name: "edit", Description: "Write content to a memory file (creates or overwrites).", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}, "content": map[string]any{"type": "string"}}, "required": []any{"path", "content"}}},
	}
}

func errResult(callID string, format string, args ...any) ToolResult {
	return ToolResult{CallID: callID, Content: fmt.Sprintf(format, args...), IsError: true}
}

func okResult(callID, content string) ToolResult {
	return ToolResult{CallID: callID, Content: content}
}

// safePath resolves rel against workdir and rejects escapes (.. traversal).
func (t *Toolset) safePath(rel string) (string, error) {
	clean := filepath.Clean(filepath.Join(t.workdir, rel))
	wd := filepath.Clean(t.workdir)
	if clean != wd && !strings.HasPrefix(clean, wd+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes workdir", rel)
	}
	return clean, nil
}

func strInput(in map[string]any, key string) string {
	if v, ok := in[key].(string); ok {
		return v
	}
	return ""
}

// intInput accepts JSON numbers (float64) or ints; 0 if absent.
func intInput(in map[string]any, key string) int {
	switch v := in[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// Dispatch routes a ToolCall by name and returns its result.
func (t *Toolset) Dispatch(ctx context.Context, call ToolCall) ToolResult {
	switch call.Name {
	case "list":
		return t.doList(call.ID)
	case "read":
		return t.doRead(call.ID, call.Input)
	case "recall":
		return t.doRecall(ctx, call.ID, call.Input)
	case "edit":
		return t.doEdit(call.ID, call.Input)
	default:
		return errResult(call.ID, "unknown tool %q", call.Name)
	}
}

func (t *Toolset) doList(callID string) ToolResult {
	var lines []string
	err := filepath.WalkDir(t.workdir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(t.workdir, path)
		lines = append(lines, fmt.Sprintf("%s: %s", rel, frontmatterDescription(path)))
		return nil
	})
	if err != nil {
		return errResult(callID, "list: %v", err)
	}
	sort.Strings(lines)
	return okResult(callID, strings.Join(lines, "\n"))
}

func (t *Toolset) doRead(callID string, in map[string]any) ToolResult {
	rel := strInput(in, "path")
	full, err := t.safePath(rel)
	if err != nil {
		return errResult(callID, "read: %v", err)
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return errResult(callID, "read %s: %v", rel, err)
	}
	_, hasStart := in["start"]
	_, hasEnd := in["end"]
	if !hasStart && !hasEnd {
		return okResult(callID, string(data))
	}
	start, end := intInput(in, "start"), intInput(in, "end")
	all := strings.Split(string(data), "\n")
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= len(all) {
		end = len(all) - 1
	}
	if start >= len(all) {
		return okResult(callID, "")
	}
	return okResult(callID, strings.Join(all[start:end+1], "\n"))
}

func (t *Toolset) doRecall(ctx context.Context, callID string, in map[string]any) ToolResult {
	hits, err := t.search.Recall(ctx, t.agentID, strInput(in, "query"), 8)
	if err != nil {
		return errResult(callID, "recall: %v", err)
	}
	if len(hits) == 0 {
		return okResult(callID, "(no matches)")
	}
	var b strings.Builder
	for _, h := range hits {
		fmt.Fprintf(&b, "%s:%d-%d: %s\n", h.Path, h.LineStart, h.LineEnd, h.Snippet)
	}
	return okResult(callID, b.String())
}

func (t *Toolset) doEdit(callID string, in map[string]any) ToolResult {
	rel := strInput(in, "path")
	full, err := t.safePath(rel)
	if err != nil {
		return errResult(callID, "edit: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return errResult(callID, "edit mkdir %s: %v", rel, err)
	}
	if err := os.WriteFile(full, []byte(strInput(in, "content")), 0o644); err != nil {
		return errResult(callID, "edit write %s: %v", rel, err)
	}
	return okResult(callID, fmt.Sprintf("wrote %s", rel))
}

// frontmatterDescription extracts the `description:` value from a leading
// YAML frontmatter block (--- ... ---). Returns "" if absent.
func frontmatterDescription(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() || strings.TrimSpace(sc.Text()) != "---" {
		return ""
	}
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "---" {
			return ""
		}
		if rest, ok := strings.CutPrefix(line, "description:"); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}
