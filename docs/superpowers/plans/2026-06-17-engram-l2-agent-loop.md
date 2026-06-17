# Engram L2 — Agent Loop + Session Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Engram's provider-agnostic agent loop: a stateful Session that runs a multi-turn tool-use loop (recall/read/edit) over a materialized working tree and persists each dirty turn as a diffable commit via L1's MemStore, plus a single-writer Router, a deterministic FakeProvider, and a real Anthropic adapter.

**Architecture:** `LLMProvider` is a provider-agnostic interface (Request/Response with tool-use). A `Session` holds chat history + a live workdir + current HEAD; `Send` loops Generate→dispatch tools→Generate until the model returns final text, then commits the workdir if any `edit` happened. A `Router` enforces single-writer-per-agent via an in-process lock and materializes the workdir on Open. `recall` is backed by a `GrepSearch` stub (L4 swaps in trigram).

**Tech Stack:** Go 1.25; stdlib `net/http` for the Anthropic adapter; L1 `internal/memstore` (already on `main`). Tests use a deterministic FakeProvider (no network) and a mock `http.RoundTripper` for the Anthropic adapter; Session/Router tests need the live Postgres.

**Prerequisites:**
- On `main` (L1 merged). Create a feature branch before Task 1: `git checkout -b feat/l2-agent-loop`.
- Live Postgres for Session/Router tests:
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```
DB-dependent tests `t.Skip` when `ENGRAM_TEST_DB` is unset; CI must set it.

**Package layout (this plan creates):**
```
internal/agent/provider.go     # protocol types + LLMProvider interface
internal/agent/fake.go         # FakeProvider (scripted, deterministic)
internal/agent/tools.go        # Toolset: list/read/recall/edit (stateless dispatch)
internal/agent/session.go      # Session: turn loop + commit-if-dirty + bounded CAS retry
internal/agent/router.go       # Router: single-writer lock, Open/Close, materialize
internal/agent/anthropic.go    # AnthropicProvider: Messages API + tool-use mapping
internal/search/search.go      # Search interface + Hit
internal/search/grep.go        # GrepSearch: recall stub over a workdir
cmd/api/main.go                # dev wiring + manual single-turn entry
```
**Dependency order:** provider+fake (1) → search (2) → tools (3, needs provider+search) → session (4, needs provider+tools+memstore) → router (5, needs session) → anthropic (6, needs provider) → cmd/api (7, needs all). Tasks 1,2,6 need no DB; 3 needs none; 4,5 need live PG; 7 builds only.

---

### Task 1: LLMProvider protocol + FakeProvider

**Files:**
- Create: `internal/agent/provider.go`
- Create: `internal/agent/fake.go`
- Test: `internal/agent/fake_test.go`

- [ ] **Step 1: Write the failing test** — `internal/agent/fake_test.go`:
```go
package agent

import (
	"context"
	"testing"
)

func TestFakeProviderScriptedInOrder(t *testing.T) {
	ctx := context.Background()
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response { return Response{ToolCalls: []ToolCall{{ID: "1", Name: "recall", Input: map[string]any{"query": "x"}}}} },
		func(r Request) Response { return Response{Text: "done"} },
	}}

	r1, err := f.Generate(ctx, Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.ToolCalls) != 1 || r1.ToolCalls[0].Name != "recall" {
		t.Fatalf("step 0 = %+v", r1)
	}
	r2, err := f.Generate(ctx, Request{})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Text != "done" || len(r2.ToolCalls) != 0 {
		t.Fatalf("step 1 = %+v", r2)
	}
}

func TestFakeProviderExhaustionErrors(t *testing.T) {
	ctx := context.Background()
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response { return Response{Text: "only"} },
	}}
	if _, err := f.Generate(ctx, Request{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Generate(ctx, Request{}); err == nil {
		t.Fatal("expected error when script exhausted")
	}
}

func TestFakeProviderSeesRequest(t *testing.T) {
	ctx := context.Background()
	var gotSystem string
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response { gotSystem = r.System; return Response{Text: "ok"} },
	}}
	_, _ = f.Generate(ctx, Request{System: "SYS"})
	if gotSystem != "SYS" {
		t.Fatalf("provider did not see request system: %q", gotSystem)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/agent/`
Expected: compile failure (FakeProvider/Request/Response/ToolCall undefined).

- [ ] **Step 3: Write the protocol** — `internal/agent/provider.go`:
```go
// Package agent implements Engram's provider-agnostic agent loop: a stateful
// Session that runs a tool-use loop over a working tree and persists edits via
// the L1 MemStore. LLMProvider abstracts the model so a deterministic fake can
// drive tests and a real Anthropic adapter can run live.
package agent

import "context"

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolDef advertises a tool to the model.
type ToolDef struct {
	Name        string
	Description string
	InputSchema map[string]any // JSON Schema for the input object
}

// ToolCall is a model-initiated tool invocation.
type ToolCall struct {
	ID    string
	Name  string
	Input map[string]any
}

// ToolResult is the outcome of executing a ToolCall, fed back to the model.
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// Message is one conversation entry.
type Message struct {
	Role      Role
	Text      string       // user/assistant text
	ToolCalls []ToolCall   // assistant-initiated calls (Role==assistant)
	Results   []ToolResult // tool results (Role==tool)
}

// Request is one Generate input.
type Request struct {
	System   string
	Messages []Message
	Tools    []ToolDef
}

// Response: if ToolCalls is non-empty the turn is not complete — the caller
// executes them and feeds results back via a new Generate call. Otherwise Text
// is the final assistant message.
type Response struct {
	Text      string
	ToolCalls []ToolCall
}

// LLMProvider produces the next model response given the conversation so far.
type LLMProvider interface {
	Generate(ctx context.Context, req Request) (Response, error)
}
```

- [ ] **Step 4: Write the fake** — `internal/agent/fake.go`:
```go
package agent

import (
	"context"
	"fmt"
)

// FakeProvider returns scripted responses by call index, driving the agent loop
// deterministically in tests. Each Step receives the Request (so a step can
// assert on what the loop sent) and returns the Response to hand back.
type FakeProvider struct {
	Steps []func(req Request) Response
	calls int
}

func (f *FakeProvider) Generate(ctx context.Context, req Request) (Response, error) {
	if f.calls >= len(f.Steps) {
		return Response{}, fmt.Errorf("fake: no scripted response for call %d (have %d)", f.calls, len(f.Steps))
	}
	step := f.Steps[f.calls]
	f.calls++
	return step(req), nil
}

var _ LLMProvider = (*FakeProvider)(nil)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/agent/`
Expected: PASS (3 cases).

- [ ] **Step 6: Commit**
```bash
git add internal/agent/provider.go internal/agent/fake.go internal/agent/fake_test.go
git commit -m "feat(agent): provider-agnostic LLMProvider protocol + scripted FakeProvider"
```

---

### Task 2: Search interface + GrepSearch stub

**Files:**
- Create: `internal/search/search.go`
- Create: `internal/search/grep.go`
- Test: `internal/search/grep_test.go`

- [ ] **Step 1: Write the failing test** — `internal/search/grep_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/search/`
Expected: compile failure (NewGrep/Hit undefined).

- [ ] **Step 3: Write the interface** — `internal/search/search.go`:
```go
// Package search is recall. The Search interface returns sub-file line ranges
// (predicate pushdown), not whole files. L2 ships a GrepSearch stub over a
// working directory; L4 replaces it with a trigram index (variant A).
package search

import "context"

// Hit is a matching line range with a snippet.
type Hit struct {
	Path      string
	LineStart int // 1-based, inclusive
	LineEnd   int // 1-based, inclusive
	Snippet   string
}

// Search answers recall queries and (eventually) maintains an index.
type Search interface {
	Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error)
	// Reindex is a no-op in L2; incremental trigram indexing is L4.
	Reindex(ctx context.Context, agentID string, from, to string) error
}
```

- [ ] **Step 4: Write the grep stub** — `internal/search/grep.go`:
```go
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
			// skip the .git-style internals if any ever appear; none in our workdir.
			return nil
		}
		if len(hits) >= k {
			return filepath.SkipAll
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("search: open %s: %w", path, err)
		}
		defer f.Close()
		rel, _ := filepath.Rel(g.root, path)
		sc := bufio.NewScanner(f)
		line := 0
		for sc.Scan() {
			line++
			text := sc.Text()
			if strings.Contains(strings.ToLower(text), needle) {
				hits = append(hits, Hit{Path: rel, LineStart: line, LineEnd: line, Snippet: text})
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/search/`
Expected: PASS (4 cases).

> Note: `filepath.SkipAll` exists since Go 1.20 — fine on 1.25. If `k` truncation lands one over (because the outer check runs per-file, not per-line), the inner `break` + `len(hits) >= k` guard caps it exactly; verify `TestGrepRecallCaseInsensitiveAndK` gets exactly 2.

- [ ] **Step 6: Commit**
```bash
git add internal/search/
git commit -m "feat(search): Search interface + GrepSearch recall stub (line ranges)"
```

---

### Task 3: Toolset (list/read/recall/edit)

**Files:**
- Create: `internal/agent/tools.go`
- Test: `internal/agent/tools_test.go`

**Design:** `Toolset` is stateless — `Dispatch` routes a ToolCall by name and returns a ToolResult. Dirty tracking lives in the Session (it knows when it dispatched a successful `edit`), so the Toolset holds no mutable state beyond its bindings (workdir, agentID, Search). Path inputs are validated to stay within `workdir`.

- [ ] **Step 1: Write the failing test** — `internal/agent/tools_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/agent/ -run 'Tool|List|Read|Edit|Recall|Unknown'`
Expected: compile failure (NewToolset/Toolset undefined).

- [ ] **Step 3: Write the toolset** — `internal/agent/tools.go`:
```go
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
	start, end := intInput(in, "start"), intInput(in, "end")
	if start <= 0 && end <= 0 {
		return okResult(callID, string(data))
	}
	all := strings.Split(string(data), "\n")
	if start <= 0 {
		start = 1
	}
	if end <= 0 || end > len(all) {
		end = len(all)
	}
	if start > len(all) {
		return okResult(callID, "")
	}
	return okResult(callID, strings.Join(all[start-1:end], "\n"))
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/agent/ -run 'Tool|List|Read|Edit|Recall|Unknown'`
Expected: PASS (8 cases).

- [ ] **Step 5: Commit**
```bash
git add internal/agent/tools.go internal/agent/tools_test.go
git commit -m "feat(agent): memory Toolset (list/read/recall/edit) with path-escape guards"
```

---

### Task 4: Session (turn loop + commit-if-dirty + bounded CAS retry)

**Files:**
- Create: `internal/agent/session.go`
- Test: `internal/agent/session_test.go`

**Design:** `Session` holds chat history + workdir + HEAD + dirty + maxSteps. `Send` runs the loop in the §3.5 sequence diagram. The Session sets `dirty=true` when it dispatches a successful `edit` ToolCall. After the loop, if dirty, it commits via `CommitWithCAS` with a bounded (1) retry on `ErrCASConflict` that re-resolves HEAD and retries with the same workdir (safe under single-writer; multi-writer is L5). Tests build a Session directly (Router is Task 5).

- [ ] **Step 1: Write the failing test** — `internal/agent/session_test.go`:
```go
package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

// sessionFixture creates an agent, materializes its HEAD into a workdir, and
// returns a Session wired to a FakeProvider with the given script.
func sessionFixture(t *testing.T, steps []func(Request) Response) (*Session, *memstore.Store, string) {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
	if err := refs.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	store := memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool))
	head, err := store.CreateAgent(ctx, "a1", map[string]string{"system/about.md": "---\ndescription: who\n---\nresident\n"})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	workdir := t.TempDir()
	if err := store.Materialize(ctx, "a1", head, workdir); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	tools := NewToolset(workdir, "a1", search.NewGrep(workdir))
	s := NewSession(store, &FakeProvider{Steps: steps}, tools, "a1", head, workdir, nil)
	return s, store, workdir
}

func TestSendEditCommitsAndAdvancesHead(t *testing.T) {
	ctx := context.Background()
	s, store, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "edit", Input: map[string]any{"path": "note.md", "content": "hello\n"}}}}
		},
		func(r Request) Response { return Response{Text: "saved your note"} },
	})
	startHead := s.Head()

	out, err := s.Send(ctx, "remember hello")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if out != "saved your note" {
		t.Fatalf("assistant text = %q", out)
	}
	if s.Head() == startHead {
		t.Fatal("HEAD did not advance after an edit")
	}
	// The edit must be in the new HEAD's tree.
	check := t.TempDir()
	if err := store.Materialize(ctx, "a1", s.Head(), check); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(check, "note.md"))
	if err != nil || string(got) != "hello\n" {
		t.Fatalf("committed note = %q %v", got, err)
	}
}

func TestSendReadOnlyDoesNotCommit(t *testing.T) {
	ctx := context.Background()
	s, _, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "read", Input: map[string]any{"path": "system/about.md"}}}}
		},
		func(r Request) Response { return Response{Text: "it says: resident"} },
	})
	startHead := s.Head()
	if _, err := s.Send(ctx, "what do you know?"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if s.Head() != startHead {
		t.Fatal("read-only turn must not advance HEAD")
	}
}

func TestSendMultiTurnAccumulatesHistory(t *testing.T) {
	ctx := context.Background()
	s, _, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response { return Response{Text: "hi"} },                 // turn 1
		func(r Request) Response {                                                  // turn 2: must see prior history
			return Response{Text: "you said hi back"}
		},
	})
	if _, err := s.Send(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	// Second turn's request must contain the first turn's messages.
	var sawFirstUser bool
	s2 := s // capture
	_ = s2
	prov := s.providerForTest()
	_ = prov
	// Simpler: assert via history length after turn 2.
	if _, err := s.Send(ctx, "again"); err != nil {
		t.Fatal(err)
	}
	if len(s.History()) < 4 {
		t.Fatalf("history should accumulate across turns, got %d entries", len(s.History()))
	}
	_ = sawFirstUser
}

func TestSendCASConflictRetries(t *testing.T) {
	ctx := context.Background()
	s, store, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "edit", Input: map[string]any{"path": "n.md", "content": "v\n"}}}}
		},
		func(r Request) Response { return Response{Text: "ok"} },
	})
	// Externally advance HEAD behind the session's back (simulating a rogue
	// second writer; impossible under the Router lock, but exercises the retry).
	other := t.TempDir()
	if err := store.Materialize(ctx, "a1", s.Head(), other); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "external.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitWithCAS(ctx, "a1", s.Head(), other, nil); err != nil {
		t.Fatalf("external commit: %v", err)
	}
	// Now the session commits against a stale parent → conflict → bounded retry.
	if _, err := s.Send(ctx, "save"); err != nil {
		t.Fatalf("send should recover via retry: %v", err)
	}
}
```

> Note for the implementer: the `TestSendMultiTurnAccumulatesHistory` test above references helper methods `s.History()`, `s.Head()`, and a `s.providerForTest()` stub line that is dead code — DELETE the `prov := s.providerForTest()` and `_ = prov` and `s2`/`sawFirstUser` scaffolding lines; keep only the two `Send` calls and the `len(s.History()) < 4` assertion. Implement exported `Head() memstore.CommitHash` and `History() []Message` accessors on Session (used by tests). Do NOT add `providerForTest`.

- [ ] **Step 2: Run test to verify it fails**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Send`
Expected: compile failure (NewSession/Session/Head/History undefined).

- [ ] **Step 3: Write the session** — `internal/agent/session.go`:
```go
package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ssy/engram/internal/memstore"
)

const defaultMaxSteps = 16

// Session is a stateful, single-writer agent conversation over a materialized
// working tree. Chat history is ephemeral (lives for the session); the durable
// state is the memory repo, advanced by CommitWithCAS on dirty turns.
type Session struct {
	agentID  string
	store    memstore.MemStore
	prov     LLMProvider
	tools    *Toolset
	head     memstore.CommitHash
	workdir  string
	history  []Message
	dirty    bool
	maxSteps int
	release  func() // called by Close; nil for direct (test) construction
}

// NewSession wires a session. release (may be nil) is invoked by Close to free
// the writer lock and clean the workdir; the Router supplies it.
func NewSession(store memstore.MemStore, prov LLMProvider, tools *Toolset, agentID string, head memstore.CommitHash, workdir string, release func()) *Session {
	return &Session{
		agentID:  agentID,
		store:    store,
		prov:     prov,
		tools:    tools,
		head:     head,
		workdir:  workdir,
		maxSteps: defaultMaxSteps,
	}
}

func (s *Session) Head() memstore.CommitHash { return s.head }
func (s *Session) History() []Message        { return s.history }

// Close frees the workdir and (if set) the writer lock.
func (s *Session) Close() error {
	if s.release != nil {
		s.release()
	}
	return nil
}

// Send runs one turn: the model may call tools (recall/read/edit) until it
// returns final text; a turn that edited memory is committed.
func (s *Session) Send(ctx context.Context, userMessage string) (string, error) {
	s.history = append(s.history, Message{Role: RoleUser, Text: userMessage})

	final := ""
	for step := 0; step < s.maxSteps; step++ {
		sys, err := s.assembleSystem()
		if err != nil {
			return "", fmt.Errorf("agent: assemble system: %w", err)
		}
		resp, err := s.prov.Generate(ctx, Request{System: sys, Messages: s.history, Tools: s.tools.Defs()})
		if err != nil {
			return "", fmt.Errorf("agent: generate: %w", err)
		}
		if len(resp.ToolCalls) == 0 {
			final = resp.Text
			s.history = append(s.history, Message{Role: RoleAssistant, Text: resp.Text})
			break
		}
		s.history = append(s.history, Message{Role: RoleAssistant, ToolCalls: resp.ToolCalls})
		results := make([]ToolResult, 0, len(resp.ToolCalls))
		for _, call := range resp.ToolCalls {
			res := s.tools.Dispatch(ctx, call)
			if call.Name == "edit" && !res.IsError {
				s.dirty = true
			}
			results = append(results, res)
		}
		s.history = append(s.history, Message{Role: RoleTool, Results: results})
		if step == s.maxSteps-1 {
			return "", fmt.Errorf("agent: tool-use loop exceeded maxSteps=%d", s.maxSteps)
		}
	}

	if s.dirty {
		if err := s.commit(ctx); err != nil {
			return "", err
		}
	}
	return final, nil
}

// commit persists the workdir, advancing HEAD. On a CAS conflict (which cannot
// happen under the single-writer Router, but is handled defensively) it
// re-resolves HEAD and retries once with the same workdir.
func (s *Session) commit(ctx context.Context) error {
	jobs := []memstore.Job{{Kind: "reindex"}, {Kind: "reflect"}}
	newHead, err := s.store.CommitWithCAS(ctx, s.agentID, s.head, s.workdir, jobs)
	if errors.Is(err, memstore.ErrCASConflict) {
		cur, rerr := s.store.ResolveHead(ctx, s.agentID)
		if rerr != nil {
			return fmt.Errorf("agent: resolve after CAS conflict: %w", rerr)
		}
		s.head = cur
		newHead, err = s.store.CommitWithCAS(ctx, s.agentID, s.head, s.workdir, jobs)
	}
	if err != nil {
		return fmt.Errorf("agent: commit: %w", err)
	}
	s.head = newHead
	s.dirty = false
	return nil
}

// assembleSystem builds the resident system prompt: all system/ file contents
// plus a tree index (path: description) of the whole memory tree.
func (s *Session) assembleSystem() (string, error) {
	var b strings.Builder
	b.WriteString("# Resident memory (system/)\n")
	systemDir := filepath.Join(s.workdir, "system")
	_ = filepath.WalkDir(systemDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // system/ may not exist yet
		}
		if d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		rel, _ := filepath.Rel(s.workdir, path)
		fmt.Fprintf(&b, "\n## %s\n%s\n", rel, string(data))
		return nil
	})

	b.WriteString("\n# Memory tree index (path: description)\n")
	var idx []string
	err := filepath.WalkDir(s.workdir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(s.workdir, path)
		idx = append(idx, fmt.Sprintf("%s: %s", rel, frontmatterDescription(path)))
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(idx)
	b.WriteString(strings.Join(idx, "\n"))
	return b.String(), nil
}

// (frontmatterDescription is defined in tools.go and reused here.)
var _ = bufio.ScanLines
```

> Note: `assembleSystem` reuses `frontmatterDescription` from `tools.go` (same package). The `var _ = bufio.ScanLines` line is only to keep the `bufio` import if the implementer's gofmt drops it — REMOVE the `bufio` import and that line if `bufio` ends up unused (it likely is; prefer removing both). Keep imports clean per `go vet`/gofmt.

- [ ] **Step 4: Run test to verify it passes**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Send -v`
Expected: PASS (4 Send* cases). Also run the full package: `ENGRAM_TEST_DB=... go test ./internal/agent/`.

- [ ] **Step 5: Commit**
```bash
git add internal/agent/session.go internal/agent/session_test.go
git commit -m "feat(agent): stateful Session turn loop with commit-if-dirty and bounded CAS retry"
```

---

### Task 5: Router (single-writer admission)

**Files:**
- Create: `internal/agent/router.go`
- Test: `internal/agent/router_test.go`

**Design:** `Router` enforces one active writer Session per agent via an in-process set guarded by a mutex. `Open` claims the agent, resolves HEAD, materializes a fresh workdir under `scratch`, builds a per-session `GrepSearch(workdir)` + Toolset + Session (passing a release closure). `Close` (via the Session) frees the claim and removes the workdir.

- [ ] **Step 1: Write the failing test** — `internal/agent/router_test.go`:
```go
package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

func routerFixture(t *testing.T) (*Router, *memstore.Store) {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
	if err := refs.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	store := memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool))
	if _, err := store.CreateAgent(ctx, "a1", map[string]string{"system/about.md": "---\ndescription: who\n---\nx\n"}); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	prov := &FakeProvider{Steps: []func(Request) Response{func(r Request) Response { return Response{Text: "ok"} }}}
	return NewRouter(store, prov, t.TempDir()), store
}

func TestRouterOpenMaterializesWorkdir(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if _, err := os.Stat(filepath.Join(s.workdir, "system", "about.md")); err != nil {
		t.Fatalf("workdir not materialized: %v", err)
	}
}

func TestRouterSingleWriter(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s1, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(ctx, "a1"); !errors.Is(err, ErrAgentBusy) {
		t.Fatalf("second Open = %v want ErrAgentBusy", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := r.Open(ctx, "a1") // available again after Close
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	s2.Close()
}

func TestRouterCloseRemovesWorkdir(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	wd := s.workdir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(wd); !os.IsNotExist(err) {
		t.Fatalf("workdir should be removed after Close, stat err = %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Router`
Expected: compile failure (NewRouter/Router/ErrAgentBusy undefined).

- [ ] **Step 3: Write the router** — `internal/agent/router.go`:
```go
package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/search"
)

// ErrAgentBusy is returned by Open when the agent already has an active session.
var ErrAgentBusy = errors.New("agent: agent already has an active session")

// Router enforces single-writer-per-agent: at most one active Session per
// agent_id (an in-process lock that rebuilds the serialization a single client
// would provide). Multi-pod sticky routing is L5; the ref CAS is the backstop.
type Router struct {
	store   memstore.MemStore
	prov    LLMProvider
	scratch string

	mu     sync.Mutex
	active map[string]bool
}

func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, active: map[string]bool{}}
}

func (r *Router) claim(agentID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[agentID] {
		return false
	}
	r.active[agentID] = true
	return true
}

func (r *Router) free(agentID string) {
	r.mu.Lock()
	delete(r.active, agentID)
	r.mu.Unlock()
}

// Open acquires the agent's writer slot, materializes HEAD into a fresh workdir,
// and returns a Session. Returns ErrAgentBusy if a session is already active.
func (r *Router) Open(ctx context.Context, agentID string) (*Session, error) {
	if !r.claim(agentID) {
		return nil, ErrAgentBusy
	}
	head, err := r.store.ResolveHead(ctx, agentID)
	if err != nil {
		r.free(agentID)
		return nil, fmt.Errorf("agent: resolve head: %w", err)
	}
	workdir, err := os.MkdirTemp(r.scratch, agentID+"-*")
	if err != nil {
		r.free(agentID)
		return nil, fmt.Errorf("agent: scratch dir: %w", err)
	}
	if err := r.store.Materialize(ctx, agentID, head, workdir); err != nil {
		os.RemoveAll(workdir)
		r.free(agentID)
		return nil, fmt.Errorf("agent: materialize: %w", err)
	}
	tools := NewToolset(workdir, agentID, search.NewGrep(workdir))
	release := func() {
		os.RemoveAll(workdir)
		r.free(agentID)
	}
	return NewSession(r.store, r.prov, tools, agentID, head, workdir, release), nil
}
```

> Note: `NewSession` ignores the `release` param in Task 4's struct literal (it sets fields explicitly and does not store release). FIX in this task: update `NewSession` in `session.go` to store `release: release` in the returned `&Session{...}`. The Task 4 struct already has the `release` field and `Close` already calls it; only the constructor assignment was omitted. Add `release: release,` to the `NewSession` body.

- [ ] **Step 4: Run test to verify it passes**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Router -v`
Expected: PASS (3 cases). Then full package: `ENGRAM_TEST_DB=... go test ./internal/agent/ -count=1`.

- [ ] **Step 5: Commit**
```bash
git add internal/agent/router.go internal/agent/router_test.go internal/agent/session.go
git commit -m "feat(agent): single-writer Router (Open/Close, materialize, ErrAgentBusy)"
```

---

### Task 6: AnthropicProvider (Messages API + tool-use mapping)

**Files:**
- Create: `internal/agent/anthropic.go`
- Test: `internal/agent/anthropic_test.go`

**Design:** maps the provider-agnostic Request to the Anthropic Messages API and back. HTTP client is injectable so tests use a mock `RoundTripper` (no real API). Default model `claude-sonnet-4-6`, `max_tokens` 4096, `anthropic-version: 2023-06-01`.

- [ ] **Step 1: Write the failing test** — `internal/agent/anthropic_test.go`:
```go
package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestAnthropicBuildsRequestAndParsesToolUse(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("x-api-key") != "k" || r.Header.Get("anthropic-version") == "" {
			t.Fatalf("missing headers: %v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return jsonResp(200, `{"content":[{"type":"tool_use","id":"tu_1","name":"recall","input":{"query":"x"}}],"stop_reason":"tool_use"}`), nil
	})}
	p := NewAnthropic("k", WithModel("claude-sonnet-4-6"), WithHTTPClient(client))

	resp, err := p.Generate(context.Background(), Request{
		System:   "SYS",
		Messages: []Message{{Role: RoleUser, Text: "hi"}},
		Tools:    []ToolDef{{Name: "recall", Description: "d", InputSchema: map[string]any{"type": "object"}}},
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if captured["model"] != "claude-sonnet-4-6" || captured["system"] != "SYS" {
		t.Fatalf("request body wrong: %v", captured)
	}
	if _, ok := captured["max_tokens"]; !ok {
		t.Fatal("max_tokens missing")
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "recall" || resp.ToolCalls[0].ID != "tu_1" {
		t.Fatalf("parsed tool calls wrong: %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Input["query"] != "x" {
		t.Fatalf("tool input wrong: %+v", resp.ToolCalls[0].Input)
	}
}

func TestAnthropicParsesFinalText(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"content":[{"type":"text","text":"all done"}],"stop_reason":"end_turn"}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	resp, err := p.Generate(context.Background(), Request{Messages: []Message{{Role: RoleUser, Text: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "all done" || len(resp.ToolCalls) != 0 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestAnthropicMapsToolResultMessages(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return jsonResp(200, `{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	_, err := p.Generate(context.Background(), Request{Messages: []Message{
		{Role: RoleUser, Text: "do it"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "tu_1", Name: "edit", Input: map[string]any{"path": "a"}}}},
		{Role: RoleTool, Results: []ToolResult{{CallID: "tu_1", Content: "wrote a"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	msgs, _ := captured["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("want 3 mapped messages, got %d: %v", len(msgs), captured["messages"])
	}
	// The tool-result message must be a user message carrying a tool_result block.
	last, _ := msgs[2].(map[string]any)
	if last["role"] != "user" {
		t.Fatalf("tool result should map to a user message, got %v", last["role"])
	}
}

func TestAnthropicNon2xxErrors(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `{"error":{"message":"rate limited"}}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	if _, err := p.Generate(context.Background(), Request{Messages: []Message{{Role: RoleUser, Text: "hi"}}}); err == nil {
		t.Fatal("expected error on 429")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/agent/ -run Anthropic`
Expected: compile failure (NewAnthropic/WithModel/WithHTTPClient undefined).

- [ ] **Step 3: Write the adapter** — `internal/agent/anthropic.go`:
```go
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	defaultAnthropicModel   = "claude-sonnet-4-6"
	defaultAnthropicMaxTok  = 4096
	defaultAnthropicBaseURL = "https://api.anthropic.com"
	anthropicVersion        = "2023-06-01"
)

// AnthropicProvider calls the Anthropic Messages API with tool-use.
type AnthropicProvider struct {
	apiKey    string
	model     string
	maxTokens int
	baseURL   string
	client    *http.Client
}

type AnthropicOption func(*AnthropicProvider)

func WithModel(m string) AnthropicOption      { return func(p *AnthropicProvider) { p.model = m } }
func WithMaxTokens(n int) AnthropicOption     { return func(p *AnthropicProvider) { p.maxTokens = n } }
func WithBaseURL(u string) AnthropicOption    { return func(p *AnthropicProvider) { p.baseURL = u } }
func WithHTTPClient(c *http.Client) AnthropicOption {
	return func(p *AnthropicProvider) { p.client = c }
}

func NewAnthropic(apiKey string, opts ...AnthropicOption) *AnthropicProvider {
	p := &AnthropicProvider{
		apiKey:    apiKey,
		model:     defaultAnthropicModel,
		maxTokens: defaultAnthropicMaxTok,
		baseURL:   defaultAnthropicBaseURL,
		client:    http.DefaultClient,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// --- wire types (Anthropic Messages API subset) ---

type apiTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"input_schema"`
}

type apiBlock struct {
	Type    string         `json:"type"`
	Text    string         `json:"text,omitempty"`
	ID      string         `json:"id,omitempty"`
	Name    string         `json:"name,omitempty"`
	Input   map[string]any `json:"input,omitempty"`
	ToolUse string         `json:"tool_use_id,omitempty"`
	Content string         `json:"content,omitempty"`
	IsError bool           `json:"is_error,omitempty"`
}

type apiMessage struct {
	Role    string     `json:"role"`
	Content []apiBlock `json:"content"`
}

type apiRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	System    string       `json:"system,omitempty"`
	Messages  []apiMessage `json:"messages"`
	Tools     []apiTool    `json:"tools,omitempty"`
}

type apiResponse struct {
	Content    []apiBlock `json:"content"`
	StopReason string     `json:"stop_reason"`
}

func toAPIMessages(msgs []Message) []apiMessage {
	out := make([]apiMessage, 0, len(msgs))
	for _, m := range msgs {
		switch m.Role {
		case RoleUser:
			out = append(out, apiMessage{Role: "user", Content: []apiBlock{{Type: "text", Text: m.Text}}})
		case RoleAssistant:
			var blocks []apiBlock
			if m.Text != "" {
				blocks = append(blocks, apiBlock{Type: "text", Text: m.Text})
			}
			for _, tc := range m.ToolCalls {
				blocks = append(blocks, apiBlock{Type: "tool_use", ID: tc.ID, Name: tc.Name, Input: tc.Input})
			}
			out = append(out, apiMessage{Role: "assistant", Content: blocks})
		case RoleTool:
			var blocks []apiBlock
			for _, r := range m.Results {
				blocks = append(blocks, apiBlock{Type: "tool_result", ToolUse: r.CallID, Content: r.Content, IsError: r.IsError})
			}
			out = append(out, apiMessage{Role: "user", Content: blocks})
		}
	}
	return out
}

func (p *AnthropicProvider) Generate(ctx context.Context, req Request) (Response, error) {
	tools := make([]apiTool, 0, len(req.Tools))
	for _, t := range req.Tools {
		tools = append(tools, apiTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
	}
	body, err := json.Marshal(apiRequest{
		Model:     p.model,
		MaxTokens: p.maxTokens,
		System:    req.System,
		Messages:  toAPIMessages(req.Messages),
		Tools:     tools,
	})
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: new request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	httpResp, err := p.client.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: do: %w", err)
	}
	defer httpResp.Body.Close()
	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: read body: %w", err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return Response{}, fmt.Errorf("anthropic: status %d: %s", httpResp.StatusCode, string(raw))
	}
	var ar apiResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return Response{}, fmt.Errorf("anthropic: unmarshal: %w", err)
	}
	var resp Response
	for _, b := range ar.Content {
		switch b.Type {
		case "text":
			resp.Text += b.Text
		case "tool_use":
			resp.ToolCalls = append(resp.ToolCalls, ToolCall{ID: b.ID, Name: b.Name, Input: b.Input})
		}
	}
	return resp, nil
}

var _ LLMProvider = (*AnthropicProvider)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/agent/ -run Anthropic -v`
Expected: PASS (4 cases).

> Note: the `apiBlock` struct is shared for both request and response encoding; `omitempty` keeps request blocks tidy. If a future Anthropic field is needed it goes here. Confirm `claude-sonnet-4-6` is the intended default model (it is, per project conventions).

- [ ] **Step 5: Commit**
```bash
git add internal/agent/anthropic.go internal/agent/anthropic_test.go
git commit -m "feat(agent): Anthropic Messages API provider with tool-use mapping"
```

---

### Task 7: cmd/api dev wiring

**Files:**
- Create: `cmd/api/main.go`

**Design:** a minimal dev entry that wires ObjStore(local) + refs + MemStore + Router + a provider (fake or anthropic per env), opens a session for one agent, reads user lines from stdin, calls Send, prints the assistant reply, and Closes on EOF. Not an automated test — verified by `go build` and a manual fake-provider run.

- [ ] **Step 1: Write the wiring** — `cmd/api/main.go`:
```go
// Command api is a dev harness for the Engram agent loop: it wires the L1
// MemStore + L2 Router + an LLM provider and runs a single interactive session
// for one agent, reading user messages from stdin. Not for production.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/agent"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	dsn := env("ENGRAM_DB", "postgres://postgres:engram@localhost:5433/engram?sslmode=disable")
	objRoot := env("ENGRAM_OBJ", "./engram-objects")
	agentID := env("ENGRAM_AGENT", "demo")

	if err := refs.Migrate(dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	store := memstore.New(objstore.NewLocal(objRoot), refs.New(pool))
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{
		"system/about.md": "---\ndescription: who this agent is\n---\nYou are a memory-keeping agent.\n",
	}); err != nil && err != memstore.ErrAgentAlreadyExists {
		log.Fatalf("create agent: %v", err)
	}

	var prov agent.LLMProvider
	switch env("ENGRAM_PROVIDER", "fake") {
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			log.Fatal("ENGRAM_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
		}
		prov = agent.NewAnthropic(key)
	default:
		prov = &agent.FakeProvider{Steps: []func(agent.Request) agent.Response{
			func(r agent.Request) agent.Response { return agent.Response{Text: "(fake) received your message"} },
		}}
	}

	router := agent.NewRouter(store, prov, os.TempDir())
	sess, err := router.Open(ctx, agentID)
	if err != nil {
		log.Fatalf("open session: %v", err)
	}
	defer sess.Close()

	fmt.Printf("engram session for agent %q (provider=%s). Type a message, Ctrl-D to exit.\n", agentID, env("ENGRAM_PROVIDER", "fake"))
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}
		reply, err := sess.Send(ctx, sc.Text())
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		fmt.Printf("%s\n", reply)
	}
}
```

- [ ] **Step 2: Build and smoke-test**

Run:
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected: build OK, vet clean, gofmt lists nothing.

Manual smoke (fake provider, needs the live PG):
```bash
echo "hello" | ENGRAM_PROVIDER=fake ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" ENGRAM_OBJ="$(mktemp -d)" ENGRAM_AGENT=smoke go run ./cmd/api
```
Expected: prints the session banner and `(fake) received your message`.

- [ ] **Step 3: Commit**
```bash
git add cmd/api/main.go
git commit -m "feat(cmd/api): dev harness wiring MemStore + Router + provider"
```

- [ ] **Step 4: Full suite green**

Run:
```bash
go build ./...
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1
```
Expected: all packages pass (objstore, gitfs, refs, memstore, agent, search).

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-10-engram-l2-agent-loop-design.md`):**
- §3.1 LLMProvider protocol → Task 1 ✅
- §3.2 FakeProvider → Task 1 ✅
- §3.3 AnthropicProvider (mapping, mock-HTTP tests, default model/max_tokens, tool_result mapping) → Task 6 ✅
- §3.4 Toolset list/read/recall/edit + path guards → Task 3 ✅
- §3.5 Session turn loop + commit-if-dirty + assembleSystem → Task 4 ✅
- §3.6 Router single-writer + Open/Close + ErrAgentBusy → Task 5 ✅
- §3.7 Search interface + GrepSearch stub → Task 2 ✅
- §3.8 cmd/api dev wiring → Task 7 ✅
- §4 error handling: `%w`, ErrCASConflict bounded retry, ErrAgentBusy, maxSteps cap, anthropic non-2xx → Tasks 4/5/6 ✅
- §5 tests: provider round-trip, anthropic mock-HTTP, toolset, grep, Session e2e (edit-commits / read-only-no-commit / multi-turn / CAS-retry), Router single-writer → Tasks 1–6 ✅
- §6 DoD: Router→Session→multi-turn tool-use→diffable commits, fake-driven auto tests, anthropic mock-tested + cmd/api manual → covered ✅

**Placeholder scan:** No TBD/TODO. Two deliberately-flagged cleanups for the implementer, each with explicit instructions: (a) Task 4 test has dead scaffolding lines (`providerForTest`/`s2`/`sawFirstUser`) the note says to delete, keeping only the two Send calls + `len(History())<4` assertion; (b) Task 4's `var _ = bufio.ScanLines` + `bufio` import are to be removed if unused. (c) Task 5 note: add `release: release,` to `NewSession`'s struct literal (the field/Close already exist from Task 4). These are real instructions, not vague placeholders.

**Type consistency:** `Request/Response/Message/ToolDef/ToolCall/ToolResult/Role` defined in Task 1 and used identically in Tasks 3/4/6. `Session` fields (`head`, `workdir`, `dirty`, `release`, `maxSteps`) defined in Task 4; Task 5 references `s.workdir` (test) and sets `release` via constructor. `memstore.MemStore/CommitHash/Job/ErrCASConflict/ErrAgentAlreadyExists` are the real L1 exports (verified against merged L1). `search.Search/Hit/NewGrep` defined Task 2, consumed Tasks 3/5. `NewToolset(workdir, agentID, search.Search)` consistent across Tasks 3/4/5. `NewSession(store, prov, tools, agentID, head, workdir, release)` consistent Tasks 4/5. `NewRouter(store, prov, scratch)` / `NewAnthropic(apiKey, ...opts)` consistent with cmd/api (Task 7).

**Known convergence points (TDD resolves; not placeholders):**
1. `filepath.SkipAll` (Go 1.20+) availability — fine on 1.25.
2. Exact Anthropic JSON field names for `tool_result` (`tool_use_id`, `content`, `is_error`) — the test asserts the mapped message role is `user`; if the real API needs `content` as an array of blocks rather than a string, adjust `apiBlock` for tool_result (the mock test only checks role + count, so the unit tests pass either way; the real-API shape is validated manually via cmd/api). Implementer: keep the string form unless the live API rejects it, then switch tool_result `content` to `[]apiBlock{{Type:"text",Text:...}}`.
3. `claude-sonnet-4-6` as default model id — confirmed per project conventions.
