# Engram L4 — Hybrid Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the L2 GrepSearch with a hybrid recall: a line-level trigram inverted index (variant A) fused via Reciprocal Rank Fusion with semantic section retrieval (Voyage embeddings, content-addressed cache), degrading to trigram-only when embeddings are unavailable.

**Architecture:** `internal/search` gains `TrigramIndex` (lexical), `Embedder` (interface + `FakeEmbedder` + `VoyageEmbedder`), `SemanticIndex` (markdown-section chunks + brute-force cosine, vectors cached content-addressed in the L3 `cache.Cache`), and `HybridSearch` (implements `Search`, RRF-fuses the two). The Router builds a per-session `HybridSearch` from the materialized workdir; the `Search` interface and all callers (recall tool / Session) are unchanged.

**Tech Stack:** Go 1.25 stdlib (`hash/fnv`, `crypto/sha256`, `encoding/binary`, `encoding/base64`, `math`, `net/http`, `sort`, `strings`); L3 `internal/cache`; Voyage AI embeddings HTTP API (behind the `Embedder` interface, mock-HTTP in tests).

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` is the first arg on every I/O path; wrap errors with `%w`.
- No real external API calls in tests — use `FakeEmbedder` and a mock `http.RoundTripper`.
- Dependency-light: trigram / cosine / RRF are hand-written over the stdlib. The only new external dependency is the Voyage HTTP endpoint, reached solely through the `Embedder` interface.
- `Hit` (from `internal/search/search.go`, L2): `Hit{Path string; LineStart, LineEnd int; Snippet string}` — line numbers 1-based inclusive.
- `Search` interface (unchanged): `Recall(ctx, agentID, query string, k int) ([]Hit, error)` + `Reindex(ctx, agentID, from, to string) error`.
- `cache.Cache` (L3): `Get(key string) (string, bool)` / `Put(key, val string)`.
- Recall is agentic / mid-loop and returns sub-file line ranges — never whole files, never a pre-inference dump.

**Prerequisites:** On `main` (L1–L3 merged). Branch before Task 1: `git checkout -b feat/l4-hybrid-search`. Tasks 1–5 need NO database (pure `internal/search`, Fake/mock-HTTP). Task 6's Router tests need `ENGRAM_TEST_DB`:
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout:**
```
internal/search/trigram.go      # NEW: TrigramIndex (lexical)
internal/search/embedder.go     # NEW: Embedder interface + FakeEmbedder
internal/search/voyage.go       # NEW: VoyageEmbedder (HTTP)
internal/search/semantic.go     # NEW: SemanticIndex (sections + cosine + emb cache)
internal/search/hybrid.go       # NEW: HybridSearch (Search impl, RRF)
internal/agent/router.go        # MODIFY: emb field, build HybridSearch in Open
internal/agent/router_test.go   # MODIFY: routerFixture passes FakeEmbedder; assert wiring
cmd/api/main.go                 # MODIFY: construct embedder by env, pass to NewRouter
```
**Dependency order:** trigram (1) → embedder+fake (2) → voyage (3) → semantic (4, uses 2 + cache) → hybrid (5, uses 1+4) → router/cmd wiring (6).

---

### Task 1: TrigramIndex (lexical, variant A)

**Files:**
- Create: `internal/search/trigram.go`
- Test: `internal/search/trigram_test.go`

**Interfaces:**
- Consumes: `Hit` (search.go, existing).
- Produces: `BuildTrigram(files map[string][]byte) *TrigramIndex`; `(*TrigramIndex).Search(query string, k int) []Hit`.

- [ ] **Step 1: Write the failing test** — `internal/search/trigram_test.go`:
```go
package search

import "testing"

func TestTrigramFindsLine(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{
		"a.md": []byte("first line\nhas needle here\nlast\n"),
		"b.md": []byte("nothing\n"),
	})
	hits := idx.Search("needle", 10)
	if len(hits) != 1 || hits[0].Path != "a.md" || hits[0].LineStart != 2 || hits[0].LineEnd != 2 {
		t.Fatalf("hits = %+v", hits)
	}
}

func TestTrigramShortQueryFallback(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("ab\nxy\nab cd\n")})
	hits := idx.Search("ab", 10) // <3 chars: no trigrams -> scan all lines
	if len(hits) != 2 {
		t.Fatalf("short-query fallback hits = %+v", hits)
	}
}

func TestTrigramNoMatch(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("abc\n")})
	if hits := idx.Search("zzz", 10); len(hits) != 0 {
		t.Fatalf("want no hits, got %+v", hits)
	}
}

func TestTrigramKTruncation(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("needle\nneedle\nneedle\n")})
	if hits := idx.Search("needle", 2); len(hits) != 2 {
		t.Fatalf("k truncation: got %d", len(hits))
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/search/ -run Trigram` → compile failure (BuildTrigram undefined).

- [ ] **Step 3: Implement** `internal/search/trigram.go`:
```go
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
	q := strings.ToLower(query)
	var matches []posting
	for p := range t.candidates(query) {
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
```

- [ ] **Step 4: Run** `go test ./internal/search/ -run Trigram -v` → 4 PASS. `gofmt -l internal/search/`, `go vet ./internal/search/` clean.

- [ ] **Step 5: Commit**
```bash
git add internal/search/trigram.go internal/search/trigram_test.go
git commit -m "feat(search): line-level trigram inverted index (variant A)"
```

---

### Task 2: Embedder interface + FakeEmbedder

**Files:**
- Create: `internal/search/embedder.go`
- Test: `internal/search/embedder_test.go`

**Interfaces:**
- Produces: `Embedder` interface { `Embed(ctx context.Context, texts []string) ([][]float32, error)`; `Model() string` }; `NewFakeEmbedder(dim int) *FakeEmbedder` (implements `Embedder`).

- [ ] **Step 1: Write the failing test** — `internal/search/embedder_test.go`:
```go
package search

import (
	"context"
	"math"
	"reflect"
	"testing"
)

// local cosine helper for this test (the package cosine lands in Task 4).
func tcos(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func TestFakeEmbedderDeterministic(t *testing.T) {
	e := NewFakeEmbedder(64)
	a, _ := e.Embed(context.Background(), []string{"hello world"})
	b, _ := e.Embed(context.Background(), []string{"hello world"})
	if !reflect.DeepEqual(a[0], b[0]) {
		t.Fatal("FakeEmbedder must be deterministic")
	}
}

func TestFakeEmbedderSharedWordsRankHigher(t *testing.T) {
	e := NewFakeEmbedder(256)
	vs, _ := e.Embed(context.Background(), []string{
		"user authentication login",
		"authentication flow for users",
		"banana bread recipe",
	})
	q, near, far := vs[0], vs[1], vs[2]
	if tcos(q, near) <= tcos(q, far) {
		t.Fatalf("shared-word text should score higher: near=%v far=%v", tcos(q, near), tcos(q, far))
	}
}

func TestFakeEmbedderModel(t *testing.T) {
	if NewFakeEmbedder(0).Model() != "fake" {
		t.Fatal("Model() should be \"fake\"")
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/search/ -run FakeEmbedder` → compile failure.

- [ ] **Step 3: Implement** `internal/search/embedder.go`:
```go
package search

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
)

// Embedder turns texts into vectors. Model() identifies the model so embedding
// cache keys never mix vectors from different models.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Model() string
}

// FakeEmbedder is a deterministic hashed bag-of-words embedder for tests: texts
// sharing words get higher cosine similarity. Not for production use.
type FakeEmbedder struct{ dim int }

func NewFakeEmbedder(dim int) *FakeEmbedder {
	if dim <= 0 {
		dim = 256
	}
	return &FakeEmbedder{dim: dim}
}

func (f *FakeEmbedder) Model() string { return "fake" }

func (f *FakeEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := make([]float32, f.dim)
		for _, w := range strings.Fields(strings.ToLower(t)) {
			h := fnv.New32a()
			_, _ = h.Write([]byte(w))
			v[h.Sum32()%uint32(f.dim)]++
		}
		var norm float64
		for _, x := range v {
			norm += float64(x) * float64(x)
		}
		if norm > 0 {
			n := float32(math.Sqrt(norm))
			for j := range v {
				v[j] /= n
			}
		}
		out[i] = v
	}
	return out, nil
}

var _ Embedder = (*FakeEmbedder)(nil)
```

- [ ] **Step 4: Run** `go test ./internal/search/ -run FakeEmbedder -v` → 3 PASS; gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/search/embedder.go internal/search/embedder_test.go
git commit -m "feat(search): Embedder interface + deterministic FakeEmbedder"
```

---

### Task 3: VoyageEmbedder (HTTP)

**Files:**
- Create: `internal/search/voyage.go`
- Test: `internal/search/voyage_test.go`

**Interfaces:**
- Consumes: `Embedder` (Task 2).
- Produces: `NewVoyage(apiKey string, opts ...VoyageOption) *VoyageEmbedder` (implements `Embedder`); options `WithVoyageModel(string)`, `WithVoyageBaseURL(string)`, `WithVoyageHTTPClient(*http.Client)`.

- [ ] **Step 1: Write the failing test** — `internal/search/voyage_test.go`:
```go
package search

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func httpResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestVoyageBuildsRequestAndParses(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("authorization") != "Bearer k" {
			t.Fatalf("missing/bad auth header: %q", r.Header.Get("authorization"))
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/embeddings") {
			t.Fatalf("path = %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return httpResp(200, `{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}]}`), nil
	})}
	v := NewVoyage("k", WithVoyageModel("voyage-3"), WithVoyageHTTPClient(client))

	vecs, err := v.Embed(context.Background(), []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if captured["model"] != "voyage-3" {
		t.Fatalf("model = %v", captured["model"])
	}
	if len(vecs) != 2 || len(vecs[0]) != 2 || vecs[1][1] != 0.4 {
		t.Fatalf("parsed vecs = %v", vecs)
	}
}

func TestVoyageNon2xxErrors(t *testing.T) {
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(429, `{"error":"rate limited"}`), nil
	})}
	v := NewVoyage("k", WithVoyageHTTPClient(client))
	if _, err := v.Embed(context.Background(), []string{"a"}); err == nil {
		t.Fatal("expected error on 429")
	}
}

func TestVoyageCountMismatchErrors(t *testing.T) {
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(200, `{"data":[{"embedding":[0.1]}]}`), nil // 1 for 2 inputs
	})}
	v := NewVoyage("k", WithVoyageHTTPClient(client))
	if _, err := v.Embed(context.Background(), []string{"a", "b"}); err == nil {
		t.Fatal("expected error on embedding count mismatch")
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/search/ -run Voyage` → compile failure.

- [ ] **Step 3: Implement** `internal/search/voyage.go`:
```go
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	defaultVoyageModel   = "voyage-3"
	defaultVoyageBaseURL = "https://api.voyageai.com"
)

// VoyageEmbedder calls the Voyage AI embeddings API.
type VoyageEmbedder struct {
	apiKey  string
	model   string
	baseURL string
	client  *http.Client
}

type VoyageOption func(*VoyageEmbedder)

func WithVoyageModel(m string) VoyageOption        { return func(v *VoyageEmbedder) { v.model = m } }
func WithVoyageBaseURL(u string) VoyageOption      { return func(v *VoyageEmbedder) { v.baseURL = u } }
func WithVoyageHTTPClient(c *http.Client) VoyageOption {
	return func(v *VoyageEmbedder) { v.client = c }
}

func NewVoyage(apiKey string, opts ...VoyageOption) *VoyageEmbedder {
	v := &VoyageEmbedder{apiKey: apiKey, model: defaultVoyageModel, baseURL: defaultVoyageBaseURL, client: http.DefaultClient}
	for _, o := range opts {
		o(v)
	}
	return v
}

func (v *VoyageEmbedder) Model() string { return v.model }

type voyageReq struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type voyageResp struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (v *VoyageEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(voyageReq{Model: v.model, Input: texts})
	if err != nil {
		return nil, fmt.Errorf("voyage: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("voyage: new request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+v.apiKey)

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("voyage: do: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("voyage: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("voyage: status %d: %s", resp.StatusCode, string(raw))
	}
	var vr voyageResp
	if err := json.Unmarshal(raw, &vr); err != nil {
		return nil, fmt.Errorf("voyage: unmarshal: %w", err)
	}
	out := make([][]float32, len(vr.Data))
	for i, d := range vr.Data {
		out[i] = d.Embedding
	}
	if len(out) != len(texts) {
		return nil, fmt.Errorf("voyage: got %d embeddings for %d texts", len(out), len(texts))
	}
	return out, nil
}

var _ Embedder = (*VoyageEmbedder)(nil)
```

- [ ] **Step 4: Run** `go test ./internal/search/ -run Voyage -v` → 3 PASS; gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/search/voyage.go internal/search/voyage_test.go
git commit -m "feat(search): Voyage AI embedder (HTTP, mock-tested)"
```

---

### Task 4: SemanticIndex (sections + cosine + content-addressed embedding cache)

**Files:**
- Create: `internal/search/semantic.go`
- Test: `internal/search/semantic_test.go`

**Interfaces:**
- Consumes: `Embedder` (Task 2), `cache.Cache` (L3), `Hit`.
- Produces: `BuildSemantic(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) (*SemanticIndex, error)`; `(*SemanticIndex).Search(ctx context.Context, query string, k int) ([]Hit, error)`; package-level `cosine(a, b []float32) float64`.

- [ ] **Step 1: Write the failing test** — `internal/search/semantic_test.go`:
```go
package search

import (
	"context"
	"testing"

	"github.com/ssy/engram/internal/cache"
)

// countEmbedder wraps an Embedder and counts how many texts it embeds.
type countEmbedder struct {
	inner Embedder
	texts int
}

func (c *countEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.texts += len(texts)
	return c.inner.Embed(ctx, texts)
}
func (c *countEmbedder) Model() string { return c.inner.Model() }

func TestSemanticSectionizeAndSearch(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{
		"m.md": []byte("# Auth\nuser authentication and login flow\n# Cooking\nbanana bread recipe steps\n"),
	}
	si, err := BuildSemantic(ctx, NewFakeEmbedder(256), nil, files)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := si.Search(ctx, "authentication login", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "m.md" {
		t.Fatalf("hits = %+v", hits)
	}
	// The Auth section starts at line 1; the Cooking section at line 3.
	if hits[0].LineStart != 1 || hits[0].LineEnd != 2 {
		t.Fatalf("expected the Auth section (lines 1-2), got %+v", hits[0])
	}
}

func TestSemanticEmbeddingCacheReuse(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{"m.md": []byte("# A\nalpha text\n# B\nbeta text\n")}
	c := cache.NewLRU(64)
	spy := &countEmbedder{inner: NewFakeEmbedder(128)}

	if _, err := BuildSemantic(ctx, spy, c, files); err != nil {
		t.Fatal(err)
	}
	first := spy.texts
	if first == 0 {
		t.Fatal("first build should embed the sections")
	}
	// Second build with the SAME cache: every section is a cache hit -> 0 embeds.
	if _, err := BuildSemantic(ctx, spy, c, files); err != nil {
		t.Fatal(err)
	}
	if spy.texts != first {
		t.Fatalf("second build should embed 0 sections (all cached); embedded %d more", spy.texts-first)
	}
}

func TestSemanticNilCacheEmbedsEachBuild(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{"m.md": []byte("# A\nalpha\n")}
	spy := &countEmbedder{inner: NewFakeEmbedder(64)}
	_, _ = BuildSemantic(ctx, spy, nil, files)
	first := spy.texts
	_, _ = BuildSemantic(ctx, spy, nil, files)
	if spy.texts != first*2 {
		t.Fatalf("nil cache must re-embed each build; got %d want %d", spy.texts, first*2)
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/search/ -run Semantic` → compile failure.

- [ ] **Step 3: Implement** `internal/search/semantic.go`:
```go
package search

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/ssy/engram/internal/cache"
)

type chunk struct {
	path      string
	lineStart int // 1-based inclusive
	lineEnd   int
	snippet   string
	vec       []float32
}

// SemanticIndex holds per-section vectors for brute-force cosine retrieval.
type SemanticIndex struct {
	emb    Embedder
	chunks []chunk
}

type section struct {
	start, end int // 1-based inclusive line range
	text       string
}

func isHeading(line string) bool { return strings.HasPrefix(strings.TrimSpace(line), "#") }

// sectionize splits content into markdown sections: each heading line ("#...")
// after line 1 starts a new section; content before the first heading is its
// own section. No heading => one section covering the whole file.
func sectionize(content string) []section {
	lines := strings.Split(content, "\n")
	var secs []section
	start := 0 // 0-based index of current section start
	for i := 1; i < len(lines); i++ {
		if isHeading(lines[i]) {
			secs = append(secs, section{start: start + 1, end: i, text: strings.Join(lines[start:i], "\n")})
			start = i
		}
	}
	secs = append(secs, section{start: start + 1, end: len(lines), text: strings.Join(lines[start:], "\n")})
	return secs
}

func firstNonEmptyLine(text string) string {
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimSpace(l) != "" {
			return l
		}
	}
	return ""
}

func embKey(model, text string) string {
	sum := sha256.Sum256([]byte(model + "\n" + text))
	return "emb:" + model + ":" + base64.RawStdEncoding.EncodeToString(sum[:])
}

func encodeVec(v []float32) string {
	buf := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return base64.RawStdEncoding.EncodeToString(buf)
}

func decodeVec(s string) []float32 {
	buf, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	v := make([]float32, len(buf)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}
	return v
}

func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// BuildSemantic chunks files into sections and fills each section's vector,
// reusing the content-addressed embedding cache (c may be nil). Missing vectors
// are fetched in a single batched Embed call.
func BuildSemantic(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) (*SemanticIndex, error) {
	si := &SemanticIndex{emb: emb}
	type pending struct {
		ci   int
		text string
		key  string
	}
	var pend []pending

	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths) // stable chunk order

	for _, p := range paths {
		for _, sec := range sectionize(string(files[p])) {
			ci := len(si.chunks)
			si.chunks = append(si.chunks, chunk{path: p, lineStart: sec.start, lineEnd: sec.end, snippet: firstNonEmptyLine(sec.text)})
			key := embKey(emb.Model(), sec.text)
			if c != nil {
				if enc, ok := c.Get(key); ok {
					si.chunks[ci].vec = decodeVec(enc)
					continue
				}
			}
			pend = append(pend, pending{ci: ci, text: sec.text, key: key})
		}
	}

	if len(pend) > 0 {
		texts := make([]string, len(pend))
		for i, pp := range pend {
			texts[i] = pp.text
		}
		vecs, err := emb.Embed(ctx, texts)
		if err != nil {
			return nil, fmt.Errorf("search: embed sections: %w", err)
		}
		if len(vecs) != len(pend) {
			return nil, fmt.Errorf("search: embed returned %d vectors for %d sections", len(vecs), len(pend))
		}
		for i, pp := range pend {
			si.chunks[pp.ci].vec = vecs[i]
			if c != nil {
				c.Put(pp.key, encodeVec(vecs[i]))
			}
		}
	}
	return si, nil
}

// Search embeds the query and returns the top-k sections by cosine similarity.
func (s *SemanticIndex) Search(ctx context.Context, query string, k int) ([]Hit, error) {
	if k <= 0 {
		k = 8
	}
	qv, err := s.emb.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("search: embed query: %w", err)
	}
	if len(qv) != 1 {
		return nil, fmt.Errorf("search: embed query returned %d vectors", len(qv))
	}
	q := qv[0]
	idxs := make([]int, len(s.chunks))
	for i := range s.chunks {
		idxs[i] = i
	}
	sort.SliceStable(idxs, func(i, j int) bool {
		return cosine(q, s.chunks[idxs[i]].vec) > cosine(q, s.chunks[idxs[j]].vec)
	})
	var hits []Hit
	for _, ci := range idxs {
		if len(hits) >= k {
			break
		}
		ch := s.chunks[ci]
		hits = append(hits, Hit{Path: ch.path, LineStart: ch.lineStart, LineEnd: ch.lineEnd, Snippet: ch.snippet})
	}
	return hits, nil
}
```

- [ ] **Step 4: Run** `go test ./internal/search/ -run Semantic -v` → 3 PASS. Then the whole package `go test ./internal/search/ -count=1`. gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/search/semantic.go internal/search/semantic_test.go
git commit -m "feat(search): SemanticIndex — markdown sections, cosine, content-addressed embedding cache"
```

---

### Task 5: HybridSearch (RRF fusion + degrade)

**Files:**
- Create: `internal/search/hybrid.go`
- Test: `internal/search/hybrid_test.go`

**Interfaces:**
- Consumes: `BuildTrigram`/`TrigramIndex.Search` (Task 1), `BuildSemantic`/`SemanticIndex.Search` (Task 4), `Embedder` (Task 2), `cache.Cache`, `Hit`, `Search` (existing).
- Produces: `NewHybrid(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) *HybridSearch` (implements `Search`).

- [ ] **Step 1: Write the failing test** — `internal/search/hybrid_test.go`:
```go
package search

import (
	"context"
	"errors"
	"testing"
)

// failingEmbedder always errors (drives the degrade path).
type failingEmbedder struct{}

func (failingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errors.New("embed unavailable")
}
func (failingEmbedder) Model() string { return "failing" }

func TestHybridTrigramExactToken(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, NewFakeEmbedder(256), nil, map[string][]byte{
		"a.md": []byte("config\ntoken=xq7z9\nmore notes\n"),
	})
	hits, err := h.Recall(ctx, "a1", "xq7z9", 5)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, hit := range hits {
		if hit.Path == "a.md" && hit.LineStart == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("exact-token line should be recalled (trigram): %+v", hits)
	}
}

func TestHybridSemanticSurfacesSectionTrigramMisses(t *testing.T) {
	ctx := context.Background()
	// No single line contains the full phrase "alpha beta gamma" -> trigram misses;
	// the section shares all three words -> semantic surfaces it.
	h := NewHybrid(ctx, NewFakeEmbedder(256), nil, map[string][]byte{
		"a.md": []byte("# notes\nalpha\nbeta\ngamma\n"),
	})
	if tri := h.(*HybridSearch).tri.Search("alpha beta gamma", 5); len(tri) != 0 {
		t.Fatalf("precondition: trigram should miss the cross-line phrase, got %+v", tri)
	}
	hits, err := h.Recall(ctx, "a1", "alpha beta gamma", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("semantic should surface the section even though no line has the full phrase")
	}
}

func TestHybridDegradesWhenEmbeddingFails(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, failingEmbedder{}, nil, map[string][]byte{
		"a.md": []byte("has needle here\n"),
	})
	hits, err := h.Recall(ctx, "a1", "needle", 5)
	if err != nil {
		t.Fatalf("must degrade to trigram, not error: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("trigram results must still be returned when embeddings are unavailable")
	}
}

func TestHybridReindexNoop(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, NewFakeEmbedder(64), nil, map[string][]byte{"a.md": []byte("x\n")})
	if err := h.Reindex(ctx, "a1", "from", "to"); err != nil {
		t.Fatalf("Reindex stub should be nil: %v", err)
	}
}
```
> Note: `h.(*HybridSearch)` in the second test works because `NewHybrid` returns `*HybridSearch`; the assignment `var h Search = NewHybrid(...)` is implied by usage. The implementer should keep `NewHybrid` returning the concrete `*HybridSearch` so both the interface use and the `.tri` field access compile (the test is in-package).

- [ ] **Step 2: Run** `go test ./internal/search/ -run Hybrid` → compile failure.

- [ ] **Step 3: Implement** `internal/search/hybrid.go`:
```go
package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/ssy/engram/internal/cache"
)

// HybridSearch fuses lexical (trigram) and semantic (embedding) recall via
// Reciprocal Rank Fusion. If semantic retrieval is unavailable (build-time or
// query-time embedding failure) it degrades to trigram-only without erroring.
type HybridSearch struct {
	tri *TrigramIndex
	sem *SemanticIndex // nil when semantic build degraded
}

// NewHybrid builds the trigram index synchronously and the semantic index best
// effort: a semantic build failure (e.g. embedding service down) leaves sem nil
// and recall falls back to trigram-only.
func NewHybrid(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) *HybridSearch {
	h := &HybridSearch{tri: BuildTrigram(files)}
	if sem, err := BuildSemantic(ctx, emb, c, files); err == nil {
		h.sem = sem
	}
	return h
}

func (h *HybridSearch) Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error) {
	if k <= 0 {
		k = 8
	}
	n := k
	if n < 10 {
		n = 10
	}
	lists := [][]Hit{h.tri.Search(query, n)}
	if h.sem != nil {
		if semHits, err := h.sem.Search(ctx, query, n); err == nil {
			lists = append(lists, semHits)
		}
		// query-time embedding failure: drop semantic, keep trigram (degrade)
	}
	return rrf(lists, k), nil
}

// Reindex is a no-op in L4; incremental git-diff reindexing is L5.
func (h *HybridSearch) Reindex(ctx context.Context, agentID, from, to string) error { return nil }

// rrf fuses ranked lists by Reciprocal Rank Fusion (k=60). Items are identified
// by path:lineStart-lineEnd; identical ranges merge their scores.
func rrf(lists [][]Hit, k int) []Hit {
	const rrfK = 60
	type agg struct {
		hit   Hit
		score float64
	}
	scores := map[string]*agg{}
	var order []string // first-seen order, for a stable tie-break
	for _, list := range lists {
		for rank, hit := range list {
			id := fmt.Sprintf("%s:%d-%d", hit.Path, hit.LineStart, hit.LineEnd)
			a, ok := scores[id]
			if !ok {
				a = &agg{hit: hit}
				scores[id] = a
				order = append(order, id)
			}
			a.score += 1.0 / float64(rrfK+rank+1)
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return scores[order[i]].score > scores[order[j]].score })
	var out []Hit
	for _, id := range order {
		if len(out) >= k {
			break
		}
		out = append(out, scores[id].hit)
	}
	return out
}

var _ Search = (*HybridSearch)(nil)
```

- [ ] **Step 4: Run** `go test ./internal/search/ -run Hybrid -v` → 4 PASS. Whole package + race: `go test -race ./internal/search/ -count=1`. gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/search/hybrid.go internal/search/hybrid_test.go
git commit -m "feat(search): HybridSearch — RRF fusion of trigram + semantic with degrade"
```

---

### Task 6: Wire HybridSearch through Router + cmd/api

**Files:**
- Modify: `internal/agent/router.go`
- Modify: `internal/agent/router_test.go`
- Modify: `cmd/api/main.go`

**Interfaces:**
- Consumes: `search.NewHybrid` (Task 5), `search.Embedder`/`search.NewFakeEmbedder`/`search.NewVoyage` (Tasks 2/3), `memstore.MemStore`, `cache.Cache`, `NewToolset` (L2).
- Produces: `NewRouter(store memstore.MemStore, prov LLMProvider, scratch string, c cache.Cache, emb search.Embedder) *Router` (signature gains a trailing `emb`); `Open` builds a per-session `*search.HybridSearch` from the materialized workdir and injects it into the Toolset.

- [ ] **Step 1: Adjust router_test.go and add a failing wiring test.** In `internal/agent/router_test.go`:
- Ensure imports include `"github.com/ssy/engram/internal/search"` (cache is already imported from L3).
- Change the `routerFixture` `NewRouter(...)` call to pass a `FakeEmbedder`:
```go
	return NewRouter(store, prov, t.TempDir(), cache.NewLRU(8), search.NewFakeEmbedder(64)), store
```
- Add:
```go
func TestRouterWiresHybridSearch(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.tools.search.(*search.HybridSearch); !ok {
		t.Fatalf("Open must wire a HybridSearch, got %T", s.tools.search)
	}
}
```
(`s.tools` and the unexported `Toolset.search` field are accessible because the test is in `package agent`.)

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Router` → compile failure (NewRouter arity; HybridSearch not yet wired).

- [ ] **Step 3: Wire the embedder + HybridSearch in `internal/agent/router.go`.**
- Add imports: `"io/fs"`, `"os"`, `"path/filepath"` (some may already be present — keep one copy), and `"github.com/ssy/engram/internal/search"` (already imported for `search.NewGrep`).
- Add field `emb search.Embedder` to the `Router` struct.
- Change `NewRouter`:
```go
func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string, c cache.Cache, emb search.Embedder) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, cache: c, emb: emb, active: map[string]bool{}}
}
```
- Add a helper to read the materialized workdir into a file map:
```go
// readWorkdirFiles loads every file under root into a path->bytes map (paths
// relative to root) for index construction.
func readWorkdirFiles(root string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, path)
		files[rel] = data
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("agent: read workdir: %w", err)
	}
	return files, nil
}
```
- In `Open`, after a successful `Materialize`, replace the `search.NewGrep(workdir)` toolset construction with a HybridSearch built from the workdir. Locate:
```go
	tools := NewToolset(workdir, agentID, search.NewGrep(workdir))
```
and replace it with:
```go
	files, err := readWorkdirFiles(workdir)
	if err != nil {
		os.RemoveAll(workdir)
		r.free(agentID)
		return nil, err
	}
	tools := NewToolset(workdir, agentID, search.NewHybrid(ctx, r.emb, r.cache, files))
```
(keep the existing `release` closure and the final `return NewSession(..., release, r.cache)` unchanged.)

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Router -v -count=1` → all Router tests PASS incl. `TestRouterWiresHybridSearch`. Then the full agent package `ENGRAM_TEST_DB=... go test ./internal/agent/ -count=1`. gofmt/vet clean.

- [ ] **Step 5: Wire `cmd/api/main.go`.**
- Add import `"github.com/ssy/engram/internal/search"`.
- Before constructing the Router, build the embedder by env:
```go
	var emb search.Embedder
	switch env("ENGRAM_EMBEDDER", "fake") {
	case "voyage":
		vkey := os.Getenv("VOYAGE_API_KEY")
		if vkey == "" {
			log.Fatal("ENGRAM_EMBEDDER=voyage requires VOYAGE_API_KEY")
		}
		emb = search.NewVoyage(vkey)
	default:
		emb = search.NewFakeEmbedder(256)
	}
```
- Change the Router construction to pass `emb`:
```go
	router := agent.NewRouter(store, prov, os.TempDir(), cache.NewLRU(1024), emb)
```

- [ ] **Step 6: Build + vet + gofmt + full suite + race.**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/search/ ./internal/agent/ -count=1
```
Expected: build OK, vet clean, gofmt nothing, all packages pass, no races.

- [ ] **Step 7: Commit**
```bash
git add internal/agent/router.go internal/agent/router_test.go cmd/api/main.go
git commit -m "feat(agent,cmd): wire HybridSearch through Router (Voyage|fake embedder)"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-22-engram-l4-hybrid-search-design.md`):**
- §3.3 TrigramIndex (line-level inverted index, trigram prefilter + substring verify, <3-char fallback, variant A line text) → Task 1 ✅
- §3.4 Embedder + FakeEmbedder + VoyageEmbedder (Model() for cache key; mock-HTTP) → Tasks 2/3 ✅
- §3.5 SemanticIndex (markdown-section chunking, content-addressed embedding cache keyed `emb:model:hash`, brute-force cosine, batched embed, nil-cache) → Task 4 ✅
- §3.6 HybridSearch (RRF k=60, dedup by path:range, build/query-time degrade to trigram, Reindex no-op) → Task 5 ✅
- §3.7 wiring (NewRouter gains emb; Open builds HybridSearch from workdir; cmd/api env-selected embedder) → Task 6 ✅
- §4 error handling (Voyage %w + non-2xx; build degrade leaves sem nil; query degrade drops semantic; nil cache ok) → Tasks 3/4/5 ✅
- §5 tests (trigram hit/fallback/no-match/k; fake determinism + shared-word ordering; voyage request/parse/non-2xx/count; semantic sectionize+search, cache-reuse via count spy, nil-cache re-embed; hybrid trigram-only, semantic-cross-line, degrade, reindex-noop; router wiring + race) → Tasks 1–6 ✅
- §6 DoD: hybrid recall, content-addressed embed-once, graceful degrade, fake auto-tests + voyage mock + cmd/api real, zero caller change, full suite+race → covered ✅

**Placeholder scan:** No TBD/TODO. Convergence note in Task 5 (the `h.(*HybridSearch)` in-package cast) is an explicit instruction, not a placeholder. Voyage response shape (`data[].embedding`) is the documented API; if the live API nests differently it surfaces in the cmd/api manual run (unit tests use the mock).

**Type consistency:** `Hit` / `Search` are the existing L2 types (unchanged). `Embedder{Embed(ctx,[]string)([][]float32,error); Model()string}` defined Task 2, consumed Tasks 3/4/5/6. `cosine` defined once (Task 4); Task 2's test uses a local `tcos` to avoid a forward dependency. `BuildTrigram`/`Search` (Task 1) ↔ Task 5. `BuildSemantic(ctx,emb,c,files)`/`(*SemanticIndex).Search(ctx,query,k)` (Task 4) ↔ Task 5. `NewHybrid(ctx,emb,c,files)*HybridSearch` (Task 5) ↔ Task 6. `NewRouter(store,prov,scratch,c,emb)` (Task 6) ↔ router_test ↔ cmd/api. `embKey`/`encodeVec`/`decodeVec` are file-local to semantic.go. The L3 `NewRouter` had 4 args (`...,c cache.Cache`); Task 6 adds the 5th (`emb`) and updates both callers (router_test.go, cmd/api) in the same task — no intermediate broken state.

**Build-ordering check:** Tasks 1–5 are additive new files in `internal/search` (no caller changes) — the package compiles after each. Task 6 changes `NewRouter`'s arity and updates its only two callers (router_test.go, cmd/api/main.go) in the same task. `GrepSearch` remains in the package (used by the L2/L3 Session/tools tests that construct `search.NewGrep` directly); only `Router.Open` switches to `NewHybrid`.
