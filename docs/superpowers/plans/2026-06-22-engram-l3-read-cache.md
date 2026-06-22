# Engram L3 — SHA-keyed Read Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache the assembled resident prompt (system/ content keyed by the system/ subtree hash, tree index keyed by the root tree hash) in a per-pod LRU so Session.assembleSystem stops re-walking the whole tree every step.

**Architecture:** A content-addressed, invalidation-free `cache.Cache` (per-pod LRU, shared across agents). `memstore.TreeKeys(commit)` reads the git commit/tree objects to yield the two cache keys. `Session.assembleSystem` uses the cache when the workdir is clean (== HEAD) and bypasses it (recomputes from workdir) when dirty; `cache==nil` recomputes (L2 behavior).

**Tech Stack:** Go 1.25 stdlib (`container/list`, `sync`); existing L1 `internal/memstore` + `internal/memstore/gitfs` (go-git) + L2 `internal/agent`.

**Prerequisites:** On `main` (L1+L2 merged). Create a branch before Task 1: `git checkout -b feat/l3-read-cache`. Tasks 1 and 2 need NO database. Task 3's new cache tests are DB-free; the pre-existing L2 Session tests it must keep green need `ENGRAM_TEST_DB`. Task 4 (Router/cmd build) — the existing Router/Session DB tests need `ENGRAM_TEST_DB`. Start Postgres if needed:
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout (this plan creates/modifies):**
```
internal/cache/cache.go        # NEW: Cache interface + LRU
internal/cache/cache_test.go   # NEW
internal/memstore/gitfs/repo.go        # MODIFY: add TreeKeys(ctx, objs, commit)
internal/memstore/gitfs/repo_test.go   # MODIFY: TreeKeys granularity test (DB-free)
internal/memstore/memstore.go          # MODIFY: add TreeKeys to MemStore iface + *Store
internal/memstore/memstore_treekeys_test.go  # NEW: delegation test (DB-free)
internal/agent/session.go      # MODIFY: cache field, assembleSystem(ctx) w/ cache+bypass, split builders, NewSession sig
internal/agent/session_test.go # MODIFY: sessionFixture passes nil cache
internal/agent/session_cache_test.go  # NEW: spy-cache tests (DB-free)
internal/agent/router.go       # MODIFY: cache field, NewRouter sig, Open injects cache
internal/agent/router_test.go  # MODIFY: routerFixture passes a cache
cmd/api/main.go                # MODIFY: construct LRU, pass to NewRouter
```
**Dependency order:** cache (1) → memstore.TreeKeys (2) → Session refactor (3, uses 1+2) → Router/cmd wiring (4).

---

### Task 1: `internal/cache` — Cache interface + LRU

**Files:**
- Create: `internal/cache/cache.go`
- Test: `internal/cache/cache_test.go`

- [ ] **Step 1: Write the failing test** — `internal/cache/cache_test.go`:
```go
package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRUPutGet(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	if v, ok := c.Get("a"); !ok || v != "1" {
		t.Fatalf("get a = %q,%v", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("missing should miss")
	}
}

func TestLRUEvictsOldest(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	c.Put("b", "2")
	c.Put("c", "3") // evicts a (least-recently-used)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should remain")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should remain")
	}
}

func TestLRUGetPromotesRecency(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	c.Put("b", "2")
	c.Get("a")     // a becomes most-recent
	c.Put("c", "3") // evicts b (now LRU)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted after a was promoted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should remain")
	}
}

func TestLRUDefaultOnNonPositive(t *testing.T) {
	c := NewLRU(0) // <=0 => default cap (1024)
	for i := 0; i < 2000; i++ {
		c.Put(fmt.Sprintf("k%d", i), "v")
	}
	if _, ok := c.Get("k1999"); !ok {
		t.Fatal("latest entry should be present under default cap")
	}
	if _, ok := c.Get("k0"); ok {
		t.Fatal("oldest entry should be evicted under default cap")
	}
}

func TestLRUConcurrent(t *testing.T) {
	c := NewLRU(64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k%d", (g*500+i)%128)
				c.Put(k, "v")
				c.Get(k)
			}
		}(g)
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cache/`
Expected: compile failure (NewLRU undefined).

- [ ] **Step 3: Write the implementation** — `internal/cache/cache.go`:
```go
// Package cache is a SHA-keyed, invalidation-free read cache. Keys are content
// hashes (immutable), so a hit is always equivalent to recomputation and
// "invalidation" degrades to LRU eviction for space. One per-pod instance is
// shared across agents/sessions; identical resident content dedups naturally.
package cache

import (
	"container/list"
	"sync"
)

// Cache stores immutable string values keyed by a content hash.
type Cache interface {
	Get(key string) (string, bool)
	Put(key, val string)
}

const defaultMaxEntries = 1024

type entry struct{ key, val string }

// LRU is a size-bounded (by entry count), mutex-guarded Cache.
type LRU struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List // front = most-recently-used
	items      map[string]*list.Element
}

// NewLRU builds an LRU holding at most maxEntries entries. maxEntries <= 0 uses
// a default cap.
func NewLRU(maxEntries int) *LRU {
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &LRU{maxEntries: maxEntries, ll: list.New(), items: make(map[string]*list.Element)}
}

func (l *LRU) Get(key string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.ll.MoveToFront(el)
		return el.Value.(*entry).val, true
	}
	return "", false
}

func (l *LRU) Put(key, val string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.ll.MoveToFront(el)
		el.Value.(*entry).val = val
		return
	}
	el := l.ll.PushFront(&entry{key: key, val: val})
	l.items[key] = el
	if l.ll.Len() > l.maxEntries {
		if oldest := l.ll.Back(); oldest != nil {
			l.ll.Remove(oldest)
			delete(l.items, oldest.Value.(*entry).key)
		}
	}
}

var _ Cache = (*LRU)(nil)
```

- [ ] **Step 4: Run tests (with race detector)**

Run: `go test -race ./internal/cache/ -v`
Expected: PASS (5 cases), no data races.

- [ ] **Step 5: Commit**
```bash
git add internal/cache/
git commit -m "feat(cache): content-addressed per-pod LRU read cache"
```

---

### Task 2: `memstore.TreeKeys` — root tree + system/ subtree hashes

**Files:**
- Modify: `internal/memstore/gitfs/repo.go` (add `TreeKeys`)
- Modify: `internal/memstore/gitfs/repo_test.go` (add granularity test)
- Modify: `internal/memstore/memstore.go` (add `TreeKeys` to `MemStore` iface + `*Store`)
- Test: `internal/memstore/memstore_treekeys_test.go` (delegation)

**Design:** `TreeKeys` reads the commit object and its root tree via go-git over our ObjStore (no Postgres). Returns the root tree hash (busts the tree-index cache on any change) and the `system` subtree hash (busts the resident cache only on system/ changes; "" when no system/).

- [ ] **Step 1: Write the failing gitfs test** — append to `internal/memstore/gitfs/repo_test.go`:
```go
func TestTreeKeysGranularity(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	write := func(dir, rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	wA := t.TempDir()
	write(wA, "system/about.md", "a\n")
	write(wA, "notes/x.md", "x\n")
	hA, err := Commit(ctx, objs, "", wA, "A")
	if err != nil {
		t.Fatal(err)
	}

	// B: change only notes/
	wB := t.TempDir()
	if err := Materialize(ctx, objs, hA, wB); err != nil {
		t.Fatal(err)
	}
	write(wB, "notes/x.md", "x2\n")
	hB, err := Commit(ctx, objs, hA, wB, "B")
	if err != nil {
		t.Fatal(err)
	}

	// C: change system/
	wC := t.TempDir()
	if err := Materialize(ctx, objs, hA, wC); err != nil {
		t.Fatal(err)
	}
	write(wC, "system/about.md", "a2\n")
	hC, err := Commit(ctx, objs, hA, wC, "C")
	if err != nil {
		t.Fatal(err)
	}

	rA, sA, err := TreeKeys(ctx, objs, hA)
	if err != nil {
		t.Fatal(err)
	}
	rB, sB, _ := TreeKeys(ctx, objs, hB)
	rC, sC, _ := TreeKeys(ctx, objs, hC)

	if sA == "" {
		t.Fatal("systemSubtree should be non-empty when system/ exists")
	}
	if rA == rB {
		t.Fatal("root tree must change when notes/ changes")
	}
	if sA != sB {
		t.Fatal("system subtree must NOT change when only notes/ changes")
	}
	if sA == sC {
		t.Fatal("system subtree must change when system/ changes")
	}
}

func TestTreeKeysNoSystemDir(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	w := t.TempDir()
	if err := os.WriteFile(filepath.Join(w, "loose.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := Commit(ctx, objs, "", w, "init")
	if err != nil {
		t.Fatal(err)
	}
	root, sys, err := TreeKeys(ctx, objs, h)
	if err != nil {
		t.Fatal(err)
	}
	if root == "" {
		t.Fatal("root tree should be non-empty")
	}
	if sys != "" {
		t.Fatalf("systemSubtree should be empty without system/, got %q", sys)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/memstore/gitfs/ -run TreeKeys`
Expected: compile failure (TreeKeys undefined).

- [ ] **Step 3: Implement `TreeKeys` in gitfs** — add to `internal/memstore/gitfs/repo.go` (add imports `"github.com/go-git/go-git/v5/plumbing/filemode"` and `"github.com/go-git/go-git/v5/plumbing/object"` if not already present):
```go
// TreeKeys returns the root tree hash and the "system" subtree hash for commit.
// systemSubtree is "" when no system/ directory exists. Both are read from the
// commit/tree objects in ObjStore (no Postgres). They are immutable cache keys:
// rootTree changes on any file change; systemSubtree only on system/ changes.
func TreeKeys(ctx context.Context, objs objstore.ObjStore, commit string) (rootTree, systemSubtree string, err error) {
	if commit == "" {
		return "", "", nil
	}
	st := NewStorage(ctx, objs)
	c, err := object.GetCommit(st, plumbing.NewHash(commit))
	if err != nil {
		return "", "", fmt.Errorf("gitfs: get commit %s: %w", commit, err)
	}
	rootTree = c.TreeHash.String()
	tree, err := c.Tree()
	if err != nil {
		return "", "", fmt.Errorf("gitfs: tree of %s: %w", commit, err)
	}
	for _, e := range tree.Entries {
		if e.Name == "system" && e.Mode == filemode.Dir {
			systemSubtree = e.Hash.String()
			break
		}
	}
	return rootTree, systemSubtree, nil
}
```

- [ ] **Step 4: Run gitfs test**

Run: `go test ./internal/memstore/gitfs/ -run TreeKeys -v`
Expected: PASS (2 cases). If `object.GetCommit` / `c.Tree()` / `tree.Entries` / `filemode.Dir` names differ in go-git v5.19.1, adjust via `go doc github.com/go-git/go-git/v5/plumbing/object` — keep the design (read commit→root tree hash; scan root tree for a `system` dir entry).

- [ ] **Step 5: Add `TreeKeys` to memstore** — in `internal/memstore/memstore.go`, add to the `MemStore` interface and `*Store`:
```go
// add to the MemStore interface (next to Materialize/CommitWithCAS):
	TreeKeys(ctx context.Context, at CommitHash) (rootTree, systemSubtree CommitHash, err error)
```
```go
// add as a *Store method:
func (s *Store) TreeKeys(ctx context.Context, at CommitHash) (CommitHash, CommitHash, error) {
	root, sys, err := gitfs.TreeKeys(ctx, s.objs, string(at))
	return CommitHash(root), CommitHash(sys), err
}
```

- [ ] **Step 6: Write the delegation test** — `internal/memstore/memstore_treekeys_test.go`:
```go
package memstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
)

// DB-free: TreeKeys reads only objects, so refs may be nil here.
func TestStoreTreeKeysDelegates(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	w := t.TempDir()
	if err := os.MkdirAll(filepath.Join(w, "system"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w, "system", "a.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := gitfs.Commit(ctx, objs, "", w, "init")
	if err != nil {
		t.Fatal(err)
	}

	s := New(objs, nil) // refs unused by TreeKeys
	root, sys, err := s.TreeKeys(ctx, CommitHash(h))
	if err != nil {
		t.Fatal(err)
	}
	if root == "" || sys == "" {
		t.Fatalf("expected non-empty keys, got root=%q sys=%q", root, sys)
	}
}
```

- [ ] **Step 7: Run + build**

Run: `go test ./internal/memstore/... -run TreeKeys -count=1` then `go build ./...` then `go vet ./internal/memstore/...`
Expected: PASS, build OK, vet clean. (These tests need no DB.)

- [ ] **Step 8: Commit**
```bash
git add internal/memstore/gitfs/repo.go internal/memstore/gitfs/repo_test.go internal/memstore/memstore.go internal/memstore/memstore_treekeys_test.go
git commit -m "feat(memstore): TreeKeys — root tree + system/ subtree hashes for cache keying"
```

---

### Task 3: Session — cache-backed assembleSystem (clean) / bypass (dirty)

**Files:**
- Modify: `internal/agent/session.go`
- Modify: `internal/agent/session_test.go` (sessionFixture passes nil cache)
- Test: `internal/agent/session_cache_test.go` (NEW, DB-free spy-cache tests)

**Design:** Split `assembleSystem` into pure `buildSystemContent()` (string) + `buildTreeIndex()` ((string,error)); `buildResident()` concatenates them. The new `assembleSystem(ctx)`: if `cache==nil || dirty` → `buildResident()` (no cache); else resolve `TreeKeys(head)` and serve/fill `sys:<systemSubtree>` and `idx:<rootTree>`; on a `TreeKeys` error, degrade to `buildResident()`. `NewSession` gains a trailing `cache cache.Cache` param (nil-safe).

- [ ] **Step 1: Write the failing spy-cache tests** — `internal/agent/session_cache_test.go`:
```go
package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/search"
)

// spyCache is a working Cache that counts Get/Put calls.
type spyCache struct {
	data       map[string]string
	gets, puts int
}

func newSpyCache() *spyCache { return &spyCache{data: map[string]string{}} }
func (c *spyCache) Get(k string) (string, bool) { c.gets++; v, ok := c.data[k]; return v, ok }
func (c *spyCache) Put(k, v string)             { c.puts++; c.data[k] = v }

var _ cache.Cache = (*spyCache)(nil)

// residentSession builds a Session over a real committed tree WITHOUT Postgres
// (refs nil; only assembleSystem/TreeKeys are exercised, which read objects only).
func residentSession(t *testing.T, c cache.Cache) *Session {
	t.Helper()
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	seed := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(seed, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("system/about.md", "---\ndescription: who\n---\nresident\n")
	write("notes/n.md", "---\ndescription: a note\n---\nbody\n")
	head, err := gitfs.Commit(ctx, objs, "", seed, "init")
	if err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	if err := gitfs.Materialize(ctx, objs, head, workdir); err != nil {
		t.Fatal(err)
	}
	store := memstore.New(objs, nil)
	tools := NewToolset(workdir, "a1", search.NewGrep(workdir))
	return NewSession(store, &FakeProvider{}, tools, "a1", memstore.CommitHash(head), workdir, nil, c)
}

func TestAssembleSystemReusesCacheWhenClean(t *testing.T) {
	ctx := context.Background()
	spy := newSpyCache()
	s := residentSession(t, spy)

	out1, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out2, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out1 != out2 {
		t.Fatal("cached output must equal first build")
	}
	if spy.puts != 2 {
		t.Fatalf("expected 2 builds (sys+idx), got %d puts", spy.puts)
	}
	if spy.gets != 4 {
		t.Fatalf("expected 4 gets (2 keys x 2 calls), got %d", spy.gets)
	}
	if !strings.Contains(out1, "resident") || !strings.Contains(out1, "a note") {
		t.Fatalf("resident output missing content:\n%s", out1)
	}
}

func TestAssembleSystemBypassesCacheWhenDirty(t *testing.T) {
	ctx := context.Background()
	spy := newSpyCache()
	s := residentSession(t, spy)
	s.dirty = true

	out, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spy.gets != 0 || spy.puts != 0 {
		t.Fatalf("dirty turn must bypass cache; gets=%d puts=%d", spy.gets, spy.puts)
	}
	if !strings.Contains(out, "resident") {
		t.Fatalf("bypass output missing content:\n%s", out)
	}
}

func TestAssembleSystemNilCacheRecomputes(t *testing.T) {
	ctx := context.Background()
	s := residentSession(t, nil)
	out, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "resident") || !strings.Contains(out, "a note") {
		t.Fatalf("nil-cache output missing content:\n%s", out)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/agent/ -run AssembleSystem`
Expected: compile failure (NewSession arity changed / assembleSystem signature).

- [ ] **Step 3: Refactor session.go** — change the import block to add `"github.com/ssy/engram/internal/cache"`, add the `cache` field, change `NewSession`, and replace `assembleSystem` with the split + cache logic.

Add field to the `Session` struct:
```go
	cache    cache.Cache // nil => always recompute (L2 behavior)
```
Change `NewSession` to take a trailing `c cache.Cache` and store it:
```go
func NewSession(store memstore.MemStore, prov LLMProvider, tools *Toolset, agentID string, head memstore.CommitHash, workdir string, release func(), c cache.Cache) *Session {
	return &Session{
		agentID:  agentID,
		store:    store,
		prov:     prov,
		tools:    tools,
		head:     head,
		workdir:  workdir,
		maxSteps: defaultMaxSteps,
		release:  release,
		cache:    c,
	}
}
```
In `Send`, change the call `s.assembleSystem()` to `s.assembleSystem(ctx)`.

Replace the existing `assembleSystem` method with:
```go
// assembleSystem returns the resident system prompt (system/ contents + tree
// index). When the workdir is clean (== HEAD) and a cache is present, it serves
// the two pieces from the cache keyed by their immutable tree hashes; when dirty
// (uncommitted edits) or cache==nil it recomputes from the workdir.
func (s *Session) assembleSystem(ctx context.Context) (string, error) {
	if s.cache == nil || s.dirty {
		return s.buildResident()
	}
	rootTree, systemSubtree, err := s.store.TreeKeys(ctx, s.head)
	if err != nil {
		return s.buildResident() // cache is an optimization; a key read must not break a turn
	}
	sysKey := "sys:" + string(systemSubtree)
	sys, ok := s.cache.Get(sysKey)
	if !ok {
		sys = s.buildSystemContent()
		s.cache.Put(sysKey, sys)
	}
	idxKey := "idx:" + string(rootTree)
	idx, ok := s.cache.Get(idxKey)
	if !ok {
		built, berr := s.buildTreeIndex()
		if berr != nil {
			return "", berr
		}
		idx = built
		s.cache.Put(idxKey, idx)
	}
	return sys + idx, nil
}

func (s *Session) buildResident() (string, error) {
	idx, err := s.buildTreeIndex()
	if err != nil {
		return "", err
	}
	return s.buildSystemContent() + idx, nil
}

// buildSystemContent reads all system/ file contents (resident set). system/
// may not exist yet; per-file read errors are skipped.
func (s *Session) buildSystemContent() string {
	var b strings.Builder
	b.WriteString("# Resident memory (system/)\n")
	systemDir := filepath.Join(s.workdir, "system")
	_ = filepath.WalkDir(systemDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
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
	return b.String()
}

// buildTreeIndex walks the whole tree producing a sorted "path: description"
// index from each file's frontmatter.
func (s *Session) buildTreeIndex() (string, error) {
	var b strings.Builder
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
```
This preserves the exact concatenated output of the old `assembleSystem` (system block, then `\n# Memory tree index...`, then sorted lines).

- [ ] **Step 4: Update the existing Session test fixture** — in `internal/agent/session_test.go`, the `sessionFixture` helper calls `NewSession(...)`. Add a trailing `nil` argument:
```go
	s := NewSession(store, &FakeProvider{Steps: steps}, tools, "a1", head, workdir, nil, nil)
```
(the new last `nil` is the cache — keeps L2 behavior). Leave everything else in that file unchanged.

- [ ] **Step 5: Update the Router caller (temporary nil)** — in `internal/agent/router.go`, the `Open` method calls `NewSession(...)`. Add a trailing `nil` for now (Task 4 replaces it with the injected cache):
```go
	return NewSession(r.store, r.prov, tools, agentID, head, workdir, release, nil), nil
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/agent/ -run AssembleSystem -v` (DB-free) — expect 3 PASS.
Then the full agent package incl. L2 regression (needs DB): `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -count=1`.
Expected: all green (L2 Session/Router tests unaffected with nil cache). `gofmt -l internal/agent/` + `go vet ./internal/agent/` clean.

- [ ] **Step 7: Commit**
```bash
git add internal/agent/session.go internal/agent/session_test.go internal/agent/session_cache_test.go internal/agent/router.go
git commit -m "feat(agent): cache-backed assembleSystem (clean) with dirty/nil bypass"
```

---

### Task 4: Router + cmd/api — inject the shared cache

**Files:**
- Modify: `internal/agent/router.go` (cache field, NewRouter sig, Open injects)
- Modify: `internal/agent/router_test.go` (routerFixture passes a cache)
- Modify: `cmd/api/main.go` (construct LRU, pass to NewRouter)

- [ ] **Step 1: Write/adjust the failing test** — in `internal/agent/router_test.go`, update `routerFixture` to construct the Router with a cache, and add a test that an Open'd session carries a non-nil cache. Change the `NewRouter` call in `routerFixture` to:
```go
	return NewRouter(store, prov, t.TempDir(), cache.NewLRU(8)), store
```
Add the import `"github.com/ssy/engram/internal/cache"` to router_test.go, and add this test:
```go
func TestRouterInjectsCacheIntoSession(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.cache == nil {
		t.Fatal("Router.Open must inject the shared cache into the Session")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -run Router`
Expected: compile failure (NewRouter arity / r unused cache).

- [ ] **Step 3: Wire the cache through Router** — in `internal/agent/router.go`: add the import `"github.com/ssy/engram/internal/cache"`, add a `cache cache.Cache` field to `Router`, take it in `NewRouter`, and pass it to `NewSession` in `Open` (replacing the `nil` from Task 3):
```go
type Router struct {
	store   memstore.MemStore
	prov    LLMProvider
	scratch string
	cache   cache.Cache

	mu     sync.Mutex
	active map[string]bool
}

func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string, c cache.Cache) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, cache: c, active: map[string]bool{}}
}
```
And in `Open`, the final return:
```go
	return NewSession(r.store, r.prov, tools, agentID, head, workdir, release, r.cache), nil
```

- [ ] **Step 4: Run agent tests**

Run: `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -count=1 -v`
Expected: all PASS incl. the new `TestRouterInjectsCacheIntoSession`. `gofmt -l internal/agent/` + `go vet ./internal/agent/` clean.

- [ ] **Step 5: Wire cmd/api** — in `cmd/api/main.go`, add the import `"github.com/ssy/engram/internal/cache"` and change the Router construction:
```go
	router := agent.NewRouter(store, prov, os.TempDir(), cache.NewLRU(1024))
```

- [ ] **Step 6: Build + vet + full suite**

Run:
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1
```
Expected: build OK, vet clean, gofmt lists nothing, all packages pass (cache, agent, memstore, gitfs, objstore, refs, search).

- [ ] **Step 7: Commit**
```bash
git add internal/agent/router.go internal/agent/router_test.go cmd/api/main.go
git commit -m "feat(agent,cmd): inject shared LRU read cache through Router"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-22-engram-l3-read-cache-design.md`):**
- §3.3 cache.Cache + LRU (per-pod, size-bounded, mutex, default cap) → Task 1 ✅
- §3.4 memstore.TreeKeys (gitfs read; rootTree + system subtree; "" when absent; interface + Store) → Task 2 ✅
- §3.5 Session refactor (split builders, assembleSystem(ctx) clean/dirty/nil, NewSession cache param, buildResident) → Task 3 ✅
- §3.6 Router cache field + NewRouter sig + Open injects; cmd/api LRU → Task 4 ✅
- §4 error handling (TreeKeys %w; degrade-to-recompute on key error; LRU no-error) → Tasks 2/3 ✅
- §5 tests: LRU put/get/evict/recency/default/race; TreeKeys granularity + no-system; spy-cache clean-reuse/dirty-bypass/nil-recompute → Tasks 1/2/3 ✅
- §6 DoD: clean multi-call reuse (build once), dirty bypass, only-notes-commit keeps system hit (TreeKeys granularity proves the key invariance), nil==L2, LRU -race → covered ✅

**Placeholder scan:** No TBD/TODO/"similar to". Every code step has complete code. The one convergence note (go-git `object.GetCommit`/`tree.Entries`/`filemode.Dir` names in v5.19.1) is a TDD adjustment with `go doc` guidance, not a placeholder.

**Type consistency:** `cache.Cache{Get(string)(string,bool); Put(string,string)}` defined Task 1, consumed Tasks 3/4 (spyCache + LRU both satisfy). `gitfs.TreeKeys(ctx, objstore.ObjStore, string)(string,string,error)` Task 2 ↔ `memstore.(*Store).TreeKeys(ctx, CommitHash)(CommitHash,CommitHash,error)` ↔ `MemStore` iface ↔ Session call `s.store.TreeKeys(ctx, s.head)` Task 3 — consistent. `NewSession(..., release func(), c cache.Cache)` defined Task 3, callers updated in Task 3 (session_test.go nil, router.go nil) then Task 4 (router.go r.cache). `NewRouter(store, prov, scratch, c cache.Cache)` Task 4 ↔ routerFixture ↔ cmd/api — consistent. `s.cache` field accessed by router_test.go (in-package) Task 4 — valid.

**Build-ordering check:** Task 3 changes `NewSession` arity and updates BOTH callers (session_test.go, router.go→nil) in the same task, so the tree compiles after Task 3. Task 4 changes `NewRouter` arity and updates BOTH callers (router_test.go, cmd/api) in the same task. No intermediate broken state.
