# Engram L4b — Search Index Persistence + Incremental Reindex Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the expensive Voyage embeddings in a separate content-addressed store so the reindex job warms them incrementally in the background and sessions stop re-embedding unchanged content.

**Architecture:** A persistent `cache.Cache` (ObjCache) backed by a dedicated `objstore.ObjStore` instance (separate from the GC'd git object store), optionally fronted by an in-process LRU (Tiered). `BuildSemantic` already does miss→embed→`c.Put`, so injecting this cache needs zero change to it. The reindex job (previously a no-op) materializes the agent's full HEAD tree and runs `BuildSemantic` for its Put side-effect; content-addressed keys make it incremental for free. Sessions and the worker share the same embedding store.

**Tech Stack:** Go 1.25 stdlib (`crypto/sha256`, `encoding/hex`); existing `internal/cache` (Cache/LRU), `internal/search` (Embedder/BuildSemantic/NewHybrid/NewFakeEmbedder), `internal/memstore` + `objstore`, L5b `internal/maintenance` (DrainJobs/processJob), `internal/agent` (Router/NewHybrid).

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` first arg on all I/O; wrap errors with `%w`.
- `cache.Cache` is the synchronous interface `Get(key string) (string, bool)` / `Put(key, val string)` — no ctx, no error. ObjCache/Tiered implement it exactly; ObjCache uses `context.Background()` internally and treats all errors as miss/best-effort (cache failure degrades to recompute, never incorrect).
- **The embedding ObjStore MUST be a different root/instance than the git object store (`ENGRAM_OBJ`).** GC (L5a) iterates the git object store and sweeps unreachable objects; it must NEVER see the embedding store (every embedding would look unreachable). GC code is unchanged — correctness comes from cmd wiring two distinct stores.
- The search embedding cache is a DIFFERENT instance than the L3 system read cache (don't persist system content into the embedding store).
- Reindex does NOT write the ref and takes NO lock (idempotent, content-addressed).
- `embKey(model, text)` (in `internal/search/semantic.go`) is the cache key; it includes `emb.Model()` so different models never mix. ObjCache must accept arbitrary string keys (with `:` and base64) — it maps them to safe object keys via `hex(sha256(key))`.
- Existing signatures (verified): `cache.Cache`, `cache.NewLRU(n)`; `search.Embedder{Embed(ctx,[]string)([][]float32,error); Model()string}`, `search.NewFakeEmbedder(dim)`, `search.BuildSemantic(ctx, emb, c, files map[string][]byte)(*SemanticIndex,error)`, `search.NewHybrid(ctx, emb, c, files)`; `objstore.ObjStore` (Has/Get/Put/Iter/Stat/Delete), `objstore.NewLocal(root)`, `objstore.ErrNotFound`; `memstore.New(objs, *refs.Refs)`, `memstore.MemStore` (ResolveHead/Materialize/CreateAgent); L5b `maintenance.DrainJobs(ctx, r, store, c Completer, maxAttempts)(int,error)`, `processJob(...)`, `maintenance.Reflect`, the `reflectStore(t)` test helper in `reflect_test.go`, `refs.InsertPendingJob`/`CountJobs`.

**Prerequisites:** On `main` (L1–L5b merged). Branch: `git checkout -b feat/l4b-index-persistence`. Tasks 2–4 need `ENGRAM_TEST_DB`.
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout:**
```
internal/cache/objcache.go        # NEW: ObjCache (cache.Cache over ObjStore)
internal/cache/objcache_test.go   # NEW
internal/cache/tiered.go          # NEW: Tiered (front+back cache.Cache)
internal/cache/tiered_test.go     # NEW
internal/maintenance/reindex.go   # NEW: Reindex (materialize full tree → BuildSemantic, discard)
internal/maintenance/reindex_test.go # NEW (live PG + counting embedder + ObjCache)
internal/maintenance/drain.go     # MODIFY: DrainJobs/processJob +emb,embCache; reindex → Reindex
internal/maintenance/drain_test.go# MODIFY: update DrainJobs calls
cmd/maintenance/main.go           # MODIFY: build embedder + embCache, pass to DrainJobs
internal/agent/router.go          # MODIFY: embCache field; NewRouter +embCache; NewHybrid uses it
internal/agent/router_test.go     # MODIFY: update NewRouter call
cmd/api/main.go                   # MODIFY: build embCache, pass to NewRouter
```
**Dependency order:** cache primitives (1) → Reindex (2) → maintenance wiring (3, maintenance side, keeps build green) → request-path wiring (4, agent/cmd-api side, keeps build green).

---

### Task 1: cache.ObjCache + cache.Tiered

**Files:**
- Create: `internal/cache/objcache.go`, `internal/cache/objcache_test.go`
- Create: `internal/cache/tiered.go`, `internal/cache/tiered_test.go`

**Interfaces:**
- Consumes: `cache.Cache`, `cache.NewLRU`, `objstore.ObjStore`, `objstore.NewLocal`, `objstore.ErrNotFound`.
- Produces: `cache.NewObjCache(objs objstore.ObjStore) *ObjCache` (implements Cache); `cache.NewTiered(front, back Cache) *Tiered` (implements Cache).

- [ ] **Step 1: Write `internal/cache/objcache_test.go`:**
```go
package cache

import (
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestObjCacheRoundTrip(t *testing.T) {
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	if _, ok := c.Get("emb:fake:abc"); ok {
		t.Fatal("empty cache should miss")
	}
	c.Put("emb:fake:abc", "vecdata")
	got, ok := c.Get("emb:fake:abc")
	if !ok || got != "vecdata" {
		t.Fatalf("get = %q %v, want vecdata true", got, ok)
	}
}

func TestObjCacheDistinctKeys(t *testing.T) {
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	c.Put("emb:fake:aaa", "v1")
	c.Put("emb:fake:bbb", "v2")
	if g, _ := c.Get("emb:fake:aaa"); g != "v1" {
		t.Fatalf("aaa = %q", g)
	}
	if g, _ := c.Get("emb:fake:bbb"); g != "v2" {
		t.Fatalf("bbb = %q", g)
	}
}

func TestObjCacheHandlesUnsafeKeyChars(t *testing.T) {
	// embKey contains ':' and base64 (which can include '+' '/'); ObjCache must
	// map any key to a safe object key without error.
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	key := "emb:voyage-3:AB+/cd=="
	c.Put(key, "v")
	if g, ok := c.Get(key); !ok || g != "v" {
		t.Fatalf("unsafe key round-trip = %q %v", g, ok)
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/cache/ -run ObjCache` → compile failure.

- [ ] **Step 3: Implement `internal/cache/objcache.go`:**
```go
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/ssy/engram/internal/memstore/objstore"
)

// ObjCache is a persistent, content-addressed Cache backed by an ObjStore. It is
// for embeddings (expensive to recompute); it MUST use an ObjStore instance that
// is separate from the GC'd git object store. All errors degrade to miss/no-op
// (the cache is best-effort: a failure means recompute, never incorrectness).
type ObjCache struct{ objs objstore.ObjStore }

func NewObjCache(objs objstore.ObjStore) *ObjCache { return &ObjCache{objs: objs} }

// safeKey maps an arbitrary cache key (which may contain ':' and base64 chars)
// to a flat, filesystem/bucket-safe object key.
func safeKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (o *ObjCache) Get(key string) (string, bool) {
	data, err := o.objs.Get(context.Background(), safeKey(key))
	if err != nil {
		return "", false
	}
	return string(data), true
}

func (o *ObjCache) Put(key, val string) {
	_ = o.objs.Put(context.Background(), safeKey(key), []byte(val))
}

var _ Cache = (*ObjCache)(nil)
```

- [ ] **Step 4: Write `internal/cache/tiered_test.go`:**
```go
package cache

import "testing"

func TestTieredFrontHit(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	front.Put("k", "front")
	tier := NewTiered(front, back)
	if g, ok := tier.Get("k"); !ok || g != "front" {
		t.Fatalf("front hit = %q %v", g, ok)
	}
}

func TestTieredBackHitPromotes(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	back.Put("k", "back")
	tier := NewTiered(front, back)
	if g, ok := tier.Get("k"); !ok || g != "back" {
		t.Fatalf("back hit = %q %v", g, ok)
	}
	// promoted into front
	if g, ok := front.Get("k"); !ok || g != "back" {
		t.Fatalf("front after promote = %q %v", g, ok)
	}
}

func TestTieredPutWritesBoth(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	NewTiered(front, back).Put("k", "v")
	if g, _ := front.Get("k"); g != "v" {
		t.Fatalf("front = %q", g)
	}
	if g, _ := back.Get("k"); g != "v" {
		t.Fatalf("back = %q", g)
	}
}

func TestTieredMiss(t *testing.T) {
	if _, ok := NewTiered(NewLRU(8), NewLRU(8)).Get("nope"); ok {
		t.Fatal("should miss")
	}
}
```

- [ ] **Step 5: Run** `go test ./internal/cache/ -run Tiered` → compile failure.

- [ ] **Step 6: Implement `internal/cache/tiered.go`:**
```go
package cache

// Tiered composes two caches: a fast front (e.g. in-process LRU) over a durable
// back (e.g. ObjCache). Get checks front then back, promoting a back hit into
// front. Put writes both. Used for embeddings: per-process LRU over the
// persistent content-addressed store.
type Tiered struct{ front, back Cache }

func NewTiered(front, back Cache) *Tiered { return &Tiered{front: front, back: back} }

func (t *Tiered) Get(key string) (string, bool) {
	if v, ok := t.front.Get(key); ok {
		return v, true
	}
	if v, ok := t.back.Get(key); ok {
		t.front.Put(key, v)
		return v, true
	}
	return "", false
}

func (t *Tiered) Put(key, val string) {
	t.front.Put(key, val)
	t.back.Put(key, val)
}

var _ Cache = (*Tiered)(nil)
```

- [ ] **Step 7: Run** `go test ./internal/cache/ -count=1 -v` → all pass (existing LRU tests + 7 new). `go test -race ./internal/cache/ -count=1`. `gofmt -l internal/cache/`, `go vet ./internal/cache/` clean.

- [ ] **Step 8: Commit**
```bash
git add internal/cache/objcache.go internal/cache/objcache_test.go internal/cache/tiered.go internal/cache/tiered_test.go
git commit -m "feat(cache): ObjCache (content-addressed persistent Cache) + Tiered"
```

---

### Task 2: maintenance.Reindex

**Files:**
- Create: `internal/maintenance/reindex.go`
- Test: `internal/maintenance/reindex_test.go`

**Interfaces:**
- Consumes: `memstore.MemStore` (ResolveHead/Materialize), `search.Embedder`, `search.BuildSemantic`, `cache.Cache`; tests reuse `reflectStore(t)` (from `reflect_test.go`, same package), `search.NewFakeEmbedder`, `cache.NewObjCache`, `objstore.NewLocal`.
- Produces: `func Reindex(ctx context.Context, store memstore.MemStore, emb search.Embedder, embCache cache.Cache, agentID string) error`.

- [ ] **Step 1: Write `internal/maintenance/reindex_test.go`:**
```go
package maintenance

import (
	"context"
	"testing"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/search"
)

// countingEmbedder counts how many texts have been embedded, to prove the second
// reindex re-embeds nothing (all persisted).
type countingEmbedder struct {
	inner search.Embedder
	calls int
}

func (c *countingEmbedder) Model() string { return c.inner.Model() }
func (c *countingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.calls += len(texts)
	return c.inner.Embed(ctx, texts)
}

func TestReindexPersistsEmbeddingsIncrementally(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t) // live PG, unique agent, scoped cleanup
	_, err := store.CreateAgent(ctx, agentID, map[string]string{
		"system/a.md": "# Alpha\nalpha facts here\n# Beta\nbeta facts here\n",
		"notes/n.md":  "# Note\nsome note content\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	ce := &countingEmbedder{inner: search.NewFakeEmbedder(64)}
	embCache := cache.NewObjCache(objstore.NewLocal(t.TempDir()))

	if err := Reindex(ctx, store, ce, embCache, agentID); err != nil {
		t.Fatalf("reindex: %v", err)
	}
	if ce.calls == 0 {
		t.Fatal("first reindex should embed chunks")
	}
	first := ce.calls

	// Second reindex: every chunk's embedding is already persisted → 0 new embeds.
	if err := Reindex(ctx, store, ce, embCache, agentID); err != nil {
		t.Fatalf("reindex 2: %v", err)
	}
	if ce.calls != first {
		t.Fatalf("second reindex embedded %d more texts, want 0 (incremental persistence broken)", ce.calls-first)
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run Reindex` → compile failure.

- [ ] **Step 3: Implement `internal/maintenance/reindex.go`:**
```go
package maintenance

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/search"
)

// Reindex warms the persistent embedding cache for an agent's current HEAD tree.
// It materializes the FULL tree (not just system/, unlike Reflect — search
// indexes everything) and runs BuildSemantic purely for its Put side-effect:
// BuildSemantic looks up each chunk's embedding in embCache and embeds+Puts only
// the misses. Because keys are content-addressed (hash of model+chunk text),
// unchanged chunks are already present, so reindex is incremental for free — no
// diff needed. The returned index is discarded. Reindex writes no ref and takes
// no lock (idempotent).
func Reindex(ctx context.Context, store memstore.MemStore, emb search.Embedder, embCache cache.Cache, agentID string) error {
	head, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		return fmt.Errorf("maintenance: reindex resolve head %s: %w", agentID, err)
	}
	dir, err := os.MkdirTemp("", "engram-reindex-*")
	if err != nil {
		return fmt.Errorf("maintenance: reindex scratch: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := store.Materialize(ctx, agentID, head, dir); err != nil {
		return fmt.Errorf("maintenance: reindex materialize: %w", err)
	}

	files, err := readAllFiles(dir)
	if err != nil {
		return fmt.Errorf("maintenance: reindex read tree: %w", err)
	}
	// BuildSemantic persists missing embeddings into embCache as a side-effect.
	if _, err := search.BuildSemantic(ctx, emb, embCache, files); err != nil {
		return fmt.Errorf("maintenance: reindex build semantic: %w", err)
	}
	return nil
}

// readAllFiles reads every regular file under dir into a path→bytes map, keyed
// by path relative to dir (matching how the session builds its search files).
func readAllFiles(dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
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
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			return rerr
		}
		files[rel] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run Reindex -v -count=1` → PASS. Whole package `ENGRAM_TEST_DB=... go test ./internal/maintenance/ -count=1` (existing Reflect/GC/Drain tests still pass). gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/maintenance/reindex.go internal/maintenance/reindex_test.go
git commit -m "feat(maintenance): Reindex — warm persistent embedding cache from full HEAD tree (incremental, content-addressed)"
```

---

### Task 3: wire Reindex into DrainJobs + cmd/maintenance

**Files:**
- Modify: `internal/maintenance/drain.go`
- Modify: `internal/maintenance/drain_test.go`
- Modify: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `Reindex` (Task 2), `search.Embedder`, `cache.Cache`, `search.NewFakeEmbedder`, `search.NewVoyage`, `cache.NewTiered`/`NewLRU`/`NewObjCache`, `objstore.NewLocal`.
- Produces: `DrainJobs(ctx, r *refs.Refs, store memstore.MemStore, c Completer, emb search.Embedder, embCache cache.Cache, maxAttempts int) (int, error)` (new params `emb`, `embCache`); `processJob(ctx, r, store, c, emb, embCache, job, maxAttempts)`.

- [ ] **Step 1: Modify `processJob` and `DrainJobs` in `internal/maintenance/drain.go`.** Add imports `"github.com/ssy/engram/internal/cache"` and `"github.com/ssy/engram/internal/search"`. Change the two signatures and the reindex branch:

`processJob` signature →
```go
func processJob(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, emb search.Embedder, embCache cache.Cache, job *refs.DequeuedJob, maxAttempts int) error {
```
reindex branch (replace the no-op `case "reindex": return r.CompleteJob(ctx, job.ID)`) →
```go
	case "reindex":
		if err := Reindex(ctx, store, emb, embCache, job.AgentID); err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
```
`DrainJobs` signature + its `processJob` call →
```go
func DrainJobs(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, emb search.Embedder, embCache cache.Cache, maxAttempts int) (int, error) {
	...
		if err := processJob(ctx, r, store, c, emb, embCache, job, maxAttempts); err != nil {
	...
}
```
(Leave the `reflect`, default, `seen`-set, and counting logic unchanged.)

- [ ] **Step 2: Update the two `DrainJobs` call sites in `internal/maintenance/drain_test.go`.** Add imports `"github.com/ssy/engram/internal/cache"`, `"github.com/ssy/engram/internal/search"`, `"github.com/ssy/engram/internal/memstore/objstore"` if missing. At `drain_test.go:103` (in `TestDrainReflectAndReindex`) change to:
```go
	drained, err := DrainJobs(ctx, r, store, fakeDrainCompleter{out: "SUMMARY\n"}, search.NewFakeEmbedder(64), cache.NewObjCache(objstore.NewLocal(t.TempDir())), 5)
```
At `drain_test.go:142` (in `TestDrainReflectRequeuesWhenAgentLockHeld`) change to:
```go
		_, derr := DrainJobs(ctx, r, store, fakeDrainCompleter{out: "X"}, search.NewFakeEmbedder(64), cache.NewObjCache(objstore.NewLocal(t.TempDir())), 5)
```
(`TestDrainReflectAndReindex` already enqueues a `reindex` job; with this change the reindex now runs Reindex over the agent's tree — the agent has `system/about.md`, so it embeds fine; `drained==2` and the empty-queue assertions still hold.)

- [ ] **Step 3: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -count=1 -v` → all pass (Reindex + Drain + Reflect + GC + AgentKey). Then `go test -race ./internal/maintenance/ -count=1`.

- [ ] **Step 4: Update `cmd/maintenance/main.go`.** The current `DrainJobs(ctx, r, store, completer, maxAttempts)` call won't compile. Add the embedder + embedding cache and pass them. Add imports `"github.com/ssy/engram/internal/cache"`, `"github.com/ssy/engram/internal/search"` (objstore already imported). After the `completer` is built, add:
```go
	var emb search.Embedder
	switch env("ENGRAM_PROVIDER", "fake") {
	case "anthropic", "voyage":
		key := os.Getenv("VOYAGE_API_KEY")
		if key == "" {
			log.Fatal("ENGRAM_PROVIDER=anthropic|voyage requires VOYAGE_API_KEY for reindex embeddings")
		}
		emb = search.NewVoyage(key)
	default:
		emb = search.NewFakeEmbedder(0)
	}
	embObjRoot := env("ENGRAM_EMB_OBJ", "./engram-embeddings")
	embCache := cache.NewTiered(cache.NewLRU(4096), cache.NewObjCache(objstore.NewLocal(embObjRoot)))
	if embObjRoot == objRoot {
		log.Fatal("ENGRAM_EMB_OBJ must differ from ENGRAM_OBJ (GC must never sweep the embedding store)")
	}
```
> NOTE to implementer: confirm the GC's git object store variable is named `objRoot` (the `ENGRAM_OBJ` value) by reading main.go; if it's named differently, compare against that. The guard enforces the spec's hard constraint that the embedding store is a distinct root from the GC'd git store.

Change the drain call (inside the WithGlobalLock round) to:
```go
			processed, derr := maintenance.DrainJobs(ctx, r, store, completer, emb, embCache, maxAttempts)
```

- [ ] **Step 5: Build + smoke.**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
ENGRAM_GC_INTERVAL=2s ENGRAM_GC_GRACE=1s ENGRAM_PROVIDER=fake ENGRAM_OBJ="$(mktemp -d)" ENGRAM_EMB_OBJ="$(mktemp -d)" ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" sh -c 'go run ./cmd/maintenance & P=$!; sleep 5; kill $P 2>/dev/null' || true
```
Expected: build/vet/gofmt clean; smoke prints the banner + `gc:` + `jobs: processed=` lines, no crash. Report actual stdout.

- [ ] **Step 6: Commit**
```bash
git add internal/maintenance/drain.go internal/maintenance/drain_test.go cmd/maintenance/main.go
git commit -m "feat(maintenance): reindex job warms persistent embeddings; wire embedder+embCache through DrainJobs"
```

---

### Task 4: request-path wiring — Router embedding cache + cmd/api

**Files:**
- Modify: `internal/agent/router.go`
- Modify: `internal/agent/router_test.go`
- Modify: `cmd/api/main.go`

**Interfaces:**
- Consumes: `cache.Cache`, `cache.NewTiered`/`NewLRU`/`NewObjCache`, `objstore.NewLocal`, `search.NewHybrid`.
- Produces: `agent.NewRouter(store, prov, scratch string, sysCache cache.Cache, embCache cache.Cache, emb search.Embedder) *Router` (new `embCache` param between sysCache and emb).

- [ ] **Step 1: Modify `internal/agent/router.go`.** Add an `embCache cache.Cache` field to `Router`, add the param to `NewRouter`, and use it in `NewHybrid`.

Struct (add field after `cache`):
```go
	cache    cache.Cache
	embCache cache.Cache
	emb      search.Embedder
```
Constructor:
```go
// NewRouter creates a Router that materializes session worktrees under scratch.
// sysCache is the L3 system read cache; embCache is the (separate) persistent
// embedding cache for search — they MUST be different instances so system
// content is not written into the embedding store.
func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string, sysCache cache.Cache, embCache cache.Cache, emb search.Embedder) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, cache: sysCache, embCache: embCache, emb: emb, active: map[string]bool{}}
}
```
NewHybrid call (the `search.NewHybrid(ctx, r.emb, r.cache, files)` line) →
```go
	tools := NewToolset(workdir, agentID, search.NewHybrid(ctx, r.emb, r.embCache, files))
```

- [ ] **Step 2: Update `internal/agent/router_test.go:47`** (the `NewRouter` call) to pass a separate embedding cache:
```go
	return NewRouter(store, prov, t.TempDir(), cache.NewLRU(8), cache.NewLRU(8), search.NewFakeEmbedder(64)), store, agentID
```
(A second LRU is fine for tests — persistence isn't under test here; the search path just needs a working cache.)

- [ ] **Step 3: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/agent/ -count=1` → all pass.

- [ ] **Step 4: Update `cmd/api/main.go:79`.** Build a persistent embedding cache (Tiered LRU + ObjCache over `ENGRAM_EMB_OBJ`) distinct from the system LRU, and pass it. Add imports `"github.com/ssy/engram/internal/memstore/objstore"` if not present (it is) — `cache` is already imported. Before the `NewRouter` call add:
```go
	embObjRoot := env("ENGRAM_EMB_OBJ", "./engram-embeddings")
	embCache := cache.NewTiered(cache.NewLRU(4096), cache.NewObjCache(objstore.NewLocal(embObjRoot)))
```
Change the call:
```go
	router := agent.NewRouter(store, prov, os.TempDir(), cache.NewLRU(1024), embCache, emb)
```
> NOTE: confirm `cmd/api/main.go` has an `env(key, def)` helper (it does — used elsewhere). If the var holding the git object-store root is available, you may optionally add the same `embObjRoot == <gitObjRoot>` guard as Task 3; if the git root isn't a simple local var here, skip the guard in cmd/api (the maintenance worker is where GC runs, so its guard is the load-bearing one).

- [ ] **Step 5: Build + full suite (serialized) + race.**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1 -p 1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/cache/ ./internal/maintenance/ ./internal/agent/ -count=1
```
Expected: build/vet/gofmt clean; all packages pass; no races.

- [ ] **Step 6: Commit**
```bash
git add internal/agent/router.go internal/agent/router_test.go cmd/api/main.go
git commit -m "feat(agent,cmd/api): dedicated persistent embedding cache for search (split from system read cache)"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-24-engram-l4b-index-persistence-design.md`):**
- §3.3 ObjCache (cache.Cache over ObjStore, safeKey) → Task 1 ✅
- §3.4 Tiered (front/back/promote/dual-write) → Task 1 ✅
- §3.5 Reindex (materialize full tree → BuildSemantic discard; no ref/lock) → Task 2 ✅
- §3.6 DrainJobs/processJob +emb,embCache; reindex→Reindex → Task 3 ✅
- §3.7 cmd/maintenance (embedder + Tiered embCache; distinct-root guard) → Task 3 ✅
- §3.8 Router embCache split + cmd/api → Task 4 ✅
- §4 error handling (ObjCache best-effort; Reindex fail→Retry; GC isolation via distinct root) → Tasks 1/3 ✅
- §5 tests (ObjCache round-trip+unsafe key; Tiered; Reindex incremental count=0 on 2nd; drain reindex path; build+smoke) → Tasks 1–4 ✅
- §6 DoD → covered ✅

**Placeholder scan:** No TBD/TODO. Two implementer NOTES (verify the git-object-root var name for the distinct-root guard; optional guard in cmd/api) — concrete, with the load-bearing guard in Task 3.

**Type consistency:** `cache.NewObjCache(objstore.ObjStore)*ObjCache` + `cache.NewTiered(front,back Cache)*Tiered` (Task 1) ↔ used in Tasks 2/3/4. `Reindex(ctx, store, emb search.Embedder, embCache cache.Cache, agentID string) error` (Task 2) ↔ processJob reindex branch (Task 3). `DrainJobs(ctx, r, store, c, emb, embCache, maxAttempts)` (Task 3) ↔ cmd/maintenance call (Task 3, same task). `NewRouter(store, prov, scratch, sysCache, embCache, emb)` (Task 4) ↔ cmd/api + router_test (Task 4, same task). All signature changes and their call sites are updated within the same task, so every task leaves the build green. `BuildSemantic(ctx, emb, c, files)` and `NewHybrid(ctx, emb, c, files)` unchanged (zero edits — the whole point). `embKey`/`encodeVec`/`decodeVec` in semantic.go unchanged.

**Build-green-per-task check:** Task 1 adds new files (green). Task 2 adds Reindex, called by nobody yet (green; tested directly). Task 3 changes DrainJobs sig + its only non-test caller (cmd/maintenance) + its test calls — all in Task 3 (green). Task 4 changes NewRouter sig + its only caller (cmd/api) + its test call — all in Task 4 (green).
