# Engram L5c — Deterministic Defrag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split oversized markdown memory files at top-level headings (deterministically, no LLM) via a background `defrag` job, keeping individual files small.

**Architecture:** `Defrag` materializes an agent's HEAD, splits each oversized + splittable `.md` file (>maxBytes AND ≥2 top-level `#` headings) into sibling `<base>.NN.md` files, removes the original, and commits via CAS with `jobs=nil` (like Reflect). The worker scans all agents each round and idempotently enqueues a `defrag` job for any agent with a splittable oversized file. Splitting converges: each produced part has exactly one top-level heading, so it is never re-split. A `Deps` struct carries the (now three) job-handler dependencies to stop DrainJobs's parameter bloat.

**Tech Stack:** Go 1.25 stdlib (`bytes`, `io/fs`, `path/filepath`, `strings`); existing `internal/maintenance` (Reflect/Reindex/DrainJobs/agentKey/ErrConflict, the `reflectStore`+`advancingStore` test helpers), `internal/memstore` (MemStore: ResolveHead/Materialize/CommitWithCAS, CommitHash, ErrCASConflict), `internal/memstore/refs` (memory_jobs + partial-unique index + isolatedPool test helper), `internal/search`/`internal/cache` (Deps fields), `cmd/maintenance`.

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` first arg; wrap errors with `%w`.
- **Defrag commits with `jobs=nil`** (no self-trigger), via `CommitWithCAS`; CAS conflict → return `ErrConflict` (the existing L5b sentinel) → caller RetryJob. Non-negotiable.
- **Convergence:** `isSplittable` (the predicate used by BOTH the scan and Defrag) requires `.md` + `len>maxBytes` + `len(splitTopLevel)>=2`. Each produced part has exactly one top-level heading, so `splitTopLevel(part)` yields 1 part → not splittable → never re-enqueued/re-split. Defrag makes NO commit when nothing changed (idempotent).
- A top-level heading is a line that, after trimming leading spaces, starts with `"# "` (exactly one `#` then a space). `## `+ are NOT split points.
- Defrag holds the per-agent advisory lock (`r.WithGlobalLock(agentKey(agentID), …)`), like Reflect — maintenance-side singleton, never the foreground writer lock.
- Existing signatures (verified): `memstore.MemStore` — `ResolveHead(ctx, agentID)(CommitHash,error)`, `Materialize(ctx, agentID, at CommitHash, dir)error`, `CommitWithCAS(ctx, agentID, parent CommitHash, dir, jobs []Job)(CommitHash,error)`; `memstore.ErrCASConflict`. `refs` — `New(pool)`, `pool *pgxpool.Pool`, `ClaimJob/CompleteJob/RetryJob/WithGlobalLock/AllHeads`, partial-unique index `memory_jobs_pending_uniq ON memory_jobs (agent_id, kind) WHERE state='pending'`, `isolatedPool(t)` + `InsertPendingJob` test helpers. `maintenance` — `Reflect(ctx, store, c Completer, agentID, fromSHA)`, `Reindex(ctx, store, emb, embCache, agentID)`, `agentKey`, `ErrConflict`, `reflectStore(t)(*memstore.Store,string)` + `advancingStore` (a MemStore wrapper that advances HEAD on first Materialize — in reflect_test.go). Current `DrainJobs(ctx, r, store, c, emb, embCache, maxAttempts)` and `processJob(ctx, r, store, c, emb, embCache, job, maxAttempts)` (to be refactored to Deps).
- cmd/maintenance current round: `WithGlobalLock(gcLockKey, fn)` where fn does GC then `DrainJobs(ctx, r, store, completer, emb, embCache, maxAttempts)`. It builds `completer`, `emb`, `embCache` already.

**Prerequisites:** On `main` (L1–L5b + L4b merged). Branch: `git checkout -b feat/l5c-defrag`. Tasks 1–4 need `ENGRAM_TEST_DB`.
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout:**
```
internal/maintenance/defrag.go        # NEW: splitTopLevel, isSplittable, dirHasSplittable, Defrag
internal/maintenance/defrag_test.go   # NEW
internal/memstore/refs/refs.go        # MODIFY: EnqueueJob (idempotent) + AllAgentIDs
internal/memstore/refs/jobqueue_test.go # MODIFY: add EnqueueJob/AllAgentIDs tests
internal/maintenance/drain.go         # MODIFY: Deps struct; DrainJobs/processJob use Deps; defrag branch
internal/maintenance/drain_test.go    # MODIFY: DrainJobs calls → Deps; add TestDrainDefrag
internal/maintenance/scan.go          # NEW: EnqueueDefrag (scan + idempotent enqueue)
internal/maintenance/scan_test.go     # NEW
cmd/maintenance/main.go               # MODIFY: build Deps; EnqueueDefrag in round; DrainJobs(deps)
```
**Dependency order:** defrag pure+handler (1) → refs EnqueueJob+AllAgentIDs (2) → Deps refactor + defrag branch + cmd Deps wiring (3, keeps build green) → EnqueueDefrag scan + cmd round wiring (4).

---

### Task 1: defrag.go — splitTopLevel, isSplittable, dirHasSplittable, Defrag

**Files:**
- Create: `internal/maintenance/defrag.go`, `internal/maintenance/defrag_test.go`

**Interfaces:**
- Consumes: `memstore.MemStore`, `memstore.ErrCASConflict`, `ErrConflict` (existing in reflect.go), `reflectStore`/`advancingStore` (reflect_test.go).
- Produces: `splitTopLevel(content []byte) [][]byte`; `isSplittable(path string, content []byte, maxBytes int) bool`; `dirHasSplittable(dir string, maxBytes int) (bool, error)`; `Defrag(ctx context.Context, store memstore.MemStore, agentID string, maxBytes int) error`.

- [ ] **Step 1: Write `internal/maintenance/defrag_test.go`:**
```go
package maintenance

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitTopLevel(t *testing.T) {
	parts := splitTopLevel([]byte("# A\nalpha\n# B\nbeta\n"))
	if len(parts) != 2 {
		t.Fatalf("want 2 parts, got %d: %q", len(parts), parts)
	}
	if !bytes.HasPrefix(parts[0], []byte("# A")) || !bytes.HasPrefix(parts[1], []byte("# B")) {
		t.Fatalf("parts mis-split: %q", parts)
	}
	// preamble becomes part[0]
	pp := splitTopLevel([]byte("intro line\n# A\nx\n"))
	if len(pp) != 2 || !bytes.HasPrefix(pp[0], []byte("intro")) {
		t.Fatalf("preamble split wrong: %q", pp)
	}
	// '##' is not a top-level split point
	if got := splitTopLevel([]byte("# A\n## sub\nx\n")); len(got) != 1 {
		t.Fatalf("## must not split: %d parts", len(got))
	}
	// single heading → 1 part (not splittable)
	if got := splitTopLevel([]byte("# only\nbody\n")); len(got) != 1 {
		t.Fatalf("single heading → 1 part, got %d", len(got))
	}
	// empty → 0 parts
	if got := splitTopLevel(nil); len(got) != 0 {
		t.Fatalf("empty → 0 parts, got %d", len(got))
	}
}

func TestIsSplittable(t *testing.T) {
	big := []byte("# A\n" + strings.Repeat("x", 40) + "\n# B\n" + strings.Repeat("y", 40) + "\n")
	if !isSplittable("notes/big.md", big, 50) {
		t.Fatal("big .md with 2 headings >50 should be splittable")
	}
	if isSplittable("notes/big.txt", big, 50) {
		t.Fatal("non-.md must not be splittable")
	}
	if isSplittable("notes/small.md", []byte("# A\nx\n# B\ny\n"), 50) {
		t.Fatal("<=maxBytes must not be splittable")
	}
	if isSplittable("notes/one.md", []byte("# A\n"+strings.Repeat("x", 80)+"\n"), 50) {
		t.Fatal("single-heading (1 part) must not be splittable even if big")
	}
}

func TestDefragSplitsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatalf("defrag: %v", err)
	}
	h1, _ := store.ResolveHead(ctx, agentID)
	dir := t.TempDir()
	if err := store.Materialize(ctx, agentID, h1, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "notes", "big.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("original big.md should be gone")
	}
	p1, err1 := os.ReadFile(filepath.Join(dir, "notes", "big.01.md"))
	p2, err2 := os.ReadFile(filepath.Join(dir, "notes", "big.02.md"))
	if err1 != nil || err2 != nil {
		t.Fatalf("split files missing: %v %v", err1, err2)
	}
	if !bytes.HasPrefix(p1, []byte("# Alpha")) || !bytes.HasPrefix(p2, []byte("# Beta")) {
		t.Fatalf("split content wrong: %q %q", p1, p2)
	}
	// Idempotent: parts each have 1 heading → not splittable → no new commit.
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatalf("defrag 2: %v", err)
	}
	h2, _ := store.ResolveHead(ctx, agentID)
	if h2 != h1 {
		t.Fatal("second defrag must not commit (converged)")
	}
}

func TestDefragNoSplittableNoCommit(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "# A\nsmall\n"})
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatal(err)
	}
	h, _ := store.ResolveHead(ctx, agentID)
	if h != head {
		t.Fatal("no splittable file → no commit, HEAD unchanged")
	}
}

func TestDefragConflictReturnsErrConflict(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"notes/big.md": big})
	_ = head
	adv := &advancingStore{Store: store} // advances HEAD during Materialize → CAS conflict
	if err := Defrag(ctx, adv, agentID, 50); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run 'Split|IsSplittable|Defrag'` → compile failure.

- [ ] **Step 3: Implement `internal/maintenance/defrag.go`:**
```go
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
// part[0] if it has non-whitespace content. A line is a top-level heading iff it
// starts with "# " after trimming leading spaces ('##'+ are not split points).
// Fewer than 2 parts ⇒ not splittable.
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
		// cheap pre-filter by name+size before reading the whole file
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
// conflict returns ErrConflict for the caller to requeue. No lock is taken here
// (the caller holds the per-agent advisory lock).
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

	// Collect splittable files first (don't mutate the tree mid-walk).
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
		return nil // nothing to do; no commit (idempotent)
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
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run 'Split|IsSplittable|Defrag' -v -count=1` → all PASS. Whole package `ENGRAM_TEST_DB=... go test ./internal/maintenance/ -count=1`. `go test -race ./internal/maintenance/ -run Defrag -count=1`. gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/maintenance/defrag.go internal/maintenance/defrag_test.go
git commit -m "feat(maintenance): deterministic defrag — split oversized .md at top-level headings (convergent, jobs=nil)"
```

---

### Task 2: refs.EnqueueJob (idempotent) + refs.AllAgentIDs

**Files:**
- Modify: `internal/memstore/refs/refs.go`
- Test: `internal/memstore/refs/jobqueue_test.go` (add tests; reuse the existing `isolatedPool` helper there)

**Interfaces:**
- Consumes: `Refs` (`pool`), the partial-unique index `(agent_id, kind) WHERE state='pending'`.
- Produces: `(*Refs).EnqueueJob(ctx, agentID, kind, fromSHA string) error` (idempotent); `(*Refs).AllAgentIDs(ctx) ([]string, error)`.

- [ ] **Step 1: Add tests to `internal/memstore/refs/jobqueue_test.go`:**
```go
func TestEnqueueJobIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	if err := r.EnqueueJob(ctx, "a1", "defrag", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := r.EnqueueJob(ctx, "a1", "defrag", "h1"); err != nil { // dup → no-op
		t.Fatal(err)
	}
	var n int
	pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id='a1' AND kind='defrag' AND state='pending'").Scan(&n)
	if n != 1 {
		t.Fatalf("idempotent enqueue should leave 1 pending, got %d", n)
	}
}

func TestAllAgentIDs(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	if _, err := pool.Exec(ctx, "INSERT INTO agent_refs (agent_id, head) VALUES ('a1','h1'),('a2','h2')"); err != nil {
		t.Fatal(err)
	}
	ids, err := r.AllAgentIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["a1"] || !got["a2"] {
		t.Fatalf("want a1,a2; got %v", ids)
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -run 'EnqueueJob|AllAgentIDs'` → compile failure.

- [ ] **Step 3: Implement in `internal/memstore/refs/refs.go`:**
```go
// EnqueueJob idempotently enqueues a pending job out-of-band (not tied to a
// commit). ON CONFLICT against the partial-unique index makes a duplicate
// enqueue (an existing pending job for the same agent+kind) a no-op.
func (r *Refs) EnqueueJob(ctx context.Context, agentID, kind, fromSHA string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)
		 ON CONFLICT (agent_id, kind) WHERE state='pending' DO NOTHING`,
		agentID, kind, fromSHA)
	if err != nil {
		return fmt.Errorf("refs: enqueue job: %w", err)
	}
	return nil
}

// AllAgentIDs returns every agent id with a ref (for maintenance scans).
func (r *Refs) AllAgentIDs(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT agent_id FROM agent_refs`)
	if err != nil {
		return nil, fmt.Errorf("refs: all agent ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("refs: scan agent id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refs: iterate agent ids: %w", err)
	}
	return ids, nil
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -count=1 -v` → all pass (existing + 2 new). gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/memstore/refs/refs.go internal/memstore/refs/jobqueue_test.go
git commit -m "feat(refs): EnqueueJob (idempotent ON CONFLICT) + AllAgentIDs for maintenance scans"
```

---

### Task 3: Deps struct refactor + defrag dispatch + cmd Deps wiring

**Files:**
- Modify: `internal/maintenance/drain.go`
- Modify: `internal/maintenance/drain_test.go`
- Modify: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `Reflect`/`Reindex`/`Defrag` (Tasks 1), `Completer`, `search.Embedder`, `cache.Cache`, `memstore.MemStore`, `agentKey`.
- Produces: `type Deps struct{ Store memstore.MemStore; Completer Completer; Emb search.Embedder; EmbCache cache.Cache; DefragMaxBytes int }`; `DrainJobs(ctx, r *refs.Refs, deps Deps, maxAttempts int) (int, error)`; `processJob(ctx, r *refs.Refs, deps Deps, job *refs.DequeuedJob, maxAttempts int) error`.

- [ ] **Step 1: Rewrite the dispatch in `internal/maintenance/drain.go`.** Add the `Deps` struct (top of file, after imports). Replace `processJob` and `DrainJobs` to take `deps Deps` instead of `store, c, emb, embCache`, and add the `defrag` case. Keep `agentKey`, the doc comments, the `seen` set, and processed-count UNCHANGED.

```go
// Deps carries the per-kind job-handler dependencies for DrainJobs.
type Deps struct {
	Store          memstore.MemStore
	Completer      Completer       // reflect
	Emb            search.Embedder // reindex
	EmbCache       cache.Cache     // reindex
	DefragMaxBytes int             // defrag
}

func processJob(ctx context.Context, r *refs.Refs, deps Deps, job *refs.DequeuedJob, maxAttempts int) error {
	switch job.Kind {
	case "reflect":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Reflect(ctx, deps.Store, deps.Completer, job.AgentID, job.FromSHA)
		})
		if !ran || err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "reindex":
		if err := Reindex(ctx, deps.Store, deps.Emb, deps.EmbCache, job.AgentID); err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "defrag":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Defrag(ctx, deps.Store, job.AgentID, deps.DefragMaxBytes)
		})
		if !ran || err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	default:
		return r.CompleteJob(ctx, job.ID)
	}
}

func DrainJobs(ctx context.Context, r *refs.Refs, deps Deps, maxAttempts int) (int, error) {
	seen := map[int64]struct{}{}
	processed := 0
	for {
		job, err := r.ClaimJob(ctx)
		if err != nil {
			return processed, fmt.Errorf("maintenance: claim: %w", err)
		}
		if job == nil {
			return processed, nil
		}
		if _, dup := seen[job.ID]; dup {
			if err := r.RetryJob(ctx, job.ID, maxAttempts); err != nil {
				return processed, fmt.Errorf("maintenance: requeue %d: %w", job.ID, err)
			}
			return processed, nil
		}
		seen[job.ID] = struct{}{}
		if err := processJob(ctx, r, deps, job, maxAttempts); err != nil {
			return processed, fmt.Errorf("maintenance: process job %d: %w", job.ID, err)
		}
		processed++
	}
}
```
> NOTE: I folded the reflect/defrag `!ran` and `err != nil` branches into one `if !ran || err != nil` (both → RetryJob) — semantically identical to the current separate branches; keep it if you prefer, or keep them separate. Either is fine.

- [ ] **Step 2: Update `internal/maintenance/drain_test.go`.** Replace the two `DrainJobs(ctx, r, store, fakeDrainCompleter{...}, search.NewFakeEmbedder(64), cache.NewObjCache(...), 5)` calls with a `Deps` value. Define a tiny helper at the top of the test file and use it at both sites:
```go
func testDeps(t *testing.T, store *memstore.Store, c Completer) Deps {
	t.Helper()
	return Deps{
		Store:          store,
		Completer:      c,
		Emb:            search.NewFakeEmbedder(64),
		EmbCache:       cache.NewObjCache(objstore.NewLocal(t.TempDir())),
		DefragMaxBytes: 16384,
	}
}
```
At `drain_test.go:105`: `drained, err := DrainJobs(ctx, r, testDeps(t, store, fakeDrainCompleter{out: "SUMMARY\n"}), 5)`.
At `drain_test.go:144`: `_, derr := DrainJobs(ctx, r, testDeps(t, store, fakeDrainCompleter{out: "X"}), 5)`.
(Keep the imports `search`, `cache`, `objstore`, `memstore` — now used by `testDeps`.)

Then ADD a defrag drain test:
```go
func TestDrainDefrag(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)
	agentID := "a1"
	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	if err := r.EnqueueJob(ctx, agentID, "defrag", "h"); err != nil {
		t.Fatal(err)
	}
	deps := Deps{Store: store, Completer: fakeDrainCompleter{}, Emb: search.NewFakeEmbedder(64), EmbCache: cache.NewObjCache(objstore.NewLocal(t.TempDir())), DefragMaxBytes: 50}
	processed, err := DrainJobs(ctx, r, deps, 5)
	if err != nil || processed != 1 {
		t.Fatalf("drain: processed=%d err=%v", processed, err)
	}
	if j, _ := r.ClaimJob(ctx); j != nil {
		t.Fatalf("queue should be empty, got %+v", j)
	}
	nh, _ := store.ResolveHead(ctx, agentID)
	dir := t.TempDir()
	if err := store.Materialize(ctx, agentID, nh, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "notes", "big.01.md")); err != nil {
		t.Fatalf("defrag via drain should have split big.md: %v", err)
	}
}
```
(Add `"strings"`, `"os"`, `"path/filepath"` to drain_test.go imports if missing.)

- [ ] **Step 3: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -count=1 -v` → all pass (defrag + drain incl TestDrainDefrag + reflect + reindex + GC). `go test -race ./internal/maintenance/ -count=1`.

- [ ] **Step 4: Update `cmd/maintenance/main.go`.** The `DrainJobs(ctx, r, store, completer, emb, embCache, maxAttempts)` call won't compile. Build a `Deps` and pass it. After `embCache` is built, add (need an `envInt` helper — if absent, add the small helper shown):
```go
	deps := maintenance.Deps{
		Store:          store,
		Completer:      completer,
		Emb:            emb,
		EmbCache:       embCache,
		DefragMaxBytes: envInt("ENGRAM_DEFRAG_MAX_BYTES", 16384),
	}
```
Change the drain call inside the round to:
```go
			processed, derr := maintenance.DrainJobs(ctx, r, deps, maxAttempts)
```
Add this helper near `env`/`dur` if `envInt` doesn't already exist:
```go
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		log.Fatalf("invalid %s: %q", key, v)
	}
	return def
}
```
(Ensure `strconv` is imported — it already is from L5b's maxAttempts parse; if the maxAttempts parse used inline strconv, keep it or switch it to `envInt` for consistency — optional.)

- [ ] **Step 5: Build + vet + gofmt**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected clean.

- [ ] **Step 6: Commit**
```bash
git add internal/maintenance/drain.go internal/maintenance/drain_test.go cmd/maintenance/main.go
git commit -m "refactor(maintenance): Deps struct for job handlers; dispatch defrag under per-agent lock"
```

---

### Task 4: EnqueueDefrag scan + wire into the worker round

**Files:**
- Create: `internal/maintenance/scan.go`, `internal/maintenance/scan_test.go`
- Modify: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `refs.(*Refs)` (AllAgentIDs/EnqueueJob), `memstore.MemStore` (ResolveHead/Materialize), `dirHasSplittable` (Task 1).
- Produces: `func EnqueueDefrag(ctx context.Context, r *refs.Refs, store memstore.MemStore, maxBytes int) error`.

- [ ] **Step 1: Write `internal/maintenance/scan_test.go`:**
```go
package maintenance

import (
	"context"
	"strings"
	"testing"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

func TestEnqueueDefragEnqueuesOnlySplittableAgents(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)

	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, "fat", map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateAgent(ctx, "lean", map[string]string{"system/s.md": "# A\nsmall\n"}); err != nil {
		t.Fatal(err)
	}

	if err := EnqueueDefrag(ctx, r, store, 50); err != nil {
		t.Fatalf("scan: %v", err)
	}
	count := func(agentID string) int {
		var n int
		pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id=$1 AND kind='defrag' AND state='pending'", agentID).Scan(&n)
		return n
	}
	if count("fat") != 1 {
		t.Fatalf("fat agent should have a defrag job, got %d", count("fat"))
	}
	if count("lean") != 0 {
		t.Fatalf("lean agent should have no defrag job, got %d", count("lean"))
	}
	// Idempotent: a second scan does not double-enqueue.
	if err := EnqueueDefrag(ctx, r, store, 50); err != nil {
		t.Fatal(err)
	}
	if count("fat") != 1 {
		t.Fatalf("second scan must not double-enqueue, got %d", count("fat"))
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run EnqueueDefrag` → compile failure.

- [ ] **Step 3: Implement `internal/maintenance/scan.go`:**
```go
package maintenance

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

// EnqueueDefrag scans every agent and idempotently enqueues a defrag job for any
// agent whose HEAD tree contains a splittable oversized file. Per-agent errors
// are logged and skipped (best-effort scan; never blocks the round). Enqueue is
// idempotent (EnqueueJob dedups against a pending job).
func EnqueueDefrag(ctx context.Context, r *refs.Refs, store memstore.MemStore, maxBytes int) error {
	ids, err := r.AllAgentIDs(ctx)
	if err != nil {
		return fmt.Errorf("maintenance: defrag scan list agents: %w", err)
	}
	for _, agentID := range ids {
		head, err := store.ResolveHead(ctx, agentID)
		if err != nil {
			log.Printf("defrag scan: resolve head %s: %v", agentID, err)
			continue
		}
		dir, err := os.MkdirTemp("", "engram-defragscan-*")
		if err != nil {
			log.Printf("defrag scan: scratch %s: %v", agentID, err)
			continue
		}
		if err := store.Materialize(ctx, agentID, head, dir); err != nil {
			os.RemoveAll(dir)
			log.Printf("defrag scan: materialize %s: %v", agentID, err)
			continue
		}
		splittable, err := dirHasSplittable(dir, maxBytes)
		os.RemoveAll(dir)
		if err != nil {
			log.Printf("defrag scan: walk %s: %v", agentID, err)
			continue
		}
		if splittable {
			if err := r.EnqueueJob(ctx, agentID, "defrag", string(head)); err != nil {
				log.Printf("defrag scan: enqueue %s: %v", agentID, err)
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run EnqueueDefrag -v -count=1` → PASS. Whole package `ENGRAM_TEST_DB=... go test ./internal/maintenance/ -count=1`.

- [ ] **Step 5: Wire into the round in `cmd/maintenance/main.go`.** Inside the `WithGlobalLock(gcLockKey, …)` callback, AFTER the `gc:` log line and BEFORE the `DrainJobs` call, add the defrag scan (log + ignore error so it never blocks GC/drain):
```go
			if derr := maintenance.EnqueueDefrag(ctx, r, store, deps.DefragMaxBytes); derr != nil {
				log.Printf("defrag scan error: %v", derr)
			}
```

- [ ] **Step 6: Build + smoke + full suite (serialized) + race.**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
ENGRAM_GC_INTERVAL=2s ENGRAM_GC_GRACE=1s ENGRAM_PROVIDER=fake ENGRAM_OBJ="$(mktemp -d)" ENGRAM_EMB_OBJ="$(mktemp -d)" ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" sh -c 'go run ./cmd/maintenance & P=$!; sleep 5; kill $P 2>/dev/null' || true
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1 -p 1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/maintenance/ ./internal/memstore/refs/ -count=1
```
Expected: build/vet/gofmt clean; smoke prints banner + `gc:` + `jobs: processed=` lines (no agents → no defrag), no crash; all packages pass; no races. Report smoke stdout.

- [ ] **Step 7: Commit**
```bash
git add internal/maintenance/scan.go internal/maintenance/scan_test.go cmd/maintenance/main.go
git commit -m "feat(maintenance): per-round defrag scan enqueues splittable agents"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-24-engram-l5c-defrag-design.md`):**
- §3.3 splitTopLevel → Task 1 ✅
- §3.4 isSplittable → Task 1 ✅
- §3.5 Defrag (split `<base>.NN.md`, jobs=nil, no-op if unchanged, ErrConflict) → Task 1 ✅
- §3.6 refs.EnqueueJob (idempotent ON CONFLICT) → Task 2 ✅ (+ AllAgentIDs needed by the scan since AllHeads returns heads not ids)
- §3.7 Deps + DrainJobs/processJob + defrag branch → Task 3 ✅
- §3.8 EnqueueDefrag scan → Task 4 ✅
- §3.9 cmd/maintenance (Deps + ENGRAM_DEFRAG_MAX_BYTES + round scan + DrainJobs(deps)) → Tasks 3 & 4 ✅
- §4 error handling (ErrConflict→Retry; scan best-effort log+skip; convergence) → Tasks 1/3/4 ✅
- §5 tests (splitTopLevel/isSplittable units; Defrag split+idempotent+no-op+conflict; EnqueueJob idempotent; EnqueueDefrag splittable-only+idempotent; DrainDefrag; build+smoke) → Tasks 1–4 ✅
- §6 DoD → covered ✅

**Placeholder scan:** No TBD/TODO. One implementer NOTE (Task 3: the folded `!ran || err` branch is optional). Concrete with code.

**Type consistency:** `Defrag(ctx, store memstore.MemStore, agentID string, maxBytes int) error` (Task 1) ↔ processJob defrag branch (Task 3). `splitTopLevel`/`isSplittable`/`dirHasSplittable` (Task 1) ↔ Defrag (Task 1) + EnqueueDefrag (Task 4). `EnqueueJob(ctx, agentID, kind, fromSHA)` + `AllAgentIDs(ctx)([]string,error)` (Task 2) ↔ EnqueueDefrag (Task 4) + TestDrainDefrag (Task 3). `Deps{Store,Completer,Emb,EmbCache,DefragMaxBytes}` + `DrainJobs(ctx, r, deps, maxAttempts)` + `processJob(ctx, r, deps, job, maxAttempts)` (Task 3) ↔ cmd/maintenance (Task 3) + drain_test (Task 3). `EnqueueDefrag(ctx, r, store, maxBytes)` (Task 4) ↔ cmd round (Task 4). `ErrConflict`/`agentKey`/`Reflect`/`Reindex` reused unchanged.

**Build-green-per-task:** Task 1 new file (green; Defrag uncalled). Task 2 new refs methods (green). Task 3 changes DrainJobs sig + its only non-test caller (cmd/maintenance) + tests together (green). Task 4 adds EnqueueDefrag + wires it additively into the round (green).

**Convergence (the key risk):** isSplittable requires ≥2 top-level-heading parts; each produced `<base>.NN.md` part has exactly one top-level heading ⇒ `splitTopLevel(part)` returns 1 ⇒ not splittable ⇒ scan won't re-enqueue and Defrag won't re-split. A single oversized heading (1 part) is never splittable ⇒ left alone (no infinite loop). Defrag commits only when `len(targets) > 0`. Verified in TestDefragSplitsAndIsIdempotent (2nd run, HEAD unchanged).
