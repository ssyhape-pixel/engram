# Engram L5a — Maintenance Worker + GC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the maintenance worker process + a global mark-sweep GC that deletes unreachable objects older than a grace period, never touching reachable or in-flight objects.

**Architecture:** GC is a pure function `GC(objs, reachable, grace, now)` over the `ObjStore` (extended with `Stat`+`Delete`); `gitfs.ReachableObjects(heads)` computes the reachability closure (commits + trees + blobs + ancestors); `refs.AllHeads` supplies the global roots and `refs.WithGlobalLock` (a Postgres advisory lock) ensures one worker GCs at a time; `cmd/maintenance` runs it on a timer. Grace-period age guards against deleting objects of an in-flight commit (written before its ref CAS).

**Tech Stack:** Go 1.25 stdlib (`time`, `os`); existing L1 `internal/memstore/{objstore,gitfs,refs}` (go-git, pgx); no LLM, no new external deps.

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` first arg on all I/O; wrap errors with `%w`.
- No real external services in tests: GC tests use an in-memory ObjStore; gitfs/objstore tests use the local FS backend; refs tests need `ENGRAM_TEST_DB` and `t.Skip` without it.
- **GC safety invariant (non-negotiable):** GC deletes an object only if it is BOTH unreachable AND older than `grace`. If the reachability set cannot be fully computed (any error), GC for that round MUST NOT run — never sweep against an incomplete reachable set.
- GC is global (reachable = union over ALL agents' HEADs); objects are content-addressed and shared across agents.
- `ObjStore` is the existing interface in `internal/memstore/objstore/objstore.go`; `ErrNotFound` is its sentinel. `Hit`/`Search` are unrelated (search package).
- Advisory-lock keys and agent ids in tests MUST be unique per test (the test DB is shared across packages; global TRUNCATE was removed — tests scope to their own agent id and a unique lock key).

**Prerequisites:** On `main` (L1–L4 + test-isolation merged). Branch before Task 1: `git checkout -b feat/l5a-maintenance-gc`. Tasks 1, 2, 4 need NO database. Task 3 (refs) needs `ENGRAM_TEST_DB`; Task 5 builds only.
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout:**
```
internal/memstore/objstore/objstore.go   # MODIFY: add Stat + Delete to interface
internal/memstore/objstore/local.go      # MODIFY: implement Stat + Delete
internal/memstore/objstore/local_test.go # MODIFY: add Stat/Delete tests
internal/memstore/gitfs/reach.go         # NEW: ReachableObjects
internal/memstore/gitfs/reach_test.go    # NEW
internal/memstore/refs/refs.go           # MODIFY: AllHeads + WithGlobalLock
internal/memstore/refs/refs_test.go      # MODIFY: add tests
internal/maintenance/gc.go               # NEW: GC + Stats
internal/maintenance/gc_test.go          # NEW (in-memory ObjStore)
cmd/maintenance/main.go                  # NEW: periodic worker
```
**Dependency order:** objstore Stat/Delete (1) → gitfs.ReachableObjects (2) → refs.AllHeads/WithGlobalLock (3) → maintenance.GC (4, needs 1) → cmd/maintenance (5, needs all).

---

### Task 1: ObjStore `Stat` + `Delete`

**Files:**
- Modify: `internal/memstore/objstore/objstore.go` (interface)
- Modify: `internal/memstore/objstore/local.go` (Local impl)
- Test: `internal/memstore/objstore/local_test.go`

**Interfaces:**
- Consumes: existing `ObjStore`, `ErrNotFound`, `Local`, `NewLocal`.
- Produces: `ObjStore.Stat(ctx, key) (time.Time, error)` (missing → ErrNotFound), `ObjStore.Delete(ctx, key) error` (missing → nil, idempotent).

- [ ] **Step 1: Write the failing tests** — append to `internal/memstore/objstore/local_test.go`:
```go
func TestLocalStatReturnsModTime(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	before := time.Now().Add(-time.Second)
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	mt, err := s.Stat(ctx, "k")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mt.Before(before) {
		t.Fatalf("mtime %v older than %v", mt, before)
	}
}

func TestLocalStatMissing(t *testing.T) {
	_, err := newLocal(t).Stat(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v want ErrNotFound", err)
	}
}

func TestLocalDelete(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	ok, _ := s.Has(ctx, "k")
	if ok {
		t.Fatal("key should be gone after Delete")
	}
}

func TestLocalDeleteMissingIsIdempotent(t *testing.T) {
	if err := newLocal(t).Delete(context.Background(), "nope"); err != nil {
		t.Fatalf("deleting a missing key must be nil, got %v", err)
	}
}
```
(`time` is needed; `errors`/`context` are already imported by the existing tests — add `"time"` to the test file's imports if absent.)

- [ ] **Step 2: Run** `go test ./internal/memstore/objstore/ -run 'Stat|Delete'` → compile failure (Stat/Delete undefined).

- [ ] **Step 3: Add to the interface** — in `internal/memstore/objstore/objstore.go`, add `"time"` to imports and add two methods to the `ObjStore` interface (after `Iter`):
```go
	// Stat returns the object's last-modified time (creation time for our
	// immutable objects); missing key -> ErrNotFound.
	Stat(ctx context.Context, key string) (time.Time, error)
	// Delete removes an object; a missing key is not an error (idempotent).
	Delete(ctx context.Context, key string) error
```

- [ ] **Step 4: Implement on Local** — in `internal/memstore/objstore/local.go`, add `"time"` to imports and:
```go
func (l *Local) Stat(ctx context.Context, key string) (time.Time, error) {
	fi, err := os.Stat(l.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return time.Time{}, ErrNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("objstore: stat %s: %w", key, err)
	}
	return fi.ModTime(), nil
}

func (l *Local) Delete(ctx context.Context, key string) error {
	if err := os.Remove(l.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("objstore: delete %s: %w", key, err)
	}
	return nil
}
```

- [ ] **Step 5: Run** `go test ./internal/memstore/objstore/ -run 'Stat|Delete' -v` → 4 PASS. Then `go build ./...` (the `var _ ObjStore = (*Local)(nil)` assertion now requires the two new methods — confirms Local satisfies the grown interface). `gofmt -l internal/memstore/objstore/`, `go vet ./internal/memstore/objstore/` clean.

- [ ] **Step 6: Commit**
```bash
git add internal/memstore/objstore/
git commit -m "feat(objstore): add Stat (mtime) and Delete to ObjStore"
```

---

### Task 2: `gitfs.ReachableObjects`

**Files:**
- Create: `internal/memstore/gitfs/reach.go`
- Test: `internal/memstore/gitfs/reach_test.go`

**Interfaces:**
- Consumes: `NewStorage(ctx, objs)` + go-git `object`/`plumbing`/`filemode`; `objstore.ObjStore`; `gitfs.Commit`/`Materialize` (tests).
- Produces: `ReachableObjects(ctx context.Context, objs objstore.ObjStore, heads []string) (map[string]struct{}, error)`.

- [ ] **Step 1: Write the failing test** — `internal/memstore/gitfs/reach_test.go`:
```go
package gitfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestReachableIncludesAncestorsExcludesOrphan(t *testing.T) {
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
	write(wA, "system/a.md", "a\n")
	write(wA, "notes/x.md", "x\n")
	hA, err := Commit(ctx, objs, "", wA, "A")
	if err != nil {
		t.Fatal(err)
	}

	wB := t.TempDir()
	if err := Materialize(ctx, objs, hA, wB); err != nil {
		t.Fatal(err)
	}
	write(wB, "notes/x.md", "x2\n")
	hB, err := Commit(ctx, objs, hA, wB, "B")
	if err != nil {
		t.Fatal(err)
	}

	// An orphan object never produced by a commit.
	orphan := "0000000000000000000000000000000000000000"
	if err := objs.Put(ctx, orphan, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	reach, err := ReachableObjects(ctx, objs, []string{hB})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reach[hB]; !ok {
		t.Fatal("HEAD commit hB must be reachable")
	}
	if _, ok := reach[hA]; !ok {
		t.Fatal("ancestor commit hA must be reachable")
	}
	if _, ok := reach[orphan]; ok {
		t.Fatal("orphan object must NOT be reachable")
	}
	// reachable should contain more than just the two commits (trees + blobs).
	if len(reach) < 4 {
		t.Fatalf("expected commits + trees + blobs, got %d objects", len(reach))
	}
}

func TestReachableEmptyHeadIsEmpty(t *testing.T) {
	ctx := context.Background()
	reach, err := ReachableObjects(ctx, objstore.NewLocal(t.TempDir()), []string{""})
	if err != nil {
		t.Fatal(err)
	}
	if len(reach) != 0 {
		t.Fatalf("empty head should yield empty set, got %d", len(reach))
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/memstore/gitfs/ -run Reachable` → compile failure.

- [ ] **Step 3: Implement** `internal/memstore/gitfs/reach.go`:
```go
package gitfs

import (
	"context"
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/ssy/engram/internal/memstore/objstore"
)

// ReachableObjects returns the set of all object hashes reachable from the given
// commits: each commit, its full tree (subtrees + blobs), and all ancestor
// commits via parent links (history is kept for diffability). Empty head
// strings are skipped. An error computing the closure is returned as-is; callers
// (GC) MUST NOT sweep against a partial set.
func ReachableObjects(ctx context.Context, objs objstore.ObjStore, heads []string) (map[string]struct{}, error) {
	st := NewStorage(ctx, objs)
	reachable := map[string]struct{}{}
	seen := map[string]struct{}{} // commits already visited (cycle/dup guard)

	var visit func(h string) error
	visit = func(h string) error {
		if h == "" {
			return nil
		}
		if _, ok := seen[h]; ok {
			return nil
		}
		seen[h] = struct{}{}
		c, err := object.GetCommit(st, plumbing.NewHash(h))
		if err != nil {
			return fmt.Errorf("gitfs: reach commit %s: %w", h, err)
		}
		reachable[h] = struct{}{}
		tree, err := c.Tree()
		if err != nil {
			return fmt.Errorf("gitfs: reach tree of %s: %w", h, err)
		}
		if err := addTree(reachable, tree); err != nil {
			return err
		}
		for _, p := range c.ParentHashes {
			if err := visit(p.String()); err != nil {
				return err
			}
		}
		return nil
	}

	for _, h := range heads {
		if err := visit(h); err != nil {
			return nil, err
		}
	}
	return reachable, nil
}

// addTree adds the tree's own hash, every entry hash (blob or subtree), and
// recurses into subtrees.
func addTree(reachable map[string]struct{}, tree *object.Tree) error {
	reachable[tree.Hash.String()] = struct{}{}
	for _, e := range tree.Entries {
		reachable[e.Hash.String()] = struct{}{}
		if e.Mode == filemode.Dir {
			sub, err := tree.Tree(e.Name)
			if err != nil {
				return fmt.Errorf("gitfs: reach subtree %s: %w", e.Name, err)
			}
			if err := addTree(reachable, sub); err != nil {
				return err
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run** `go test ./internal/memstore/gitfs/ -run Reachable -v` → 2 PASS. Whole package `go test ./internal/memstore/gitfs/`. gofmt/vet clean.
> If go-git v5.19.1 names differ (`c.ParentHashes`, `c.Tree()`, `tree.Entries`, `tree.Tree(name)`, `filemode.Dir`), adjust via `go doc github.com/go-git/go-git/v5/plumbing/object` — these match the L3 TreeKeys usage already in repo.go, so they should be correct.

- [ ] **Step 5: Commit**
```bash
git add internal/memstore/gitfs/reach.go internal/memstore/gitfs/reach_test.go
git commit -m "feat(gitfs): ReachableObjects — reachability closure (commits+trees+blobs+ancestors)"
```

---

### Task 3: `refs.AllHeads` + `refs.WithGlobalLock`

**Files:**
- Modify: `internal/memstore/refs/refs.go`
- Test: `internal/memstore/refs/refs_test.go`

**Interfaces:**
- Consumes: existing `Refs`, `New`, `Bootstrap`; the existing `testPool(t)` helper which now returns `(*pgxpool.Pool, string)` (pool, unique agentID).
- Produces: `(*Refs).AllHeads(ctx) ([]string, error)`; `(*Refs).WithGlobalLock(ctx, key int64, fn func(context.Context) error) (ran bool, err error)`.

- [ ] **Step 1: Write the failing test** — append to `internal/memstore/refs/refs_test.go` (READ the file first to confirm `testPool` returns `(*pgxpool.Pool, string)` after the test-isolation change, and reuse its pattern):
```go
func TestAllHeadsContainsBootstrapped(t *testing.T) {
	ctx := context.Background()
	pool, agentID := testPool(t)
	r := New(pool)
	head := "head-" + agentID // unique per test
	if err := r.Bootstrap(ctx, agentID, head); err != nil {
		t.Fatal(err)
	}
	heads, err := r.AllHeads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, h := range heads {
		if h == head {
			found = true
		}
	}
	if !found {
		t.Fatalf("AllHeads should contain %q; got %v", head, heads)
	}
}

func TestWithGlobalLockMutualExclusion(t *testing.T) {
	ctx := context.Background()
	pool, agentID := testPool(t)
	r := New(pool)
	// Unique advisory-lock key per test (shared DB): derive from agentID.
	var key int64
	for _, c := range agentID {
		key = key*131 + int64(c)
	}
	if key < 0 {
		key = -key
	}

	ran, err := r.WithGlobalLock(ctx, key, func(ctx context.Context) error {
		// While the outer lock is held (on one pooled conn), a second attempt on
		// the SAME key from another conn must fail to acquire.
		inner, ierr := r.WithGlobalLock(ctx, key, func(ctx context.Context) error { return nil })
		if ierr != nil {
			t.Errorf("inner lock errored: %v", ierr)
		}
		if inner {
			t.Error("inner WithGlobalLock acquired the same key while held — must not")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer lock fn errored: %v", err)
	}
	if !ran {
		t.Fatal("outer WithGlobalLock should have acquired and run")
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -run 'AllHeads|GlobalLock'` → compile failure.

- [ ] **Step 3: Implement** — in `internal/memstore/refs/refs.go`, add:
```go
// AllHeads returns every agent's current HEAD — the reachability roots for GC.
func (r *Refs) AllHeads(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT head FROM agent_refs`)
	if err != nil {
		return nil, fmt.Errorf("refs: all heads: %w", err)
	}
	defer rows.Close()
	var heads []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, fmt.Errorf("refs: scan head: %w", err)
		}
		heads = append(heads, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refs: rows: %w", err)
	}
	return heads, nil
}

// WithGlobalLock tries a session-level Postgres advisory lock on key. If
// acquired it runs fn and releases the lock, returning ran=true; if another
// session already holds it, returns ran=false without running fn. Used so only
// one maintenance worker GCs at a time.
func (r *Refs) WithGlobalLock(ctx context.Context, key int64, fn func(context.Context) error) (bool, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("refs: acquire conn: %w", err)
	}
	defer conn.Release()

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		return false, fmt.Errorf("refs: try advisory lock: %w", err)
	}
	if !got {
		return false, nil
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key)

	if err := fn(ctx); err != nil {
		return true, err
	}
	return true, nil
}
```
(`fmt` is already imported in refs.go. `r.pool` is `*pgxpool.Pool`, which has `.Query` and `.Acquire`.)

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -count=1 -v` → all refs tests PASS incl. the 2 new. gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/memstore/refs/refs.go internal/memstore/refs/refs_test.go
git commit -m "feat(refs): AllHeads + WithGlobalLock (advisory lock for maintenance)"
```

---

### Task 4: `maintenance.GC`

**Files:**
- Create: `internal/maintenance/gc.go`
- Test: `internal/maintenance/gc_test.go`

**Interfaces:**
- Consumes: `objstore.ObjStore` (with `Iter`/`Stat`/`Delete` from Task 1), `objstore.ErrNotFound`.
- Produces: `type Stats struct{ Scanned, Swept, Kept int }`; `GC(ctx context.Context, objs objstore.ObjStore, reachable map[string]struct{}, grace time.Duration, now time.Time) (Stats, error)`.

- [ ] **Step 1: Write the failing test** — `internal/maintenance/gc_test.go`:
```go
package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/ssy/engram/internal/memstore/objstore"
)

// memObj is an in-memory ObjStore with settable mtimes, for GC tests.
type memObj struct {
	data map[string][]byte
	mt   map[string]time.Time
}

func newMemObj() *memObj { return &memObj{data: map[string][]byte{}, mt: map[string]time.Time{}} }

func (m *memObj) Has(ctx context.Context, k string) (bool, error) { _, ok := m.data[k]; return ok, nil }
func (m *memObj) Get(ctx context.Context, k string) ([]byte, error) {
	d, ok := m.data[k]
	if !ok {
		return nil, objstore.ErrNotFound
	}
	return d, nil
}
func (m *memObj) Put(ctx context.Context, k string, d []byte) error {
	m.data[k] = d
	if m.mt[k].IsZero() {
		m.mt[k] = time.Now()
	}
	return nil
}
func (m *memObj) Iter(ctx context.Context, fn func(string) error) error {
	for k := range m.data {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}
func (m *memObj) Stat(ctx context.Context, k string) (time.Time, error) {
	t, ok := m.mt[k]
	if !ok {
		return time.Time{}, objstore.ErrNotFound
	}
	return t, nil
}
func (m *memObj) Delete(ctx context.Context, k string) error {
	delete(m.data, k)
	delete(m.mt, k)
	return nil
}

var _ objstore.ObjStore = (*memObj)(nil)

func TestGCSweepsOldOrphansKeepsReachableAndFresh(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m := newMemObj()
	put := func(k string, age time.Duration) { m.data[k] = []byte("x"); m.mt[k] = now.Add(-age) }
	put("reach", 48*time.Hour)         // reachable + very old -> KEEP
	put("oldorphan", 2*time.Hour)      // unreachable + older than grace -> SWEEP
	put("freshorphan", 5*time.Minute)  // unreachable + within grace -> KEEP

	reachable := map[string]struct{}{"reach": {}}
	stats, err := GC(ctx, m, reachable, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Scanned != 3 || stats.Swept != 1 || stats.Kept != 2 {
		t.Fatalf("stats = %+v want {3,1,2}", stats)
	}
	if ok, _ := m.Has(ctx, "oldorphan"); ok {
		t.Fatal("old orphan must be swept")
	}
	if ok, _ := m.Has(ctx, "reach"); !ok {
		t.Fatal("reachable object must be kept")
	}
	if ok, _ := m.Has(ctx, "freshorphan"); !ok {
		t.Fatal("fresh orphan must be kept (within grace)")
	}
}

func TestGCEmptyStore(t *testing.T) {
	stats, err := GC(context.Background(), newMemObj(), map[string]struct{}{}, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{}) {
		t.Fatalf("empty store stats = %+v want zero", stats)
	}
}
```

- [ ] **Step 2: Run** `go test ./internal/maintenance/ -run GC` → compile failure.

- [ ] **Step 3: Implement** `internal/maintenance/gc.go`:
```go
// Package maintenance holds the off-critical-path maintenance operations. GC is
// a global mark-sweep: delete objects that are both unreachable and older than a
// grace period (the grace guards in-flight commits whose objects are written
// before their ref CAS). It is a pure function over an ObjStore + a precomputed
// reachability set, so it is fully testable with an in-memory store.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ssy/engram/internal/memstore/objstore"
)

type Stats struct {
	Scanned int
	Swept   int
	Kept    int
}

// GC deletes objects that are unreachable AND older than grace (measured against
// now). Callers MUST pass a COMPLETE reachable set — never call GC if reachability
// computation failed. Per-object Delete failures are best-effort (kept, not
// aborted). Returns an error only if iterating the store itself fails.
func GC(ctx context.Context, objs objstore.ObjStore, reachable map[string]struct{}, grace time.Duration, now time.Time) (Stats, error) {
	var s Stats
	// Snapshot keys before mutating (deleting during Iter is unsafe for some backends).
	var keys []string
	if err := objs.Iter(ctx, func(k string) error {
		keys = append(keys, k)
		return nil
	}); err != nil {
		return s, fmt.Errorf("maintenance: iter objects: %w", err)
	}
	for _, k := range keys {
		s.Scanned++
		if _, ok := reachable[k]; ok {
			s.Kept++
			continue
		}
		mt, err := objs.Stat(ctx, k)
		if err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue // already gone since the snapshot
			}
			s.Kept++ // stat error: be conservative, keep
			continue
		}
		if now.Sub(mt) > grace {
			if err := objs.Delete(ctx, k); err != nil {
				s.Kept++ // delete failed: keep (best effort)
				continue
			}
			s.Swept++
		} else {
			s.Kept++
		}
	}
	return s, nil
}
```

- [ ] **Step 4: Run** `go test ./internal/maintenance/ -run GC -v` → 2 PASS. Then `go test -race ./internal/maintenance/ -count=1`. gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/maintenance/
git commit -m "feat(maintenance): global mark-sweep GC with grace-period safety"
```

---

### Task 5: `cmd/maintenance` periodic worker

**Files:**
- Create: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `refs.New`/`refs.Migrate`/`(*Refs).AllHeads`/`(*Refs).WithGlobalLock`, `gitfs.ReachableObjects`, `maintenance.GC`, `objstore.NewLocal`, `pgxpool`.
- Produces: a runnable maintenance binary (no exported API).

- [ ] **Step 1: Implement** `cmd/maintenance/main.go`:
```go
// Command maintenance is the off-critical-path maintenance worker. On a timer it
// acquires a global advisory lock and runs GC (mark-sweep of unreachable objects
// older than the grace period) across all agents. Dev/L5a scope: GC only.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/maintenance"
	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

const gcLockKey int64 = 1

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dur(key, def string) time.Duration {
	d, err := time.ParseDuration(env(key, def))
	if err != nil {
		log.Fatalf("invalid duration for %s: %v", key, err)
	}
	return d
}

func main() {
	ctx := context.Background()
	dsn := env("ENGRAM_DB", "postgres://postgres:engram@localhost:5433/engram?sslmode=disable")
	objRoot := env("ENGRAM_OBJ", "./engram-objects")
	interval := dur("ENGRAM_GC_INTERVAL", "5m")
	grace := dur("ENGRAM_GC_GRACE", "1h")

	if err := refs.Migrate(dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	r := refs.New(pool)
	objs := objstore.NewLocal(objRoot)

	log.Printf("maintenance worker started: interval=%s grace=%s obj=%s", interval, grace, objRoot)
	for {
		ran, err := r.WithGlobalLock(ctx, gcLockKey, func(ctx context.Context) error {
			heads, err := r.AllHeads(ctx)
			if err != nil {
				return err
			}
			reachable, err := gitfs.ReachableObjects(ctx, objs, heads)
			if err != nil {
				return err // do NOT GC against a partial reachable set
			}
			stats, err := maintenance.GC(ctx, objs, reachable, grace, time.Now())
			if err != nil {
				return err
			}
			log.Printf("gc: agents=%d scanned=%d swept=%d kept=%d", len(heads), stats.Scanned, stats.Swept, stats.Kept)
			return nil
		})
		if err != nil {
			log.Printf("gc round error: %v", err)
		} else if !ran {
			log.Printf("gc: another worker holds the lock; skipping this round")
		}
		time.Sleep(interval)
	}
}
```

- [ ] **Step 2: Build + vet + gofmt**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected: build OK, vet clean, gofmt nothing.

- [ ] **Step 3: Manual smoke (needs live PG)** — seed an agent + an old orphan object, run one round, confirm the orphan is swept:
```bash
# (optional) the worker loops forever; run it briefly then Ctrl-C, or rely on the unit tests.
ENGRAM_GC_INTERVAL=2s ENGRAM_GC_GRACE=1s ENGRAM_OBJ="$(mktemp -d)" ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" timeout 5 go run ./cmd/maintenance || true
```
Expected: prints the worker banner and at least one `gc: agents=… scanned=… swept=… kept=…` line.

- [ ] **Step 4: Full suite + race**
```bash
go build ./...
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/maintenance/ ./internal/memstore/objstore/ ./internal/memstore/gitfs/ -count=1
```
Expected: build OK, all packages pass, no races.

- [ ] **Step 5: Commit**
```bash
git add cmd/maintenance/main.go
git commit -m "feat(cmd/maintenance): periodic GC worker (advisory lock + grace)"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-23-engram-l5a-maintenance-gc-design.md`):**
- §3.3 ObjStore Stat+Delete (local impl, mtime, idempotent delete) → Task 1 ✅
- §3.4 gitfs.ReachableObjects (commit+tree+blob+ancestors, empty-head skip, error propagation) → Task 2 ✅
- §3.5 refs.AllHeads + WithGlobalLock (advisory lock, ran=false on contention) → Task 3 ✅
- §3.6 maintenance.GC (pure, reachable-set input, grace+now, best-effort delete, Stats) → Task 4 ✅
- §3.7 cmd/maintenance (env, advisory lock, AllHeads→Reachable→GC, skip on partial reachable) → Task 5 ✅
- §4 error handling (%w; lock-not-acquired skip; per-object delete best-effort; reachable-fail aborts the round — enforced in Task 5's `return err` before GC) → Tasks 3/4/5 ✅
- §5 tests (objstore Stat/Delete; ReachableObjects ancestors+orphan+empty; GC three object classes + empty; refs AllHeads+lock mutual exclusion; cmd build+smoke) → Tasks 1–5 ✅
- §6 DoD: periodic GC, global reachability, grace safety, never sweep on partial set, pure-fn GC tested in-memory, ObjStore extended, refs verified on live PG, full suite+race → covered ✅

**Placeholder scan:** No TBD/TODO. The go-git convergence note in Task 2 is a `go doc` fallback (the names already match repo.go's TreeKeys), not a placeholder. Task 5's smoke step is optional/manual (a `main`), with the unit tests as the real gate.

**Type consistency:** `ObjStore.Stat(ctx,key)(time.Time,error)` + `Delete(ctx,key)error` defined Task 1, consumed by `maintenance.GC` (Task 4 via `objs.Stat`/`objs.Delete`) and the in-memory `memObj`, and by `cmd` (Task 5). `ReachableObjects(ctx,objs,[]string)(map[string]struct{},error)` (Task 2) → cmd (Task 5). `AllHeads(ctx)([]string,error)` + `WithGlobalLock(ctx,int64,func(ctx)error)(bool,error)` (Task 3) → cmd (Task 5). `GC(ctx,objs,reachable,grace,now)(Stats,error)` + `Stats{Scanned,Swept,Kept}` (Task 4) → cmd (Task 5). `testPool(t)(*pgxpool.Pool,string)` is the post-isolation signature — Task 3's tests use both returns. The `gcLockKey int64 = 1` constant lives only in cmd; tests use unique per-test keys (no collision with the worker's key under the shared test DB, since tests derive keys from their unique agentID).

**Build-ordering check:** Task 1 grows the `ObjStore` interface; the only implementer is `Local` (updated in the same task) — `go build ./...` in Task 1 Step 5 confirms nothing else breaks (gitfs/memstore use ObjStore but don't implement it). Tasks 2–4 are additive (new funcs/new package). Task 5 is a new `main`. No intermediate broken state. The `maintenance` test's `memObj` implements the full grown interface (6 methods).
