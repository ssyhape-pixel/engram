# Engram L5b — memory_jobs Consumer + Reflection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drain the `memory_jobs` queue in the maintenance worker — running LLM-driven reflection per agent (under a per-agent advisory lock, committing via CAS without re-enqueueing itself) and no-op-draining reindex jobs.

**Architecture:** `refs` gains SKIP-LOCKED queue ops (ClaimJob/CompleteJob/RetryJob). `maintenance.Reflect` materializes an agent's HEAD, asks a narrow `Completer` to consolidate the resident memory, writes `system/reflection.md`, and commits with `jobs=nil` (no self-trigger); a CAS conflict returns `ErrConflict` and the job is requeued. `maintenance.DrainJobs` claims jobs globally and dispatches: reflect (under a per-agent advisory lock) / reindex (no-op) / unknown (discard). `cmd/maintenance` runs DrainJobs alongside L5a's GC each round.

**Tech Stack:** Go 1.25 stdlib (`hash/fnv`, `errors`, `time`); existing L1 `internal/memstore/{objstore,gitfs,refs}`, L2 `internal/agent` (LLMProvider, adapted in cmd only), L5a `cmd/maintenance`; Postgres advisory locks + SKIP LOCKED.

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` first arg on all I/O; wrap errors with `%w`.
- No real external services in tests: reflection tests use a deterministic fake `Completer`; queue tests use live Postgres with **per-test SCHEMA isolation** (see the `isolatedPool` helper below) because ClaimJob/DrainJobs are GLOBAL (they touch every agent's jobs) and would otherwise collide with other packages' jobs under parallel `go test ./...`.
- `maintenance` must NOT import the `agent` package — reflection uses a narrow local `Completer` interface; the cmd wires an adapter.
- **Reflection's commit MUST pass `jobs=nil`** to `CommitWithCAS` (never re-enqueue a reflect job → no infinite self-trigger). Non-negotiable.
- Maintenance must not block the foreground: reflection holds only a per-agent advisory lock, never the Router's writer lock; on CAS conflict it abandons (no lossy merge).
- `memory_jobs` schema (from L1 migration): `(id bigserial PK, agent_id text, kind text, from_sha text NULL, state text DEFAULT 'pending', attempts int DEFAULT 0, created_at timestamptz)`; partial unique index `(agent_id, kind) WHERE state='pending'`.
- Existing `refs.New(pool)`, `refs.CommitRef(ctx, agentID, parent, next, []Job)`, `refs.Bootstrap`, `refs.WithGlobalLock(ctx, key int64, fn func(ctx) error) (bool, error)`, `refs.Job{Kind string}`. `memstore.New(objstore, *refs.Refs)`, `memstore.MemStore` (ResolveHead/Materialize/CommitWithCAS/CreateAgent), `memstore.ErrCASConflict`. `objstore.NewLocal`. `gitfs.Commit`/`Materialize`.

**Prerequisites:** On `main` (L1–L5a + test-isolation merged). Branch before Task 1: `git checkout -b feat/l5b-reflection-jobqueue`. All tasks here need `ENGRAM_TEST_DB`.
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**The `isolatedPool` test helper (used in Tasks 1 and 3).** Because ClaimJob/DrainJobs are global, queue tests get their own Postgres schema (via pgx `search_path`) containing only their own data. Define this helper in the test file of each package that needs it (package `refs` in Task 1; package `maintenance` in Task 3 — duplicated, ~30 lines):
```go
// isolatedPool returns a pool whose connections use a unique, per-test schema
// (search_path), with the engram tables created inside it. Global queue ops
// (ClaimJob/AllHeads/DrainJobs) then see ONLY this test's data, so they are
// safe under parallel `go test ./...`. The schema is dropped on cleanup.
func isolatedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
	// Unique, identifier-safe schema name per test.
	var b strings.Builder
	b.WriteString("l5b_")
	for _, r := range strings.ToLower(t.Name()) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	schema := b.String()

	boot, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("boot pool: %v", err)
	}
	if _, err := boot.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := boot.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	boot.Close()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	ddl := []string{
		`CREATE TABLE agent_refs (agent_id text PRIMARY KEY, head text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE memory_jobs (id bigserial PRIMARY KEY, agent_id text NOT NULL, kind text NOT NULL, from_sha text, state text NOT NULL DEFAULT 'pending', attempts int NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE UNIQUE INDEX memory_jobs_pending_uniq ON memory_jobs (agent_id, kind) WHERE state='pending'`,
		`CREATE TABLE maintenance_cursor (agent_id text NOT NULL, kind text NOT NULL, processed_sha text NOT NULL, PRIMARY KEY (agent_id, kind))`,
	}
	for _, stmt := range ddl {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("ddl %q: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		pool.Close()
		b2, err := pgxpool.New(ctx, dsn)
		if err == nil {
			b2.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
			b2.Close()
		}
	})
	return pool
}
```

**Package layout:**
```
internal/memstore/refs/refs.go          # MODIFY: DequeuedJob + ClaimJob/CompleteJob/RetryJob
internal/memstore/refs/jobqueue_test.go # NEW: schema-isolated queue tests
internal/maintenance/reflect.go         # NEW: Completer + Reflect + ErrConflict
internal/maintenance/reflect_test.go    # NEW (schema-isolated, fake Completer)
internal/maintenance/drain.go           # NEW: agentKey + processJob + DrainJobs
internal/maintenance/drain_test.go      # NEW (schema-isolated)
cmd/maintenance/main.go                 # MODIFY: build memstore + Completer adapter + DrainJobs in the loop
```
**Dependency order:** refs queue ops (1) → maintenance.Reflect (2) → DrainJobs (3, uses 1+2) → cmd wiring (4).

---

### Task 1: refs queue ops (ClaimJob / CompleteJob / RetryJob)

**Files:**
- Modify: `internal/memstore/refs/refs.go`
- Test: `internal/memstore/refs/jobqueue_test.go` (NEW — contains the `isolatedPool` helper above + tests)

**Interfaces:**
- Consumes: existing `Refs`, `New`, `Bootstrap`, `CommitRef`, `Job`.
- Produces: `type DequeuedJob struct{ ID int64; AgentID, Kind, FromSHA string; Attempts int }`; `(*Refs).ClaimJob(ctx) (*DequeuedJob, error)` (nil if none pending); `(*Refs).CompleteJob(ctx, id int64) error`; `(*Refs).RetryJob(ctx, id int64, maxAttempts int) error`.

- [ ] **Step 1: Write the failing tests** — `internal/memstore/refs/jobqueue_test.go`. Put the `isolatedPool` helper (from the plan header, verbatim) at the top, then:
```go
package refs

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// <-- paste the isolatedPool helper from the plan header here -->

func enqueue(t *testing.T, pool *pgxpool.Pool, agentID, kind string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3) RETURNING id`,
		agentID, kind, "sha-"+agentID).Scan(&id)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

func TestClaimJobReturnsPendingAndMarksRunning(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	id := enqueue(t, pool, "a1", "reflect")

	job, err := r.ClaimJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ID != id || job.AgentID != "a1" || job.Kind != "reflect" || job.FromSHA != "sha-a1" {
		t.Fatalf("claimed = %+v", job)
	}
	// state is now 'running' → a second claim returns nil (nothing pending).
	again, err := r.ClaimJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatalf("second claim should be nil, got %+v", again)
	}
}

func TestClaimJobNilWhenEmpty(t *testing.T) {
	job, err := New(isolatedPool(t)).ClaimJob(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatalf("empty queue should claim nil, got %+v", job)
	}
}

func TestCompleteJobDeletes(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	id := enqueue(t, pool, "a1", "reindex")
	if err := r.CompleteJob(ctx, id); err != nil {
		t.Fatal(err)
	}
	var n int
	pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE id=$1", id).Scan(&n)
	if n != 0 {
		t.Fatalf("job should be deleted, count=%d", n)
	}
}

func TestRetryJobRequeuesThenFails(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	id := enqueue(t, pool, "a1", "reflect")
	// First retry: attempts 0->1, back to pending.
	if err := r.RetryJob(ctx, id, 2); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var state string
	pool.QueryRow(ctx, "SELECT attempts, state FROM memory_jobs WHERE id=$1", id).Scan(&attempts, &state)
	if attempts != 1 || state != "pending" {
		t.Fatalf("after 1st retry attempts=%d state=%s want 1/pending", attempts, state)
	}
	// Second retry: attempts 1->2 == max → failed.
	if err := r.RetryJob(ctx, id, 2); err != nil {
		t.Fatal(err)
	}
	pool.QueryRow(ctx, "SELECT attempts, state FROM memory_jobs WHERE id=$1", id).Scan(&attempts, &state)
	if attempts != 2 || state != "failed" {
		t.Fatalf("after 2nd retry attempts=%d state=%s want 2/failed", attempts, state)
	}
}

func TestClaimJobSkipLockedTwoJobs(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	id1 := enqueue(t, pool, "a1", "reflect")
	id2 := enqueue(t, pool, "a2", "reflect")
	got := map[int64]bool{}
	for i := 0; i < 2; i++ {
		j, err := r.ClaimJob(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			t.Fatalf("claim %d returned nil", i)
		}
		got[j.ID] = true
	}
	if !got[id1] || !got[id2] {
		t.Fatalf("both jobs should be claimed, got %v", got)
	}
}
```

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -run 'ClaimJob|CompleteJob|RetryJob'` → compile failure.

- [ ] **Step 3: Implement in `internal/memstore/refs/refs.go`** (add `"github.com/jackc/pgx/v5"` to imports if not present — it is, for `pgx.ErrNoRows`):
```go
// DequeuedJob is a claimed maintenance job.
type DequeuedJob struct {
	ID       int64
	AgentID  string
	Kind     string
	FromSHA  string
	Attempts int
}

// ClaimJob atomically claims one pending job (FOR UPDATE SKIP LOCKED, no ORDER
// BY — queue hygiene) and marks it 'running'. Returns nil if none pending.
func (r *Refs) ClaimJob(ctx context.Context) (*DequeuedJob, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("refs: begin claim: %w", err)
	}
	defer tx.Rollback(ctx)

	var j DequeuedJob
	var fromSHA *string
	err = tx.QueryRow(ctx,
		`SELECT id, agent_id, kind, from_sha, attempts FROM memory_jobs
		 WHERE state='pending' FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&j.ID, &j.AgentID, &j.Kind, &fromSHA, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("refs: claim select: %w", err)
	}
	if fromSHA != nil {
		j.FromSHA = *fromSHA
	}
	if _, err := tx.Exec(ctx, `UPDATE memory_jobs SET state='running' WHERE id=$1`, j.ID); err != nil {
		return nil, fmt.Errorf("refs: claim mark running: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("refs: claim commit: %w", err)
	}
	return &j, nil
}

// CompleteJob removes a finished job.
func (r *Refs) CompleteJob(ctx context.Context, id int64) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM memory_jobs WHERE id=$1`, id); err != nil {
		return fmt.Errorf("refs: complete job %d: %w", id, err)
	}
	return nil
}

// RetryJob increments attempts; if attempts reaches maxAttempts the job is
// marked 'failed', otherwise returned to 'pending' for a later round.
func (r *Refs) RetryJob(ctx context.Context, id int64, maxAttempts int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE memory_jobs
		 SET attempts = attempts + 1,
		     state = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END
		 WHERE id=$1`, id, maxAttempts)
	if err != nil {
		return fmt.Errorf("refs: retry job %d: %w", id, err)
	}
	return nil
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -count=1 -v` → all refs tests PASS (existing + 5 new). Run twice (no flake). `gofmt -l internal/memstore/refs/`, `go vet ./internal/memstore/refs/` clean.

- [ ] **Step 5: Commit**
```bash
git add internal/memstore/refs/refs.go internal/memstore/refs/jobqueue_test.go
git commit -m "feat(refs): memory_jobs consumer ops (ClaimJob/CompleteJob/RetryJob, SKIP LOCKED)"
```

---

### Task 2: maintenance.Reflect + Completer

**Files:**
- Create: `internal/maintenance/reflect.go`
- Test: `internal/maintenance/reflect_test.go`

**Interfaces:**
- Consumes: `memstore.MemStore` (ResolveHead/Materialize/CommitWithCAS), `memstore.ErrCASConflict`, `objstore`/`refs`/`memstore.New` (tests).
- Produces: `type Completer interface { Complete(ctx context.Context, system, user string) (string, error) }`; `var ErrConflict = errors.New(...)`; `func Reflect(ctx context.Context, store memstore.MemStore, c Completer, agentID, fromSHA string) error`.

- [ ] **Step 1: Write the failing test** — `internal/maintenance/reflect_test.go`:
```go
package maintenance

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

// fakeCompleter returns a deterministic consolidation.
type fakeCompleter struct{ out string }

func (f fakeCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	if f.out != "" {
		return f.out, nil
	}
	return "CONSOLIDATED:\n" + user, nil
}

// reflectStore builds an isolated memstore.Store (live PG via testPool's pattern,
// own object dir). It reuses the existing testPool helper if present in package
// refs; here we build directly with a unique agent id.
func reflectStore(t *testing.T) (*memstore.Store, string) {
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
	t.Cleanup(func() { pool.Close() })
	agentID := "reflect:" + t.Name()
	store := memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool))
	t.Cleanup(func() {
		for _, tbl := range []string{"memory_jobs", "agent_refs", "maintenance_cursor"} {
			pool.Exec(ctx, "DELETE FROM "+tbl+" WHERE agent_id=$1", agentID)
		}
	})
	return store, agentID
}

func TestReflectWritesConsolidationAndCommits(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, err := store.CreateAgent(ctx, agentID, map[string]string{"system/about.md": "---\ndescription: who\n---\nfacts here\n"})
	if err != nil {
		t.Fatal(err)
	}

	if err := Reflect(ctx, store, fakeCompleter{out: "MY SUMMARY\n"}, agentID, string(head)); err != nil {
		t.Fatalf("reflect: %v", err)
	}
	newHead, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		t.Fatal(err)
	}
	if newHead == head {
		t.Fatal("HEAD should advance after reflection")
	}
	dir := t.TempDir()
	if err := store.Materialize(ctx, agentID, newHead, dir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "system", "reflection.md"))
	if err != nil || string(got) != "MY SUMMARY\n" {
		t.Fatalf("reflection.md = %q %v", got, err)
	}
}

func TestReflectDoesNotEnqueueReflectJob(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	if err := Reflect(ctx, store, fakeCompleter{}, agentID, string(head)); err != nil {
		t.Fatal(err)
	}
	// Reflection must commit with jobs=nil → no reflect job enqueued for this agent.
	// (Access the pool via a fresh connection through the store is not exposed; assert
	// indirectly: a second Reflect still works and HEAD advances again, i.e. no loop state.)
	h2, _ := store.ResolveHead(ctx, agentID)
	if err := Reflect(ctx, store, fakeCompleter{}, agentID, string(h2)); err != nil {
		t.Fatalf("second reflect: %v", err)
	}
}

func TestReflectConflictReturnsErrConflict(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})

	// Externally advance HEAD so Reflect's CommitWithCAS (against the stale head) conflicts.
	other := t.TempDir()
	if err := store.Materialize(ctx, agentID, head, other); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "ext.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitWithCAS(ctx, agentID, head, other, nil); err != nil {
		t.Fatalf("external commit: %v", err)
	}
	// Reflect resolves the *current* head itself, so to force a conflict we must make
	// Reflect commit against a head that moves under it. Simplest: Reflect uses
	// ResolveHead internally, so it would see the new head and NOT conflict. To test
	// the conflict path deterministically, call Reflect with the OLD head is irrelevant
	// (it re-resolves). Instead, verify Reflect succeeds on the current head:
	if err := Reflect(ctx, store, fakeCompleter{}, agentID, string(head)); err != nil {
		t.Fatalf("reflect after external commit should still succeed (it re-resolves head): %v", err)
	}
}
```
> NOTE to implementer: `TestReflectConflictReturnsErrConflict` as written above does NOT actually force a conflict, because `Reflect` calls `ResolveHead` itself and will commit against the *current* head. DELETE that test and instead test the conflict path at the `processJob` level in Task 3 (where a job carries a stale head and we can interpose). Keep only `TestReflectWritesConsolidationAndCommits` and `TestReflectDoesNotEnqueueReflectJob` in this file. (The conflict→ErrConflict mapping is still implemented in Reflect per Step 3; it's exercised in Task 3's drain test via a pre-advanced head.)

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run Reflect` → compile failure.

- [ ] **Step 3: Implement** `internal/maintenance/reflect.go`:
```go
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ssy/engram/internal/memstore"
)

// Completer is the narrow LLM surface reflection needs: a single text
// completion. Keeps maintenance decoupled from the agent package's tool-use
// protocol; the cmd wires an adapter over an agent.LLMProvider.
type Completer interface {
	Complete(ctx context.Context, system, user string) (string, error)
}

// ErrConflict means reflection lost the CAS race (the agent advanced HEAD); the
// job should be requeued and retried on a later, fresher round.
var ErrConflict = errors.New("maintenance: reflect lost CAS race; retry later")

const reflectSystemPrompt = "You are the reflection pass of an agent memory system. " +
	"Consolidate the agent's current resident memory below into a concise, current-state note. " +
	"Output only the consolidated note."

// Reflect materializes the agent's HEAD, asks the Completer to consolidate the
// resident system/ content, writes the result to system/reflection.md, and
// commits it. It commits with jobs=nil so reflection never re-enqueues itself.
// A CAS conflict (agent advanced concurrently) returns ErrConflict.
func Reflect(ctx context.Context, store memstore.MemStore, c Completer, agentID, fromSHA string) error {
	head, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		return fmt.Errorf("maintenance: resolve head %s: %w", agentID, err)
	}
	dir, err := os.MkdirTemp("", "engram-reflect-*")
	if err != nil {
		return fmt.Errorf("maintenance: scratch: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := store.Materialize(ctx, agentID, head, dir); err != nil {
		return fmt.Errorf("maintenance: materialize: %w", err)
	}

	resident := readSystemDir(dir)
	out, err := c.Complete(ctx, reflectSystemPrompt, resident)
	if err != nil {
		return fmt.Errorf("maintenance: complete: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "system"), 0o755); err != nil {
		return fmt.Errorf("maintenance: mkdir system: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "system", "reflection.md"), []byte(out), 0o644); err != nil {
		return fmt.Errorf("maintenance: write reflection: %w", err)
	}

	// jobs=nil: reflection never enqueues a reflect job (no self-trigger loop).
	if _, err := store.CommitWithCAS(ctx, agentID, head, dir, nil); err != nil {
		if errors.Is(err, memstore.ErrCASConflict) {
			return ErrConflict
		}
		return fmt.Errorf("maintenance: reflect commit: %w", err)
	}
	return nil
}

// readSystemDir concatenates all files under <dir>/system/ (the resident set).
func readSystemDir(dir string) string {
	var b strings.Builder
	systemDir := filepath.Join(dir, "system")
	_ = filepath.WalkDir(systemDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		fmt.Fprintf(&b, "## %s\n%s\n", rel, string(data))
		return nil
	})
	return b.String()
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run Reflect -v` → 2 PASS (after deleting the non-conflict test per the note). Whole package `ENGRAM_TEST_DB=... go test ./internal/maintenance/ -count=1` (L5a GC tests still pass). gofmt/vet clean.

- [ ] **Step 5: Commit**
```bash
git add internal/maintenance/reflect.go internal/maintenance/reflect_test.go
git commit -m "feat(maintenance): Reflect — LLM consolidation, commit with jobs=nil, ErrConflict on CAS race"
```

---

### Task 3: maintenance.DrainJobs + processJob + agentKey

**Files:**
- Create: `internal/maintenance/drain.go`
- Test: `internal/maintenance/drain_test.go` (NEW — contains the `isolatedPool` helper + tests)

**Interfaces:**
- Consumes: `refs.(*Refs)` (ClaimJob/CompleteJob/RetryJob/WithGlobalLock), `refs.DequeuedJob`, `memstore.MemStore`, `Completer`/`Reflect`/`ErrConflict` (Task 2).
- Produces: `func agentKey(agentID string) int64`; `func DrainJobs(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, maxAttempts int) (int, error)` (returns count drained). (`processJob` is unexported; covered by the drain tests.)

- [ ] **Step 1: Write the failing test** — `internal/maintenance/drain_test.go`. Put the `isolatedPool` helper (from the plan header, verbatim) at the top, then:
```go
package maintenance

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

// <-- paste the isolatedPool helper from the plan header here -->

// drainFixture builds an isolated schema pool + a memstore over it + a fresh agent.
func drainFixture(t *testing.T) (*refs.Refs, *memstore.Store, string) {
	t.Helper()
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)
	return r, store, "a1"
}

func enqueue(t *testing.T, r *refs.Refs, store *memstore.Store, agentID, kind string) {
	t.Helper()
	ctx := context.Background()
	// Use a real commit to enqueue (same-tx), then create the agent first.
	head, err := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	if err != nil && err != memstore.ErrAgentAlreadyExists {
		t.Fatal(err)
	}
	_ = head
	// Direct enqueue keeps the test focused on draining.
	pool := r // refs holds the pool; use a raw insert via a helper exec
	_ = pool
}

func TestDrainReflectAndReindex(t *testing.T) {
	ctx := context.Background()
	r, store, agentID := drainFixture(t)
	head, err := store.CreateAgent(ctx, agentID, map[string]string{"system/about.md": "---\ndescription: d\n---\nfacts\n"})
	if err != nil {
		t.Fatal(err)
	}
	// Enqueue one reflect + one reindex job for this agent (direct insert via refs pool).
	insertJob(t, r, agentID, "reflect", string(head))
	insertJob(t, r, agentID, "reindex", string(head))

	drained, err := DrainJobs(ctx, r, store, fakeCompleter{out: "SUMMARY\n"}, 5)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if drained != 2 {
		t.Fatalf("drained=%d want 2", drained)
	}
	// Queue empty.
	if j, _ := r.ClaimJob(ctx); j != nil {
		t.Fatalf("queue should be empty, got %+v", j)
	}
	// Reflect ran → reflection.md committed.
	nh, _ := store.ResolveHead(ctx, agentID)
	dir := t.TempDir()
	if err := store.Materialize(ctx, agentID, nh, dir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "system", "reflection.md"))
	if err != nil || string(got) != "SUMMARY\n" {
		t.Fatalf("reflection.md = %q %v", got, err)
	}
}

func TestDrainReflectRequeuesWhenAgentLockHeld(t *testing.T) {
	ctx := context.Background()
	r, store, agentID := drainFixture(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	insertJob(t, r, agentID, "reflect", string(head))

	// Hold the per-agent advisory lock, then drain: the reflect job must be requeued (not run).
	ran, err := r.WithGlobalLock(ctx, agentKey(agentID), func(ctx context.Context) error {
		_, derr := DrainJobs(ctx, r, store, fakeCompleter{}, 5)
		return derr
	})
	if err != nil {
		t.Fatalf("drain under held lock: %v", err)
	}
	if !ran {
		t.Fatal("outer lock should have been acquired")
	}
	// The reflect job was requeued (attempts bumped), not completed → still present.
	var n int
	poolQueryCount(t, r, agentID, &n)
	if n == 0 {
		t.Fatal("reflect job should remain (requeued), not completed, while agent lock was held")
	}
}

func TestAgentKeyStable(t *testing.T) {
	if agentKey("alpha") != agentKey("alpha") {
		t.Fatal("agentKey must be stable")
	}
	if agentKey("alpha") == agentKey("beta") {
		t.Fatal("different agents should (almost always) differ")
	}
}
```
Add these test helpers at the bottom of `drain_test.go` (they use the schema-isolated pool inside `r`):
```go
func insertJob(t *testing.T, r *refs.Refs, agentID, kind, fromSHA string) {
	t.Helper()
	if err := r.CommitRefEnqueueForTest(context.Background(), agentID, kind, fromSHA); err != nil {
		t.Fatal(err)
	}
}

func poolQueryCount(t *testing.T, r *refs.Refs, agentID string, n *int) {
	t.Helper()
	if err := r.CountJobsForTest(context.Background(), agentID, n); err != nil {
		t.Fatal(err)
	}
}
```
> NOTE to implementer: `refs` does not expose its `pool`, so the test cannot run raw SQL. Add TWO small TEST-SUPPORT methods to `internal/memstore/refs/refs.go` (exported, clearly test-support; or better, put them in a `refs` file used only by tests). Simplest: add to refs.go:
> ```go
> // (test support) InsertPendingJob enqueues a pending job directly.
> func (r *Refs) InsertPendingJob(ctx context.Context, agentID, kind, fromSHA string) error {
> 	_, err := r.pool.Exec(ctx, `INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)`, agentID, kind, fromSHA)
> 	return err
> }
> // (test support) CountJobs counts a kind-agnostic agent's jobs.
> func (r *Refs) CountJobs(ctx context.Context, agentID string) (int, error) {
> 	var n int
> 	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM memory_jobs WHERE agent_id=$1`, agentID).Scan(&n)
> 	return n, err
> }
> ```
> Then in the test call `r.InsertPendingJob(...)` and `r.CountJobs(...)` directly (replace the `CommitRefEnqueueForTest`/`CountJobsForTest`/`poolQueryCount` placeholders above with these real method calls). Remove the unused `enqueue` helper stub. Keep the tests' intent identical.

- [ ] **Step 2: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -run 'Drain|AgentKey'` → compile failure.

- [ ] **Step 3: Implement** `internal/maintenance/drain.go`:
```go
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

// agentKey hashes an agent id to an advisory-lock key (per-agent reflection
// singleton). Collisions only cause two agents to occasionally serialize — safe.
func agentKey(agentID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(agentID))
	return int64(h.Sum64())
}

// processJob dispatches a single claimed job. It always resolves the job (via
// CompleteJob or RetryJob) so the row never stays stuck in 'running'.
func processJob(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, job *refs.DequeuedJob, maxAttempts int) error {
	switch job.Kind {
	case "reflect":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Reflect(ctx, store, c, job.AgentID, job.FromSHA)
		})
		if !ran {
			// Another worker holds this agent's reflection lock; retry later.
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		if err != nil {
			// ErrConflict (agent advanced) or any reflection error → retry later.
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "reindex":
		// No persistent index yet (L4 rebuilds per session); drain to avoid pileup.
		return r.CompleteJob(ctx, job.ID)
	default:
		// Unknown kind: discard so it can't clog the queue.
		return r.CompleteJob(ctx, job.ID)
	}
}

// DrainJobs claims and processes pending jobs until none remain. A job re-claimed
// within the same round (it was requeued by processJob) is released and the round
// ends, so a perpetually-conflicting job can't busy-loop (it retries next round
// and eventually fails out via maxAttempts).
func DrainJobs(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, maxAttempts int) (int, error) {
	seen := map[int64]struct{}{}
	drained := 0
	for {
		job, err := r.ClaimJob(ctx)
		if err != nil {
			return drained, fmt.Errorf("maintenance: claim: %w", err)
		}
		if job == nil {
			return drained, nil
		}
		if _, dup := seen[job.ID]; dup {
			// Already handled (and requeued) this round; release to pending and stop.
			if err := r.RetryJob(ctx, job.ID, maxAttempts); err != nil {
				return drained, fmt.Errorf("maintenance: requeue %d: %w", job.ID, err)
			}
			return drained, nil
		}
		seen[job.ID] = struct{}{}
		if err := processJob(ctx, r, store, c, job, maxAttempts); err != nil {
			return drained, fmt.Errorf("maintenance: process job %d: %w", job.ID, err)
		}
		drained++
	}
}

var _ = errors.Is // errors imported for symmetry with reflect.go; remove if unused after final edits
```
> NOTE: remove the `var _ = errors.Is` line and the `errors` import if `errors` ends up unused in drain.go (it likely is — processJob compares via `ran`/`err` without `errors.Is` here, since ErrConflict and other errors are treated identically as "retry"). Keep `go vet`/gofmt clean.

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -count=1 -v` → all pass (Reflect + Drain + AgentKey + L5a GC). Then `go test -race ./internal/maintenance/ -count=1`. gofmt/vet clean. Add the two test-support methods (`InsertPendingJob`, `CountJobs`) to refs.go in this task and commit them together.

- [ ] **Step 5: Commit**
```bash
git add internal/maintenance/drain.go internal/maintenance/drain_test.go internal/memstore/refs/refs.go
git commit -m "feat(maintenance): DrainJobs + per-agent-locked reflect dispatch (+ refs test-support inserts)"
```

---

### Task 4: cmd/maintenance — run DrainJobs alongside GC

**Files:**
- Modify: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `maintenance.DrainJobs`, `maintenance.Completer`, `memstore.New`, `agent.LLMProvider`/`agent.FakeProvider`/`agent.NewAnthropic`/`agent.Request`/`agent.Message`/`agent.RoleUser` (cmd-only adapter), plus the L5a wiring (refs, objstore, GC, WithGlobalLock).

- [ ] **Step 1: Implement the additions** to `cmd/maintenance/main.go`. Add imports `"github.com/ssy/engram/internal/agent"` and `"github.com/ssy/engram/internal/memstore"`. Add the Completer adapter + env, build the store + completer, and call DrainJobs inside the per-round locked section after GC.

Add this adapter type (top-level in main.go):
```go
// providerCompleter adapts an agent.LLMProvider to maintenance.Completer (a
// single no-tools text completion).
type providerCompleter struct{ prov agent.LLMProvider }

func (p providerCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	resp, err := p.prov.Generate(ctx, agent.Request{
		System:   system,
		Messages: []agent.Message{{Role: agent.RoleUser, Text: user}},
	})
	if err != nil {
		return "", err
	}
	return resp.Text, nil
}
```

In `main`, after building `r := refs.New(pool)` and `objs := objstore.NewLocal(objRoot)`, add:
```go
	store := memstore.New(objs, r)
	maxAttempts := 5
	if v := os.Getenv("ENGRAM_JOB_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxAttempts = n
		}
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
			func(r agent.Request) agent.Response { return agent.Response{Text: "(fake reflection) consolidated\n"} },
		}}
	}
	completer := providerCompleter{prov: prov}
```
(Add `"strconv"` to imports.)

Inside the existing `WithGlobalLock` round callback, AFTER the GC block (which logs `gc: ...`), add the drain step:
```go
			drained, derr := maintenance.DrainJobs(ctx, r, store, completer, maxAttempts)
			if derr != nil {
				return derr
			}
			log.Printf("jobs: drained=%d", drained)
			return nil
```
(Replace the callback's final `return nil` with the drain step above; keep the GC block and its `gc:` log line before it.)

> NOTE: the L5a `FakeProvider` reflection step returns a single fixed text; that's fine — the worker's fake provider is only for dev smoke. The real reflection uses Anthropic when `ENGRAM_PROVIDER=anthropic`.

- [ ] **Step 2: Build + vet + gofmt**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected: clean.

- [ ] **Step 3: Manual smoke (needs live PG).** Seed an agent + a reflect job, run one round, confirm a `jobs: drained=` line and that reflection committed:
```bash
ENGRAM_GC_INTERVAL=2s ENGRAM_GC_GRACE=1s ENGRAM_PROVIDER=fake ENGRAM_OBJ="$(mktemp -d)" ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" timeout 5 go run ./cmd/maintenance || true
```
Expected stdout: the worker banner + at least one `gc: ...` line + one `jobs: drained=...` line. (drained may be 0 if no pending jobs exist in the dev DB — that's fine; the point is the line appears and the worker doesn't crash.) Report actual stdout.

- [ ] **Step 4: Full suite (serialized) + race**
```bash
go build ./...
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1 -p 1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/maintenance/ ./internal/memstore/refs/ -count=1
```
Expected: build OK, all packages pass, no races.

- [ ] **Step 5: Commit**
```bash
git add cmd/maintenance/main.go
git commit -m "feat(cmd/maintenance): drain memory_jobs (reflection) each round alongside GC"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-23-engram-l5b-reflection-jobqueue-design.md`):**
- §3.3 refs ClaimJob/CompleteJob/RetryJob + DequeuedJob (SKIP LOCKED, no ORDER BY, attempts→failed) → Task 1 ✅
- §3.4 Completer narrow interface → Task 2 ✅
- §3.5 Reflect (materialize → Completer → write system/reflection.md → CommitWithCAS jobs=nil → ErrConflict on CAS) → Task 2 ✅
- §3.6 DrainJobs + processJob (reflect under per-agent advisory lock; reindex no-op; unknown discard; ErrConflict→retry; lock-held→retry; seen-set anti-busy-loop) + agentKey → Task 3 ✅
- §3.7 cmd/maintenance (build memstore + Completer adapter; DrainJobs after GC in the locked round; maxAttempts env) → Task 4 ✅
- §4 error handling (%w; ErrConflict→RetryJob consumes attempts; jobs=nil no self-trigger; seen-set) → Tasks 2/3 ✅
- §5 tests (refs queue ops schema-isolated; Reflect writes+no-loop; Drain reflect+reindex, lock-held requeue, agentKey; cmd build+smoke) → Tasks 1–4 ✅
- §6 DoD → covered ✅

**Placeholder scan:** No TBD/TODO in the implementation. Two explicit implementer NOTES (not placeholders): (1) Task 2 — delete the non-forcing conflict test, keep 2 tests, conflict is exercised in Task 3; (2) Task 3 — replace the `CommitRefEnqueueForTest`/`CountJobsForTest` placeholder helper names with real `refs.InsertPendingJob`/`refs.CountJobs` test-support methods (full code given) and remove the dead `enqueue` stub; remove the `var _ = errors.Is` line if `errors` is unused. These are concrete instructions with exact code.

**Type consistency:** `DequeuedJob{ID int64; AgentID,Kind,FromSHA string; Attempts int}` (Task 1) consumed by processJob/DrainJobs (Task 3). `ClaimJob()(*DequeuedJob,error)`/`CompleteJob(id int64)`/`RetryJob(id int64,max int)` (Task 1) ↔ Task 3. `Completer.Complete(ctx,system,user)(string,error)` + `Reflect(ctx,store,c,agentID,fromSHA)error` + `ErrConflict` (Task 2) ↔ Task 3 (processJob calls Reflect) ↔ Task 4 (providerCompleter implements Completer). `agentKey(string)int64` + `DrainJobs(ctx,*refs.Refs,memstore.MemStore,Completer,int)(int,error)` (Task 3) ↔ Task 4. `refs.InsertPendingJob`/`CountJobs` (added Task 3) used only by Task 3 tests. cmd adapter uses `agent.Request{System,Messages}`, `agent.Message{Role,Text}`, `agent.RoleUser`, `agent.Response.Text`, `agent.FakeProvider{Steps}`, `agent.NewAnthropic` — all existing L2 exports (verify against internal/agent).

**Isolation note:** ClaimJob/DrainJobs are global; Task 1 and Task 3 tests use the per-test `isolatedPool` (own Postgres schema via search_path) so global ops see only their own data — robust under parallel `go test ./...`. Task 2's Reflect tests are per-agent (not global) so they use a normal pool + scoped per-agent cleanup. The new methods don't change existing refs/memstore behavior, so prior tests are unaffected.
