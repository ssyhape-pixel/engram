# Engram L5d — Stale-`running` Job Reaper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover jobs stranded in `running` by a crashed worker — a `claimed_at` timestamp + a per-round reaper that resets stale `running` jobs to `pending` (or `failed` at the attempt cap).

**Architecture:** Migration adds `memory_jobs.claimed_at`. `ClaimJob` stamps it on claim; `RetryJob` clears it on requeue. `ReapStaleJobs(before, maxAttempts)` bumps attempts and returns stale `running` rows (claimed before `before`, or with NULL `claimed_at`) to `pending`/`failed`. The worker reaps each round before draining, so reaped jobs are reprocessed the same round.

**Tech Stack:** Go 1.25 stdlib (`time`); existing `internal/memstore/refs` (memory_jobs, golang-migrate via `//go:embed all:migrations`, ClaimJob/RetryJob, the `isolatedPool` test helper), `cmd/maintenance`.

## Global Constraints

- Go 1.25; module `github.com/ssy/engram`.
- `context.Context` first arg; wrap errors with `%w`.
- Migration is **expand** (add a nullable column) — non-destructive.
- `before` is passed as an explicit `time.Time` (NOT `now()` in SQL) so the reaper is injectable/testable; the worker passes `time.Now().Add(-reapAfter)`.
- The reaper bumps `attempts` (poison-job protection — a job that crashes the worker on every claim fails out at `maxAttempts`), mirroring `RetryJob`.
- Reaper is best-effort in the worker round: an error is logged, never aborts GC/defrag/drain.
- **Both** `isolatedPool` test DDLs (`internal/memstore/refs/jobqueue_test.go` and `internal/maintenance/drain_test.go`) hand-build `memory_jobs` and MUST gain the `claimed_at` column in the SAME task that makes `ClaimJob` reference it — otherwise `ClaimJob`'s `UPDATE ... claimed_at=now()` fails ("column does not exist") and every test that claims a job breaks. (`reflectStore` uses `refs.Migrate`, so it gets the column automatically.)
- Existing (verified): migrations at `internal/memstore/refs/migrations/000001_init.{up,down}.sql`; `refs.Migrate` embeds via `//go:embed all:migrations` (a new `000002_*.sql` is auto-included). `ClaimJob` mark-running is `UPDATE memory_jobs SET state='running' WHERE id=$1` (refs.go:143). `RetryJob` is `UPDATE memory_jobs SET attempts = attempts + 1, state = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END WHERE id=$1`. The `isolatedPool` memory_jobs DDL (both copies): `CREATE TABLE memory_jobs (id bigserial PRIMARY KEY, agent_id text NOT NULL, kind text NOT NULL, from_sha text, state text NOT NULL DEFAULT 'pending', attempts int NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now())`. cmd/maintenance round (in WithGlobalLock(gcLockKey)): GC → `EnqueueDefrag` → `DrainJobs(ctx, r, deps, maxAttempts)`; has `env`/`dur` helpers, `maxAttempts`, `r`.

**Prerequisites:** On `main` (L1–L5c merged). Branch: `git checkout -b feat/l5d-reaper`. Tasks need `ENGRAM_TEST_DB`.
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```

**Package layout:**
```
internal/memstore/refs/migrations/000002_claimed_at.up.sql    # NEW
internal/memstore/refs/migrations/000002_claimed_at.down.sql  # NEW
internal/memstore/refs/refs.go        # MODIFY: ClaimJob +claimed_at=now(); RetryJob +claimed_at=NULL; ReapStaleJobs
internal/memstore/refs/jobqueue_test.go   # MODIFY: isolatedPool DDL +claimed_at; reaper tests
internal/maintenance/drain_test.go    # MODIFY: isolatedPool DDL +claimed_at (so ClaimJob still works)
cmd/maintenance/main.go               # MODIFY: ENGRAM_JOB_REAP_AFTER; reap before drain
```
**Dependency order:** migration + refs + DDLs (1, atomic — keeps all tests green) → cmd wiring (2).

---

### Task 1: migration + claimed_at on claim/retry + ReapStaleJobs

**Files:**
- Create: `internal/memstore/refs/migrations/000002_claimed_at.up.sql`, `…/000002_claimed_at.down.sql`
- Modify: `internal/memstore/refs/refs.go`, `internal/memstore/refs/jobqueue_test.go`, `internal/maintenance/drain_test.go`

**Interfaces:**
- Consumes: `Refs` (`pool`), existing ClaimJob/RetryJob, the partial-unique index, `isolatedPool`.
- Produces: `(*Refs).ReapStaleJobs(ctx context.Context, before time.Time, maxAttempts int) (int, error)`; `ClaimJob` now stamps `claimed_at`; `RetryJob` now clears it.

- [ ] **Step 1: Create the migration files.**
`internal/memstore/refs/migrations/000002_claimed_at.up.sql`:
```sql
ALTER TABLE memory_jobs ADD COLUMN claimed_at timestamptz;
```
`internal/memstore/refs/migrations/000002_claimed_at.down.sql`:
```sql
ALTER TABLE memory_jobs DROP COLUMN claimed_at;
```

- [ ] **Step 2: Add the `claimed_at` column to BOTH `isolatedPool` DDLs** so hand-built test tables match the migrated schema. In `internal/memstore/refs/jobqueue_test.go` AND `internal/maintenance/drain_test.go`, change the `memory_jobs` CREATE line to:
```go
		`CREATE TABLE memory_jobs (id bigserial PRIMARY KEY, agent_id text NOT NULL, kind text NOT NULL, from_sha text, state text NOT NULL DEFAULT 'pending', attempts int NOT NULL DEFAULT 0, claimed_at timestamptz, created_at timestamptz NOT NULL DEFAULT now())`,
```

- [ ] **Step 3: Write the failing reaper tests** in `internal/memstore/refs/jobqueue_test.go` (reuse `isolatedPool`; add `"time"` to the test imports if missing):
```go
func TestClaimJobStampsClaimedAt(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	enqueue(t, pool, "a1", "reflect")
	if _, err := r.ClaimJob(ctx); err != nil {
		t.Fatal(err)
	}
	var claimedAt *time.Time
	pool.QueryRow(ctx, "SELECT claimed_at FROM memory_jobs WHERE agent_id='a1'").Scan(&claimedAt)
	if claimedAt == nil {
		t.Fatal("ClaimJob must stamp claimed_at")
	}
}

func TestRetryJobClearsClaimedAt(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	id := enqueue(t, pool, "a1", "reflect")
	if _, err := r.ClaimJob(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.RetryJob(ctx, id, 5); err != nil {
		t.Fatal(err)
	}
	var claimedAt *time.Time
	var state string
	pool.QueryRow(ctx, "SELECT claimed_at, state FROM memory_jobs WHERE id=$1", id).Scan(&claimedAt, &state)
	if claimedAt != nil || state != "pending" {
		t.Fatalf("after retry: claimed_at=%v state=%s (want nil/pending)", claimedAt, state)
	}
}

func TestReapStaleJobs(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	base := time.Now()

	// stale running (claimed an hour ago)
	var staleID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('stale','reflect','running',$1,0) RETURNING id",
		base.Add(-time.Hour)).Scan(&staleID)
	// fresh running (claimed just now)
	var freshID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('fresh','reflect','running',$1,0) RETURNING id",
		base).Scan(&freshID)
	// NULL claimed_at running (legacy/anomalous)
	var nullID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('legacy','reflect','running',NULL,0) RETURNING id").Scan(&nullID)
	// a pending job — must NOT be touched
	pool.Exec(ctx, "INSERT INTO memory_jobs (agent_id,kind,state) VALUES ('pend','reflect','pending')")

	// reap anything claimed before (base - 1min): stale + null qualify, fresh does not.
	n, err := r.ReapStaleJobs(ctx, base.Add(-time.Minute), 5)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("reaped %d, want 2 (stale + null)", n)
	}
	check := func(id int64) (string, *time.Time, int) {
		var st string
		var ca *time.Time
		var att int
		pool.QueryRow(ctx, "SELECT state, claimed_at, attempts FROM memory_jobs WHERE id=$1", id).Scan(&st, &ca, &att)
		return st, ca, att
	}
	if st, ca, att := check(staleID); st != "pending" || ca != nil || att != 1 {
		t.Fatalf("stale reaped wrong: %s %v %d", st, ca, att)
	}
	if st, _, _ := check(nullID); st != "pending" {
		t.Fatalf("null-claimed_at running should be reaped, state=%s", st)
	}
	if st, _, _ := check(freshID); st != "running" {
		t.Fatalf("fresh running must NOT be reaped, state=%s", st)
	}
}

func TestReapStaleJobsFailsAtCap(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := New(pool)
	var id int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('a1','reflect','running',$1,4) RETURNING id",
		time.Now().Add(-time.Hour)).Scan(&id)
	if _, err := r.ReapStaleJobs(ctx, time.Now(), 5); err != nil { // attempts 4→5 == cap → failed
		t.Fatal(err)
	}
	var state string
	pool.QueryRow(ctx, "SELECT state FROM memory_jobs WHERE id=$1", id).Scan(&state)
	if state != "failed" {
		t.Fatalf("at the attempt cap a reaped job must fail, got %s", state)
	}
}
```

- [ ] **Step 4: Run** `ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -run 'ClaimJobStamps|RetryJobClears|Reap'` → compile failure (ReapStaleJobs undefined).

- [ ] **Step 5: Implement in `internal/memstore/refs/refs.go`.** Add `"time"` to imports if missing.
Change the `ClaimJob` mark-running statement (currently `UPDATE memory_jobs SET state='running' WHERE id=$1`) to:
```go
	if _, err := tx.Exec(ctx, `UPDATE memory_jobs SET state='running', claimed_at=now() WHERE id=$1`, j.ID); err != nil {
```
Change the `RetryJob` statement to clear `claimed_at`:
```go
	_, err := r.pool.Exec(ctx,
		`UPDATE memory_jobs
		 SET attempts = attempts + 1,
		     state = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END,
		     claimed_at = NULL
		 WHERE id=$1`, id, maxAttempts)
```
Add `ReapStaleJobs`:
```go
// ReapStaleJobs returns stale 'running' jobs (claimed before `before`, or with a
// NULL claimed_at) to the queue, so a worker that crashed mid-job doesn't strand
// them. attempts is bumped (poison-job protection: a job that crashes the worker
// on every claim fails out at maxAttempts), state becomes 'pending' (or 'failed'
// at the cap), and claimed_at is cleared. Returns the number of jobs reaped.
func (r *Refs) ReapStaleJobs(ctx context.Context, before time.Time, maxAttempts int) (int, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE memory_jobs
		 SET attempts = attempts + 1,
		     state = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END,
		     claimed_at = NULL
		 WHERE state='running' AND (claimed_at IS NULL OR claimed_at < $1)`,
		before, maxAttempts)
	if err != nil {
		return 0, fmt.Errorf("refs: reap stale jobs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
```

- [ ] **Step 6: Run** the refs suite + the maintenance suite (the latter to confirm the drain_test DDL change keeps ClaimJob working):
```bash
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/memstore/refs/ -count=1 -v
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./internal/maintenance/ -count=1
go test -race ./internal/memstore/refs/ -count=1
```
Expected: all pass (refs: existing + 4 new; maintenance: unchanged, still green). gofmt + vet clean.

- [ ] **Step 7: Commit**
```bash
git add internal/memstore/refs/migrations/000002_claimed_at.up.sql internal/memstore/refs/migrations/000002_claimed_at.down.sql internal/memstore/refs/refs.go internal/memstore/refs/jobqueue_test.go internal/maintenance/drain_test.go
git commit -m "feat(refs): claimed_at + ReapStaleJobs to recover crashed-worker jobs (migration 000002)"
```

---

### Task 2: wire the reaper into the worker round

**Files:**
- Modify: `cmd/maintenance/main.go`

**Interfaces:**
- Consumes: `r.ReapStaleJobs` (Task 1), the `dur` env helper, `maxAttempts`.
- Produces: per-round reap (no new exported API).

- [ ] **Step 1: Add the env + reap call in `cmd/maintenance/main.go`.** After `maxAttempts` is parsed (and near the other env reads), add:
```go
	reapAfter := dur("ENGRAM_JOB_REAP_AFTER", "10m")
```
Inside the `WithGlobalLock(gcLockKey, …)` callback, AFTER the `gc:` log line and BEFORE the `EnqueueDefrag` scan, add the reap (best-effort: log error, don't abort the round):
```go
			reaped, rerr := r.ReapStaleJobs(ctx, time.Now().Add(-reapAfter), maxAttempts)
			if rerr != nil {
				log.Printf("reap error: %v", rerr)
			} else if reaped > 0 {
				log.Printf("reaped: %d stale running jobs", reaped)
			}
```
(`time` is already imported in main.go — it's used for GC's `time.Now()`. Confirm `dur` exists; it does, used for ENGRAM_GC_INTERVAL/GRACE.)

- [ ] **Step 2: Build + vet + gofmt**
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected clean.

- [ ] **Step 3: Smoke (needs live PG).** Confirm the worker still runs a round cleanly (no stale jobs → no `reaped:` line, which is fine):
```bash
ENGRAM_GC_INTERVAL=2s ENGRAM_GC_GRACE=1s ENGRAM_JOB_REAP_AFTER=2s ENGRAM_PROVIDER=fake ENGRAM_OBJ="$(mktemp -d)" ENGRAM_EMB_OBJ="$(mktemp -d)" ENGRAM_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" sh -c 'go run ./cmd/maintenance & P=$!; sleep 5; kill $P 2>/dev/null' || true
```
Expected: banner + `gc:` + `jobs: processed=` lines each round, no crash. Report actual stdout.

- [ ] **Step 4: Full suite (serialized) + race**
```bash
go build ./...
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test ./... -count=1 -p 1
ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable" go test -race ./internal/memstore/refs/ ./internal/maintenance/ -count=1
```
Expected: build OK, all packages pass, no races.

- [ ] **Step 5: Commit**
```bash
git add cmd/maintenance/main.go
git commit -m "feat(cmd/maintenance): reap stale running jobs each round before draining"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-25-engram-l5d-stale-job-reaper-design.md`):**
- §3.2 migration 000002 (ADD/DROP claimed_at) + both isolatedPool DDLs → Task 1 ✅
- §3.3 ClaimJob +claimed_at=now(); RetryJob +claimed_at=NULL; ReapStaleJobs (bump attempts, IS NULL OR < before, explicit `before`) → Task 1 ✅
- §3.4 cmd/maintenance (ENGRAM_JOB_REAP_AFTER default 10m; reap after GC, before defrag scan / drain; best-effort log) → Task 2 ✅
- §4 error handling (%w; best-effort in round); §5 tests (ClaimJob stamps; RetryJob clears; reap stale+null but not fresh, not pending; fail-at-cap; both DDLs updated; reflectStore via real Migrate) → Tasks 1/2 ✅
- §6 DoD → covered ✅

**Placeholder scan:** No TBD/TODO. No vague steps — all code provided.

**Type consistency:** `ReapStaleJobs(ctx, before time.Time, maxAttempts int) (int, error)` (Task 1) ↔ cmd call `r.ReapStaleJobs(ctx, time.Now().Add(-reapAfter), maxAttempts)` (Task 2). `claimed_at` column added in migration + both isolatedPool DDLs + referenced by ClaimJob/RetryJob/ReapStaleJobs consistently. `dur`/`maxAttempts` reused from existing cmd code.

**Build-green-per-task:** Task 1 is atomic — the migration, both test DDLs, and the ClaimJob/RetryJob/ReapStaleJobs changes land together, so no test ever sees a `ClaimJob` referencing a missing column. (Critically: the `drain_test.go` DDL update is in Task 1, not deferred — otherwise maintenance tests would break the moment ClaimJob references claimed_at.) Task 2 only adds a cmd call to an existing method.

**Migration/DDL parity:** the `claimed_at timestamptz` column is added in three places that must agree — the migration (production + reflectStore), and the two isolatedPool DDLs (refs + maintenance tests). All three are in Task 1. (Known wart: test DDL duplicates the migration; out of scope to refactor to migrate-into-schema here.)
