package refs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// isolatedPool returns a pool whose connections use a unique, per-test schema
// (search_path), with the engram tables created inside it. Global queue ops
// (ClaimJob) then see ONLY this test's data — safe under parallel go test.
func isolatedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
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
		`CREATE TABLE memory_jobs (id bigserial PRIMARY KEY, agent_id text NOT NULL, kind text NOT NULL, from_sha text, state text NOT NULL DEFAULT 'pending', attempts int NOT NULL DEFAULT 0, claimed_at timestamptz, created_at timestamptz NOT NULL DEFAULT now())`,
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
	if err := r.RetryJob(ctx, id, 2); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var state string
	pool.QueryRow(ctx, "SELECT attempts, state FROM memory_jobs WHERE id=$1", id).Scan(&attempts, &state)
	if attempts != 1 || state != "pending" {
		t.Fatalf("after 1st retry attempts=%d state=%s want 1/pending", attempts, state)
	}
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
	var staleID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('stale','reflect','running',$1,0) RETURNING id",
		base.Add(-time.Hour)).Scan(&staleID)
	var freshID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('fresh','reflect','running',$1,0) RETURNING id",
		base).Scan(&freshID)
	var nullID int64
	pool.QueryRow(ctx,
		"INSERT INTO memory_jobs (agent_id,kind,state,claimed_at,attempts) VALUES ('legacy','reflect','running',NULL,0) RETURNING id").Scan(&nullID)
	pool.Exec(ctx, "INSERT INTO memory_jobs (agent_id,kind,state) VALUES ('pend','reflect','pending')")

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
	if _, err := r.ReapStaleJobs(ctx, time.Now(), 5); err != nil {
		t.Fatal(err)
	}
	var state string
	pool.QueryRow(ctx, "SELECT state FROM memory_jobs WHERE id=$1", id).Scan(&state)
	if state != "failed" {
		t.Fatalf("at the attempt cap a reaped job must fail, got %s", state)
	}
}
