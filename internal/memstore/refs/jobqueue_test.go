package refs

import (
	"context"
	"os"
	"strings"
	"testing"

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
