package refs

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set; skipping Postgres test")
	}
	ctx := context.Background()
	if err := Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor")
		pool.Close()
	})
	pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor")
	return pool
}

func TestBootstrapAndResolve(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	if err := r.Bootstrap(ctx, "a1", "deadbeef"); err != nil {
		t.Fatal(err)
	}
	h, err := r.ResolveHead(ctx, "a1")
	if err != nil || h != "deadbeef" {
		t.Fatalf("resolve = %q,%v", h, err)
	}
}

func TestResolveUnknownAgent(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	_, err := r.ResolveHead(ctx, "ghost")
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("err = %v want ErrAgentNotFound", err)
	}
}

func TestCASSuccessAndEnqueue(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	r := New(pool)
	r.Bootstrap(ctx, "a1", "p0")
	if err := r.CommitRef(ctx, "a1", "p0", "p1", []Job{{Kind: "reindex"}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h, _ := r.ResolveHead(ctx, "a1")
	if h != "p1" {
		t.Fatalf("head = %q want p1", h)
	}
	var n int
	pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id='a1' AND kind='reindex'").Scan(&n)
	if n != 1 {
		t.Fatalf("jobs = %d want 1", n)
	}
}

func TestCASConflict(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	r.Bootstrap(ctx, "a1", "p0")
	if err := r.CommitRef(ctx, "a1", "p0", "p1", nil); err != nil {
		t.Fatal(err)
	}
	// Stale parent p0 -> conflict, and must NOT enqueue.
	err := r.CommitRef(ctx, "a1", "p0", "pX", []Job{{Kind: "reindex"}})
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("err = %v want ErrCASConflict", err)
	}
	var cnt int
	r.pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs").Scan(&cnt)
	if cnt != 0 {
		t.Fatalf("conflicting commit must not enqueue; jobs=%d", cnt)
	}
}
