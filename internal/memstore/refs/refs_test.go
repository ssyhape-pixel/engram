package refs

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const refsTag = "refs"

func testPool(t *testing.T) (*pgxpool.Pool, string) {
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
	agentID := refsTag + ":" + t.Name()
	// LIFO: pool.Close runs last, DELETE cleanup runs first (pool still open).
	t.Cleanup(func() { pool.Close() })
	t.Cleanup(func() {
		for _, tbl := range []string{"memory_jobs", "agent_refs", "maintenance_cursor"} {
			pool.Exec(ctx, "DELETE FROM "+tbl+" WHERE agent_id=$1", agentID)
		}
	})
	return pool, agentID
}

func TestBootstrapAndResolve(t *testing.T) {
	ctx := context.Background()
	pool, agentID := testPool(t)
	r := New(pool)
	if err := r.Bootstrap(ctx, agentID, "deadbeef"); err != nil {
		t.Fatal(err)
	}
	h, err := r.ResolveHead(ctx, agentID)
	if err != nil || h != "deadbeef" {
		t.Fatalf("resolve = %q,%v", h, err)
	}
}

func TestResolveUnknownAgent(t *testing.T) {
	ctx := context.Background()
	pool, _ := testPool(t)
	r := New(pool)
	_, err := r.ResolveHead(ctx, "ghost")
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("err = %v want ErrAgentNotFound", err)
	}
}

func TestCASSuccessAndEnqueue(t *testing.T) {
	ctx := context.Background()
	pool, agentID := testPool(t)
	r := New(pool)
	r.Bootstrap(ctx, agentID, "p0")
	if err := r.CommitRef(ctx, agentID, "p0", "p1", []Job{{Kind: "reindex"}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h, _ := r.ResolveHead(ctx, agentID)
	if h != "p1" {
		t.Fatalf("head = %q want p1", h)
	}
	var n int
	pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id=$1 AND kind='reindex'", agentID).Scan(&n)
	if n != 1 {
		t.Fatalf("jobs = %d want 1", n)
	}
}

func TestCASConflict(t *testing.T) {
	ctx := context.Background()
	pool, agentID := testPool(t)
	r := New(pool)
	r.Bootstrap(ctx, agentID, "p0")
	if err := r.CommitRef(ctx, agentID, "p0", "p1", nil); err != nil {
		t.Fatal(err)
	}
	// Stale parent p0 -> conflict, and must NOT enqueue.
	err := r.CommitRef(ctx, agentID, "p0", "pX", []Job{{Kind: "reindex"}})
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("err = %v want ErrCASConflict", err)
	}
	var cnt int
	r.pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id=$1", agentID).Scan(&cnt)
	if cnt != 0 {
		t.Fatalf("conflicting commit must not enqueue; jobs=%d", cnt)
	}
}

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
	// Unique advisory-lock key per test (the test DB is shared): derive from agentID.
	var key int64
	for _, c := range agentID {
		key = key*131 + int64(c)
	}
	if key < 0 {
		key = -key
	}

	ran, err := r.WithGlobalLock(ctx, key, func(ctx context.Context) error {
		// While the outer lock is held on one pooled conn, a second attempt on the
		// SAME key from another conn must fail to acquire.
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
