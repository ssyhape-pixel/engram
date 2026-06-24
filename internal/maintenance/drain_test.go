package maintenance

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

// isolatedPool returns a pool whose connections use a unique, per-test schema
// (search_path), with the engram tables created inside it. Global ops
// (ClaimJob/DrainJobs) then see ONLY this test's data — safe under parallel go test.
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

type fakeDrainCompleter struct{ out string }

func (f fakeDrainCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	return f.out, nil
}

func TestDrainReflectAndReindex(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)
	agentID := "a1"
	head, err := store.CreateAgent(ctx, agentID, map[string]string{"system/about.md": "---\ndescription: d\n---\nfacts\n"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.InsertPendingJob(ctx, agentID, "reflect", string(head)); err != nil {
		t.Fatal(err)
	}
	if err := r.InsertPendingJob(ctx, agentID, "reindex", string(head)); err != nil {
		t.Fatal(err)
	}

	drained, err := DrainJobs(ctx, r, store, fakeDrainCompleter{out: "SUMMARY\n"}, search.NewFakeEmbedder(64), cache.NewObjCache(objstore.NewLocal(t.TempDir())), 5)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if drained != 2 {
		t.Fatalf("drained=%d want 2", drained)
	}
	if j, _ := r.ClaimJob(ctx); j != nil {
		t.Fatalf("queue should be empty, got %+v", j)
	}
	n, _ := r.CountJobs(ctx, agentID)
	if n != 0 {
		t.Fatalf("no jobs should remain, got %d", n)
	}
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
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)
	agentID := "a1"
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	if err := r.InsertPendingJob(ctx, agentID, "reflect", string(head)); err != nil {
		t.Fatal(err)
	}

	// Hold the per-agent advisory lock, then drain inside it: the reflect job's
	// inner per-agent lock acquire must fail → job requeued (not completed).
	ran, err := r.WithGlobalLock(ctx, agentKey(agentID), func(ctx context.Context) error {
		_, derr := DrainJobs(ctx, r, store, fakeDrainCompleter{out: "X"}, search.NewFakeEmbedder(64), cache.NewObjCache(objstore.NewLocal(t.TempDir())), 5)
		return derr
	})
	if err != nil {
		t.Fatalf("drain under held lock: %v", err)
	}
	if !ran {
		t.Fatal("outer lock should have been acquired")
	}
	n, _ := r.CountJobs(ctx, agentID)
	if n == 0 {
		t.Fatal("reflect job should remain (requeued) while agent lock was held")
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
