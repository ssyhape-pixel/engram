package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

func routerFixture(t *testing.T) (*Router, *memstore.Store) {
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
	if _, err := pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	store := memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool))
	if _, err := store.CreateAgent(ctx, "a1", map[string]string{"system/about.md": "---\ndescription: who\n---\nx\n"}); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	prov := &FakeProvider{Steps: []func(Request) Response{func(r Request) Response { return Response{Text: "ok"} }}}
	return NewRouter(store, prov, t.TempDir(), cache.NewLRU(8), search.NewFakeEmbedder(64)), store
}

func TestRouterOpenMaterializesWorkdir(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if _, err := os.Stat(filepath.Join(s.workdir, "system", "about.md")); err != nil {
		t.Fatalf("workdir not materialized: %v", err)
	}
}

func TestRouterSingleWriter(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s1, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(ctx, "a1"); !errors.Is(err, ErrAgentBusy) {
		t.Fatalf("second Open = %v want ErrAgentBusy", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	s2.Close()
}

func TestRouterCloseRemovesWorkdir(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	wd := s.workdir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(wd); !os.IsNotExist(err) {
		t.Fatalf("workdir should be removed after Close, stat err = %v", err)
	}
}

func TestRouterDoubleCloseIsSafe(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s1, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	// A second Open must succeed (slot freed)...
	s2, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	// ...and a stale double-Close of s1 must NOT free s2's claim.
	_ = s1.Close()
	if _, err := r.Open(ctx, "a1"); !errors.Is(err, ErrAgentBusy) {
		t.Fatalf("stale double-Close freed the live session's claim: got %v want ErrAgentBusy", err)
	}
}

func TestRouterInjectsCacheIntoSession(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.cache == nil {
		t.Fatal("Router.Open must inject the shared cache into the Session")
	}
}

func TestRouterWiresHybridSearch(t *testing.T) {
	ctx := context.Background()
	r, _ := routerFixture(t)
	s, err := r.Open(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.tools.search.(*search.HybridSearch); !ok {
		t.Fatalf("Open must wire a HybridSearch, got %T", s.tools.search)
	}
}
