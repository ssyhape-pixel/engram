package memstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

const msTag = "ms"

func newStore(t *testing.T) (*Store, context.Context, string) {
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
	agentID := msTag + ":" + t.Name()
	// LIFO: pool.Close runs last, DELETE cleanup runs first (pool still open).
	t.Cleanup(func() { pool.Close() })
	t.Cleanup(func() {
		for _, tbl := range []string{"memory_jobs", "agent_refs", "maintenance_cursor"} {
			pool.Exec(ctx, "DELETE FROM "+tbl+" WHERE agent_id=$1", agentID)
		}
	})
	return New(objstore.NewLocal(t.TempDir()), refs.New(pool)), ctx, agentID
}

func TestCreateMaterializeCommit(t *testing.T) {
	s, ctx, agentID := newStore(t)
	head, err := s.CreateAgent(ctx, agentID, map[string]string{"system/about.md": "---\ndescription: who\n---\nhi\n"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	dir := t.TempDir()
	if err := s.Materialize(ctx, agentID, head, dir); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "system", "about.md"))
	if len(got) == 0 {
		t.Fatal("seed file missing after materialize")
	}

	os.WriteFile(filepath.Join(dir, "note.md"), []byte("note\n"), 0o644)
	newHead, err := s.CommitWithCAS(ctx, agentID, head, dir, []Job{{Kind: "reindex"}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if newHead == head {
		t.Fatal("head did not advance")
	}
	resolved, _ := s.ResolveHead(ctx, agentID)
	if resolved != newHead {
		t.Fatalf("resolved %q want %q", resolved, newHead)
	}
}

func TestCommitWithStaleParentConflicts(t *testing.T) {
	s, ctx, agentID := newStore(t)
	head, _ := s.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})

	dir := t.TempDir()
	if err := s.Materialize(ctx, agentID, head, dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.md"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitWithCAS(ctx, agentID, head, dir, nil); err != nil {
		t.Fatal(err)
	}
	// Stale parent -> conflict.
	if err := os.WriteFile(filepath.Join(dir, "b.md"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.CommitWithCAS(ctx, agentID, head, dir, nil)
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("err = %v want ErrCASConflict", err)
	}
}

func TestCreateAgentDuplicate(t *testing.T) {
	s, ctx, agentID := newStore(t)
	if _, err := s.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := s.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	if !errors.Is(err, ErrAgentAlreadyExists) {
		t.Fatalf("err = %v want ErrAgentAlreadyExists", err)
	}
}
