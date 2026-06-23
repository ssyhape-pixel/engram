package maintenance

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

type fakeCompleter struct{ out string }

func (f fakeCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	if f.out != "" {
		return f.out, nil
	}
	return "CONSOLIDATED:\n" + user, nil
}

// reflectStore builds a memstore.Store over live PG (per-agent-isolated; Reflect
// is per-agent so a normal pool + scoped cleanup suffices) with a unique agent id.
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
	t.Cleanup(func() {
		for _, tbl := range []string{"memory_jobs", "agent_refs", "maintenance_cursor"} {
			pool.Exec(ctx, "DELETE FROM "+tbl+" WHERE agent_id=$1", agentID)
		}
	})
	return memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool)), agentID
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

func TestReflectSkipsWhenNoResident(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, err := store.CreateAgent(ctx, agentID, map[string]string{"notes/n.md": "just a note\n"})
	if err != nil {
		t.Fatal(err)
	}
	if err := Reflect(ctx, store, fakeCompleter{out: "X"}, agentID, string(head)); err != nil {
		t.Fatalf("reflect: %v", err)
	}
	h2, _ := store.ResolveHead(ctx, agentID)
	if h2 != head {
		t.Fatal("reflection with no system/ content must not commit (HEAD unchanged)")
	}
}

func TestReflectDoesNotLoopOnRepeatedCalls(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "x"})
	if err := Reflect(ctx, store, fakeCompleter{}, agentID, string(head)); err != nil {
		t.Fatal(err)
	}
	h2, _ := store.ResolveHead(ctx, agentID)
	// Reflection committed with jobs=nil (no self-trigger); a second reflection
	// still works and advances HEAD again — no broken loop state.
	if err := Reflect(ctx, store, fakeCompleter{}, agentID, string(h2)); err != nil {
		t.Fatalf("second reflect: %v", err)
	}
	h3, _ := store.ResolveHead(ctx, agentID)
	if h3 == h2 {
		t.Fatal("second reflection should advance HEAD")
	}
}
