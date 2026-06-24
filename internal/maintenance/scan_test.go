package maintenance

import (
	"context"
	"strings"
	"testing"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

func TestEnqueueDefragEnqueuesOnlySplittableAgents(t *testing.T) {
	ctx := context.Background()
	pool := isolatedPool(t)
	r := refs.New(pool)
	store := memstore.New(objstore.NewLocal(t.TempDir()), r)

	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, "fat", map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateAgent(ctx, "lean", map[string]string{"system/s.md": "# A\nsmall\n"}); err != nil {
		t.Fatal(err)
	}

	if err := EnqueueDefrag(ctx, r, store, 50); err != nil {
		t.Fatalf("scan: %v", err)
	}
	count := func(agentID string) int {
		var n int
		pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id=$1 AND kind='defrag' AND state='pending'", agentID).Scan(&n)
		return n
	}
	if count("fat") != 1 {
		t.Fatalf("fat agent should have a defrag job, got %d", count("fat"))
	}
	if count("lean") != 0 {
		t.Fatalf("lean agent should have no defrag job, got %d", count("lean"))
	}
	// Idempotent: a second scan does not double-enqueue (pending job already exists).
	if err := EnqueueDefrag(ctx, r, store, 50); err != nil {
		t.Fatal(err)
	}
	if count("fat") != 1 {
		t.Fatalf("second scan must not double-enqueue, got %d", count("fat"))
	}
}
