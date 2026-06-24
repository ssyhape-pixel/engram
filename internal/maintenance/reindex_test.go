package maintenance

import (
	"context"
	"testing"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/search"
)

// countingEmbedder counts how many texts have been embedded, to prove the second
// reindex re-embeds nothing (all persisted).
type countingEmbedder struct {
	inner search.Embedder
	calls int
}

func (c *countingEmbedder) Model() string { return c.inner.Model() }
func (c *countingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.calls += len(texts)
	return c.inner.Embed(ctx, texts)
}

func TestReindexPersistsEmbeddingsIncrementally(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t) // live PG, unique agent, scoped cleanup
	_, err := store.CreateAgent(ctx, agentID, map[string]string{
		"system/a.md": "# Alpha\nalpha facts here\n# Beta\nbeta facts here\n",
		"notes/n.md":  "# Note\nsome note content\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	ce := &countingEmbedder{inner: search.NewFakeEmbedder(64)}
	embCache := cache.NewObjCache(objstore.NewLocal(t.TempDir()))

	if err := Reindex(ctx, store, ce, embCache, agentID); err != nil {
		t.Fatalf("reindex: %v", err)
	}
	if ce.calls == 0 {
		t.Fatal("first reindex should embed chunks")
	}
	first := ce.calls

	// Second reindex: every chunk's embedding is already persisted → 0 new embeds.
	if err := Reindex(ctx, store, ce, embCache, agentID); err != nil {
		t.Fatalf("reindex 2: %v", err)
	}
	if ce.calls != first {
		t.Fatalf("second reindex embedded %d more texts, want 0 (incremental persistence broken)", ce.calls-first)
	}
}
