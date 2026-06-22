package search

import (
	"context"
	"testing"

	"github.com/ssy/engram/internal/cache"
)

// countEmbedder wraps an Embedder and counts how many texts it embeds.
type countEmbedder struct {
	inner Embedder
	texts int
}

func (c *countEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.texts += len(texts)
	return c.inner.Embed(ctx, texts)
}
func (c *countEmbedder) Model() string { return c.inner.Model() }

func TestSemanticSectionizeAndSearch(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{
		"m.md": []byte("# Auth\nuser authentication and login flow\n# Cooking\nbanana bread recipe steps\n"),
	}
	si, err := BuildSemantic(ctx, NewFakeEmbedder(256), nil, files)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := si.Search(ctx, "authentication login", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "m.md" {
		t.Fatalf("hits = %+v", hits)
	}
	// The Auth section starts at line 1; the Cooking section at line 3.
	if hits[0].LineStart != 1 || hits[0].LineEnd != 2 {
		t.Fatalf("expected the Auth section (lines 1-2), got %+v", hits[0])
	}
}

func TestSemanticEmbeddingCacheReuse(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{"m.md": []byte("# A\nalpha text\n# B\nbeta text\n")}
	c := cache.NewLRU(64)
	spy := &countEmbedder{inner: NewFakeEmbedder(128)}

	if _, err := BuildSemantic(ctx, spy, c, files); err != nil {
		t.Fatal(err)
	}
	first := spy.texts
	if first == 0 {
		t.Fatal("first build should embed the sections")
	}
	// Second build with the SAME cache: every section is a cache hit -> 0 embeds.
	if _, err := BuildSemantic(ctx, spy, c, files); err != nil {
		t.Fatal(err)
	}
	if spy.texts != first {
		t.Fatalf("second build should embed 0 sections (all cached); embedded %d more", spy.texts-first)
	}
}

func TestSemanticNilCacheEmbedsEachBuild(t *testing.T) {
	ctx := context.Background()
	files := map[string][]byte{"m.md": []byte("# A\nalpha\n")}
	spy := &countEmbedder{inner: NewFakeEmbedder(64)}
	_, _ = BuildSemantic(ctx, spy, nil, files)
	first := spy.texts
	_, _ = BuildSemantic(ctx, spy, nil, files)
	if spy.texts != first*2 {
		t.Fatalf("nil cache must re-embed each build; got %d want %d", spy.texts, first*2)
	}
}
