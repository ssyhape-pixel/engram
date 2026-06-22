package search

import (
	"context"
	"errors"
	"testing"
)

// failingEmbedder always errors (drives the degrade path).
type failingEmbedder struct{}

func (failingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errors.New("embed unavailable")
}
func (failingEmbedder) Model() string { return "failing" }

func TestHybridTrigramExactToken(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, NewFakeEmbedder(256), nil, map[string][]byte{
		"a.md": []byte("config\ntoken=xq7z9\nmore notes\n"),
	})
	hits, err := h.Recall(ctx, "a1", "xq7z9", 5)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, hit := range hits {
		if hit.Path == "a.md" && hit.LineStart == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("exact-token line should be recalled (trigram): %+v", hits)
	}
}

func TestHybridSemanticSurfacesSectionTrigramMisses(t *testing.T) {
	ctx := context.Background()
	// No single line contains the full phrase "alpha beta gamma" -> trigram misses;
	// the section shares all three words -> semantic surfaces it.
	h := NewHybrid(ctx, NewFakeEmbedder(256), nil, map[string][]byte{
		"a.md": []byte("# notes\nalpha\nbeta\ngamma\n"),
	})
	if tri := h.tri.Search("alpha beta gamma", 5); len(tri) != 0 {
		t.Fatalf("precondition: trigram should miss the cross-line phrase, got %+v", tri)
	}
	hits, err := h.Recall(ctx, "a1", "alpha beta gamma", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("semantic should surface the section even though no line has the full phrase")
	}
}

func TestHybridDegradesWhenEmbeddingFails(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, failingEmbedder{}, nil, map[string][]byte{
		"a.md": []byte("has needle here\n"),
	})
	hits, err := h.Recall(ctx, "a1", "needle", 5)
	if err != nil {
		t.Fatalf("must degrade to trigram, not error: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("trigram results must still be returned when embeddings are unavailable")
	}
}

func TestHybridEmptyQuery(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, NewFakeEmbedder(64), nil, map[string][]byte{"a.md": []byte("# h\nstuff\n")})
	hits, err := h.Recall(ctx, "a1", "", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("empty query should return no hits, got %+v", hits)
	}
}

func TestHybridReindexNoop(t *testing.T) {
	ctx := context.Background()
	h := NewHybrid(ctx, NewFakeEmbedder(64), nil, map[string][]byte{"a.md": []byte("x\n")})
	if err := h.Reindex(ctx, "a1", "from", "to"); err != nil {
		t.Fatalf("Reindex stub should be nil: %v", err)
	}
}
