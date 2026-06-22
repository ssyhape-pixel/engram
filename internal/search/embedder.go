package search

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
)

// Embedder turns texts into vectors. Model() identifies the model so embedding
// cache keys never mix vectors from different models.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Model() string
}

// FakeEmbedder is a deterministic hashed bag-of-words embedder for tests: texts
// sharing words get higher cosine similarity. Not for production use.
type FakeEmbedder struct{ dim int }

func NewFakeEmbedder(dim int) *FakeEmbedder {
	if dim <= 0 {
		dim = 256
	}
	return &FakeEmbedder{dim: dim}
}

func (f *FakeEmbedder) Model() string { return "fake" }

func (f *FakeEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := make([]float32, f.dim)
		for _, w := range strings.Fields(strings.ToLower(t)) {
			h := fnv.New32a()
			_, _ = h.Write([]byte(w))
			v[h.Sum32()%uint32(f.dim)]++
		}
		var norm float64
		for _, x := range v {
			norm += float64(x) * float64(x)
		}
		if norm > 0 {
			n := float32(math.Sqrt(norm))
			for j := range v {
				v[j] /= n
			}
		}
		out[i] = v
	}
	return out, nil
}

var _ Embedder = (*FakeEmbedder)(nil)
