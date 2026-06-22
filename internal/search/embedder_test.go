package search

import (
	"context"
	"math"
	"reflect"
	"testing"
)

// local cosine helper for this test (the package cosine lands in a later task).
func tcos(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func TestFakeEmbedderDeterministic(t *testing.T) {
	e := NewFakeEmbedder(64)
	a, _ := e.Embed(context.Background(), []string{"hello world"})
	b, _ := e.Embed(context.Background(), []string{"hello world"})
	if !reflect.DeepEqual(a[0], b[0]) {
		t.Fatal("FakeEmbedder must be deterministic")
	}
}

func TestFakeEmbedderSharedWordsRankHigher(t *testing.T) {
	e := NewFakeEmbedder(256)
	vs, _ := e.Embed(context.Background(), []string{
		"user authentication login",
		"authentication flow for users",
		"banana bread recipe",
	})
	q, near, far := vs[0], vs[1], vs[2]
	if tcos(q, near) <= tcos(q, far) {
		t.Fatalf("shared-word text should score higher: near=%v far=%v", tcos(q, near), tcos(q, far))
	}
}

func TestFakeEmbedderModel(t *testing.T) {
	if NewFakeEmbedder(0).Model() != "fake" {
		t.Fatal("Model() should be \"fake\"")
	}
}
