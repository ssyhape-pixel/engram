package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/ssy/engram/internal/cache"
)

// HybridSearch fuses lexical (trigram) and semantic (embedding) recall via
// Reciprocal Rank Fusion. If semantic retrieval is unavailable (build-time or
// query-time embedding failure) it degrades to trigram-only without erroring.
type HybridSearch struct {
	tri *TrigramIndex
	sem *SemanticIndex // nil when semantic build degraded
}

// NewHybrid builds the trigram index synchronously and the semantic index best
// effort: a semantic build failure (e.g. embedding service down) leaves sem nil
// and recall falls back to trigram-only.
func NewHybrid(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) *HybridSearch {
	h := &HybridSearch{tri: BuildTrigram(files)}
	if sem, err := BuildSemantic(ctx, emb, c, files); err == nil {
		h.sem = sem
	}
	return h
}

func (h *HybridSearch) Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error) {
	if query == "" {
		return nil, nil
	}
	if k <= 0 {
		k = 8
	}
	n := k
	if n < 10 {
		n = 10
	}
	lists := [][]Hit{h.tri.Search(query, n)}
	if h.sem != nil {
		if semHits, err := h.sem.Search(ctx, query, n); err == nil {
			lists = append(lists, semHits)
		}
		// query-time embedding failure: drop semantic, keep trigram (degrade)
	}
	return rrf(lists, k), nil
}

// Reindex is a no-op in L4; incremental git-diff reindexing is L5.
func (h *HybridSearch) Reindex(ctx context.Context, agentID, from, to string) error { return nil }

// rrf fuses ranked lists by Reciprocal Rank Fusion (k=60). Items are identified
// by path:lineStart-lineEnd; identical ranges merge their scores.
func rrf(lists [][]Hit, k int) []Hit {
	const rrfK = 60
	type agg struct {
		hit   Hit
		score float64
	}
	scores := map[string]*agg{}
	var order []string // first-seen order, for a stable tie-break
	for _, list := range lists {
		for rank, hit := range list {
			id := fmt.Sprintf("%s:%d-%d", hit.Path, hit.LineStart, hit.LineEnd)
			a, ok := scores[id]
			if !ok {
				a = &agg{hit: hit}
				scores[id] = a
				order = append(order, id)
			}
			a.score += 1.0 / float64(rrfK+rank+1)
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return scores[order[i]].score > scores[order[j]].score })
	var out []Hit
	for _, id := range order {
		if len(out) >= k {
			break
		}
		out = append(out, scores[id].hit)
	}
	return out
}

var _ Search = (*HybridSearch)(nil)
