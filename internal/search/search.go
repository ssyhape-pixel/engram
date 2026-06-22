// Package search is recall. The Search interface returns sub-file line ranges
// (predicate pushdown), not whole files. L2 ships a GrepSearch stub over a
// working directory; L4 replaces it with a trigram index (variant A).
package search

import "context"

// Hit is a matching line range with a snippet.
type Hit struct {
	Path      string
	LineStart int // 1-based, inclusive
	LineEnd   int // 1-based, inclusive
	Snippet   string
}

// Search answers recall queries and (eventually) maintains an index.
type Search interface {
	Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error)
	// Reindex is a no-op in L2; incremental trigram indexing is L4.
	Reindex(ctx context.Context, agentID string, from, to string) error
}
