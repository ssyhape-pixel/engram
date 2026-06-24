package maintenance

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/search"
)

// Reindex warms the persistent embedding cache for an agent's current HEAD tree.
// It materializes the FULL tree (not just system/, unlike Reflect — search
// indexes everything) and runs BuildSemantic purely for its Put side-effect:
// BuildSemantic looks up each chunk's embedding in embCache and embeds+Puts only
// the misses. Because keys are content-addressed (hash of model+chunk text),
// unchanged chunks are already present, so reindex is incremental for free — no
// diff needed. The returned index is discarded. Reindex writes no ref and takes
// no lock (idempotent).
func Reindex(ctx context.Context, store memstore.MemStore, emb search.Embedder, embCache cache.Cache, agentID string) error {
	head, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		return fmt.Errorf("maintenance: reindex resolve head %s: %w", agentID, err)
	}
	dir, err := os.MkdirTemp("", "engram-reindex-*")
	if err != nil {
		return fmt.Errorf("maintenance: reindex scratch: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := store.Materialize(ctx, agentID, head, dir); err != nil {
		return fmt.Errorf("maintenance: reindex materialize: %w", err)
	}

	files, err := readAllFiles(dir)
	if err != nil {
		return fmt.Errorf("maintenance: reindex read tree: %w", err)
	}
	// BuildSemantic persists missing embeddings into embCache as a side-effect.
	if _, err := search.BuildSemantic(ctx, emb, embCache, files); err != nil {
		return fmt.Errorf("maintenance: reindex build semantic: %w", err)
	}
	return nil
}

// readAllFiles reads every regular file under dir into a path→bytes map, keyed
// by path relative to dir (matching how the session builds its search files).
func readAllFiles(dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			return rerr
		}
		files[rel] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
