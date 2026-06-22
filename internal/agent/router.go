package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/search"
)

// ErrAgentBusy is returned by Open when the agent already has an active session.
var ErrAgentBusy = errors.New("agent: agent already has an active session")

// Router enforces single-writer-per-agent: at most one active Session per
// agent_id (an in-process lock that rebuilds the serialization a single client
// would provide). Multi-pod sticky routing is L5; the ref CAS is the backstop.
type Router struct {
	store   memstore.MemStore
	prov    LLMProvider
	scratch string
	cache   cache.Cache
	emb     search.Embedder

	mu     sync.Mutex
	active map[string]bool
}

// NewRouter creates a Router that materializes session worktrees under scratch.
func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string, c cache.Cache, emb search.Embedder) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, cache: c, emb: emb, active: map[string]bool{}}
}

// claim marks agentID as active. Returns false if already claimed.
func (r *Router) claim(agentID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[agentID] {
		return false
	}
	r.active[agentID] = true
	return true
}

// free releases the active claim for agentID.
func (r *Router) free(agentID string) {
	r.mu.Lock()
	delete(r.active, agentID)
	r.mu.Unlock()
}

// readWorkdirFiles loads every file under root into a path->bytes map (paths
// relative to root) for index construction.
func readWorkdirFiles(root string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("agent: rel path %s: %w", path, relErr)
		}
		files[rel] = data
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("agent: read workdir: %w", err)
	}
	return files, nil
}

// Open acquires the agent's writer slot, materializes HEAD into a fresh workdir,
// and returns a Session. Returns ErrAgentBusy if a session is already active.
func (r *Router) Open(ctx context.Context, agentID string) (*Session, error) {
	if !r.claim(agentID) {
		return nil, ErrAgentBusy
	}
	head, err := r.store.ResolveHead(ctx, agentID)
	if err != nil {
		r.free(agentID)
		return nil, fmt.Errorf("agent: resolve head: %w", err)
	}
	// MkdirTemp rejects patterns containing a path separator, so a hierarchical
	// id like "tenant/alice" must be sanitized for the dir name only — the claim
	// map keeps the real agentID.
	safeID := strings.ReplaceAll(agentID, string(os.PathSeparator), "_")
	safeID = strings.ReplaceAll(safeID, "/", "_")
	workdir, err := os.MkdirTemp(r.scratch, safeID+"-*")
	if err != nil {
		r.free(agentID)
		return nil, fmt.Errorf("agent: scratch dir: %w", err)
	}
	if err := r.store.Materialize(ctx, agentID, head, workdir); err != nil {
		os.RemoveAll(workdir)
		r.free(agentID)
		return nil, fmt.Errorf("agent: materialize: %w", err)
	}
	files, err := readWorkdirFiles(workdir)
	if err != nil {
		os.RemoveAll(workdir)
		r.free(agentID)
		return nil, err
	}
	tools := NewToolset(workdir, agentID, search.NewHybrid(ctx, r.emb, r.cache, files))
	// sync.Once makes release idempotent: a double Close (e.g. defer + explicit)
	// must not free a claim a *different* session may have re-acquired in between.
	var once sync.Once
	release := func() {
		once.Do(func() {
			os.RemoveAll(workdir)
			r.free(agentID)
		})
	}
	return NewSession(r.store, r.prov, tools, agentID, head, workdir, release, r.cache), nil
}
