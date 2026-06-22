package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

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

	mu     sync.Mutex
	active map[string]bool
}

// NewRouter creates a Router that materializes session worktrees under scratch.
func NewRouter(store memstore.MemStore, prov LLMProvider, scratch string) *Router {
	return &Router{store: store, prov: prov, scratch: scratch, active: map[string]bool{}}
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
	tools := NewToolset(workdir, agentID, search.NewGrep(workdir))
	// sync.Once makes release idempotent: a double Close (e.g. defer + explicit)
	// must not free a claim a *different* session may have re-acquired in between.
	var once sync.Once
	release := func() {
		once.Do(func() {
			os.RemoveAll(workdir)
			r.free(agentID)
		})
	}
	return NewSession(r.store, r.prov, tools, agentID, head, workdir, release), nil
}
