// Package memstore is the authoritative store: content-addressed objects
// (objstore) + a CAS-guarded HEAD ref (refs), glued through a custom go-git
// storer (gitfs). All durable writes go through it.
package memstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

type CommitHash string

// Job aliases refs.Job so callers depend only on memstore.
type Job = refs.Job

// ErrCASConflict is returned when HEAD moved under a commit.
var ErrCASConflict = refs.ErrCASConflict

// ErrAgentNotFound is returned when an agent has never been bootstrapped.
var ErrAgentNotFound = refs.ErrAgentNotFound

// ErrAgentAlreadyExists is returned by CreateAgent when the agent already has a HEAD.
var ErrAgentAlreadyExists = errors.New("memstore: agent already exists")

// MemStore is the authoritative store interface (see docs/architecture.md §11).
type MemStore interface {
	ResolveHead(ctx context.Context, agentID string) (CommitHash, error)
	Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error
	CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error)
	TreeKeys(ctx context.Context, at CommitHash) (rootTree, systemSubtree CommitHash, err error)
}

type Store struct {
	objs objstore.ObjStore
	refs *refs.Refs
}

func New(objs objstore.ObjStore, r *refs.Refs) *Store {
	return &Store{objs: objs, refs: r}
}

func (s *Store) ResolveHead(ctx context.Context, agentID string) (CommitHash, error) {
	h, err := s.refs.ResolveHead(ctx, agentID)
	return CommitHash(h), err
}

func (s *Store) Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error {
	return gitfs.Materialize(ctx, s.objs, string(at), dir)
}

// CommitWithCAS writes objects for the working tree (objects FIRST), then
// atomically advances HEAD parent->new and enqueues jobs (ref SECOND).
func (s *Store) CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error) {
	newHash, err := gitfs.Commit(ctx, s.objs, string(parent), dir, "agent commit")
	if err != nil {
		return "", fmt.Errorf("memstore: write objects: %w", err)
	}
	if err := s.refs.CommitRef(ctx, agentID, string(parent), newHash, jobs); err != nil {
		return "", err // ErrCASConflict propagates as-is
	}
	return CommitHash(newHash), nil
}

// TreeKeys returns the root tree hash and the "system" subtree hash for a
// commit. Both are immutable cache keys read directly from objects (no Postgres).
func (s *Store) TreeKeys(ctx context.Context, at CommitHash) (CommitHash, CommitHash, error) {
	root, sys, err := gitfs.TreeKeys(ctx, s.objs, string(at))
	return CommitHash(root), CommitHash(sys), err
}

// CreateAgent seeds an agent's initial commit from `seed` (path->content) and
// registers its HEAD. Returns the initial commit hash.
func (s *Store) CreateAgent(ctx context.Context, agentID string, seed map[string]string) (CommitHash, error) {
	// Best-effort duplicate check. Under single-writer-per-agent the TOCTOU
	// window between this check and Bootstrap is not a concern.
	if _, err := s.refs.ResolveHead(ctx, agentID); err == nil {
		return "", ErrAgentAlreadyExists
	} else if !errors.Is(err, refs.ErrAgentNotFound) {
		return "", fmt.Errorf("memstore: check agent %s: %w", agentID, err)
	}

	dir, err := os.MkdirTemp("", "engram-seed-*")
	if err != nil {
		return "", fmt.Errorf("memstore: seed dir: %w", err)
	}
	defer os.RemoveAll(dir)
	for p, content := range seed {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return "", fmt.Errorf("memstore: seed write %s: %w", p, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return "", fmt.Errorf("memstore: seed write %s: %w", p, err)
		}
	}
	h, err := gitfs.Commit(ctx, s.objs, "", dir, "init")
	if err != nil {
		return "", fmt.Errorf("memstore: seed commit: %w", err)
	}
	if err := s.refs.Bootstrap(ctx, agentID, h); err != nil {
		return "", err
	}
	return CommitHash(h), nil
}

var _ MemStore = (*Store)(nil)
