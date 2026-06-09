package gitfs

import (
	"context"
	"fmt"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/ssy/engram/internal/memstore/objstore"
)

const branchRef = plumbing.ReferenceName("refs/heads/main")

func author() *object.Signature {
	return &object.Signature{Name: "engram-agent", Email: "agent@engram", When: time.Now()}
}

// seedRefs points the session Storage's HEAD at parent so go-git can build a
// child commit / checkout on top of it.
func seedRefs(st *Storage, parent string) error {
	h := plumbing.NewHash(parent)
	if err := st.SetReference(plumbing.NewHashReference(branchRef, h)); err != nil {
		return err
	}
	return st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branchRef))
}

// Materialize checks out the tree at `at` into dir. `at` empty means an empty
// tree (nothing written).
func Materialize(ctx context.Context, objs objstore.ObjStore, at string, dir string) error {
	if at == "" {
		return nil
	}
	st := NewStorage(ctx, objs)
	if err := seedRefs(st, at); err != nil {
		return fmt.Errorf("gitfs: seed refs: %w", err)
	}
	repo, err := git.Open(st, osfs.New(dir))
	if err != nil {
		return fmt.Errorf("gitfs: open: %w", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("gitfs: worktree: %w", err)
	}
	if err := wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(at), Force: true}); err != nil {
		return fmt.Errorf("gitfs: checkout %s: %w", at, err)
	}
	return nil
}

// Commit stages everything under dir and builds a commit on `parent` (empty =>
// initial commit). New objects are written through Storage into ObjStore.
func Commit(ctx context.Context, objs objstore.ObjStore, parent string, dir string, msg string) (string, error) {
	st := NewStorage(ctx, objs)
	fs := osfs.New(dir)

	var repo *git.Repository
	var err error
	if parent == "" {
		repo, err = git.Init(st, fs)
		if err != nil {
			return "", fmt.Errorf("gitfs: init: %w", err)
		}
	} else {
		if err := seedRefs(st, parent); err != nil {
			return "", fmt.Errorf("gitfs: seed refs: %w", err)
		}
		repo, err = git.Open(st, fs)
		if err != nil {
			return "", fmt.Errorf("gitfs: open: %w", err)
		}
	}

	wt, err := repo.Worktree()
	if err != nil {
		return "", fmt.Errorf("gitfs: worktree: %w", err)
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		return "", fmt.Errorf("gitfs: add: %w", err)
	}
	h, err := wt.Commit(msg, &git.CommitOptions{Author: author(), AllowEmptyCommits: true})
	if err != nil {
		return "", fmt.Errorf("gitfs: commit: %w", err)
	}
	return h.String(), nil
}
