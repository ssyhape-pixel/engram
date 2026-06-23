package gitfs

import (
	"context"
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/ssy/engram/internal/memstore/objstore"
)

// ReachableObjects returns the set of all object hashes reachable from the given
// commits: each commit, its full tree (subtrees + blobs), and all ancestor
// commits via parent links (history is kept for diffability). Empty head
// strings are skipped. If the closure cannot be fully computed an error is
// returned; callers (GC) MUST NOT sweep against a partial set.
func ReachableObjects(ctx context.Context, objs objstore.ObjStore, heads []string) (map[string]struct{}, error) {
	st := NewStorage(ctx, objs)
	reachable := map[string]struct{}{}
	seen := map[string]struct{}{} // commits already visited (cycle/dup guard)

	var visit func(h string) error
	visit = func(h string) error {
		if h == "" {
			return nil
		}
		if _, ok := seen[h]; ok {
			return nil
		}
		seen[h] = struct{}{}
		c, err := object.GetCommit(st, plumbing.NewHash(h))
		if err != nil {
			return fmt.Errorf("gitfs: reach commit %s: %w", h, err)
		}
		reachable[h] = struct{}{}
		tree, err := c.Tree()
		if err != nil {
			return fmt.Errorf("gitfs: reach tree of %s: %w", h, err)
		}
		if err := addTree(reachable, tree); err != nil {
			return err
		}
		for _, p := range c.ParentHashes {
			if err := visit(p.String()); err != nil {
				return err
			}
		}
		return nil
	}

	for _, h := range heads {
		if err := visit(h); err != nil {
			return nil, err
		}
	}
	return reachable, nil
}

// addTree adds the tree's own hash, every entry hash (blob or subtree), and
// recurses into subtrees.
func addTree(reachable map[string]struct{}, tree *object.Tree) error {
	reachable[tree.Hash.String()] = struct{}{}
	for _, e := range tree.Entries {
		reachable[e.Hash.String()] = struct{}{}
		if e.Mode == filemode.Dir {
			sub, err := tree.Tree(e.Name)
			if err != nil {
				return fmt.Errorf("gitfs: reach subtree %s: %w", e.Name, err)
			}
			if err := addTree(reachable, sub); err != nil {
				return err
			}
		}
	}
	return nil
}
