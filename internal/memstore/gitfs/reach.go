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
	seenCommits := map[string]struct{}{}
	seenTrees := map[string]struct{}{}

	stack := make([]string, 0, len(heads))
	stack = append(stack, heads...)
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if h == "" {
			continue
		}
		if _, ok := seenCommits[h]; ok {
			continue
		}
		seenCommits[h] = struct{}{}
		c, err := object.GetCommit(st, plumbing.NewHash(h))
		if err != nil {
			return nil, fmt.Errorf("gitfs: reach commit %s: %w", h, err)
		}
		reachable[h] = struct{}{}
		tree, err := c.Tree()
		if err != nil {
			return nil, fmt.Errorf("gitfs: reach tree of %s: %w", h, err)
		}
		if err := addTree(reachable, seenTrees, st, tree); err != nil {
			return nil, err
		}
		for _, p := range c.ParentHashes {
			stack = append(stack, p.String())
		}
	}
	return reachable, nil
}

// addTree adds the tree's own hash, every entry hash (blob or subtree), and
// recurses into unseen subtrees. seenTrees dedups shared subtrees across commits.
func addTree(reachable, seenTrees map[string]struct{}, st *Storage, tree *object.Tree) error {
	th := tree.Hash.String()
	if _, ok := seenTrees[th]; ok {
		return nil
	}
	seenTrees[th] = struct{}{}
	reachable[th] = struct{}{}
	for _, e := range tree.Entries {
		reachable[e.Hash.String()] = struct{}{}
		if e.Mode == filemode.Dir {
			sub, err := object.GetTree(st, e.Hash)
			if err != nil {
				return fmt.Errorf("gitfs: reach subtree %s: %w", e.Name, err)
			}
			if err := addTree(reachable, seenTrees, st, sub); err != nil {
				return err
			}
		}
	}
	return nil
}
