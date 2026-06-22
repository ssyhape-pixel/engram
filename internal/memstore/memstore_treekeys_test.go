package memstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
)

// DB-free: TreeKeys reads only objects, so refs may be nil here.
func TestStoreTreeKeysDelegates(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	w := t.TempDir()
	if err := os.MkdirAll(filepath.Join(w, "system"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w, "system", "a.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := gitfs.Commit(ctx, objs, "", w, "init")
	if err != nil {
		t.Fatal(err)
	}

	s := New(objs, nil) // refs unused by TreeKeys
	root, sys, err := s.TreeKeys(ctx, CommitHash(h))
	if err != nil {
		t.Fatal(err)
	}
	if root == "" || sys == "" {
		t.Fatalf("expected non-empty keys, got root=%q sys=%q", root, sys)
	}
}
