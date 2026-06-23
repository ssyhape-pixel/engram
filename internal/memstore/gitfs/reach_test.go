package gitfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestReachableIncludesAncestorsExcludesOrphan(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	write := func(dir, rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	wA := t.TempDir()
	write(wA, "system/a.md", "a\n")
	write(wA, "notes/x.md", "x\n")
	hA, err := Commit(ctx, objs, "", wA, "A")
	if err != nil {
		t.Fatal(err)
	}

	wB := t.TempDir()
	if err := Materialize(ctx, objs, hA, wB); err != nil {
		t.Fatal(err)
	}
	write(wB, "notes/x.md", "x2\n")
	hB, err := Commit(ctx, objs, hA, wB, "B")
	if err != nil {
		t.Fatal(err)
	}

	orphan := "0000000000000000000000000000000000000000"
	if err := objs.Put(ctx, orphan, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	reach, err := ReachableObjects(ctx, objs, []string{hB})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reach[hB]; !ok {
		t.Fatal("HEAD commit hB must be reachable")
	}
	if _, ok := reach[hA]; !ok {
		t.Fatal("ancestor commit hA must be reachable")
	}
	if _, ok := reach[orphan]; ok {
		t.Fatal("orphan object must NOT be reachable")
	}
	if len(reach) < 4 {
		t.Fatalf("expected commits + trees + blobs, got %d objects", len(reach))
	}
}

func TestReachableEmptyHeadIsEmpty(t *testing.T) {
	ctx := context.Background()
	reach, err := ReachableObjects(ctx, objstore.NewLocal(t.TempDir()), []string{""})
	if err != nil {
		t.Fatal(err)
	}
	if len(reach) != 0 {
		t.Fatalf("empty head should yield empty set, got %d", len(reach))
	}
}
