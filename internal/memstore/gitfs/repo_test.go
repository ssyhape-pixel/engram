package gitfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestInitialCommitAndMaterialize(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())

	work1 := t.TempDir()
	if err := os.WriteFile(filepath.Join(work1, "hello.md"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h1, err := Commit(ctx, objs, "", work1, "init")
	if err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	if h1 == "" {
		t.Fatal("empty hash")
	}

	mat := t.TempDir()
	if err := Materialize(ctx, objs, h1, mat); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mat, "hello.md"))
	if err != nil || string(got) != "v1\n" {
		t.Fatalf("materialized content = %q,%v want v1", got, err)
	}
}

func TestSecondCommitHasParentAndDiff(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())

	w1 := t.TempDir()
	os.WriteFile(filepath.Join(w1, "f.md"), []byte("one\n"), 0o644)
	h1, err := Commit(ctx, objs, "", w1, "c1")
	if err != nil {
		t.Fatal(err)
	}

	w2 := t.TempDir()
	if err := Materialize(ctx, objs, h1, w2); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(w2, "f.md"), []byte("one\ntwo\n"), 0o644)
	h2, err := Commit(ctx, objs, h1, w2, "c2")
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	if h2 == h1 {
		t.Fatal("h2 should differ from h1")
	}

	m := t.TempDir()
	if err := Materialize(ctx, objs, h2, m); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(m, "f.md"))
	if string(got) != "one\ntwo\n" {
		t.Fatalf("h2 content = %q", got)
	}
}
