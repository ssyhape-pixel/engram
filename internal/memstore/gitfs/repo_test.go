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

func TestTreeKeysGranularity(t *testing.T) {
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
	write(wA, "system/about.md", "a\n")
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

	wC := t.TempDir()
	if err := Materialize(ctx, objs, hA, wC); err != nil {
		t.Fatal(err)
	}
	write(wC, "system/about.md", "a2\n")
	hC, err := Commit(ctx, objs, hA, wC, "C")
	if err != nil {
		t.Fatal(err)
	}

	rA, sA, err := TreeKeys(ctx, objs, hA)
	if err != nil {
		t.Fatal(err)
	}
	rB, sB, err := TreeKeys(ctx, objs, hB)
	if err != nil {
		t.Fatal(err)
	}
	_, sC, err := TreeKeys(ctx, objs, hC)
	if err != nil {
		t.Fatal(err)
	}

	if sA == "" {
		t.Fatal("systemSubtree should be non-empty when system/ exists")
	}
	if rA == rB {
		t.Fatal("root tree must change when notes/ changes")
	}
	if sA != sB {
		t.Fatal("system subtree must NOT change when only notes/ changes")
	}
	if sA == sC {
		t.Fatal("system subtree must change when system/ changes")
	}
}

func TestTreeKeysNoSystemDir(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	w := t.TempDir()
	if err := os.WriteFile(filepath.Join(w, "loose.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := Commit(ctx, objs, "", w, "init")
	if err != nil {
		t.Fatal(err)
	}
	root, sys, err := TreeKeys(ctx, objs, h)
	if err != nil {
		t.Fatal(err)
	}
	if root == "" {
		t.Fatal("root tree should be non-empty")
	}
	if sys != "" {
		t.Fatalf("systemSubtree should be empty without system/, got %q", sys)
	}
}
