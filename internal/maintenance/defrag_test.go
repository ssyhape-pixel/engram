package maintenance

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitTopLevel(t *testing.T) {
	parts := splitTopLevel([]byte("# A\nalpha\n# B\nbeta\n"))
	if len(parts) != 2 {
		t.Fatalf("want 2 parts, got %d: %q", len(parts), parts)
	}
	if !bytes.HasPrefix(parts[0], []byte("# A")) || !bytes.HasPrefix(parts[1], []byte("# B")) {
		t.Fatalf("parts mis-split: %q", parts)
	}
	pp := splitTopLevel([]byte("intro line\n# A\nx\n"))
	if len(pp) != 2 || !bytes.HasPrefix(pp[0], []byte("intro")) {
		t.Fatalf("preamble split wrong: %q", pp)
	}
	if got := splitTopLevel([]byte("# A\n## sub\nx\n")); len(got) != 1 {
		t.Fatalf("## must not split: %d parts", len(got))
	}
	if got := splitTopLevel([]byte("# only\nbody\n")); len(got) != 1 {
		t.Fatalf("single heading → 1 part, got %d", len(got))
	}
	if got := splitTopLevel(nil); len(got) != 0 {
		t.Fatalf("empty → 0 parts, got %d", len(got))
	}
}

func TestIsSplittable(t *testing.T) {
	big := []byte("# A\n" + strings.Repeat("x", 40) + "\n# B\n" + strings.Repeat("y", 40) + "\n")
	if !isSplittable("notes/big.md", big, 50) {
		t.Fatal("big .md with 2 headings >50 should be splittable")
	}
	if isSplittable("notes/big.txt", big, 50) {
		t.Fatal("non-.md must not be splittable")
	}
	if isSplittable("notes/small.md", []byte("# A\nx\n# B\ny\n"), 50) {
		t.Fatal("<=maxBytes must not be splittable")
	}
	if isSplittable("notes/one.md", []byte("# A\n"+strings.Repeat("x", 80)+"\n"), 50) {
		t.Fatal("single-heading (1 part) must not be splittable even if big")
	}
}

func TestDefragSplitsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatalf("defrag: %v", err)
	}
	h1, _ := store.ResolveHead(ctx, agentID)
	dir := t.TempDir()
	if err := store.Materialize(ctx, agentID, h1, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "notes", "big.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("original big.md should be gone")
	}
	p1, err1 := os.ReadFile(filepath.Join(dir, "notes", "big.01.md"))
	p2, err2 := os.ReadFile(filepath.Join(dir, "notes", "big.02.md"))
	if err1 != nil || err2 != nil {
		t.Fatalf("split files missing: %v %v", err1, err2)
	}
	if !bytes.HasPrefix(p1, []byte("# Alpha")) || !bytes.HasPrefix(p2, []byte("# Beta")) {
		t.Fatalf("split content wrong: %q %q", p1, p2)
	}
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatalf("defrag 2: %v", err)
	}
	h2, _ := store.ResolveHead(ctx, agentID)
	if h2 != h1 {
		t.Fatal("second defrag must not commit (converged)")
	}
}

func TestDefragNoSplittableNoCommit(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	head, _ := store.CreateAgent(ctx, agentID, map[string]string{"system/x.md": "# A\nsmall\n"})
	if err := Defrag(ctx, store, agentID, 50); err != nil {
		t.Fatal(err)
	}
	h, _ := store.ResolveHead(ctx, agentID)
	if h != head {
		t.Fatal("no splittable file → no commit, HEAD unchanged")
	}
}

func TestDefragConflictReturnsErrConflict(t *testing.T) {
	ctx := context.Background()
	store, agentID := reflectStore(t)
	big := "# Alpha\n" + strings.Repeat("a", 40) + "\n# Beta\n" + strings.Repeat("b", 40) + "\n"
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{"notes/big.md": big}); err != nil {
		t.Fatal(err)
	}
	adv := &advancingStore{Store: store}
	if err := Defrag(ctx, adv, agentID, 50); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}
