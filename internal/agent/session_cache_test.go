package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/search"
)

type spyCache struct {
	data       map[string]string
	gets, puts int
}

func newSpyCache() *spyCache                    { return &spyCache{data: map[string]string{}} }
func (c *spyCache) Get(k string) (string, bool) { c.gets++; v, ok := c.data[k]; return v, ok }
func (c *spyCache) Put(k, v string)             { c.puts++; c.data[k] = v }

var _ cache.Cache = (*spyCache)(nil)

// residentSession builds a Session over a real committed tree WITHOUT Postgres
// (refs nil; only assembleSystem/TreeKeys are exercised — object reads only).
func residentSession(t *testing.T, c cache.Cache) *Session {
	t.Helper()
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	seed := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(seed, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("system/about.md", "---\ndescription: who\n---\nresident\n")
	write("notes/n.md", "---\ndescription: a note\n---\nbody\n")
	head, err := gitfs.Commit(ctx, objs, "", seed, "init")
	if err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	if err := gitfs.Materialize(ctx, objs, head, workdir); err != nil {
		t.Fatal(err)
	}
	store := memstore.New(objs, nil)
	tools := NewToolset(workdir, "a1", search.NewGrep(workdir))
	return NewSession(store, &FakeProvider{}, tools, "a1", memstore.CommitHash(head), workdir, nil, c)
}

func TestAssembleSystemReusesCacheWhenClean(t *testing.T) {
	ctx := context.Background()
	spy := newSpyCache()
	s := residentSession(t, spy)

	out1, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out2, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out1 != out2 {
		t.Fatal("cached output must equal first build")
	}
	if spy.puts != 2 {
		t.Fatalf("expected 2 builds (sys+idx), got %d puts", spy.puts)
	}
	if spy.gets != 4 {
		t.Fatalf("expected 4 gets (2 keys x 2 calls), got %d", spy.gets)
	}
	if !strings.Contains(out1, "resident") || !strings.Contains(out1, "a note") {
		t.Fatalf("resident output missing content:\n%s", out1)
	}
}

func TestAssembleSystemBypassesCacheWhenDirty(t *testing.T) {
	ctx := context.Background()
	spy := newSpyCache()
	s := residentSession(t, spy)
	s.dirty = true

	out, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spy.gets != 0 || spy.puts != 0 {
		t.Fatalf("dirty turn must bypass cache; gets=%d puts=%d", spy.gets, spy.puts)
	}
	if !strings.Contains(out, "resident") {
		t.Fatalf("bypass output missing content:\n%s", out)
	}
}

func TestAssembleSystemNilCacheRecomputes(t *testing.T) {
	ctx := context.Background()
	s := residentSession(t, nil)
	out, err := s.assembleSystem(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "resident") || !strings.Contains(out, "a note") {
		t.Fatalf("nil-cache output missing content:\n%s", out)
	}
}
