package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

func sessionFixture(t *testing.T, steps []func(Request) Response) (*Session, *memstore.Store, string) {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
	if err := refs.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	store := memstore.New(objstore.NewLocal(t.TempDir()), refs.New(pool))
	head, err := store.CreateAgent(ctx, "a1", map[string]string{"system/about.md": "---\ndescription: who\n---\nresident\n"})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	workdir := t.TempDir()
	if err := store.Materialize(ctx, "a1", head, workdir); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	tools := NewToolset(workdir, "a1", search.NewGrep(workdir))
	s := NewSession(store, &FakeProvider{Steps: steps}, tools, "a1", head, workdir, nil)
	return s, store, workdir
}

func TestSendEditCommitsAndAdvancesHead(t *testing.T) {
	ctx := context.Background()
	s, store, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "edit", Input: map[string]any{"path": "note.md", "content": "hello\n"}}}}
		},
		func(r Request) Response { return Response{Text: "saved your note"} },
	})
	startHead := s.Head()

	out, err := s.Send(ctx, "remember hello")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if out != "saved your note" {
		t.Fatalf("assistant text = %q", out)
	}
	if s.Head() == startHead {
		t.Fatal("HEAD did not advance after an edit")
	}
	check := t.TempDir()
	if err := store.Materialize(ctx, "a1", s.Head(), check); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(check, "note.md"))
	if err != nil || string(got) != "hello\n" {
		t.Fatalf("committed note = %q %v", got, err)
	}
}

func TestSendReadOnlyDoesNotCommit(t *testing.T) {
	ctx := context.Background()
	s, _, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "read", Input: map[string]any{"path": "system/about.md"}}}}
		},
		func(r Request) Response { return Response{Text: "it says: resident"} },
	})
	startHead := s.Head()
	if _, err := s.Send(ctx, "what do you know?"); err != nil {
		t.Fatalf("send: %v", err)
	}
	if s.Head() != startHead {
		t.Fatal("read-only turn must not advance HEAD")
	}
}

func TestSendMultiTurnAccumulatesHistory(t *testing.T) {
	ctx := context.Background()
	turn2SawHistory := false
	s, _, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response { return Response{Text: "hi"} },
		func(r Request) Response {
			if len(r.Messages) >= 3 {
				turn2SawHistory = true
			}
			return Response{Text: "you said hi back"}
		},
	})
	if _, err := s.Send(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(ctx, "again"); err != nil {
		t.Fatal(err)
	}
	if !turn2SawHistory {
		t.Fatal("second turn's request did not include accumulated history")
	}
	if len(s.History()) < 4 {
		t.Fatalf("history should accumulate across turns, got %d entries", len(s.History()))
	}
}

func TestSendCASConflictSurfacesError(t *testing.T) {
	ctx := context.Background()
	s, store, _ := sessionFixture(t, []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "edit", Input: map[string]any{"path": "n.md", "content": "v\n"}}}}
		},
		func(r Request) Response { return Response{Text: "ok"} },
	})
	other := t.TempDir()
	if err := store.Materialize(ctx, "a1", s.Head(), other); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "external.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitWithCAS(ctx, "a1", s.Head(), other, nil); err != nil {
		t.Fatalf("external commit: %v", err)
	}
	_, err := s.Send(ctx, "save")
	if !errors.Is(err, memstore.ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict surfaced, got %v", err)
	}
}
