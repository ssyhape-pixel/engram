package maintenance

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ssy/engram/internal/memstore"
)

// Completer is the narrow LLM surface reflection needs: a single text
// completion. Keeps maintenance decoupled from the agent package's tool-use
// protocol; the cmd wires an adapter over an agent.LLMProvider.
type Completer interface {
	Complete(ctx context.Context, system, user string) (string, error)
}

// ErrConflict means reflection lost the CAS race (the agent advanced HEAD); the
// job should be requeued and retried on a later, fresher round.
var ErrConflict = errors.New("maintenance: reflect lost CAS race; retry later")

const reflectSystemPrompt = "You are the reflection pass of an agent memory system. " +
	"Consolidate the agent's current resident memory below into a concise, current-state note. " +
	"Output only the consolidated note."

// Reflect materializes the agent's HEAD, asks the Completer to consolidate the
// resident system/ content, writes the result to system/reflection.md, and
// commits it with jobs=nil so reflection never re-enqueues itself. A CAS
// conflict (agent advanced concurrently) returns ErrConflict.
func Reflect(ctx context.Context, store memstore.MemStore, c Completer, agentID, fromSHA string) error {
	head, err := store.ResolveHead(ctx, agentID)
	if err != nil {
		return fmt.Errorf("maintenance: resolve head %s: %w", agentID, err)
	}
	dir, err := os.MkdirTemp("", "engram-reflect-*")
	if err != nil {
		return fmt.Errorf("maintenance: scratch: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := store.Materialize(ctx, agentID, head, dir); err != nil {
		return fmt.Errorf("maintenance: materialize: %w", err)
	}

	resident := readSystemDir(dir)
	out, err := c.Complete(ctx, reflectSystemPrompt, resident)
	if err != nil {
		return fmt.Errorf("maintenance: complete: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "system"), 0o755); err != nil {
		return fmt.Errorf("maintenance: mkdir system: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "system", "reflection.md"), []byte(out), 0o644); err != nil {
		return fmt.Errorf("maintenance: write reflection: %w", err)
	}

	// jobs=nil: reflection never enqueues a reflect job (no self-trigger loop).
	if _, err := store.CommitWithCAS(ctx, agentID, head, dir, nil); err != nil {
		if errors.Is(err, memstore.ErrCASConflict) {
			return ErrConflict
		}
		return fmt.Errorf("maintenance: reflect commit: %w", err)
	}
	return nil
}

// readSystemDir concatenates all files under <dir>/system/ (the resident set).
func readSystemDir(dir string) string {
	var b strings.Builder
	systemDir := filepath.Join(dir, "system")
	_ = filepath.WalkDir(systemDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		fmt.Fprintf(&b, "## %s\n%s\n", rel, string(data))
		return nil
	})
	return b.String()
}
