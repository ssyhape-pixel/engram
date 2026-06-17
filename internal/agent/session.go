package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ssy/engram/internal/memstore"
)

const defaultMaxSteps = 16

// Session is a stateful, single-writer agent conversation over a materialized
// working tree. Chat history is ephemeral (lives for the session); the durable
// state is the memory repo, advanced by CommitWithCAS on dirty turns.
type Session struct {
	agentID  string
	store    memstore.MemStore
	prov     LLMProvider
	tools    *Toolset
	head     memstore.CommitHash
	workdir  string
	history  []Message
	dirty    bool
	maxSteps int
	release  func() // called by Close; nil for direct (test) construction
}

// NewSession wires a session. release (may be nil) is invoked by Close to free
// the writer lock and clean the workdir; the Router supplies it.
func NewSession(store memstore.MemStore, prov LLMProvider, tools *Toolset, agentID string, head memstore.CommitHash, workdir string, release func()) *Session {
	return &Session{
		agentID:  agentID,
		store:    store,
		prov:     prov,
		tools:    tools,
		head:     head,
		workdir:  workdir,
		maxSteps: defaultMaxSteps,
		release:  release,
	}
}

func (s *Session) Head() memstore.CommitHash { return s.head }
func (s *Session) History() []Message        { return s.history }

// Close frees the workdir and (if set) the writer lock.
func (s *Session) Close() error {
	if s.release != nil {
		s.release()
	}
	return nil
}

// Send runs one turn: the model may call tools (recall/read/edit) until it
// returns final text; a turn that edited memory is committed.
func (s *Session) Send(ctx context.Context, userMessage string) (string, error) {
	s.history = append(s.history, Message{Role: RoleUser, Text: userMessage})

	final := ""
	for step := 0; step < s.maxSteps; step++ {
		sys, err := s.assembleSystem()
		if err != nil {
			return "", fmt.Errorf("agent: assemble system: %w", err)
		}
		resp, err := s.prov.Generate(ctx, Request{System: sys, Messages: s.history, Tools: s.tools.Defs()})
		if err != nil {
			return "", fmt.Errorf("agent: generate: %w", err)
		}
		if len(resp.ToolCalls) == 0 {
			final = resp.Text
			s.history = append(s.history, Message{Role: RoleAssistant, Text: resp.Text})
			break
		}
		s.history = append(s.history, Message{Role: RoleAssistant, ToolCalls: resp.ToolCalls})
		results := make([]ToolResult, 0, len(resp.ToolCalls))
		for _, call := range resp.ToolCalls {
			res := s.tools.Dispatch(ctx, call)
			if call.Name == "edit" && !res.IsError {
				s.dirty = true
			}
			results = append(results, res)
		}
		s.history = append(s.history, Message{Role: RoleTool, Results: results})
		if step == s.maxSteps-1 {
			return "", fmt.Errorf("agent: tool-use loop exceeded maxSteps=%d", s.maxSteps)
		}
	}

	if s.dirty {
		if err := s.commit(ctx); err != nil {
			return "", err
		}
	}
	return final, nil
}

// commit persists the workdir, advancing HEAD. On a CAS conflict (which cannot
// happen under the single-writer Router, but is handled defensively) it
// re-resolves HEAD and retries once with the same workdir.
func (s *Session) commit(ctx context.Context) error {
	jobs := []memstore.Job{{Kind: "reindex"}, {Kind: "reflect"}}
	newHead, err := s.store.CommitWithCAS(ctx, s.agentID, s.head, s.workdir, jobs)
	if errors.Is(err, memstore.ErrCASConflict) {
		cur, rerr := s.store.ResolveHead(ctx, s.agentID)
		if rerr != nil {
			return fmt.Errorf("agent: resolve after CAS conflict: %w", rerr)
		}
		s.head = cur
		newHead, err = s.store.CommitWithCAS(ctx, s.agentID, s.head, s.workdir, jobs)
	}
	if err != nil {
		return fmt.Errorf("agent: commit: %w", err)
	}
	s.head = newHead
	s.dirty = false
	return nil
}

// assembleSystem builds the resident system prompt: all system/ file contents
// plus a tree index (path: description) of the whole memory tree.
func (s *Session) assembleSystem() (string, error) {
	var b strings.Builder
	b.WriteString("# Resident memory (system/)\n")
	systemDir := filepath.Join(s.workdir, "system")
	_ = filepath.WalkDir(systemDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // system/ may not exist yet
		}
		if d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		rel, _ := filepath.Rel(s.workdir, path)
		fmt.Fprintf(&b, "\n## %s\n%s\n", rel, string(data))
		return nil
	})

	b.WriteString("\n# Memory tree index (path: description)\n")
	var idx []string
	err := filepath.WalkDir(s.workdir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(s.workdir, path)
		idx = append(idx, fmt.Sprintf("%s: %s", rel, frontmatterDescription(path)))
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(idx)
	b.WriteString(strings.Join(idx, "\n"))
	return b.String(), nil
}
