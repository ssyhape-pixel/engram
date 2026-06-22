// Package agent implements Engram's provider-agnostic agent loop: a stateful
// Session that runs a tool-use loop over a working tree and persists edits via
// the L1 MemStore. LLMProvider abstracts the model so a deterministic fake can
// drive tests and a real Anthropic adapter can run live.
package agent

import "context"

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolDef advertises a tool to the model.
type ToolDef struct {
	Name        string
	Description string
	InputSchema map[string]any // JSON Schema object describing the tool's input parameters
}

// ToolCall is a model-initiated tool invocation.
type ToolCall struct {
	ID    string
	Name  string
	Input map[string]any
}

// ToolResult is the outcome of executing a ToolCall, fed back to the model.
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// Message is one conversation entry. Role determines which fields are set: RoleUser/RoleAssistant use Text (assistant may also set ToolCalls); RoleTool uses Results.
type Message struct {
	Role      Role
	Text      string       // user/assistant text
	ToolCalls []ToolCall   // assistant-initiated calls (Role==assistant)
	Results   []ToolResult // tool results (Role==tool)
}

// Request is one Generate input.
type Request struct {
	System   string
	Messages []Message
	Tools    []ToolDef
}

// Response: if ToolCalls is non-empty the turn is not complete — the caller
// executes them and feeds results back via a new Generate call. Otherwise Text
// is the final assistant message.
type Response struct {
	Text      string
	ToolCalls []ToolCall
}

// LLMProvider produces the next model response given the conversation so far.
type LLMProvider interface {
	Generate(ctx context.Context, req Request) (Response, error)
}
