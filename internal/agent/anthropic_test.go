package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestAnthropicBuildsRequestAndParsesToolUse(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("x-api-key") != "k" || r.Header.Get("anthropic-version") == "" {
			t.Fatalf("missing headers: %v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return jsonResp(200, `{"content":[{"type":"tool_use","id":"tu_1","name":"recall","input":{"query":"x"}}],"stop_reason":"tool_use"}`), nil
	})}
	p := NewAnthropic("k", WithModel("claude-sonnet-4-6"), WithHTTPClient(client))

	resp, err := p.Generate(context.Background(), Request{
		System:   "SYS",
		Messages: []Message{{Role: RoleUser, Text: "hi"}},
		Tools:    []ToolDef{{Name: "recall", Description: "d", InputSchema: map[string]any{"type": "object"}}},
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if captured["model"] != "claude-sonnet-4-6" || captured["system"] != "SYS" {
		t.Fatalf("request body wrong: %v", captured)
	}
	if _, ok := captured["max_tokens"]; !ok {
		t.Fatal("max_tokens missing")
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "recall" || resp.ToolCalls[0].ID != "tu_1" {
		t.Fatalf("parsed tool calls wrong: %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Input["query"] != "x" {
		t.Fatalf("tool input wrong: %+v", resp.ToolCalls[0].Input)
	}
}

func TestAnthropicParsesFinalText(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"content":[{"type":"text","text":"all done"}],"stop_reason":"end_turn"}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	resp, err := p.Generate(context.Background(), Request{Messages: []Message{{Role: RoleUser, Text: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "all done" || len(resp.ToolCalls) != 0 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestAnthropicMapsToolResultMessages(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return jsonResp(200, `{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	_, err := p.Generate(context.Background(), Request{Messages: []Message{
		{Role: RoleUser, Text: "do it"},
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "tu_1", Name: "edit", Input: map[string]any{"path": "a"}}}},
		{Role: RoleTool, Results: []ToolResult{{CallID: "tu_1", Content: "wrote a"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	msgs, _ := captured["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("want 3 mapped messages, got %d: %v", len(msgs), captured["messages"])
	}
	last, _ := msgs[2].(map[string]any)
	if last["role"] != "user" {
		t.Fatalf("tool result should map to a user message, got %v", last["role"])
	}
}

func TestAnthropicNon2xxErrors(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `{"error":{"message":"rate limited"}}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	if _, err := p.Generate(context.Background(), Request{Messages: []Message{{Role: RoleUser, Text: "hi"}}}); err == nil {
		t.Fatal("expected error on 429")
	}
}

func TestAnthropicToolUseAlwaysHasInput(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return jsonResp(200, `{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}`), nil
	})}
	p := NewAnthropic("k", WithHTTPClient(client))
	_, err := p.Generate(context.Background(), Request{Messages: []Message{
		{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "tu_1", Name: "noargs", Input: nil}}},
		{Role: RoleTool, Results: []ToolResult{{CallID: "tu_1", Content: "done"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := captured["messages"].([]any)
	asst := msgs[0].(map[string]any)
	blocks := asst["content"].([]any)
	tu := blocks[0].(map[string]any)
	if _, ok := tu["input"]; !ok {
		t.Fatalf("tool_use block must always carry input (even {}): %v", tu)
	}
}
