package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	defaultAnthropicModel   = "claude-sonnet-4-6"
	defaultAnthropicMaxTok  = 4096
	defaultAnthropicBaseURL = "https://api.anthropic.com"
	anthropicVersion        = "2023-06-01"
)

// AnthropicProvider calls the Anthropic Messages API with tool-use.
type AnthropicProvider struct {
	apiKey    string
	model     string
	maxTokens int
	baseURL   string
	client    *http.Client
}

type AnthropicOption func(*AnthropicProvider)

func WithModel(m string) AnthropicOption   { return func(p *AnthropicProvider) { p.model = m } }
func WithMaxTokens(n int) AnthropicOption  { return func(p *AnthropicProvider) { p.maxTokens = n } }
func WithBaseURL(u string) AnthropicOption { return func(p *AnthropicProvider) { p.baseURL = u } }
func WithHTTPClient(c *http.Client) AnthropicOption {
	return func(p *AnthropicProvider) { p.client = c }
}

func NewAnthropic(apiKey string, opts ...AnthropicOption) *AnthropicProvider {
	p := &AnthropicProvider{
		apiKey:    apiKey,
		model:     defaultAnthropicModel,
		maxTokens: defaultAnthropicMaxTok,
		baseURL:   defaultAnthropicBaseURL,
		client:    http.DefaultClient,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

type apiTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"input_schema"`
}

type apiBlock struct {
	Type    string         `json:"type"`
	Text    string         `json:"text,omitempty"`
	ID      string         `json:"id,omitempty"`
	Name    string         `json:"name,omitempty"`
	Input   map[string]any `json:"input,omitempty"`
	ToolUse string         `json:"tool_use_id,omitempty"`
	Content string         `json:"content,omitempty"`
	IsError bool           `json:"is_error,omitempty"`
}

type apiMessage struct {
	Role    string     `json:"role"`
	Content []apiBlock `json:"content"`
}

type apiRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	System    string       `json:"system,omitempty"`
	Messages  []apiMessage `json:"messages"`
	Tools     []apiTool    `json:"tools,omitempty"`
}

type apiResponse struct {
	Content    []apiBlock `json:"content"`
	StopReason string     `json:"stop_reason"`
}

func toAPIMessages(msgs []Message) []apiMessage {
	out := make([]apiMessage, 0, len(msgs))
	for _, m := range msgs {
		switch m.Role {
		case RoleUser:
			out = append(out, apiMessage{Role: "user", Content: []apiBlock{{Type: "text", Text: m.Text}}})
		case RoleAssistant:
			var blocks []apiBlock
			if m.Text != "" {
				blocks = append(blocks, apiBlock{Type: "text", Text: m.Text})
			}
			for _, tc := range m.ToolCalls {
				blocks = append(blocks, apiBlock{Type: "tool_use", ID: tc.ID, Name: tc.Name, Input: tc.Input})
			}
			out = append(out, apiMessage{Role: "assistant", Content: blocks})
		case RoleTool:
			var blocks []apiBlock
			for _, r := range m.Results {
				blocks = append(blocks, apiBlock{Type: "tool_result", ToolUse: r.CallID, Content: r.Content, IsError: r.IsError})
			}
			out = append(out, apiMessage{Role: "user", Content: blocks})
		}
	}
	return out
}

func (p *AnthropicProvider) Generate(ctx context.Context, req Request) (Response, error) {
	tools := make([]apiTool, 0, len(req.Tools))
	for _, t := range req.Tools {
		tools = append(tools, apiTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
	}
	body, err := json.Marshal(apiRequest{
		Model:     p.model,
		MaxTokens: p.maxTokens,
		System:    req.System,
		Messages:  toAPIMessages(req.Messages),
		Tools:     tools,
	})
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: new request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	httpResp, err := p.client.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: do: %w", err)
	}
	defer httpResp.Body.Close()
	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("anthropic: read body: %w", err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return Response{}, fmt.Errorf("anthropic: status %d: %s", httpResp.StatusCode, string(raw))
	}
	var ar apiResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return Response{}, fmt.Errorf("anthropic: unmarshal: %w", err)
	}
	var resp Response
	for _, b := range ar.Content {
		switch b.Type {
		case "text":
			resp.Text += b.Text
		case "tool_use":
			resp.ToolCalls = append(resp.ToolCalls, ToolCall{ID: b.ID, Name: b.Name, Input: b.Input})
		}
	}
	return resp, nil
}

var _ LLMProvider = (*AnthropicProvider)(nil)
