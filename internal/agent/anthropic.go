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

type apiMessage struct {
	Role    string `json:"role"`
	Content []any  `json:"content"`
}

type apiRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	System    string       `json:"system,omitempty"`
	Messages  []apiMessage `json:"messages"`
	Tools     []apiTool    `json:"tools,omitempty"`
}

// apiRespBlock is the response-only content block shape we parse from the API.
//
// TODO(task7): the live API may return tool_result content as an array of
// blocks; validate the real response shape and adjust if needed.
type apiRespBlock struct {
	Type  string         `json:"type"`
	Text  string         `json:"text"`
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

// apiResponse: StopReason is captured but not yet surfaced on Response.
type apiResponse struct {
	Content    []apiRespBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
}

func toAPIMessages(msgs []Message) []apiMessage {
	out := make([]apiMessage, 0, len(msgs))
	for _, m := range msgs {
		switch m.Role {
		case RoleUser:
			out = append(out, apiMessage{Role: "user", Content: []any{
				map[string]any{"type": "text", "text": m.Text},
			}})
		case RoleAssistant:
			var blocks []any
			if m.Text != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Text})
			}
			for _, tc := range m.ToolCalls {
				input := tc.Input
				if input == nil {
					input = map[string]any{} // Anthropic requires input present on tool_use
				}
				blocks = append(blocks, map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": input})
			}
			if len(blocks) == 0 {
				continue // assistant message must carry >=1 content block
			}
			out = append(out, apiMessage{Role: "assistant", Content: blocks})
		case RoleTool:
			var blocks []any
			for _, r := range m.Results {
				blocks = append(blocks, map[string]any{"type": "tool_result", "tool_use_id": r.CallID, "content": r.Content, "is_error": r.IsError})
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
