package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	defaultVoyageModel   = "voyage-3"
	defaultVoyageBaseURL = "https://api.voyageai.com"
)

// VoyageEmbedder calls the Voyage AI embeddings API.
type VoyageEmbedder struct {
	apiKey  string
	model   string
	baseURL string
	client  *http.Client
}

type VoyageOption func(*VoyageEmbedder)

func WithVoyageModel(m string) VoyageOption   { return func(v *VoyageEmbedder) { v.model = m } }
func WithVoyageBaseURL(u string) VoyageOption { return func(v *VoyageEmbedder) { v.baseURL = u } }
func WithVoyageHTTPClient(c *http.Client) VoyageOption {
	return func(v *VoyageEmbedder) { v.client = c }
}

func NewVoyage(apiKey string, opts ...VoyageOption) *VoyageEmbedder {
	v := &VoyageEmbedder{apiKey: apiKey, model: defaultVoyageModel, baseURL: defaultVoyageBaseURL, client: http.DefaultClient}
	for _, o := range opts {
		o(v)
	}
	return v
}

func (v *VoyageEmbedder) Model() string { return v.model }

type voyageReq struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type voyageResp struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (v *VoyageEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(voyageReq{Model: v.model, Input: texts})
	if err != nil {
		return nil, fmt.Errorf("voyage: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("voyage: new request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+v.apiKey)

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("voyage: do: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("voyage: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("voyage: status %d: %s", resp.StatusCode, string(raw))
	}
	var vr voyageResp
	if err := json.Unmarshal(raw, &vr); err != nil {
		return nil, fmt.Errorf("voyage: unmarshal: %w", err)
	}
	out := make([][]float32, len(vr.Data))
	for i, d := range vr.Data {
		out[i] = d.Embedding
	}
	if len(out) != len(texts) {
		return nil, fmt.Errorf("voyage: got %d embeddings for %d texts", len(out), len(texts))
	}
	return out, nil
}

var _ Embedder = (*VoyageEmbedder)(nil)
