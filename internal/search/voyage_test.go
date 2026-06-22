package search

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func httpResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestVoyageBuildsRequestAndParses(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("authorization") != "Bearer k" {
			t.Fatalf("missing/bad auth header: %q", r.Header.Get("authorization"))
		}
		if !strings.HasSuffix(r.URL.Path, "/v1/embeddings") {
			t.Fatalf("path = %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		return httpResp(200, `{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}]}`), nil
	})}
	v := NewVoyage("k", WithVoyageModel("voyage-3"), WithVoyageHTTPClient(client))

	vecs, err := v.Embed(context.Background(), []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if captured["model"] != "voyage-3" {
		t.Fatalf("model = %v", captured["model"])
	}
	if len(vecs) != 2 || len(vecs[0]) != 2 || vecs[1][1] != 0.4 {
		t.Fatalf("parsed vecs = %v", vecs)
	}
}

func TestVoyageNon2xxErrors(t *testing.T) {
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(429, `{"error":"rate limited"}`), nil
	})}
	v := NewVoyage("k", WithVoyageHTTPClient(client))
	if _, err := v.Embed(context.Background(), []string{"a"}); err == nil {
		t.Fatal("expected error on 429")
	}
}

func TestVoyageCountMismatchErrors(t *testing.T) {
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		return httpResp(200, `{"data":[{"embedding":[0.1]}]}`), nil // 1 for 2 inputs
	})}
	v := NewVoyage("k", WithVoyageHTTPClient(client))
	if _, err := v.Embed(context.Background(), []string{"a", "b"}); err == nil {
		t.Fatal("expected error on embedding count mismatch")
	}
}
