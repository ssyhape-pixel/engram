package agent

import (
	"context"
	"testing"
)

func TestFakeProviderScriptedInOrder(t *testing.T) {
	ctx := context.Background()
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response {
			return Response{ToolCalls: []ToolCall{{ID: "1", Name: "recall", Input: map[string]any{"query": "x"}}}}
		},
		func(r Request) Response { return Response{Text: "done"} },
	}}

	r1, err := f.Generate(ctx, Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.ToolCalls) != 1 || r1.ToolCalls[0].Name != "recall" {
		t.Fatalf("step 0 = %+v", r1)
	}
	r2, err := f.Generate(ctx, Request{})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Text != "done" || len(r2.ToolCalls) != 0 {
		t.Fatalf("step 1 = %+v", r2)
	}
}

func TestFakeProviderExhaustionErrors(t *testing.T) {
	ctx := context.Background()
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response { return Response{Text: "only"} },
	}}
	if _, err := f.Generate(ctx, Request{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Generate(ctx, Request{}); err == nil {
		t.Fatal("expected error when script exhausted")
	}
}

func TestFakeProviderSeesRequest(t *testing.T) {
	ctx := context.Background()
	var gotSystem string
	f := &FakeProvider{Steps: []func(Request) Response{
		func(r Request) Response { gotSystem = r.System; return Response{Text: "ok"} },
	}}
	_, _ = f.Generate(ctx, Request{System: "SYS"})
	if gotSystem != "SYS" {
		t.Fatalf("provider did not see request system: %q", gotSystem)
	}
}
