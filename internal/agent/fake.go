package agent

import (
	"context"
	"fmt"
)

// FakeProvider returns scripted responses by call index, driving the agent loop
// deterministically in tests. Each Step receives the Request (so a step can
// assert on what the loop sent) and returns the Response to hand back.
type FakeProvider struct {
	Steps []func(req Request) Response
	calls int
}

func (f *FakeProvider) Generate(ctx context.Context, req Request) (Response, error) {
	if f.calls >= len(f.Steps) {
		return Response{}, fmt.Errorf("fake: no scripted response for call %d (have %d)", f.calls, len(f.Steps))
	}
	step := f.Steps[f.calls]
	f.calls++
	return step(req), nil
}

var _ LLMProvider = (*FakeProvider)(nil)
