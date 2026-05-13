package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// CompleteFunc is the canonical seam for "ask an LLM to complete a prompt."
//
// It is a function type, not an interface, deliberately:
//
//   - Single-method interfaces in Go are usually better as function types.
//     No nominal-typing erasure, no wrapper struct ceremony, and any value
//     with a compatible method becomes a CompleteFunc via a method value:
//     `var f CompleteFunc = client.Complete`.
//   - Middleware (retries, logging, rate limiting, caching) becomes a
//     function that takes a CompleteFunc and returns a CompleteFunc — no
//     interface gymnastics.
//   - Test doubles are plain closures, no struct boilerplate required.
//
// Implementations must be safe for concurrent use. Errors from the underlying
// provider are returned as-is; Temporal retries them per the activity's
// RetryPolicy.
type CompleteFunc func(ctx context.Context, systemPrompt, userMessage string) (string, error)

// Middleware wraps a CompleteFunc with additional behavior. Compose via Chain.
type Middleware func(CompleteFunc) CompleteFunc

// Chain composes middlewares around an inner CompleteFunc. The first
// middleware sees the call first and the response last.
//
//	chained := Chain(inner, WithLogging(log), WithRateLimit(...))
//	// equivalent to: WithLogging(WithRateLimit(inner))
func Chain(inner CompleteFunc, mws ...Middleware) CompleteFunc {
	for i := len(mws) - 1; i >= 0; i-- {
		inner = mws[i](inner)
	}
	return inner
}

// ScriptedLLM is a deterministic completion source for tests and offline runs.
// Use its Complete method as a CompleteFunc via a method value:
//
//	s := &ScriptedLLM{Responses: []string{"hi"}}
//	var f CompleteFunc = s.Complete
//
// On each call, it returns the next response from Responses (cycling if Cycle
// is true; otherwise erroring once exhausted). It records every call it
// receives so tests can assert on the conversation.
type ScriptedLLM struct {
	// Responses, returned in order, one per call to Complete.
	Responses []string
	// Cycle, if true, wraps around to Responses[0] after the last response.
	Cycle bool

	mu    sync.Mutex
	calls []ScriptedCall
	idx   int
}

// ScriptedCall is a single recorded invocation of ScriptedLLM.Complete.
type ScriptedCall struct {
	SystemPrompt string
	UserMessage  string
}

// Complete satisfies CompleteFunc via a method value.
func (s *ScriptedLLM) Complete(_ context.Context, systemPrompt, userMessage string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, ScriptedCall{
		SystemPrompt: systemPrompt,
		UserMessage:  userMessage,
	})

	if len(s.Responses) == 0 {
		return "", errors.New("ScriptedLLM: no responses configured")
	}
	if s.idx >= len(s.Responses) {
		if !s.Cycle {
			return "", fmt.Errorf("ScriptedLLM: exhausted after %d calls", s.idx)
		}
		s.idx = 0
	}
	resp := s.Responses[s.idx]
	s.idx++
	return resp, nil
}

// Calls returns a snapshot of every call made to this client so far.
func (s *ScriptedLLM) Calls() []ScriptedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ScriptedCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// CallCount returns the number of times Complete has been called.
func (s *ScriptedLLM) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// trimResponse normalizes LLM output by stripping surrounding whitespace and
// common code-fence wrappers some models add around structured output.
func trimResponse(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}
