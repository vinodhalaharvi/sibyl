package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// LLMClient is the abstraction over any LLM provider (Anthropic, OpenAI, local,
// etc). Implementations should be safe for concurrent use.
//
// Complete returns the assistant's textual response to the given system prompt
// and user message. Errors from the underlying provider are returned as-is;
// Temporal retries them per the activity's RetryPolicy.
type LLMClient interface {
	Complete(ctx context.Context, systemPrompt, userMessage string) (string, error)
}

// ScriptedLLM is a deterministic LLMClient for tests and offline runs.
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

// Complete implements LLMClient.
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
