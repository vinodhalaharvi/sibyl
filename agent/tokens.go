package agent

import (
	"context"
	"sync/atomic"
)

// TokenUsage is an approximate count of tokens consumed by an LLM call.
// "Approximate" because we use a fast char-based heuristic rather than a
// real tokenizer (tiktoken, sentencepiece, etc). The heuristic is good
// enough for budget tracking — typically within ~15% of the true count
// for English text, less accurate for code or non-Latin scripts.
//
// To get exact counts, swap in a real tokenizer behind the same interface
// (the MetricsSink contract doesn't care how counts are produced).
type TokenUsage struct {
	// Input is the approximate token count of (system + user) prompts.
	Input int
	// Output is the approximate token count of the LLM response.
	Output int
}

// Total returns Input + Output.
func (t TokenUsage) Total() int { return t.Input + t.Output }

// Add accumulates u into the receiver in place.
func (t *TokenUsage) Add(u TokenUsage) {
	t.Input += u.Input
	t.Output += u.Output
}

// MetricsSink receives per-call observations. Implementations must be
// safe for concurrent use — workflows fan out multiple LLM calls in
// parallel and they all hit the same sink.
//
// The zero-value Sink (nil) is fine — middleware skips reporting when
// the sink is nil.
type MetricsSink interface {
	RecordCall(ctx context.Context, usage TokenUsage)
}

// AtomicTokenSink is a minimal MetricsSink that just sums totals.
// Useful for tests, demos, and the cmd/worker default — gives you a
// "tokens spent so far" counter without setting up Prometheus.
//
// Concurrent-safe via atomic counters.
type AtomicTokenSink struct {
	input  atomic.Int64
	output atomic.Int64
	calls  atomic.Int64
}

// RecordCall implements MetricsSink.
func (s *AtomicTokenSink) RecordCall(_ context.Context, u TokenUsage) {
	s.input.Add(int64(u.Input))
	s.output.Add(int64(u.Output))
	s.calls.Add(1)
}

// Snapshot returns the totals seen so far. Atomic; safe to call concurrently
// with RecordCall.
func (s *AtomicTokenSink) Snapshot() (input, output, calls int64) {
	return s.input.Load(), s.output.Load(), s.calls.Load()
}

// Total is a convenience for input+output.
func (s *AtomicTokenSink) Total() int64 {
	return s.input.Load() + s.output.Load()
}

// WithTokenAccounting wraps a CompleteFunc to measure token usage on
// every call and report it to sink. If sink is nil, the middleware
// is a no-op (zero overhead).
//
// Counts are estimated as len(text) / 4 — a fast English-text heuristic.
// For real counts, replace this middleware with one calling a proper
// tokenizer.
func WithTokenAccounting(sink MetricsSink) Middleware {
	return func(next CompleteFunc) CompleteFunc {
		return func(ctx context.Context, system, user string) (string, error) {
			out, err := next(ctx, system, user)
			if sink == nil {
				return out, err
			}
			// Report even on error: the input prompt was constructed and
			// (typically) sent over the wire, so it's "real" cost.
			usage := TokenUsage{
				Input:  approxTokens(system) + approxTokens(user),
				Output: approxTokens(out),
			}
			sink.RecordCall(ctx, usage)
			return out, err
		}
	}
}

// approxTokens estimates tokens from a UTF-8 string. The 1-token-per-4-chars
// heuristic is the rule of thumb used in Anthropic and OpenAI docs for
// English text. We measure bytes rather than runes here, which slightly
// over-estimates non-ASCII content — fine for cost budgeting since
// non-ASCII typically costs more tokens anyway.
func approxTokens(s string) int {
	if s == "" {
		return 0
	}
	n := (len(s) + 3) / 4 // round up
	if n < 1 {
		n = 1
	}
	return n
}
