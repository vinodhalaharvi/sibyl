package agent

import (
	"context"

	"github.com/vinodhalaharvi/weft/weft"
)

// lift.go bridges Temporal's execution model and weft's Arrow algebra.
//
// Two directions matter:
//
//   - Arrow -> Activity: build composed arrows in weft (LLM + parsing +
//     middleware), then register each as a Temporal activity. One arrow,
//     one activity, replayable, retryable.
//
//   - CompleteFunc -> Arrow: lift our existing CompleteFunc seam into
//     the Arrow algebra so it composes with weft.Pipe2, weft.Map,
//     weft.PreMap and the rest. This is the entry point for everything
//     downstream.

// ArrowAsActivity adapts a weft.Arrow into the function shape Temporal
// expects for an activity registration.
//
// A weft.Arrow[A, B] is structurally `func(ctx, A) (B, error)` — already
// the right shape — but Go's type system treats the two as nominally
// distinct. This thin wrapper makes the conversion explicit and lets
// Temporal's reflection-based RegisterActivity see a plain function.
func ArrowAsActivity[A, B any](a weft.Arrow[A, B]) func(context.Context, A) (B, error) {
	return func(ctx context.Context, in A) (B, error) {
		return a(ctx, in)
	}
}

// CompletionRequest is the input shape used when an LLM completion
// participates in a weft pipeline. A two-field struct lets us compose
// with weft.PreMap (build the request from typed upstream output) and
// weft.Map (parse the string into typed downstream output) using the
// standard combinators.
type CompletionRequest struct {
	SystemPrompt string
	UserMessage  string
}

// CompleteAsArrow lifts a CompleteFunc into the weft Arrow algebra.
// The method-value-as-CompleteFunc convention extends naturally to
// method-value-as-Arrow via this single adapter:
//
//	rawLLM := someClient.Complete                   // CompleteFunc
//	llmArrow := agent.CompleteAsArrow(rawLLM)        // weft.Arrow[CompletionRequest, string]
//
//	researcher := weft.Pipe3(
//	    buildResearchRequest, // weft.Arrow[ResearchInput, CompletionRequest]
//	    llmArrow,             // weft.Arrow[CompletionRequest, string]
//	    trim,                 // weft.Arrow[string, string]
//	)
func CompleteAsArrow(c CompleteFunc) weft.Arrow[CompletionRequest, string] {
	return func(ctx context.Context, req CompletionRequest) (string, error) {
		return c(ctx, req.SystemPrompt, req.UserMessage)
	}
}

// ArrowAsComplete is the inverse: given a weft Arrow that produces a
// string from a CompletionRequest, produce a CompleteFunc. Useful when
// a composed-and-middleware-wrapped weft pipeline needs to be plugged
// back into the existing worker.Register seam.
func ArrowAsComplete(a weft.Arrow[CompletionRequest, string]) CompleteFunc {
	return func(ctx context.Context, system, user string) (string, error) {
		return a(ctx, CompletionRequest{SystemPrompt: system, UserMessage: user})
	}
}
