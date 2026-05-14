package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vinodhalaharvi/weft/weft"
	"go.temporal.io/sdk/temporal"
)

// Activities holds the completion function and overrideable arrows used by
// the Temporal activities. Register an instance with the worker so the
// methods become Temporal activities.
//
// Internally each activity is a composed weft Arrow: build prompt ->
// call LLM (lifted CompleteFunc) -> parse. This factoring is what lets
// us add middleware (retry, logging, caching) by composing more arrows
// without touching the activity entry points themselves.
type Activities struct {
	// Complete backs the Researcher and Critic activities.
	Complete CompleteFunc
	// Synthesizer, if set, replaces the default heuristic synthesizer
	// in Activities.Synthesize. Use agent.LLMSynthesizer(complete) to
	// get an LLM-backed one, or roll your own weft.Arrow.
	Synthesizer weft.Arrow[[]SubAnswer, string]
	// Tools, if set, makes the RunToolAgent activity available. Register
	// real tools (web_search, calculator, file_read, your own custom
	// tools) before passing the registry here.
	Tools *ToolRegistry
	// Stream, if set, enables token-by-token streaming for activities
	// that opt in via CompleteWithStreaming. Backends that don't support
	// streaming (claude-code, scripted) leave this nil; the streaming
	// helper falls back to atomic Complete and emits one chunk event.
	Stream CompleteStreamFunc
}

// ResearchInput is the input to the Research activity.
type ResearchInput struct {
	Question       string
	PreviousAnswer string // empty on first round
	CriticFeedback string // empty on first round
}

// CritiqueInput is the input to the Critique activity.
type CritiqueInput struct {
	Question string
	Answer   string
	Round    int
}

// --- Researcher pipeline ----------------------------------------------------
//
// researcher : Arrow[ResearchInput, string]
//            = Pipe3(buildResearchRequest, llmArrow, trimArrow)
//
// Each stage is independently testable. Composition is the API: to add
// behavior (caching, logging, rate limiting), wrap one of the stages
// with weft.Map / weft.PreMap or insert another Arrow in the pipe.

func buildResearchRequest(_ context.Context, in ResearchInput) (CompletionRequest, error) {
	const system = `You are a careful researcher. Answer the user's question concisely (2-4 sentences).
If you previously answered and received critic feedback, revise your answer to address it.
Return only the answer text, no preamble.`

	var user strings.Builder
	fmt.Fprintf(&user, "Question: %s\n", in.Question)
	if in.PreviousAnswer != "" {
		fmt.Fprintf(&user, "\nYour previous answer:\n%s\n", in.PreviousAnswer)
	}
	if in.CriticFeedback != "" {
		fmt.Fprintf(&user, "\nCritic feedback to address:\n%s\n", in.CriticFeedback)
	}
	return CompletionRequest{SystemPrompt: system, UserMessage: user.String()}, nil
}

// makeResearcherArrow composes the Researcher pipeline from a CompleteFunc.
// Exposed so tests (and curious callers) can exercise the pipeline directly
// without going through Temporal.
func makeResearcherArrow(c CompleteFunc) weft.Arrow[ResearchInput, string] {
	return weft.Pipe3(
		weft.Arrow[ResearchInput, CompletionRequest](buildResearchRequest),
		CompleteAsArrow(c),
		weft.Pure(trimResponse),
	)
}

// Research is the Temporal activity entry point. The body is the composed
// arrow above; this method exists so worker.RegisterActivity has something
// reflectable to bind to.
func (a *Activities) Research(ctx context.Context, in ResearchInput) (string, error) {
	if a.Complete == nil {
		return "", temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}
	// OTel span: shows up in traces alongside the (separate) event emission.
	// The span exists even when no exporter is configured; the global no-op
	// TracerProvider makes this nearly free.
	ctx, span := StartActivitySpan(ctx, ResearchActivityName)
	defer span.End()

	emitter := EmitterForActivity(ctx)
	start := time.Now()
	emitter.Emit(NewActivityStarted("", ResearchActivityName, ""))

	out, err := makeResearcherArrow(a.Complete)(ctx, in)

	emitter.Emit(NewActivityCompleted("", ResearchActivityName, "", err, time.Since(start)))
	if err != nil {
		RecordError(span, err)
		return "", fmt.Errorf("researcher pipeline failed: %w", err)
	}
	return out, nil
}

// --- Critic pipeline --------------------------------------------------------
//
// critic : Arrow[CritiqueInput, Verdict]
//        = Pipe3(buildCriticRequest, llmArrow, parseVerdict)
//
// parseVerdict is the only stage that can produce a non-retryable error:
// if the model returns malformed JSON, retrying won't help. We surface
// that as a temporal.NonRetryableApplicationError so the workflow stops
// instead of burning cost on retries.

func buildCriticRequest(_ context.Context, in CritiqueInput) (CompletionRequest, error) {
	const system = `You are a strict critic evaluating an answer to a question.
Return a single JSON object with these fields:
  - "approved": boolean, true only if the answer is clearly correct and complete
  - "confidence": number between 0.0 and 1.0
  - "feedback":  string, short critique pointing out what to improve (empty if approved)
Do not include any text outside the JSON object.`

	user := fmt.Sprintf("Question: %s\n\nAnswer to evaluate (round %d):\n%s",
		in.Question, in.Round, in.Answer)
	return CompletionRequest{SystemPrompt: system, UserMessage: user}, nil
}

func parseVerdict(_ context.Context, raw string) (Verdict, error) {
	var v Verdict
	if err := json.Unmarshal([]byte(trimResponse(raw)), &v); err != nil {
		return Verdict{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("critic returned malformed JSON: %v; raw response: %q", err, raw),
			"InvalidLLMResponse", nil)
	}
	switch {
	case v.Confidence < 0:
		v.Confidence = 0
	case v.Confidence > 1:
		v.Confidence = 1
	}
	return v, nil
}

// makeCriticArrow composes the Critic pipeline from a CompleteFunc.
func makeCriticArrow(c CompleteFunc) weft.Arrow[CritiqueInput, Verdict] {
	return weft.Pipe3(
		weft.Arrow[CritiqueInput, CompletionRequest](buildCriticRequest),
		CompleteAsArrow(c),
		weft.Arrow[string, Verdict](parseVerdict),
	)
}

// Critique is the Temporal activity entry point.
func (a *Activities) Critique(ctx context.Context, in CritiqueInput) (Verdict, error) {
	if a.Complete == nil {
		return Verdict{}, temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}
	ctx, span := StartActivitySpan(ctx, CritiqueActivityName)
	defer span.End()

	emitter := EmitterForActivity(ctx)
	start := time.Now()
	emitter.Emit(NewActivityStarted("", CritiqueActivityName, ""))

	v, err := makeCriticArrow(a.Complete)(ctx, in)

	emitter.Emit(NewActivityCompleted("", CritiqueActivityName, "", err, time.Since(start)))
	if err != nil {
		RecordError(span, err)
	}
	return v, err
}
