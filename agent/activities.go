package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// Activities holds the completion function used by all agent activities.
// Register an instance with the worker so the methods become Temporal
// activities.
//
// Complete is a CompleteFunc — a plain function value. Wire it to any source:
// a real provider client (`client.Complete`), a Chain of middlewares around
// one, or a closure for tests.
type Activities struct {
	Complete CompleteFunc
}

// ResearchInput is the input to the Research activity.
type ResearchInput struct {
	Question       string
	PreviousAnswer string // empty on first round
	CriticFeedback string // empty on first round
}

// Research produces a candidate answer to the question. On subsequent rounds
// it incorporates the critic's feedback to refine the previous answer.
//
// This is an Activity, not workflow code. It can use time.Now, randomness,
// HTTP calls, etc. Its result is recorded in workflow history.
func (a *Activities) Research(ctx context.Context, in ResearchInput) (string, error) {
	if a.Complete == nil {
		return "", temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}

	system := `You are a careful researcher. Answer the user's question concisely (2-4 sentences).
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

	resp, err := a.Complete(ctx, system, user.String())
	if err != nil {
		return "", fmt.Errorf("researcher LLM call failed: %w", err)
	}
	return trimResponse(resp), nil
}

// CritiqueInput is the input to the Critique activity.
type CritiqueInput struct {
	Question string
	Answer   string
	Round    int
}

// Critique evaluates a candidate answer and returns a Verdict.
//
// The critic is instructed to return JSON. If the model returns malformed JSON,
// this activity returns a NonRetryable error (retrying won't help — it's a
// prompt/model problem, not a transient failure).
func (a *Activities) Critique(ctx context.Context, in CritiqueInput) (Verdict, error) {
	if a.Complete == nil {
		return Verdict{}, temporal.NewNonRetryableApplicationError(
			"Activities.Complete is nil", "ConfigurationError", nil)
	}

	system := `You are a strict critic evaluating an answer to a question.
Return a single JSON object with these fields:
  - "approved": boolean, true only if the answer is clearly correct and complete
  - "confidence": number between 0.0 and 1.0
  - "feedback":  string, short critique pointing out what to improve (empty if approved)
Do not include any text outside the JSON object.`

	user := fmt.Sprintf("Question: %s\n\nAnswer to evaluate (round %d):\n%s",
		in.Question, in.Round, in.Answer)

	resp, err := a.Complete(ctx, system, user)
	if err != nil {
		return Verdict{}, fmt.Errorf("critic LLM call failed: %w", err)
	}

	var v Verdict
	if err := json.Unmarshal([]byte(trimResponse(resp)), &v); err != nil {
		return Verdict{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("critic returned malformed JSON: %v; raw response: %q", err, resp),
			"InvalidLLMResponse", nil)
	}

	// Clamp confidence to a sensible range — defensive against model drift.
	switch {
	case v.Confidence < 0:
		v.Confidence = 0
	case v.Confidence > 1:
		v.Confidence = 1
	}
	return v, nil
}
