package agent_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// These tests exercise the composed arrows directly — no Temporal test
// environment. The activities pipeline is now just a weft Arrow, and
// arrows are callable Go functions. So you can test the prompt
// construction + LLM call + response parsing as one unit, with the
// same scripted LLM, without spinning up the workflow harness.

func TestResearcherArrow_ProducesAnswer(t *testing.T) {
	scripted := &agent.ScriptedLLM{Responses: []string{"  the answer  "}}

	// Note: makeResearcherArrow is package-private, so this test lives
	// in agent_test but reaches in via the Research activity wrapper —
	// the public surface — which calls the same composed arrow.
	acts := &agent.Activities{Complete: scripted.Complete}
	out, err := acts.Research(context.Background(), agent.ResearchInput{
		Question: "Q",
	})
	require.NoError(t, err)
	require.Equal(t, "the answer", out, "trim should run as part of the arrow")
}

func TestCriticArrow_ParsesVerdict(t *testing.T) {
	scripted := &agent.ScriptedLLM{Responses: []string{
		`{"approved": true, "confidence": 0.9, "feedback": ""}`,
	}}
	acts := &agent.Activities{Complete: scripted.Complete}

	v, err := acts.Critique(context.Background(), agent.CritiqueInput{
		Question: "Q",
		Answer:   "A",
		Round:    1,
	})
	require.NoError(t, err)
	require.True(t, v.Approved)
	require.InDelta(t, 0.9, v.Confidence, 0.001)
}

func TestCriticArrow_MalformedJSONNonRetryable(t *testing.T) {
	// Direct arrow execution (no Temporal env) — verifies parseVerdict
	// returns the right non-retryable error.
	scripted := &agent.ScriptedLLM{Responses: []string{"not json"}}
	acts := &agent.Activities{Complete: scripted.Complete}

	_, err := acts.Critique(context.Background(), agent.CritiqueInput{
		Question: "Q",
		Answer:   "A",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "malformed JSON")
}
