package agent_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestLLMSynthesizer_HappyPath(t *testing.T) {
	scripted := &agent.ScriptedLLM{
		Responses: []string{
			"Go was designed at Google. Rust takes a different approach with ownership semantics.",
		},
	}
	arrow := agent.LLMSynthesizer(scripted.Complete)

	out, err := arrow(context.Background(), []agent.SubAnswer{
		{
			SubQuestion: agent.SubQuestion{Index: 0, Text: "What is Go?"},
			Answer:      "Go is a language by Google.",
			Converged:   true,
		},
		{
			SubQuestion: agent.SubQuestion{Index: 1, Text: "What is Rust?"},
			Answer:      "Rust uses ownership.",
			Converged:   true,
		},
	})
	require.NoError(t, err)
	require.Contains(t, out, "Go was designed at Google")
	require.Contains(t, out, "Rust takes a different approach")

	calls := scripted.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0].UserMessage, "Go is a language by Google.")
	require.Contains(t, calls[0].UserMessage, "Rust uses ownership.")
	require.Contains(t, calls[0].SystemPrompt, "synthesis editor")
}

func TestLLMSynthesizer_IncludesFailedSubAnswers(t *testing.T) {
	scripted := &agent.ScriptedLLM{
		Responses: []string{"unified answer noting the failure"},
	}
	arrow := agent.LLMSynthesizer(scripted.Complete)

	_, err := arrow(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Good"}, Answer: "good", Converged: true},
		{SubQuestion: agent.SubQuestion{Index: 1, Text: "Failed"}, Error: "child timed out"},
	})
	require.NoError(t, err)

	user := scripted.Calls()[0].UserMessage
	require.Contains(t, user, "Failed")
	require.Contains(t, user, "FAILED")
	require.Contains(t, user, "child timed out")
}

func TestLLMSynthesizer_NilCompleteFailsCleanly(t *testing.T) {
	arrow := agent.LLMSynthesizer(nil)
	_, err := arrow(context.Background(), []agent.SubAnswer{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CompleteFunc is nil")
}

func TestLLMSynthesizer_PropagatesLLMError(t *testing.T) {
	boom := errors.New("LLM unavailable")
	complete := agent.CompleteFunc(func(_ context.Context, _, _ string) (string, error) {
		return "", boom
	})
	arrow := agent.LLMSynthesizer(complete)

	_, err := arrow(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Q"}, Answer: "A"},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
}

func TestLLMSynthesizer_StripsCodeFences(t *testing.T) {
	scripted := &agent.ScriptedLLM{
		Responses: []string{"```\nThe synthesized answer.\n```"},
	}
	arrow := agent.LLMSynthesizer(scripted.Complete)

	out, err := arrow(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Q"}, Answer: "A"},
	})
	require.NoError(t, err)
	require.Equal(t, "The synthesized answer.", out)
}

func TestActivities_Synthesize_UsesCustomSynthesizer(t *testing.T) {
	called := false
	customArrow := func(_ context.Context, _ []agent.SubAnswer) (string, error) {
		called = true
		return "custom output", nil
	}
	acts := &agent.Activities{Synthesizer: customArrow}

	out, err := acts.Synthesize(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Q"}, Answer: "A"},
	})
	require.NoError(t, err)
	require.True(t, called, "custom synthesizer should be invoked")
	require.Equal(t, "custom output", out)
}

func TestActivities_Synthesize_FallsBackToHeuristic(t *testing.T) {
	acts := &agent.Activities{}

	out, err := acts.Synthesize(context.Background(), []agent.SubAnswer{
		{SubQuestion: agent.SubQuestion{Index: 0, Text: "Q1"}, Answer: "A1", Converged: true},
		{SubQuestion: agent.SubQuestion{Index: 1, Text: "Q2"}, Answer: "A2", Converged: true},
	})
	require.NoError(t, err)
	require.True(t, strings.Contains(out, "## Q1") && strings.Contains(out, "## Q2"),
		"expected heuristic markdown headings, got: %s", out)
}
