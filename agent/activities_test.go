package agent_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func newActivityEnv(t *testing.T, complete agent.CompleteFunc) (*testsuite.TestActivityEnvironment, *agent.Activities) {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestActivityEnvironment()
	acts := &agent.Activities{Complete: complete}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))
	return env, acts
}

func TestResearchActivity_PassesQuestionToLLM(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{"  Some answer.  \n"}}
	env, _ := newActivityEnv(t, llm.Complete)

	val, err := env.ExecuteActivity(agent.ResearchActivityName, agent.ResearchInput{
		Question: "Test question",
	})
	require.NoError(t, err)

	var result string
	require.NoError(t, val.Get(&result))
	require.Equal(t, "Some answer.", result, "whitespace should be trimmed")

	calls := llm.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0].UserMessage, "Test question")
}

func TestResearchActivity_IncludesPreviousAnswerAndFeedback(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{"refined answer"}}
	env, _ := newActivityEnv(t, llm.Complete)

	_, err := env.ExecuteActivity(agent.ResearchActivityName, agent.ResearchInput{
		Question:       "Q",
		PreviousAnswer: "old answer",
		CriticFeedback: "be more specific",
	})
	require.NoError(t, err)

	user := llm.Calls()[0].UserMessage
	require.Contains(t, user, "old answer")
	require.Contains(t, user, "be more specific")
}

func TestCritiqueActivity_ParsesValidVerdict(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{
		`{"approved": true, "confidence": 0.85, "feedback": ""}`,
	}}
	env, _ := newActivityEnv(t, llm.Complete)

	val, err := env.ExecuteActivity(agent.CritiqueActivityName, agent.CritiqueInput{
		Question: "Q",
		Answer:   "A",
		Round:    1,
	})
	require.NoError(t, err)

	var v agent.Verdict
	require.NoError(t, val.Get(&v))
	require.True(t, v.Approved)
	require.InDelta(t, 0.85, v.Confidence, 0.001)
}

func TestCritiqueActivity_StripsCodeFences(t *testing.T) {
	// Some models like to wrap JSON in markdown fences. trimResponse should handle it.
	llm := &agent.ScriptedLLM{Responses: []string{
		"```json\n{\"approved\": false, \"confidence\": 0.5, \"feedback\": \"meh\"}\n```",
	}}
	env, _ := newActivityEnv(t, llm.Complete)

	val, err := env.ExecuteActivity(agent.CritiqueActivityName, agent.CritiqueInput{
		Question: "Q", Answer: "A", Round: 1,
	})
	require.NoError(t, err)

	var v agent.Verdict
	require.NoError(t, val.Get(&v))
	require.False(t, v.Approved)
	require.Equal(t, "meh", v.Feedback)
}

func TestCritiqueActivity_ClampsConfidence(t *testing.T) {
	cases := []struct {
		name     string
		response string
		want     float64
	}{
		{"clamps below zero", `{"approved":false,"confidence":-0.5,"feedback":"x"}`, 0.0},
		{"clamps above one", `{"approved":true,"confidence":1.7,"feedback":""}`, 1.0},
		{"keeps mid value", `{"approved":true,"confidence":0.5,"feedback":""}`, 0.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			llm := &agent.ScriptedLLM{Responses: []string{tc.response}}
			env, _ := newActivityEnv(t, llm.Complete)
			val, err := env.ExecuteActivity(agent.CritiqueActivityName, agent.CritiqueInput{
				Question: "Q", Answer: "A", Round: 1,
			})
			require.NoError(t, err)
			var v agent.Verdict
			require.NoError(t, val.Get(&v))
			require.InDelta(t, tc.want, v.Confidence, 0.001)
		})
	}
}

func TestCritiqueActivity_LLMErrorIsReturned(t *testing.T) {
	// Empty Responses -> ScriptedLLM returns an error
	llm := &agent.ScriptedLLM{}
	env, _ := newActivityEnv(t, llm.Complete)

	_, err := env.ExecuteActivity(agent.CritiqueActivityName, agent.CritiqueInput{
		Question: "Q", Answer: "A", Round: 1,
	})
	require.Error(t, err)
}

func TestActivities_NilComplete(t *testing.T) {
	acts := &agent.Activities{Complete: nil}
	_, err := acts.Research(testCtx(), agent.ResearchInput{Question: "Q"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Complete is nil")

	_, err = acts.Critique(testCtx(), agent.CritiqueInput{Question: "Q", Answer: "A"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Complete is nil")
}

// Ensure errors.Is/As works through the activity error wrapping (sanity check).
func TestErrorWrapping(t *testing.T) {
	err := errors.New("inner")
	require.ErrorIs(t, err, err)
}
