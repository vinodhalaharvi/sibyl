package agent_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// newTestEnv builds a Temporal test environment with the given LLM client and
// registers the workflow + activities, mirroring what the worker package does.
func newTestEnv(t *testing.T, llm agent.LLMClient) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(agent.ConvergeWorkflow)

	acts := &agent.Activities{LLM: llm}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))

	return env
}

func TestConvergeWorkflow_ConvergesOnFirstRound(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{
		// Researcher: a strong first answer
		"Paris is the capital of France.",
		// Critic: approves immediately
		`{"approved": true, "confidence": 0.95, "feedback": ""}`,
	}}

	env := newTestEnv(t, llm)
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "What is the capital of France?",
		MaxRounds: 5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var ans agent.Answer
	require.NoError(t, env.GetWorkflowResult(&ans))
	require.True(t, ans.Converged)
	require.Equal(t, 1, ans.Rounds)
	require.Contains(t, ans.Text, "Paris")
	require.Len(t, ans.History, 1)
	require.True(t, ans.History[0].Verdict.Approved)
	require.InDelta(t, 0.95, ans.History[0].Verdict.Confidence, 0.001)
}

func TestConvergeWorkflow_ConvergesAfterRevision(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{
		// Round 1
		"Paris.",
		`{"approved": false, "confidence": 0.3, "feedback": "Too terse. Use a complete sentence."}`,
		// Round 2
		"Paris is the capital of France.",
		`{"approved": true, "confidence": 0.9, "feedback": ""}`,
	}}

	env := newTestEnv(t, llm)
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "What is the capital of France?",
		MaxRounds: 5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var ans agent.Answer
	require.NoError(t, env.GetWorkflowResult(&ans))
	require.True(t, ans.Converged)
	require.Equal(t, 2, ans.Rounds)
	require.Equal(t, "Paris is the capital of France.", ans.Text)

	// Verify the critic's feedback flowed into the second researcher call.
	calls := llm.Calls()
	require.Len(t, calls, 4, "expected 4 LLM calls (2 research + 2 critique)")
	secondResearch := calls[2].UserMessage
	require.Contains(t, secondResearch, "Too terse",
		"second research call should include critic feedback")
}

func TestConvergeWorkflow_HitsMaxRoundsWithoutConverging(t *testing.T) {
	// Critic always rejects.
	llm := &agent.ScriptedLLM{Cycle: true, Responses: []string{
		"A candidate answer.",
		`{"approved": false, "confidence": 0.2, "feedback": "Not good enough."}`,
	}}

	env := newTestEnv(t, llm)
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "Hard question",
		MaxRounds: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"workflow should still complete successfully even if it doesn't converge")

	var ans agent.Answer
	require.NoError(t, env.GetWorkflowResult(&ans))
	require.False(t, ans.Converged)
	require.Equal(t, 3, ans.Rounds)
	require.Len(t, ans.History, 3)
	require.Equal(t, 6, llm.CallCount(), "3 rounds × 2 calls each")
}

func TestConvergeWorkflow_RejectsEmptyQuestion(t *testing.T) {
	env := newTestEnv(t, &agent.ScriptedLLM{})
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "",
		MaxRounds: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Question text must not be empty")
}

func TestConvergeWorkflow_RejectsZeroMaxRounds(t *testing.T) {
	env := newTestEnv(t, &agent.ScriptedLLM{})
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "Anything",
		MaxRounds: 0,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "MaxRounds must be > 0")
}

func TestConvergeWorkflow_MalformedCriticJSONFailsFast(t *testing.T) {
	// Critic returns invalid JSON — must NOT be retried, must fail the workflow.
	llm := &agent.ScriptedLLM{Responses: []string{
		"An answer.",
		"this is not json at all",
	}}

	env := newTestEnv(t, llm)
	env.ExecuteWorkflow(agent.ConvergeWorkflow, agent.Question{
		Text:      "Q",
		MaxRounds: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "malformed json")
	// Should have made only ONE critic call — non-retryable means no retry.
	require.Equal(t, 2, llm.CallCount(), "no retry on InvalidLLMResponse")
}

// TestScriptedLLM_Exhaustion verifies the test helper itself behaves sanely.
func TestScriptedLLM_Exhaustion(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{"only one"}}
	_, err := llm.Complete(testCtx(), "sys", "msg")
	require.NoError(t, err)

	_, err = llm.Complete(testCtx(), "sys", "msg")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exhausted")
}

func TestScriptedLLM_NoResponsesConfigured(t *testing.T) {
	llm := &agent.ScriptedLLM{}
	_, err := llm.Complete(testCtx(), "", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, err)) // sanity: it's a real error
	require.Contains(t, err.Error(), "no responses")
}
