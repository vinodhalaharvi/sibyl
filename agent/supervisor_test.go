package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// newSupervisorTestEnv registers everything needed for an end-to-end
// supervisor test in Temporal's in-process test environment: the supervisor
// workflow, the child convergence workflow, and all four activities.
func newSupervisorTestEnv(t *testing.T, complete agent.CompleteFunc) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	// Register both workflows. Note we name them explicitly so the supervisor
	// can spawn children by string ("ConvergeWorkflow") instead of by function ref —
	// the same pattern the real worker uses.
	env.RegisterWorkflowWithOptions(agent.SupervisorWorkflow, workflow.RegisterOptions{
		Name: agent.SupervisorWorkflowName,
	})
	env.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})

	acts := &agent.Activities{Complete: complete}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))
	env.RegisterActivityWithOptions(acts.Decompose, registerOpts(agent.DecomposeActivityName))
	env.RegisterActivityWithOptions(acts.Synthesize, registerOpts(agent.SynthesizeActivityName))

	return env
}

func TestSupervisorWorkflow_FansOutAndSynthesizes(t *testing.T) {
	// Children run in parallel under Temporal, so we can't rely on a
	// strict response *order* in ScriptedLLM. Instead, use a routing
	// CompleteFunc that distinguishes researcher from critic by the
	// system prompt (and per-child by the user message). This is the
	// general pattern for fan-out tests with non-deterministic ordering.
	complete := agent.CompleteFunc(func(_ context.Context, system, user string) (string, error) {
		isCritic := strings.Contains(system, "strict critic")
		switch {
		case isCritic:
			return `{"approved": true, "confidence": 0.9, "feedback": ""}`, nil
		case strings.Contains(user, "Go"):
			return "Go is a statically typed language designed at Google.", nil
		case strings.Contains(user, "Rust"):
			return "Rust uses ownership to manage memory without garbage collection.", nil
		default:
			return "fallback answer", nil
		}
	})

	env := newSupervisorTestEnv(t, complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "What is Go and how does it differ from Rust",
		MaxRoundsPerChild: 3,
		MaxSubQuestions:   5,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out agent.SupervisorOutput
	require.NoError(t, env.GetWorkflowResult(&out))

	require.Equal(t, 2, out.SuccessCount, "both children should succeed")
	require.Equal(t, 0, out.FailureCount)
	require.Len(t, out.SubAnswers, 2)

	// Both child answers should appear in the synthesis.
	require.Contains(t, out.Synthesis, "Go is a statically typed")
	require.Contains(t, out.Synthesis, "Rust uses ownership")

	// Both subanswers should have converged.
	for i, sa := range out.SubAnswers {
		require.True(t, sa.Converged, "child %d should have converged", i)
		require.Empty(t, sa.Error, "child %d should not have errored", i)
		require.Equal(t, i, sa.SubQuestion.Index)
	}
}

func TestSupervisorWorkflow_SingleSubQuestion(t *testing.T) {
	// No split markers in the question → exactly one child.
	llm := &agent.ScriptedLLM{
		Cycle: true,
		Responses: []string{
			"Paris.",
			`{"approved": true, "confidence": 0.95, "feedback": ""}`,
		},
	}

	env := newSupervisorTestEnv(t, llm.Complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "What is the capital of France",
		MaxRoundsPerChild: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out agent.SupervisorOutput
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, 1, out.SuccessCount)
	require.Len(t, out.SubAnswers, 1)
	require.Contains(t, out.Synthesis, "Paris")
}

func TestSupervisorWorkflow_RejectsEmptyQuestion(t *testing.T) {
	env := newSupervisorTestEnv(t, (&agent.ScriptedLLM{}).Complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "",
		MaxRoundsPerChild: 3,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "Question must not be empty")
}

func TestSupervisorWorkflow_RejectsZeroMaxRounds(t *testing.T) {
	env := newSupervisorTestEnv(t, (&agent.ScriptedLLM{}).Complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "Anything",
		MaxRoundsPerChild: 0,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "MaxRoundsPerChild must be > 0")
}

func TestSupervisorWorkflow_AppliesMaxSubQuestionsCap(t *testing.T) {
	// Same routing pattern: critic always approves, researcher returns a
	// fixed string. We don't care about the answer content here, just the
	// count.
	complete := agent.CompleteFunc(func(_ context.Context, system, _ string) (string, error) {
		if strings.Contains(system, "strict critic") {
			return `{"approved": true, "confidence": 0.9, "feedback": ""}`, nil
		}
		return "an answer", nil
	})

	env := newSupervisorTestEnv(t, complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "What is A? What is B? What is C? What is D?",
		MaxRoundsPerChild: 3,
		MaxSubQuestions:   2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out agent.SupervisorOutput
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Len(t, out.SubAnswers, 2, "MaxSubQuestions=2 should have capped to 2 children")
}

// TestSupervisorWorkflow_OneChildFailsSupervisorStillSynthesizes verifies
// the "swallow individual failures" policy: one bad child shouldn't tank
// the whole supervisor run as long as at least one child succeeded.
func TestSupervisorWorkflow_OneChildFailsSupervisorStillSynthesizes(t *testing.T) {
	// Two subquestions. Child 1 will succeed, Child 2 will fail.
	// Child 1 takes 2 calls (research+critic).
	// Child 2 has its critic return invalid JSON -> non-retryable failure
	// after 1 research + 1 critic call.
	//
	// Note: Temporal child workflows run in some order in the test
	// environment, but each ScriptedLLM call is consumed in sequence.
	// We sequence the responses to match a serialized execution; with
	// Cycle:true and enough responses, even concurrent ordering will
	// converge to the same per-child verdicts because we provide both
	// a success and failure path.
	//
	// To make this deterministic, we use a custom CompleteFunc that
	// routes by content: if the user message references the "good"
	// subquestion, it returns a valid response; otherwise it returns
	// invalid JSON for the critic.
	complete := agent.CompleteFunc(func(_ context.Context, system, user string) (string, error) {
		isCritic := strings.Contains(system, "strict critic")
		isBadSub := strings.Contains(user, "bad subject")

		switch {
		case isCritic && isBadSub:
			return "this is not json", nil
		case isCritic:
			return `{"approved": true, "confidence": 0.9, "feedback": ""}`, nil
		default:
			return "some answer", nil
		}
	})

	env := newSupervisorTestEnv(t, complete)
	env.ExecuteWorkflow(agent.SupervisorWorkflow, agent.SupervisorInput{
		Question:          "good subject and bad subject",
		MaxRoundsPerChild: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"supervisor should still succeed even with one failed child")

	var out agent.SupervisorOutput
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, 1, out.SuccessCount)
	require.Equal(t, 1, out.FailureCount)
	require.Len(t, out.SubAnswers, 2)

	// Synthesis should include the successful child's answer and
	// note the failed one.
	require.Contains(t, out.Synthesis, "some answer")
	require.Contains(t, out.Synthesis, "could not be answered")
}
