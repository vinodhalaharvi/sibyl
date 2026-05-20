package agent_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// === ChildConvergeArrow ====================================================

// wrapperWorkflowForChildConverge is a thin parent workflow that invokes
// ChildConvergeArrow against an inner Question, used to drive the child-arrow
// from inside Temporal's deterministic context. Real callers would do
// something more interesting between the parent's other steps; here we
// just want to confirm the arrow composes.
func wrapperWorkflowForChildConverge(ctx workflow.Context, q agent.Question) (agent.Answer, error) {
	arrow := agent.ChildConvergeArrow(ctx, workflow.ChildWorkflowOptions{})
	return arrow(ctx, q)
}

func TestChildConvergeArrow_RunsConvergeWorkflowAsChild(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{
		// Researcher answers
		"Tokyo is the capital of Japan.",
		// Critic approves immediately
		`{"approved": true, "confidence": 0.95, "feedback": ""}`,
	}}

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	// Register the wrapper as the "main" workflow and ConvergeWorkflow as the
	// child (under its canonical registered name, which the arrow looks up).
	env.RegisterWorkflow(wrapperWorkflowForChildConverge)
	env.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: agent.ConvergeWorkflowName,
	})

	acts := &agent.Activities{Complete: llm.Complete}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))

	env.ExecuteWorkflow(wrapperWorkflowForChildConverge, agent.Question{
		Text:      "What is the capital of Japan?",
		MaxRounds: 3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var ans agent.Answer
	require.NoError(t, env.GetWorkflowResult(&ans))
	require.True(t, ans.Converged)
	require.Equal(t, 1, ans.Rounds)
	require.Contains(t, ans.Text, "Tokyo")
}

func TestChildConvergeArrow_PropagatesConvergeWorkflowError(t *testing.T) {
	llm := &agent.ScriptedLLM{Responses: []string{}} // empty — Research will fail

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(wrapperWorkflowForChildConverge)
	env.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: agent.ConvergeWorkflowName,
	})

	acts := &agent.Activities{Complete: llm.Complete}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))

	env.ExecuteWorkflow(wrapperWorkflowForChildConverge, agent.Question{
		Text:      "Anything",
		MaxRounds: 0, // non-retryable application error from ConvergeWorkflow
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "ChildConvergeArrow") ||
			strings.Contains(err.Error(), "MaxRounds"),
		"error should reference the wrapper or the underlying cause, got: %v", err)
}

// === ChildSupervisorArrow ==================================================

func wrapperWorkflowForChildSupervisor(ctx workflow.Context, in agent.SupervisorInput) (agent.SupervisorOutput, error) {
	arrow := agent.ChildSupervisorArrow(ctx, workflow.ChildWorkflowOptions{})
	return arrow(ctx, in)
}

func TestChildSupervisorArrow_RunsSupervisorWorkflowAsChild(t *testing.T) {
	// Children run in parallel inside the supervisor — response order is
	// non-deterministic. Route by content rather than indexing into a
	// pre-baked slice. Mirrors agent/supervisor_test.go's pattern.
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
			return "fallback answer for: " + user, nil
		}
	})

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(wrapperWorkflowForChildSupervisor)
	env.RegisterWorkflowWithOptions(agent.SupervisorWorkflow, workflow.RegisterOptions{
		Name: agent.SupervisorWorkflowName,
	})
	env.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: agent.ConvergeWorkflowName,
	})

	acts := &agent.Activities{Complete: complete}
	env.RegisterActivityWithOptions(acts.Research, registerOpts(agent.ResearchActivityName))
	env.RegisterActivityWithOptions(acts.Critique, registerOpts(agent.CritiqueActivityName))
	env.RegisterActivityWithOptions(acts.Decompose, registerOpts(agent.DecomposeActivityName))
	env.RegisterActivityWithOptions(acts.Synthesize, registerOpts(agent.SynthesizeActivityName))

	env.ExecuteWorkflow(wrapperWorkflowForChildSupervisor, agent.SupervisorInput{
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
}

// === Client-flavored arrows (input validation only) =========================

// The client-flavored ConvergeArrow and SupervisorArrow require a real
// Temporal client to be meaningfully tested. The functional behavior they
// add over the underlying workflow is just "wrap ExecuteWorkflow + Get",
// which is straightforward — unit tests here verify the input-validation
// guards. End-to-end behavior is covered by integration tests against a
// real Temporal cluster, not by these unit tests.

func TestConvergeArrow_RejectsNilClient(t *testing.T) {
	arrow := agent.ConvergeArrow(nil, client.StartWorkflowOptions{TaskQueue: "x"})
	_, err := arrow(context.Background(), agent.Question{Text: "test", MaxRounds: 1})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "client is nil"),
		"expected nil-client error, got: %v", err)
}

func TestConvergeArrow_RejectsEmptyTaskQueue(t *testing.T) {
	// We don't have a real client here, but the function should reject the
	// empty task queue before it ever touches the client. Pass a sentinel
	// non-nil value via a stub.
	arrow := agent.ConvergeArrow(stubClient{}, client.StartWorkflowOptions{})
	_, err := arrow(context.Background(), agent.Question{Text: "test", MaxRounds: 1})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "TaskQueue is empty"),
		"expected empty-task-queue error, got: %v", err)
}

func TestSupervisorArrow_RejectsNilClient(t *testing.T) {
	arrow := agent.SupervisorArrow(nil, client.StartWorkflowOptions{TaskQueue: "x"})
	_, err := arrow(context.Background(), agent.SupervisorInput{Question: "q", MaxRoundsPerChild: 1})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "client is nil"),
		"expected nil-client error, got: %v", err)
}

func TestSupervisorArrow_RejectsEmptyTaskQueue(t *testing.T) {
	arrow := agent.SupervisorArrow(stubClient{}, client.StartWorkflowOptions{})
	_, err := arrow(context.Background(), agent.SupervisorInput{Question: "q", MaxRoundsPerChild: 1})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "TaskQueue is empty"),
		"expected empty-task-queue error, got: %v", err)
}

// stubClient is a minimal client.Client whose only purpose is to satisfy the
// non-nil check. Its methods are not exercised by the input-validation tests.
type stubClient struct{ client.Client }

func (stubClient) ExecuteWorkflow(_ context.Context, _ client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	return nil, errors.New("stub")
}
