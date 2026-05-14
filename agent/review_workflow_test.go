package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/agent"
)

func TestPRReviewWorkflow_HappyPath(t *testing.T) {
	// Spin up a fake LLM that gives canned responses for each prompt.
	// The workflow makes 4 LLM calls (3 analyzers + 1 synthesizer).
	scripted := &agent.ScriptedLLM{
		Cycle: true,
		Responses: []string{
			"Security analysis: no SQL injection, parameter binding throughout. APPROVE.",
			"Test coverage: 9 new tests added, all paths covered. APPROVE.",
			"Style check: passes gofmt and staticcheck. APPROVE.",
			"All three reviewers approved. **APPROVE**.",
		},
	}

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	acts := &agent.Activities{Complete: scripted.Complete}
	env.RegisterActivityWithOptions(acts.ParseDiff, activity.RegisterOptions{Name: agent.ParseDiffActivityName})
	env.RegisterActivityWithOptions(acts.SecurityAudit, activity.RegisterOptions{Name: agent.SecurityAuditActivityName})
	env.RegisterActivityWithOptions(acts.TestCoverage, activity.RegisterOptions{Name: agent.TestCoverageActivityName})
	env.RegisterActivityWithOptions(acts.StyleCheck, activity.RegisterOptions{Name: agent.StyleCheckActivityName})
	env.RegisterActivityWithOptions(acts.SynthesizeReview, activity.RegisterOptions{Name: agent.SynthesizeReviewActivity})

	env.ExecuteWorkflow(agent.PRReviewWorkflow, agent.PRReviewInput{
		Diff: "test diff content",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out agent.PRReviewOutput
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Contains(t, out.Parse, "Parsed diff")
	require.NotEmpty(t, out.Security)
	require.NotEmpty(t, out.Tests)
	require.NotEmpty(t, out.Style)
	require.Contains(t, strings.ToLower(out.Synthesis), "approve")
}

func TestPRReviewWorkflow_EmptyDiffRejected(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(agent.PRReviewWorkflow, agent.PRReviewInput{Diff: ""})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "InvalidInput")
}

func TestActivities_ParseDiff_Stub(t *testing.T) {
	a := &agent.Activities{
		Complete: func(_ context.Context, _, _ string) (string, error) { return "", nil },
	}
	out, err := a.ParseDiff(context.Background(), agent.PRReviewInput{Diff: "hello"})
	require.NoError(t, err)
	require.Contains(t, out, "Parsed diff")
	require.Contains(t, out, "5") // length of "hello"
}

func TestActivities_SecurityAudit_CallsLLM(t *testing.T) {
	called := false
	a := &agent.Activities{
		Complete: func(_ context.Context, sys, user string) (string, error) {
			called = true
			require.Contains(t, sys, "security-focused")
			require.Contains(t, user, "Audit")
			return "looks fine", nil
		},
	}
	out, err := a.SecurityAudit(context.Background(), agent.AnalyzerInput{
		NodeID:    "security_audit",
		ParseText: "some parsed diff",
	})
	require.NoError(t, err)
	require.Equal(t, "looks fine", out)
	require.True(t, called)
}

func TestActivities_SynthesizeReview_RequiresComplete(t *testing.T) {
	a := &agent.Activities{}
	_, err := a.SynthesizeReview(context.Background(), agent.SynthesizeInput{
		NodeID: "synthesize",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Complete is nil")
}
