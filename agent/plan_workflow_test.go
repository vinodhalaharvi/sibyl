package agent_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// registerEcho wires the Echo activity into a test environment under its
// registered name, so PlanWorkflow can dispatch it.
func registerEcho(env interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	env.RegisterActivityWithOptions(agent.Echo, activity.RegisterOptions{Name: agent.EchoActivityName})
}

func TestPlanWorkflow_SingleNode(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	plan := agent.Plan{Nodes: []agent.PlanNode{
		{ID: "a", Activity: agent.EchoActivityName, Args: []string{"hello"}},
	}}

	env.ExecuteWorkflow(agent.PlanWorkflow, plan)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res agent.PlanResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "hello", res.Outputs["a"])
	require.Equal(t, []string{"a"}, res.Leaves)
}

func TestPlanWorkflow_Pipeline(t *testing.T) {
	// a -> b -> c, threading outputs Unix-pipe style.
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	plan := agent.Plan{Nodes: []agent.PlanNode{
		{ID: "a", Activity: agent.EchoActivityName, Args: []string{"one"}},
		{ID: "b", Activity: agent.EchoActivityName, Args: []string{"two"}, Requires: []agent.PlanNodeID{"a"}},
		{ID: "c", Activity: agent.EchoActivityName, Requires: []agent.PlanNodeID{"b"}},
	}}

	env.ExecuteWorkflow(agent.PlanWorkflow, plan)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res agent.PlanResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "one", res.Outputs["a"])
	// b sees a's output downstream: "two | one"
	require.Equal(t, "two | one", res.Outputs["b"])
	// c is passthrough of b
	require.Equal(t, "two | one", res.Outputs["c"])
	require.Equal(t, []string{"c"}, res.Leaves)
}

func TestPlanWorkflow_FanOutFanIn(t *testing.T) {
	// a -> (b, c) -> d   (b and c run in parallel, d joins)
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	plan := agent.Plan{Nodes: []agent.PlanNode{
		{ID: "a", Activity: agent.EchoActivityName, Args: []string{"root"}},
		{ID: "b", Activity: agent.EchoActivityName, Args: []string{"B"}, Requires: []agent.PlanNodeID{"a"}},
		{ID: "c", Activity: agent.EchoActivityName, Args: []string{"C"}, Requires: []agent.PlanNodeID{"a"}},
		{ID: "d", Activity: agent.EchoActivityName, Requires: []agent.PlanNodeID{"b", "c"}},
	}}

	env.ExecuteWorkflow(agent.PlanWorkflow, plan)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res agent.PlanResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "root", res.Outputs["a"])
	require.Equal(t, "B | root", res.Outputs["b"])
	require.Equal(t, "C | root", res.Outputs["c"])
	// d joins b and c (sorted key order: b then c): "B | root C | root"
	require.Equal(t, "B | root C | root", res.Outputs["d"])
	require.Equal(t, []string{"d"}, res.Leaves)
}

func TestPlanWorkflow_RejectsInvalidPlan(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	// Cycle: a <-> b
	plan := agent.Plan{Nodes: []agent.PlanNode{
		{ID: "a", Activity: agent.EchoActivityName, Requires: []agent.PlanNodeID{"b"}},
		{ID: "b", Activity: agent.EchoActivityName, Requires: []agent.PlanNodeID{"a"}},
	}}

	env.ExecuteWorkflow(agent.PlanWorkflow, plan)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "InvalidPlan")
}
