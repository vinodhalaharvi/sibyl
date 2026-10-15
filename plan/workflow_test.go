package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/plan"
)

// registerEcho wires the Echo activity into a test environment under its
// registered name, so PlanWorkflow can dispatch it.
func registerEcho(env interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	env.RegisterActivityWithOptions(plan.Echo, activity.RegisterOptions{Name: plan.EchoName})
}

func TestPlanWorkflow_SingleNode(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: plan.EchoName, Args: []string{"hello"}},
	}}

	env.ExecuteWorkflow(plan.Workflow, p)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res plan.Result
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "hello", res.Outputs["a"])
	require.Equal(t, []string{"a"}, res.Leaves)
}

func TestPlanWorkflow_Pipeline(t *testing.T) {
	// a -> b -> c, threading outputs Unix-pipe style.
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	registerEcho(env)

	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: plan.EchoName, Args: []string{"one"}},
		{ID: "b", Activity: plan.EchoName, Args: []string{"two"}, Requires: []plan.NodeID{"a"}},
		{ID: "c", Activity: plan.EchoName, Requires: []plan.NodeID{"b"}},
	}}

	env.ExecuteWorkflow(plan.Workflow, p)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res plan.Result
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

	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: plan.EchoName, Args: []string{"root"}},
		{ID: "b", Activity: plan.EchoName, Args: []string{"B"}, Requires: []plan.NodeID{"a"}},
		{ID: "c", Activity: plan.EchoName, Args: []string{"C"}, Requires: []plan.NodeID{"a"}},
		{ID: "d", Activity: plan.EchoName, Requires: []plan.NodeID{"b", "c"}},
	}}

	env.ExecuteWorkflow(plan.Workflow, p)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var res plan.Result
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
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: plan.EchoName, Requires: []plan.NodeID{"b"}},
		{ID: "b", Activity: plan.EchoName, Requires: []plan.NodeID{"a"}},
	}}

	env.ExecuteWorkflow(plan.Workflow, p)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "InvalidPlan")
}
