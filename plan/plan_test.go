package plan_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vinodhalaharvi/sibyl/plan"
)

func TestPlan_Validate_Empty(t *testing.T) {
	require.Error(t, plan.Plan{}.Validate())
}

func TestPlan_Validate_HappyPipeline(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: plan.EchoName, Args: []string{"hello"}},
		{ID: "b", Activity: plan.EchoName, Requires: []plan.NodeID{"a"}},
	}}
	require.NoError(t, p.Validate())
}

func TestPlan_Validate_DuplicateID(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: "Echo"},
		{ID: "a", Activity: "Echo"},
	}}
	require.ErrorContains(t, p.Validate(), "duplicate")
}

func TestPlan_Validate_EmptyActivity(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{{ID: "a"}}}
	require.ErrorContains(t, p.Validate(), "no activity")
}

func TestPlan_Validate_UnknownRequire(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: "Echo", Requires: []plan.NodeID{"ghost"}},
	}}
	require.ErrorContains(t, p.Validate(), "unknown node")
}

func TestPlan_Validate_SelfRequire(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: "Echo", Requires: []plan.NodeID{"a"}},
	}}
	require.ErrorContains(t, p.Validate(), "requires itself")
}

func TestPlan_Validate_Cycle(t *testing.T) {
	// a -> b -> a (each requires the other): no zero-indegree node.
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "a", Activity: "Echo", Requires: []plan.NodeID{"b"}},
		{ID: "b", Activity: "Echo", Requires: []plan.NodeID{"a"}},
	}}
	require.ErrorContains(t, p.Validate(), "cycle")
}

// === Echo activity (pure, no Temporal) ====================================

func TestEcho_ArgsOnly(t *testing.T) {
	out, err := plan.Echo(context.Background(), plan.Input{
		NodeID: "a", Args: []string{"hello", "world"},
	})
	require.NoError(t, err)
	require.Equal(t, "hello world", out)
}

func TestEcho_UpstreamPassthrough(t *testing.T) {
	out, err := plan.Echo(context.Background(), plan.Input{
		NodeID:   "b",
		Upstream: map[string]string{"a": "from upstream"},
	})
	require.NoError(t, err)
	require.Equal(t, "from upstream", out)
}

func TestEcho_ArgsAndUpstream(t *testing.T) {
	out, err := plan.Echo(context.Background(), plan.Input{
		NodeID:   "b",
		Args:     []string{"stage2"},
		Upstream: map[string]string{"a": "stage1out"},
	})
	require.NoError(t, err)
	require.Equal(t, "stage2 | stage1out", out)
}

func TestEcho_MultipleUpstreamSortedJoin(t *testing.T) {
	out, err := plan.Echo(context.Background(), plan.Input{
		NodeID:   "join",
		Upstream: map[string]string{"z": "Z", "a": "A", "m": "M"},
	})
	require.NoError(t, err)
	// Sorted by key: a, m, z → "A M Z"
	require.Equal(t, "A M Z", out)
}

func TestEcho_Empty(t *testing.T) {
	out, err := plan.Echo(context.Background(), plan.Input{NodeID: "a"})
	require.NoError(t, err)
	require.Equal(t, "", out)
}
