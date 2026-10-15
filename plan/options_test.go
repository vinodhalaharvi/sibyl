package plan_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/vinodhalaharvi/sibyl/plan"
)

func TestOptions_LayeredOverDefaults(t *testing.T) {
	p := plan.Plan{
		Defaults: plan.Options{TaskQueue: "build", Timeout: 10 * time.Minute},
		Nodes: []plan.Node{
			{ID: "a", Activity: plan.EchoName},
			{ID: "b", Activity: plan.EchoName, Options: plan.Options{
				TaskQueue: "lab", MaxAttempts: 1, NonRetryable: []string{"Refused", "InvalidPlan"},
			}},
		},
	}
	a, _ := p.Node("a")
	b, _ := p.Node("b")

	oa := p.OptionsFor(a)
	require.Equal(t, "build", oa.TaskQueue)
	require.Equal(t, 10*time.Minute, oa.Timeout)
	require.Equal(t, plan.DefaultOptions.MaxAttempts, oa.MaxAttempts)
	require.Equal(t, plan.DefaultOptions.NonRetryable, oa.NonRetryable)

	ob := p.OptionsFor(b)
	require.Equal(t, "lab", ob.TaskQueue)
	require.Equal(t, 10*time.Minute, ob.Timeout)
	require.Equal(t, int32(1), ob.MaxAttempts)
	// Added to, never replaced, and no duplicates.
	require.Equal(t, []string{"InvalidInput", "InvalidPlan", "ConfigurationError", "Refused"}, ob.NonRetryable)
}

func TestOptions_DefaultsUntouched(t *testing.T) {
	before := append([]string{}, plan.DefaultOptions.NonRetryable...)
	plan.DefaultOptions.Over(plan.Options{NonRetryable: []string{"X"}})
	require.Equal(t, before, plan.DefaultOptions.NonRetryable)
}

func TestValidate_NegativeOptions(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{{ID: "a", Activity: "Echo", Options: plan.Options{Timeout: -time.Second}}}}
	require.ErrorContains(t, p.Validate(), `node "a": negative timeout`)

	p = plan.Plan{Defaults: plan.Options{MaxAttempts: -1}, Nodes: []plan.Node{{ID: "a", Activity: "Echo"}}}
	require.ErrorContains(t, p.Validate(), "defaults: negative maxAttempts")
}

func TestPlan_JSONRoundTrip(t *testing.T) {
	p := plan.Plan{
		Defaults: plan.Options{TaskQueue: "build"},
		Nodes: []plan.Node{
			{ID: "fetch", Activity: "Fetch", Args: []string{"r2://bases/pi3"}},
			{ID: "make", Activity: "Assemble", Requires: []plan.NodeID{"fetch"},
				Options: plan.Options{Timeout: 5 * time.Minute, Heartbeat: 30 * time.Second}},
		},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	// A node with no options carries none.
	require.NotContains(t, string(b), `"id":"fetch","activity":"Fetch","args":["r2://bases/pi3"],"options"`)

	var back plan.Plan
	require.NoError(t, json.Unmarshal(b, &back))
	require.Equal(t, p, back)
}

// Each node is dispatched with its own options: queue, timeout, attempts.
func TestWorkflow_PerNodeOptions(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	plan.Register(env)

	var mu sync.Mutex
	seen := map[string]activity.Info{}
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		mu.Lock()
		defer mu.Unlock()
		seen[info.ActivityID] = *info
	})

	p := plan.Plan{
		Defaults: plan.Options{TaskQueue: "build"},
		Nodes: []plan.Node{
			{ID: "a", Activity: plan.EchoName, Args: []string{"x"}},
			{ID: "b", Activity: plan.EchoName, Requires: []plan.NodeID{"a"},
				Options: plan.Options{TaskQueue: "lab", Timeout: 7 * time.Minute}},
		},
	}
	env.ExecuteWorkflow(plan.Workflow, p)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	queues := map[string]bool{}
	timeouts := map[time.Duration]bool{}
	for _, info := range seen {
		queues[info.TaskQueue] = true
		timeouts[info.StartToCloseTimeout] = true
	}
	require.Equal(t, map[string]bool{"build": true, "lab": true}, queues)
	require.True(t, timeouts[7*time.Minute], "node b's own timeout: %v", timeouts)
	require.True(t, timeouts[plan.DefaultOptions.Timeout], "node a's default timeout: %v", timeouts)
}

func TestSubmit_RequiresTaskQueue(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{{ID: "a", Activity: "Echo"}}}
	_, err := plan.Submit(context.Background(), nil, p, client.StartWorkflowOptions{})
	require.ErrorContains(t, err, "client is nil")
}

// The package's reason to exist: importing it brings in Temporal and the
// standard library, not the agent package's vendors.
func TestImportsOnlyTemporal(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/vinodhalaharvi/sibyl/plan").Output()
	if err != nil {
		t.Skipf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"sibyl/agent", "weft", "slack", "sqlite", "prometheus", "anthropic", "otel/exporters"} {
			require.NotContains(t, dep, banned, "plan imports %s", dep)
		}
	}
}
