package agent

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/vinodhalaharvi/sibyl/plan"
)

// The serializable plan and its workflow live in package plan, which
// depends on the Temporal SDK alone. These names keep existing callers of
// the agent package working.

type (
	PlanNodeID = plan.NodeID
	PlanNode   = plan.Node
	Plan       = plan.Plan
	NodeInput  = plan.Input
	PlanResult = plan.Result
)

const (
	PlanWorkflowName = plan.WorkflowName
	EchoActivityName = plan.EchoName
)

var (
	// PlanWorkflow is plan.Workflow.
	PlanWorkflow = plan.Workflow
	// Echo is plan.Echo.
	Echo = plan.Echo
)

// SubmitPlan starts a PlanWorkflow on taskQueue (TaskQueue if empty) with
// workflowID (generated if empty). For start options of your own (an ID
// conflict policy, say), use plan.Submit.
func SubmitPlan(ctx context.Context, c client.Client, p Plan, workflowID, taskQueue string) (client.WorkflowRun, error) {
	if taskQueue == "" {
		taskQueue = TaskQueue
	}
	if workflowID == "" {
		workflowID = fmt.Sprintf("plan-%d", time.Now().UnixNano())
	}
	run, err := plan.Submit(ctx, c, p, client.StartWorkflowOptions{ID: workflowID, TaskQueue: taskQueue})
	if err != nil {
		return nil, fmt.Errorf("SubmitPlan: %w", err)
	}
	return run, nil
}
