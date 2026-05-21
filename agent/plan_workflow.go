// Package agent — plan_workflow.go runs a serializable Plan as durable
// Temporal activities. It generalizes the hand-written PRReviewWorkflow:
// instead of hard-coded layers, it walks the plan's topological layers
// and dispatches each node's named activity, threading outputs to
// dependents.
//
// Each node becomes one Temporal activity. Durability granularity and
// per-node visibility in the Temporal UI come for free, exactly as with
// PRReviewWorkflow — but the graph is now data, not code, so any plan
// the translator emits runs through this single workflow.
package agent

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// PlanWorkflowName is the registered name for the generic plan workflow.
const PlanWorkflowName = "PlanWorkflow"

// EchoActivityName is the registered name of the built-in echo activity,
// the minimal no-vendor activity a plan can reference. It exists so the
// translator's first end-to-end demo needs no LLM or OAuth.
const EchoActivityName = "Echo"

// PlanResult is what PlanWorkflow returns.
type PlanResult struct {
	// Outputs maps every node ID to its string output.
	Outputs map[string]string
	// Leaves are the terminal node IDs (those nothing depends on), in
	// sorted order. For a straight pipeline this is the single final
	// stage; callers usually want Outputs[Leaves[len-1]] or the join.
	Leaves []string
	// DurationMs is the wall-clock duration of the whole plan.
	DurationMs int64
}

// PlanWorkflow executes a Plan as Temporal activities.
//
// It walks topological layers; within a layer, all nodes are dispatched
// in parallel (independent by construction) and awaited before the next
// layer. Each node's NodeInput carries its static Args plus the outputs
// of its required upstream nodes. The first node failure fails the
// workflow.
func PlanWorkflow(ctx workflow.Context, plan Plan) (PlanResult, error) {
	logger := workflow.GetLogger(ctx)
	start := workflow.Now(ctx)

	if err := plan.Validate(); err != nil {
		return PlanResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "InvalidPlan", nil)
	}

	layers, err := plan.topoLayers()
	if err != nil {
		// Validate already checked this, but be defensive.
		return PlanResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "InvalidPlan", nil)
	}

	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput", "InvalidPlan", "ConfigurationError"},
		},
	})

	outputs := make(map[string]string, len(plan.Nodes))

	for li, layer := range layers {
		logger.Info("PlanWorkflow layer", "index", li, "nodes", len(layer))

		// Dispatch every node in this layer, collecting futures.
		type pending struct {
			id     PlanNodeID
			future workflow.Future
		}
		futures := make([]pending, 0, len(layer))

		for _, id := range layer {
			n, _ := plan.node(id)
			in := NodeInput{
				NodeID:   string(n.ID),
				Args:     n.Args,
				Upstream: make(map[string]string, len(n.Requires)),
			}
			for _, req := range n.Requires {
				in.Upstream[string(req)] = outputs[string(req)]
			}
			f := workflow.ExecuteActivity(actCtx, n.Activity, in)
			futures = append(futures, pending{id: n.ID, future: f})
		}

		// Await all nodes in the layer.
		for _, p := range futures {
			var out string
			if err := p.future.Get(ctx, &out); err != nil {
				return PlanResult{}, fmt.Errorf("node %q (activity) failed: %w", p.id, err)
			}
			outputs[string(p.id)] = out
		}
	}

	leaves := plan.leaves()
	leafStrs := make([]string, len(leaves))
	for i, l := range leaves {
		leafStrs[i] = string(l)
	}

	return PlanResult{
		Outputs:    outputs,
		Leaves:     leafStrs,
		DurationMs: workflow.Now(ctx).Sub(start).Milliseconds(),
	}, nil
}

// === Echo activity =========================================================

// Echo is the minimal plan activity: it returns its argument (or the
// joined upstream outputs if it has dependencies). No LLM, no vendor, no
// credentials — so a plan referencing only Echo runs end-to-end with
// just a worker and a Temporal cluster. It is the first builtin the
// AgentScript translator targets.
//
// Behavior:
//   - With Args and no upstream: returns the joined Args.
//   - With upstream and no Args: returns the joined upstream outputs
//     (Unix-pipe passthrough).
//   - With both: returns "args | upstream" so the threading is visible.
func Echo(_ context.Context, in NodeInput) (string, error) {
	argText := joinNonEmpty(in.Args, " ")

	var upText string
	if len(in.Upstream) > 0 {
		// Deterministic order: the workflow passes a single upstream for
		// a pipeline; for multiple, join in sorted key order.
		ups := make([]string, 0, len(in.Upstream))
		keys := make([]string, 0, len(in.Upstream))
		for k := range in.Upstream {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			ups = append(ups, in.Upstream[k])
		}
		upText = joinNonEmpty(ups, " ")
	}

	switch {
	case argText != "" && upText != "":
		return argText + " | " + upText, nil
	case argText != "":
		return argText, nil
	default:
		return upText, nil
	}
}

// === Submit ================================================================

// SubmitPlan starts a PlanWorkflow for the given plan and returns the
// started workflow handle. It does not wait for completion — callers
// that want the result call handle.Get, or (like loom) correlate and
// await in the background.
//
// taskQueue defaults to TaskQueue if empty. workflowID is generated if
// empty.
func SubmitPlan(ctx context.Context, c client.Client, plan Plan, workflowID, taskQueue string) (client.WorkflowRun, error) {
	if c == nil {
		return nil, fmt.Errorf("SubmitPlan: client is nil")
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("SubmitPlan: %w", err)
	}
	if taskQueue == "" {
		taskQueue = TaskQueue
	}
	opts := client.StartWorkflowOptions{ID: workflowID, TaskQueue: taskQueue}
	if opts.ID == "" {
		opts.ID = fmt.Sprintf("plan-%d", time.Now().UnixNano())
	}
	return c.ExecuteWorkflow(ctx, opts, PlanWorkflowName, plan)
}

// --- small local helpers (no new deps) ---

func joinNonEmpty(parts []string, sep string) string {
	out := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if out != "" {
			out += sep
		}
		out += p
	}
	return out
}
