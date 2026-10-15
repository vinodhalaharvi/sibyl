package plan

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// WorkflowName is the registered name of Workflow.
const WorkflowName = "PlanWorkflow"

// EchoName is the registered name of the built-in Echo activity.
const EchoName = "Echo"

// Result is what Workflow returns.
type Result struct {
	// Outputs maps every node ID to its string output.
	Outputs map[string]string
	// Leaves are the terminal node IDs (those nothing depends on), in
	// sorted order. For a straight pipeline this is the single final
	// stage.
	Leaves []string
	// DurationMs is the wall-clock duration of the whole plan.
	DurationMs int64
}

// Workflow executes a Plan as Temporal activities.
//
// It walks topological layers; within a layer, all nodes are dispatched
// in parallel (independent by construction) and awaited before the next
// layer. Each node runs with the plan's OptionsFor it: its task queue,
// timeout and retry policy. Each node's Input carries its static Args plus
// the outputs of its required upstream nodes. The first node failure fails
// the workflow.
func Workflow(ctx workflow.Context, p Plan) (Result, error) {
	logger := workflow.GetLogger(ctx)
	start := workflow.Now(ctx)

	if err := p.Validate(); err != nil {
		return Result{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidPlan", nil)
	}
	layers, err := p.Layers()
	if err != nil {
		return Result{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidPlan", nil)
	}

	outputs := make(map[string]string, len(p.Nodes))

	for li, layer := range layers {
		logger.Info("PlanWorkflow layer", "index", li, "nodes", len(layer))

		type pending struct {
			id     NodeID
			future workflow.Future
		}
		futures := make([]pending, 0, len(layer))

		for _, id := range layer {
			n, _ := p.Node(id)
			in := Input{
				NodeID:   string(n.ID),
				Args:     n.Args,
				Upstream: make(map[string]string, len(n.Requires)),
			}
			for _, req := range n.Requires {
				in.Upstream[string(req)] = outputs[string(req)]
			}
			actCtx := workflow.WithActivityOptions(ctx, activityOptions(p.OptionsFor(n)))
			futures = append(futures, pending{id: n.ID, future: workflow.ExecuteActivity(actCtx, n.Activity, in)})
		}

		for _, f := range futures {
			var out string
			if err := f.future.Get(ctx, &out); err != nil {
				return Result{}, fmt.Errorf("node %q failed: %w", f.id, err)
			}
			outputs[string(f.id)] = out
		}
	}

	leaves := p.Leaves()
	leafStrs := make([]string, len(leaves))
	for i, l := range leaves {
		leafStrs[i] = string(l)
	}
	return Result{
		Outputs:    outputs,
		Leaves:     leafStrs,
		DurationMs: workflow.Now(ctx).Sub(start).Milliseconds(),
	}, nil
}

func activityOptions(o Options) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           o.TaskQueue,
		StartToCloseTimeout: o.Timeout,
		HeartbeatTimeout:    o.Heartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        o.MaxAttempts,
			NonRetryableErrorTypes: o.NonRetryable,
		},
	}
}

// === Echo activity =========================================================

// Echo is the minimal plan activity: it returns its argument (or the
// joined upstream outputs if it has dependencies). No vendor, no
// credentials, so a plan referencing only Echo runs end-to-end with just a
// worker and a Temporal cluster.
//
//   - With Args and no upstream: returns the joined Args.
//   - With upstream and no Args: returns the joined upstream outputs, in
//     sorted key order (Unix-pipe passthrough).
//   - With both: returns "args | upstream" so the threading is visible.
func Echo(_ context.Context, in Input) (string, error) {
	argText := joinNonEmpty(in.Args)

	var upText string
	if len(in.Upstream) > 0 {
		keys := make([]string, 0, len(in.Upstream))
		for k := range in.Upstream {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ups := make([]string, 0, len(keys))
		for _, k := range keys {
			ups = append(ups, in.Upstream[k])
		}
		upText = joinNonEmpty(ups)
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

func joinNonEmpty(parts []string) string {
	var kept []string
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " ")
}

// === Registration and submission ===========================================

// Registry is the part of a Temporal worker (or test environment) Register
// needs.
type Registry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// Register adds Workflow and Echo to a worker under their names. The
// activities a plan names are the caller's to register.
func Register(r Registry) {
	r.RegisterWorkflowWithOptions(Workflow, workflow.RegisterOptions{Name: WorkflowName})
	r.RegisterActivityWithOptions(Echo, activity.RegisterOptions{Name: EchoName})
}

// Submit validates p and starts a Workflow for it, with the caller's start
// options. It does not wait; call Get on the returned run for the Result.
//
// opts.TaskQueue is required: it is where the workflow itself runs, and
// the default queue for nodes that name none. To run each distinct piece
// of work once, set opts.ID to a key derived from the work (a hash of its
// request) and opts.WorkflowIDConflictPolicy to
// enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING: a second submission of
// the same work then joins the run already in progress.
func Submit(ctx context.Context, c client.Client, p Plan, opts client.StartWorkflowOptions) (client.WorkflowRun, error) {
	if c == nil {
		return nil, fmt.Errorf("plan.Submit: client is nil")
	}
	if opts.TaskQueue == "" {
		return nil, fmt.Errorf("plan.Submit: opts.TaskQueue is empty")
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("plan.Submit: %w", err)
	}
	return c.ExecuteWorkflow(ctx, opts, WorkflowName, p)
}
