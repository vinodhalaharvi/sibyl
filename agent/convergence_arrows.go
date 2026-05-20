// Package agent — convergence_arrows.go exposes ConvergeWorkflow and
// SupervisorWorkflow as composable arrow values, matching the shape of
// ToolAgentArrow in tool_agent.go.
//
// Two flavors per primitive:
//
//	ChildConvergeArrow      Arrow[Question, Answer]      — for use inside a Temporal workflow
//	ConvergeArrow           Arrow[Question, Answer]      — for use from outside Temporal (CLI, translator)
//
//	ChildSupervisorArrow    Arrow[SupervisorInput, SupervisorOutput]
//	SupervisorArrow         Arrow[SupervisorInput, SupervisorOutput]
//
// The "Child" variants use workflow.ExecuteChildWorkflow; they MUST be
// called from within a workflow's deterministic context. The non-Child
// variants take a Temporal client and use client.ExecuteWorkflow; they
// are appropriate for the translator's Submit phase, CLI entry points,
// and tests.
//
// Why two flavors:
//
//	ToolAgentArrow can be called from either context because it doesn't
//	actually invoke Temporal — it runs the tool-use loop inline, with
//	the choice of activity-vs-inline made at the call site. Converge
//	and Supervisor ARE Temporal workflows, so the wrapper has to know
//	which side of the workflow boundary the caller is on.
//
// Both flavors are plain Go function values, matching ToolAgentArrow's
// shape rather than the typed weft.Arrow[...] alias used in agents/.
// This keeps the wrapper consistent with surrounding code in this
// package; consumers that want a weft.Arrow can use the function value
// directly (they're structurally identical) or assign it to a typed
// variable.
package agent

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

// ConvergeWorkflowName is the name under which ConvergeWorkflow is
// registered with the Temporal worker. Use this string when invoking
// the workflow by name (e.g. from another workflow's
// ExecuteChildWorkflow or from a client's ExecuteWorkflow) — it
// matches the name registered by the worker package.
const ConvergeWorkflowName = "ConvergeWorkflow"

// ChildConvergeArrow returns an arrow that runs ConvergeWorkflow as a
// child workflow. The returned function MUST be called from within a
// parent workflow's deterministic context — it uses workflow.Context
// derived from the parent, sets a child workflow ID derived from the
// parent's run, and waits for the result via Future.Get.
//
// Use this when composing convergence inside a larger Temporal
// workflow — for example, a translator-emitted DAG node whose body
// is a converge primitive, executed in the same workflow as the rest
// of the DAG.
//
// The optional childOpts let the caller override workflow options
// (task queue, ID, retry policy). Pass workflow.ChildWorkflowOptions{}
// to use parent defaults.
func ChildConvergeArrow(parentCtx workflow.Context, childOpts workflow.ChildWorkflowOptions) func(workflow.Context, Question) (Answer, error) {
	return func(ctx workflow.Context, q Question) (Answer, error) {
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		var out Answer
		fut := workflow.ExecuteChildWorkflow(childCtx, ConvergeWorkflowName, q)
		if err := fut.Get(childCtx, &out); err != nil {
			return Answer{}, fmt.Errorf("ChildConvergeArrow: %w", err)
		}
		return out, nil
	}
}

// ConvergeArrow returns an arrow that runs ConvergeWorkflow via a
// Temporal client. The returned function blocks until the workflow
// completes, returning the final Answer.
//
// Use this for the translator's Submit phase, CLI invocations, and
// tests — anywhere the caller is OUTSIDE a Temporal workflow context.
//
// The opts parameter is required: it sets the workflow ID, task queue,
// timeouts, and retry policy. Callers that want defaults can pass
// client.StartWorkflowOptions{TaskQueue: TaskQueue} and supply only the
// ID; the rest accept Temporal's defaults.
func ConvergeArrow(c client.Client, opts client.StartWorkflowOptions) func(context.Context, Question) (Answer, error) {
	return func(ctx context.Context, q Question) (Answer, error) {
		if c == nil {
			return Answer{}, errors.New("ConvergeArrow: client is nil")
		}
		if opts.TaskQueue == "" {
			return Answer{}, errors.New("ConvergeArrow: opts.TaskQueue is empty")
		}
		we, err := c.ExecuteWorkflow(ctx, opts, ConvergeWorkflowName, q)
		if err != nil {
			return Answer{}, fmt.Errorf("ConvergeArrow: start workflow: %w", err)
		}
		var out Answer
		if err := we.Get(ctx, &out); err != nil {
			return Answer{}, fmt.Errorf("ConvergeArrow: workflow failed: %w", err)
		}
		return out, nil
	}
}

// ChildSupervisorArrow returns an arrow that runs SupervisorWorkflow
// as a child workflow. Same calling-convention story as
// ChildConvergeArrow: this MUST be called from within a parent
// workflow's deterministic context.
//
// Use this when composing a supervised fanout-of-convergence-loops
// inside a larger Temporal workflow.
func ChildSupervisorArrow(parentCtx workflow.Context, childOpts workflow.ChildWorkflowOptions) func(workflow.Context, SupervisorInput) (SupervisorOutput, error) {
	return func(ctx workflow.Context, in SupervisorInput) (SupervisorOutput, error) {
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		var out SupervisorOutput
		fut := workflow.ExecuteChildWorkflow(childCtx, SupervisorWorkflowName, in)
		if err := fut.Get(childCtx, &out); err != nil {
			return SupervisorOutput{}, fmt.Errorf("ChildSupervisorArrow: %w", err)
		}
		return out, nil
	}
}

// SupervisorArrow returns an arrow that runs SupervisorWorkflow via a
// Temporal client. Use this from outside a Temporal workflow context
// (translator Submit, CLI, tests).
func SupervisorArrow(c client.Client, opts client.StartWorkflowOptions) func(context.Context, SupervisorInput) (SupervisorOutput, error) {
	return func(ctx context.Context, in SupervisorInput) (SupervisorOutput, error) {
		if c == nil {
			return SupervisorOutput{}, errors.New("SupervisorArrow: client is nil")
		}
		if opts.TaskQueue == "" {
			return SupervisorOutput{}, errors.New("SupervisorArrow: opts.TaskQueue is empty")
		}
		we, err := c.ExecuteWorkflow(ctx, opts, SupervisorWorkflowName, in)
		if err != nil {
			return SupervisorOutput{}, fmt.Errorf("SupervisorArrow: start workflow: %w", err)
		}
		var out SupervisorOutput
		if err := we.Get(ctx, &out); err != nil {
			return SupervisorOutput{}, fmt.Errorf("SupervisorArrow: workflow failed: %w", err)
		}
		return out, nil
	}
}
