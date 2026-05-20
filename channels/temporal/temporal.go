// Package channeltemporal provides Temporal activity wrappers for the
// channels package, plus helper functions for invoking them from workflows.
//
// # Why this exists
//
// channels.Channel{Notify, Await} are plain Go functions — they have no idea
// about Temporal. Running them inside a workflow needs durability: the post
// must record a receipt in workflow history, and the await must survive
// worker restarts. Both requirements are met by wrapping the calls in
// Temporal activities, which is what this package provides.
//
// # Workflow integration
//
// Workflows should not call dispatcher.Notify directly. They should
// ExecuteActivity against ActivityPost (and ActivityAwaitVerdict). The
// activity functions, registered on the worker by NewActivities(...).Register,
// internally call the Dispatcher and return results. From the workflow's
// perspective, posting and awaiting are just activity calls — durable,
// retryable, replayable.
package channeltemporal

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// Activity names — workflows ExecuteActivity these names rather than
// passing function pointers, so registration and invocation can be
// decoupled (e.g. if you split workers).
const (
	ActivityPost         = "channels.Post"
	ActivityAwaitVerdict = "channels.AwaitVerdict"
	ActivityAwaitReplies = "channels.AwaitReplies"
)

// Activities is a Temporal-activity wrapper around a channels.Dispatcher.
// Construct one with NewActivities; register it on a worker with Register.
type Activities struct {
	dispatcher *channels.Dispatcher
}

// NewActivities constructs an Activities binding for the given Dispatcher.
func NewActivities(d *channels.Dispatcher) (*Activities, error) {
	if d == nil {
		return nil, fmt.Errorf("channels/temporal.NewActivities: dispatcher is required")
	}
	return &Activities{dispatcher: d}, nil
}

// Register adds the channels activities to a Temporal worker under the
// canonical names (ActivityPost, ActivityAwaitVerdict). Call once at worker
// setup.
func (a *Activities) Register(w worker.Worker) {
	w.RegisterActivityWithOptions(a.Post, activity.RegisterOptions{Name: ActivityPost})
	w.RegisterActivityWithOptions(a.AwaitVerdict, activity.RegisterOptions{Name: ActivityAwaitVerdict})
}

// Post is the Temporal activity for posting a Message. The workflow
// invokes this; its return value (PostResult) is recorded in workflow
// history and is replayable.
func (a *Activities) Post(ctx context.Context, msg channels.Message) (PostResult, error) {
	r, err := a.dispatcher.Notify(ctx, msg)
	if err != nil {
		return PostResult{}, err
	}
	return PostResult{
		Receipts: r.Receipts,
		Errors:   r.Errors,
	}, nil
}

// AwaitVerdict is the Temporal activity for awaiting a verdict on previously-
// posted Receipts. Heartbeats periodically so worker crashes during the wait
// are recoverable.
//
// On timeout this returns a sentinel error that callers should check for via
// errors.Is or activity-error inspection. Inside the workflow, this is handled
// by AwaitVerdictFromWorkflow helper if you want it.
func (a *Activities) AwaitVerdict(ctx context.Context, receipts []channels.Receipt, opts channels.AwaitOpts) (channels.Verdict, error) {
	// Heartbeat ticker — separate from the channel's internal poll, just
	// keeps Temporal's heartbeat happy so worker restarts can resume.
	heartbeatStop := make(chan struct{})
	defer close(heartbeatStop)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx, "awaiting verdict")
			}
		}
	}()
	return a.dispatcher.Await(ctx, receipts, opts)
}

// PostResult is the activity-output shape — same as channels.PostResult but
// declared here so workflows can import it without dragging the slack
// adapter's transitive imports into the workflow build target.
type PostResult struct {
	Receipts []channels.Receipt
	Errors   []string
}

// ===== Workflow-side helpers =====

// PostFromWorkflow is a convenience for invoking ActivityPost from a workflow
// with sane defaults. Returns the PostResult or any activity error.
//
// Customize options by overriding ActivityOptions on the context yourself
// and calling workflow.ExecuteActivity(ctx, ActivityPost, msg) directly.
func PostFromWorkflow(ctx workflow.Context, msg channels.Message) (PostResult, error) {
	opts := workflow.GetActivityOptions(ctx)
	if opts.StartToCloseTimeout == 0 {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumAttempts:    3,
			},
		})
	}
	var result PostResult
	err := workflow.ExecuteActivity(ctx, ActivityPost, msg).Get(ctx, &result)
	return result, err
}

// AwaitVerdictFromWorkflow is a convenience for invoking ActivityAwaitVerdict
// with the AwaitOpts.Timeout flowing into StartToCloseTimeout (plus a buffer)
// so the activity can run to its full configured length without Temporal-side
// premature termination.
//
// Returns the Verdict (with Choice empty on timeout), the Temporal error if
// any (including ActivityTaskTimedOut, which the caller may interpret as a
// natural timeout outcome).
func AwaitVerdictFromWorkflow(ctx workflow.Context, receipts []channels.Receipt, opts channels.AwaitOpts) (channels.Verdict, error) {
	wait := opts.Timeout
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: wait + 5*time.Minute, // buffer so Temporal doesn't kill it early
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			// Don't retry on timeout — a timed-out wait is a deliberate outcome,
			// not a transient failure to recover from.
			MaximumAttempts: 1,
		},
	})
	var v channels.Verdict
	err := workflow.ExecuteActivity(ctx, ActivityAwaitVerdict, receipts, opts).Get(ctx, &v)
	return v, err
}
