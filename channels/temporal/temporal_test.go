package channeltemporal_test

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/channels"
	channeltemporal "github.com/vinodhalaharvi/sibyl/channels/temporal"
	channelstest "github.com/vinodhalaharvi/sibyl/channels/test"
)

// testHarness builds a dispatcher with one in-memory channel + the temporal
// binding, returning everything the workflow test needs.
type testHarness struct {
	*testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
	mem *channelstest.Channel
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	mem := channelstest.New("inmem")
	dispatcher, err := channels.New(
		channels.WithChannel(mem.Channel()),
		channels.WithRouter(channels.FixedRouter("inmem", "default")),
	)
	if err != nil {
		t.Fatal(err)
	}
	acts, err := channeltemporal.NewActivities(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	env.RegisterActivityWithOptions(acts.Post, activity.RegisterOptions{Name: channeltemporal.ActivityPost})
	env.RegisterActivityWithOptions(acts.AwaitVerdict, activity.RegisterOptions{Name: channeltemporal.ActivityAwaitVerdict})

	return &testHarness{
		WorkflowTestSuite: suite,
		env:               env,
		mem:               mem,
	}
}

// === Workflow under test: post then await ===

// postAndAwaitWorkflow is a minimal workflow that exercises both activities.
func postAndAwaitWorkflow(ctx workflow.Context, msg channels.Message) (workflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	post, err := channeltemporal.PostFromWorkflow(ctx, msg)
	if err != nil {
		return workflowResult{}, err
	}
	if len(post.Receipts) == 0 {
		return workflowResult{}, errors.New("no receipts")
	}

	verdict, err := channeltemporal.AwaitVerdictFromWorkflow(ctx, post.Receipts, channels.AwaitOpts{
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		return workflowResult{Posted: true, TimedOut: true}, nil
	}
	return workflowResult{
		Posted:  true,
		Verdict: verdict,
	}, nil
}

type workflowResult struct {
	Posted   bool
	Verdict  channels.Verdict
	TimedOut bool
}

func TestPostFromWorkflow(t *testing.T) {
	h := newHarness(t)
	h.env.RegisterWorkflow(postAndAwaitWorkflow)

	// Queue a verdict ahead of time so the await resolves quickly.
	h.mem.EnqueueVerdict(channels.Verdict{
		Choice:  "accept",
		Channel: "inmem",
		Actor:   channels.Identity{Canonical: "inmem:U1"},
	})

	h.env.ExecuteWorkflow(postAndAwaitWorkflow, channels.Message{
		Title:    "test finding",
		Severity: channels.SeverityHigh,
	})

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var got workflowResult
	if err := h.env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if !got.Posted {
		t.Error("expected Posted=true")
	}
	if got.Verdict.Choice != "accept" {
		t.Errorf("Verdict.Choice = %q, want accept", got.Verdict.Choice)
	}
	if got.Verdict.Actor.Canonical != "inmem:U1" {
		t.Errorf("Actor.Canonical = %q, want inmem:U1", got.Verdict.Actor.Canonical)
	}

	posts := h.mem.Posts()
	if len(posts) != 1 || posts[0].Message.Title != "test finding" {
		t.Errorf("expected one recorded post, got %+v", posts)
	}
}

func TestAwaitTimeoutSurfacesAsWorkflowResult(t *testing.T) {
	h := newHarness(t)
	h.env.RegisterWorkflow(postAndAwaitWorkflow)

	// No verdict enqueued — the await should time out.
	h.env.ExecuteWorkflow(postAndAwaitWorkflow, channels.Message{Title: "no verdict here"})

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var got workflowResult
	if err := h.env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if !got.TimedOut {
		t.Error("expected TimedOut=true")
	}
}

// === Test for direct activity registration via dispatcher round-trip ===

func TestActivitiesRequireDispatcher(t *testing.T) {
	_, err := channeltemporal.NewActivities(nil)
	if err == nil {
		t.Error("expected error for nil dispatcher")
	}
}
