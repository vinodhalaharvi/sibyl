package agent

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// SupervisorTaskQueue is the Temporal task queue the supervisor and its
// activities listen on. We deliberately use the same queue as the convergence
// workers so a single worker process can serve both supervisor and child
// workflows. In production you might split them for isolation.
const SupervisorTaskQueue = TaskQueue

// Activity name constants for the supervisor's helpers.
const (
	DecomposeActivityName  = "Decompose"
	SynthesizeActivityName = "Synthesize"
)

// SupervisorWorkflowName is the registered name of the supervisor workflow.
const SupervisorWorkflowName = "SupervisorWorkflow"

// Decompose is the Temporal activity entry point for the decomposer.
// It just dispatches to the package-level decomposeArrow, then applies
// the MaxSubQuestions cap. Pure / deterministic; safe to retry.
//
// Lives on the Activities struct so it shares a registration site with
// Research and Critique. Decomposition doesn't currently use a.Complete
// but keeping it as a method preserves the option to swap in an LLM
// decomposer without changing the worker registration.
func (a *Activities) Decompose(ctx context.Context, in DecomposeInput) ([]SubQuestion, error) {
	qs, err := decomposeArrow(ctx, Question{Text: in.Question})
	if err != nil {
		return nil, err
	}
	return applyMaxSubQuestions(qs, in.MaxSubQuestions), nil
}

// DecomposeInput is the activity input for Decompose.
type DecomposeInput struct {
	Question        string
	MaxSubQuestions int
}

// Synthesize is the Temporal activity entry point for the synthesizer.
// If a.Synthesizer is set (via Activities.Synthesizer field assignment
// or worker.RegisterWithOptions), that arrow is used; otherwise the
// default heuristic concatenator runs. This makes "use an LLM for
// synthesis" a one-line swap at registration time without touching the
// workflow.
func (a *Activities) Synthesize(ctx context.Context, in []SubAnswer) (string, error) {
	syn := a.Synthesizer
	if syn == nil {
		syn = synthesizeArrow
	}
	return syn(ctx, in)
}

// SupervisorWorkflow decomposes the input question, fans out child
// convergence workflows in parallel, waits for all of them, and synthesizes
// the result.
//
// Failure handling: individual child failures are recorded as SubAnswer
// entries with Error set, but do not fail the supervisor. The supervisor
// fails only if (a) decomposition itself fails, (b) ALL children fail,
// or (c) synthesis fails.
//
// All child workflows run on the same task queue as the supervisor (so
// the same worker process serves both). Each child gets a deterministic
// ID derived from the supervisor's workflow ID so the Web UI shows them
// as a coherent group.
func SupervisorWorkflow(ctx workflow.Context, in SupervisorInput) (SupervisorOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Supervisor starting", "question", in.Question)

	if in.Question == "" {
		return SupervisorOutput{}, temporal.NewNonRetryableApplicationError(
			"Question must not be empty", "InvalidInput", nil)
	}
	if in.MaxRoundsPerChild <= 0 {
		return SupervisorOutput{}, temporal.NewNonRetryableApplicationError(
			"MaxRoundsPerChild must be > 0", "InvalidInput", nil)
	}
	maxSubs := in.MaxSubQuestions
	if maxSubs == 0 {
		maxSubs = 5
	}

	// --- Phase 1: decompose ------------------------------------------------
	decomposeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"InvalidInput"},
		},
	})

	var subQuestions []SubQuestion
	err := workflow.ExecuteActivity(decomposeCtx, DecomposeActivityName, DecomposeInput{
		Question:        in.Question,
		MaxSubQuestions: maxSubs,
	}).Get(ctx, &subQuestions)
	if err != nil {
		return SupervisorOutput{}, fmt.Errorf("decompose failed: %w", err)
	}
	logger.Info("Decomposed", "subquestion_count", len(subQuestions))

	// --- Phase 2: fan-out to child convergence workflows -------------------
	parentID := workflow.GetInfo(ctx).WorkflowExecution.ID
	futures := make([]workflow.ChildWorkflowFuture, len(subQuestions))
	for i, sq := range subQuestions {
		childOpts := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("%s-sub-%d", parentID, sq.Index),
			TaskQueue:  SupervisorTaskQueue,
			// Default ParentClosePolicy is TERMINATE — if the supervisor
			// is cancelled or fails, in-flight children are cancelled too.
			// That's the policy we want for "supervised work."
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		futures[i] = workflow.ExecuteChildWorkflow(childCtx, "ConvergeWorkflow", Question{
			Text:      sq.Text,
			MaxRounds: in.MaxRoundsPerChild,
		})
		logger.Info("Spawned child", "index", i, "subquestion", sq.Text)
	}

	// --- Phase 3: wait for all, swallow failures ---------------------------
	subAnswers := make([]SubAnswer, len(subQuestions))
	successCount, failureCount := 0, 0
	for i, f := range futures {
		var childAns Answer
		// Get() blocks until the child completes (success or failure).
		// We deliberately do NOT bail on err here — collect per-child results.
		childErr := f.Get(ctx, &childAns)
		sa := SubAnswer{
			SubQuestion: subQuestions[i],
		}
		if childErr != nil {
			sa.Error = childErr.Error()
			failureCount++
			logger.Warn("Child workflow failed", "index", i, "err", childErr)
		} else {
			sa.Answer = childAns.Text
			sa.Converged = childAns.Converged
			sa.Rounds = childAns.Rounds
			successCount++
		}
		subAnswers[i] = sa
	}

	if successCount == 0 {
		return SupervisorOutput{
				SubAnswers:   subAnswers,
				FailureCount: failureCount,
			}, fmt.Errorf("all %d children failed; first error: %s",
				failureCount, subAnswers[0].Error)
	}

	// --- Phase 4: synthesize -----------------------------------------------
	synthCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	})

	var synthesis string
	err = workflow.ExecuteActivity(synthCtx, SynthesizeActivityName, subAnswers).Get(ctx, &synthesis)
	if err != nil {
		return SupervisorOutput{}, fmt.Errorf("synthesis failed: %w", err)
	}

	logger.Info("Supervisor complete",
		"successes", successCount, "failures", failureCount)

	return SupervisorOutput{
		Synthesis:    synthesis,
		SubAnswers:   subAnswers,
		SuccessCount: successCount,
		FailureCount: failureCount,
	}, nil
}
