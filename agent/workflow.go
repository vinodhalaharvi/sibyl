package agent

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the default Temporal task queue Sibyl workers listen on.
const TaskQueue = "sibyl-agents"

// ConvergeWorkflow runs the Researcher/Critic convergence loop.
//
// On each round:
//  1. Researcher produces a candidate answer (refining the previous one
//     if critic feedback is available).
//  2. Critic evaluates the candidate and returns a Verdict.
//  3. If Verdict.Approved is true, return the answer.
//  4. Otherwise, loop with the critic's feedback (up to MaxRounds).
//
// The workflow itself is deterministic. All non-determinism (LLM calls,
// HTTP, randomness) lives in the activities.
func ConvergeWorkflow(ctx workflow.Context, q Question) (Answer, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Sibyl converge starting", "question", q.Text, "max_rounds", q.MaxRounds)

	if q.MaxRounds <= 0 {
		return Answer{}, temporal.NewNonRetryableApplicationError(
			"MaxRounds must be > 0", "InvalidInput", nil)
	}
	if q.Text == "" {
		return Answer{}, temporal.NewNonRetryableApplicationError(
			"Question text must not be empty", "InvalidInput", nil)
	}

	// Activity options for LLM calls. Generous timeouts (LLMs are slow),
	// retries on transient failures, no retry on configuration/prompt errors.
	llmOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        30 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"ConfigurationError", "InvalidLLMResponse"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, llmOpts)

	answer := Answer{}
	var previousAnswer, criticFeedback string

	for round := 1; round <= q.MaxRounds; round++ {
		logger.Info("Round starting", "round", round)

		// Step 1: Researcher
		var candidate string
		err := workflow.ExecuteActivity(ctx, ResearchActivityName, ResearchInput{
			Question:       q.Text,
			PreviousAnswer: previousAnswer,
			CriticFeedback: criticFeedback,
		}).Get(ctx, &candidate)
		if err != nil {
			return Answer{}, fmt.Errorf("research failed on round %d: %w", round, err)
		}

		// Step 2: Critic
		var verdict Verdict
		err = workflow.ExecuteActivity(ctx, CritiqueActivityName, CritiqueInput{
			Question: q.Text,
			Answer:   candidate,
			Round:    round,
		}).Get(ctx, &verdict)
		if err != nil {
			return Answer{}, fmt.Errorf("critique failed on round %d: %w", round, err)
		}

		answer.History = append(answer.History, Round{
			Number:   round,
			Research: candidate,
			Verdict:  verdict,
		})

		// Step 3: Did we converge?
		if verdict.Approved {
			logger.Info("Converged", "round", round, "confidence", verdict.Confidence)
			answer.Text = candidate
			answer.Rounds = round
			answer.Converged = true
			return answer, nil
		}

		// Step 4: Carry forward for next round
		previousAnswer = candidate
		criticFeedback = verdict.Feedback
	}

	// Max rounds reached without convergence — return last candidate.
	logger.Warn("Max rounds reached without convergence", "rounds", q.MaxRounds)
	last := answer.History[len(answer.History)-1]
	answer.Text = last.Research
	answer.Rounds = q.MaxRounds
	answer.Converged = false
	return answer, nil
}

// Activity name constants. Using string names rather than function references
// keeps the workflow decoupled from the Activities struct (the worker registers
// the methods under these names).
const (
	ResearchActivityName = "Research"
	CritiqueActivityName = "Critique"
)
