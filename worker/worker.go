// Package worker provides helpers to register Sibyl workflows and activities
// on a Temporal worker.
package worker

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/agent"
	"github.com/vinodhalaharvi/weft/weft"
)

// Options configures optional behavior at worker registration time.
// The zero value is "heuristic synthesizer, no extras."
type Options struct {
	// Synthesizer overrides the default heuristic synthesizer with a
	// weft.Arrow of your choosing. Use agent.LLMSynthesizer(complete)
	// to get an LLM-backed one.
	Synthesizer weft.Arrow[[]agent.SubAnswer, string]
	// Stream, if set, gives the PR-review workflow's activities access
	// to a streaming LLM. Backends without streaming support leave this
	// nil; the activities fall back to atomic Complete.
	Stream agent.CompleteStreamFunc
}

// Register adds Sibyl's workflows and activities to a Temporal worker
// with default options.
func Register(w worker.Worker, complete agent.CompleteFunc) {
	RegisterWithOptions(w, complete, Options{})
}

// RegisterWithOptions is Register with explicit options.
func RegisterWithOptions(w worker.Worker, complete agent.CompleteFunc, opts Options) {
	w.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})
	w.RegisterWorkflowWithOptions(agent.SupervisorWorkflow, workflow.RegisterOptions{
		Name: agent.SupervisorWorkflowName,
	})
	w.RegisterWorkflowWithOptions(agent.PRReviewWorkflow, workflow.RegisterOptions{
		Name: agent.ReviewWorkflowName,
	})
	w.RegisterWorkflowWithOptions(agent.PlanWorkflow, workflow.RegisterOptions{
		Name: agent.PlanWorkflowName,
	})

	acts := &agent.Activities{
		Complete:    complete,
		Synthesizer: opts.Synthesizer,
		Stream:      opts.Stream,
	}
	w.RegisterActivityWithOptions(acts.Research, activity.RegisterOptions{
		Name: agent.ResearchActivityName,
	})
	w.RegisterActivityWithOptions(acts.Critique, activity.RegisterOptions{
		Name: agent.CritiqueActivityName,
	})
	w.RegisterActivityWithOptions(acts.Decompose, activity.RegisterOptions{
		Name: agent.DecomposeActivityName,
	})
	w.RegisterActivityWithOptions(acts.Synthesize, activity.RegisterOptions{
		Name: agent.SynthesizeActivityName,
	})

	// PR-review DAG activities.
	w.RegisterActivityWithOptions(acts.ParseDiff, activity.RegisterOptions{
		Name: agent.ParseDiffActivityName,
	})
	w.RegisterActivityWithOptions(acts.SecurityAudit, activity.RegisterOptions{
		Name: agent.SecurityAuditActivityName,
	})
	w.RegisterActivityWithOptions(acts.TestCoverage, activity.RegisterOptions{
		Name: agent.TestCoverageActivityName,
	})
	w.RegisterActivityWithOptions(acts.StyleCheck, activity.RegisterOptions{
		Name: agent.StyleCheckActivityName,
	})
	w.RegisterActivityWithOptions(acts.SynthesizeReview, activity.RegisterOptions{
		Name: agent.SynthesizeReviewActivity,
	})

	// Generic plan activities. Echo is the no-vendor builtin the
	// AgentScript translator targets first.
	w.RegisterActivityWithOptions(agent.Echo, activity.RegisterOptions{
		Name: agent.EchoActivityName,
	})
}
