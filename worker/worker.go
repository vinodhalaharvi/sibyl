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
}

// Register adds Sibyl's workflows and activities to a Temporal worker
// with default options.
func Register(w worker.Worker, complete agent.CompleteFunc) {
	RegisterWithOptions(w, complete, Options{})
}

// RegisterWithOptions is Register with explicit options.
//
// Use this when you want LLM-backed synthesis or any other configurable
// behavior:
//
//	worker.RegisterWithOptions(w, complete, worker.Options{
//	    Synthesizer: agent.LLMSynthesizer(complete),
//	})
func RegisterWithOptions(w worker.Worker, complete agent.CompleteFunc, opts Options) {
	w.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})
	w.RegisterWorkflowWithOptions(agent.SupervisorWorkflow, workflow.RegisterOptions{
		Name: agent.SupervisorWorkflowName,
	})

	acts := &agent.Activities{
		Complete:    complete,
		Synthesizer: opts.Synthesizer,
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
}
