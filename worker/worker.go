// Package worker provides helpers to register Sibyl workflows and activities
// on a Temporal worker.
package worker

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// Register adds Sibyl's workflows and activities to a Temporal worker.
//
// The provided CompleteFunc backs the Researcher and Critic activities.
// The supervisor's Decompose and Synthesize activities are heuristic
// (no LLM) and ignore the CompleteFunc — they are registered on the
// same Activities struct so a single worker hosts everything.
//
// Pass agent.ScriptedLLM.Complete in tests; pass your real provider's
// Complete method (or a Chain of middlewares around it) in production.
func Register(w worker.Worker, complete agent.CompleteFunc) {
	w.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})
	w.RegisterWorkflowWithOptions(agent.SupervisorWorkflow, workflow.RegisterOptions{
		Name: agent.SupervisorWorkflowName,
	})

	acts := &agent.Activities{Complete: complete}
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
