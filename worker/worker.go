// Package worker provides helpers to register Sibyl workflows and activities
// on a Temporal worker.
package worker

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/vinodhalaharvi/sibyl/agent"
)

// Register adds Sibyl's workflow and activities to a Temporal worker.
//
// The provided CompleteFunc backs both the Researcher and Critic activities.
// Pass agent.ScriptedLLM.Complete in tests; pass your real provider's
// Complete method (or a Chain of middlewares around it) in production.
func Register(w worker.Worker, complete agent.CompleteFunc) {
	w.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})

	acts := &agent.Activities{Complete: complete}
	w.RegisterActivityWithOptions(acts.Research, activity.RegisterOptions{
		Name: agent.ResearchActivityName,
	})
	w.RegisterActivityWithOptions(acts.Critique, activity.RegisterOptions{
		Name: agent.CritiqueActivityName,
	})
}
