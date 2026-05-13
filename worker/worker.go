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
// The provided LLMClient backs both the Researcher and Critic activities.
// Pass an agent.ScriptedLLM in tests; pass your real provider client
// (Anthropic, OpenAI, etc) in production.
func Register(w worker.Worker, llm agent.LLMClient) {
	w.RegisterWorkflowWithOptions(agent.ConvergeWorkflow, workflow.RegisterOptions{
		Name: "ConvergeWorkflow",
	})

	acts := &agent.Activities{LLM: llm}
	w.RegisterActivityWithOptions(acts.Research, activity.RegisterOptions{
		Name: agent.ResearchActivityName,
	})
	w.RegisterActivityWithOptions(acts.Critique, activity.RegisterOptions{
		Name: agent.CritiqueActivityName,
	})
}
