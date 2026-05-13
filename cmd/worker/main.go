// Command sibyl-worker runs a Temporal worker hosting Sibyl's
// ConvergeWorkflow and its Researcher/Critic activities.
//
// In its default form it uses a ScriptedLLM so you can run end-to-end
// against a local Temporal dev server without any API keys. Swap in a
// real CompleteFunc in main() to go live.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vinodhalaharvi/sibyl/agent"
	sibylworker "github.com/vinodhalaharvi/sibyl/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client:", err)
	}
	defer c.Close()

	// Demo LLM: a scripted client that converges in two rounds.
	// Replace with a real CompleteFunc for production use.
	demoLLM := &agent.ScriptedLLM{
		Cycle: true,
		Responses: []string{
			// Round 1: Researcher's first draft (too short)
			"Paris is the capital of France.",
			// Round 1: Critic rejects, asks for more detail
			`{"approved": false, "confidence": 0.4, "feedback": "Answer is correct but too terse. Add a sentence about why Paris is significant."}`,
			// Round 2: Researcher revises
			"Paris is the capital of France. It has been the political, cultural, and economic center of the country for over a thousand years.",
			// Round 2: Critic approves
			`{"approved": true, "confidence": 0.92, "feedback": ""}`,
		},
	}

	w := worker.New(c, agent.TaskQueue, worker.Options{})
	sibylworker.Register(w, demoLLM.Complete)

	log.Println("Sibyl worker started on task queue:", agent.TaskQueue)
	log.Println("Press Ctrl+C to stop.")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped with error:", err)
	}
}
